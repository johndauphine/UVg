use std::time::{Duration, Instant};

use anyhow::Result;

use crate::cli::{ConnectionConfig, GeneratorOptions};
use crate::introspect;
use crate::schema::IntrospectedSchema;
use crate::table_filter::TableFilter;

/// Introspect a database given a ConnectionConfig.
pub(crate) async fn introspect_with_config(
    config: ConnectionConfig,
    schemas: &[String],
    table_filter: &TableFilter,
    noviews: bool,
    options: &GeneratorOptions,
) -> Result<IntrospectedSchema> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s = introspect::pg::introspect(&pool, schemas, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client =
                introspect::mssql::connect(&host, port, &database, &user, &password, trust_cert)
                    .await?;
            Ok(
                introspect::mssql::introspect(
                    &mut client,
                    schemas,
                    table_filter,
                    noviews,
                    options,
                )
                .await?,
            )
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s =
                introspect::mysql::introspect(&pool, schemas, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s = introspect::sqlite::introspect(&pool, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
    }
}

/// Result of executing a single DDL statement.
pub(crate) struct StmtResult {
    pub sql: String,
    pub error: Option<String>,
    /// Wall-clock time the statement took to execute on the target.
    /// Used by the per-statement progress reporter (#45).
    pub duration: Duration,
}

/// Split DDL output into individual statements using a SQL-aware splitter.
/// Handles semicolons inside single-quoted strings (with `''` escape),
/// dollar-quoted strings (PostgreSQL `$$...$$` / `$tag$...$tag$`), and
/// line comments (`--`). Strips leading comment-only/blank lines from each
/// statement chunk so header comments don't become empty executions.
pub(crate) fn split_statements(ddl: &str) -> Vec<String> {
    let bytes = ddl.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_line_comment = false;
    let mut dollar_tag: Option<String> = None;

    while i < bytes.len() {
        // Inside a dollar-quoted string: scan for closing tag
        if let Some(ref tag) = dollar_tag {
            if ddl[i..].starts_with(tag.as_str()) {
                i += tag.len();
                dollar_tag = None;
            } else {
                i += 1;
            }
            continue;
        }

        // Inside a line comment: skip until newline
        if in_line_comment {
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        // Inside a single-quoted string
        if in_single_quote {
            if bytes[i] == b'\'' {
                // Check for escaped quote ('')
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                } else {
                    in_single_quote = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        match bytes[i] {
            b'\'' => {
                in_single_quote = true;
                i += 1;
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                in_line_comment = true;
                i += 2;
            }
            b'$' => {
                if let Some(tag) = dollar_quote_tag_at(ddl, i) {
                    i += tag.len();
                    dollar_tag = Some(tag);
                } else {
                    i += 1;
                }
            }
            b';' => {
                let chunk = &ddl[start..i];
                if let Some(stmt) = strip_leading_comments(chunk) {
                    statements.push(stmt);
                }
                i += 1;
                start = i;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Trailing content after last semicolon
    if start < ddl.len() {
        if let Some(stmt) = strip_leading_comments(&ddl[start..]) {
            statements.push(stmt);
        }
    }

    statements
}

/// Try to match a dollar-quote tag at position `start` (e.g. `$$` or `$foo$`).
fn dollar_quote_tag_at(s: &str, start: usize) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut end = start + 1;
    while let Some(&b) = bytes.get(end) {
        if b == b'$' {
            return Some(s[start..=end].to_string());
        }
        if !(b == b'_' || b.is_ascii_alphanumeric()) {
            return None;
        }
        end += 1;
    }
    None
}

/// Strip leading blank/comment-only lines from a statement chunk.
fn strip_leading_comments(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    let stripped: String = trimmed
        .lines()
        .skip_while(|line| {
            let t = line.trim();
            t.is_empty() || t.starts_with("--")
        })
        .collect::<Vec<_>>()
        .join("\n");
    if stripped.is_empty() {
        None
    } else {
        Some(stripped)
    }
}

/// Execute DDL statements one-by-one against the target database.
/// Stops on first non-retryable error.
///
/// `max_retries` controls retry behavior on transient errors only
/// (deadlocks, lock-wait timeouts, brief connection drops). Logical
/// errors (constraint violations, syntax errors, missing columns)
/// surface immediately without consuming the retry budget — see the
/// per-dialect `classify_*_retryable` helpers. Backoff between attempts
/// is `retry_delay_ms(attempt)` with ±10% jitter.
///
/// `on_statement` is invoked AFTER the final outcome of each statement
/// (success or terminal failure), once — the user sees the wall-clock
/// duration including any retries, not per attempt. TUI passes a no-op
/// closure.
pub(crate) async fn execute_ddl<F>(
    config: &ConnectionConfig,
    ddl: &str,
    max_retries: u8,
    mut on_statement: F,
) -> Result<Vec<StmtResult>>
where
    F: FnMut(&StmtResult, usize, usize),
{
    let statements = split_statements(ddl);
    let total = statements.len();
    let mut results = Vec::new();

    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            for (i, stmt) in statements.iter().enumerate() {
                let result = run_with_retry(max_retries, |_attempt| {
                    let pool = &pool;
                    let stmt = stmt.as_str();
                    async move {
                        sqlx::query(stmt)
                            .execute(pool)
                            .await
                            .map(|_| ())
                            .map_err(|e| (e.to_string(), is_retryable_sqlx_pg_error(&e)))
                    }
                })
                .await;
                let result = StmtResult {
                    sql: stmt.to_string(),
                    error: result.error,
                    duration: result.duration,
                };
                on_statement(&result, i + 1, total);
                let failed = result.error.is_some();
                results.push(result);
                if failed {
                    break;
                }
            }
            pool.close().await;
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            for (i, stmt) in statements.iter().enumerate() {
                let result = run_with_retry(max_retries, |_attempt| {
                    let pool = &pool;
                    let stmt = stmt.as_str();
                    async move {
                        sqlx::query(stmt)
                            .execute(pool)
                            .await
                            .map(|_| ())
                            .map_err(|e| (e.to_string(), is_retryable_sqlx_mysql_error(&e)))
                    }
                })
                .await;
                let result = StmtResult {
                    sql: stmt.to_string(),
                    error: result.error,
                    duration: result.duration,
                };
                on_statement(&result, i + 1, total);
                let failed = result.error.is_some();
                results.push(result);
                if failed {
                    break;
                }
            }
            pool.close().await;
        }
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            for (i, stmt) in statements.iter().enumerate() {
                // SQLite has no contention model worth retrying for in
                // the DDL path — the apply runs against a single file
                // we typically own exclusively. Treat every error as
                // terminal (max_retries effectively ignored).
                let start = Instant::now();
                let r = sqlx::query(stmt).execute(&pool).await;
                let result = StmtResult {
                    sql: stmt.to_string(),
                    error: r.err().map(|e| e.to_string()),
                    duration: start.elapsed(),
                };
                on_statement(&result, i + 1, total);
                let failed = result.error.is_some();
                results.push(result);
                if failed {
                    break;
                }
            }
            pool.close().await;
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client =
                introspect::mssql::connect(host, *port, database, user, password, *trust_cert)
                    .await?;
            for (i, stmt) in statements.iter().enumerate() {
                // MSSQL retry loop is inlined: `run_with_retry`'s
                // `FnMut(u8) -> Fut` bound can't accept a closure that
                // re-borrows `&mut client` on each attempt (the inner
                // async block would outlive the closure body). Inline
                // keeps the same backoff/classifier semantics.
                let start = Instant::now();
                let mut attempt: u8 = 0;
                let error_msg = loop {
                    let r = client.execute(stmt.to_string(), &[]).await;
                    match r {
                        Ok(_) => break None,
                        Err(e) => {
                            let retryable = is_retryable_tiberius_error(&e);
                            if !retryable || attempt >= max_retries {
                                break Some(e.to_string());
                            }
                            let delay = retry_delay_ms(attempt + 1);
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                            attempt += 1;
                        }
                    }
                };
                let result = StmtResult {
                    sql: stmt.to_string(),
                    error: error_msg,
                    duration: start.elapsed(),
                };
                on_statement(&result, i + 1, total);
                let failed = result.error.is_some();
                results.push(result);
                if failed {
                    break;
                }
            }
        }
    }

    Ok(results)
}

/// Internal: retry an async DDL action up to `max_retries` times when
/// the per-call classifier reports the failure is transient. Returns a
/// `RetryOutcome` carrying the final error (if any) plus the
/// wall-clock duration spanning all attempts including sleeps — that's
/// what the user sees on the progress line, which is intentional: the
/// duration reflects the actual wait, not a single attempt.
async fn run_with_retry<F, Fut>(max_retries: u8, mut action: F) -> RetryOutcome
where
    F: FnMut(u8) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), (String, bool)>>,
{
    let start = Instant::now();
    let mut attempt = 0u8;
    loop {
        match action(attempt).await {
            Ok(_) => {
                return RetryOutcome { error: None, duration: start.elapsed() };
            }
            Err((msg, retryable)) => {
                if !retryable || attempt >= max_retries {
                    return RetryOutcome { error: Some(msg), duration: start.elapsed() };
                }
                let delay = retry_delay_ms(attempt + 1);
                tokio::time::sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
        }
    }
}

struct RetryOutcome {
    error: Option<String>,
    duration: Duration,
}

/// Backoff schedule for retry attempt N (1-indexed). 100ms / 500ms /
/// 2000ms per the issue spec, with ±10% jitter so simultaneous
/// retries from multiple workers don't synchronize. Attempts beyond 3
/// stay at the 2s ceiling.
pub(crate) fn retry_delay_ms(attempt: u8) -> u64 {
    let base_ms: u64 = match attempt {
        0 | 1 => 100,
        2 => 500,
        _ => 2000,
    };
    // ±10% jitter without pulling in `rand`: derive a pseudo-random
    // offset from the wall clock's sub-second nanos.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter_pct = (nanos % 21) as i64 - 10; // -10..=+10
    let jittered = base_ms as i64 + (base_ms as i64 * jitter_pct) / 100;
    jittered.max(10) as u64
}

// ---- per-dialect error classification -------------------------------
//
// Each `is_retryable_*` function answers: "is this error transient
// enough that re-running the same statement might succeed?" Logical
// errors (constraint violations, syntax errors, missing tables) get
// `false` and propagate immediately — retrying wastes the budget and
// delays the inevitable.

fn is_retryable_sqlx_pg_error(err: &sqlx::Error) -> bool {
    // Network-layer disruption: always retry.
    if matches!(err, sqlx::Error::Io(_) | sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut) {
        return true;
    }
    err.as_database_error()
        .and_then(|e| e.code())
        .map(|c| classify_pg_sqlstate_retryable(&c))
        .unwrap_or(false)
}

fn is_retryable_sqlx_mysql_error(err: &sqlx::Error) -> bool {
    if matches!(err, sqlx::Error::Io(_) | sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut) {
        return true;
    }
    err.as_database_error()
        .and_then(|e| e.code())
        .map(|c| classify_mysql_code_retryable(&c))
        .unwrap_or(false)
}

fn is_retryable_tiberius_error(err: &tiberius::error::Error) -> bool {
    use tiberius::error::Error;
    match err {
        // Network / driver-level disruption.
        Error::Io { .. } | Error::Tls(_) => true,
        // Server-side error with a numeric code.
        Error::Server(token) => classify_mssql_code_retryable(token.code()),
        _ => false,
    }
}

/// Pure classifier — PostgreSQL SQLSTATE codes worth retrying.
/// Class 40 is transaction-rollback (serialization failure, deadlock);
/// class 08 is connection exception (broken pipe, server lost, etc.).
/// Everything else surfaces immediately.
pub(crate) fn classify_pg_sqlstate_retryable(code: &str) -> bool {
    code.starts_with("40") || code.starts_with("08")
}

/// Pure classifier — MySQL numeric error codes worth retrying.
/// 1213 = deadlock, 1205 = lock wait timeout, 2006 = server gone away,
/// 2013 = connection lost during query.
pub(crate) fn classify_mysql_code_retryable(code: &str) -> bool {
    matches!(code, "1213" | "1205" | "2006" | "2013")
}

/// Pure classifier — MSSQL error numbers worth retrying.
/// 1205 = deadlock victim, 4060 = cannot open database (transient
/// connection), 11001 = host unreachable.
pub(crate) fn classify_mssql_code_retryable(code: u32) -> bool {
    matches!(code, 1205 | 4060 | 11001)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_split() {
        let ddl = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
        assert_eq!(stmts[1], "CREATE TABLE b (id INT)");
    }

    #[test]
    fn test_semicolon_in_single_quotes() {
        let ddl = "COMMENT ON TABLE foo IS 'has; semicolons; inside';\nCREATE TABLE bar (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "COMMENT ON TABLE foo IS 'has; semicolons; inside'");
        assert_eq!(stmts[1], "CREATE TABLE bar (id INT)");
    }

    #[test]
    fn test_escaped_single_quotes() {
        let ddl = "COMMENT ON TABLE foo IS 'it''s a test; with quotes';\nSELECT 1;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0], "COMMENT ON TABLE foo IS 'it''s a test; with quotes'");
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_dollar_quoting() {
        let ddl = "CREATE FUNCTION f() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE plpgsql;\nSELECT 1;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("BEGIN; END;"));
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn test_named_dollar_quoting() {
        let ddl = "CREATE FUNCTION f() AS $body$ x; y; $body$ LANGUAGE sql;\nSELECT 2;";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("x; y;"));
    }

    #[test]
    fn test_line_comments_skipped() {
        let ddl = "-- header comment\n-- another\nCREATE TABLE a (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    }

    #[test]
    fn test_semicolon_in_line_comment() {
        let ddl = "-- this; has; semicolons\nCREATE TABLE a (id INT);";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    }

    #[test]
    fn test_comment_only_blocks_stripped() {
        let ddl = "-- just a comment;\n-- nothing here;\n";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 0);
    }

    #[test]
    fn test_trailing_content_without_semicolon() {
        let ddl = "CREATE TABLE a (id INT)";
        let stmts = split_statements(ddl);
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE a (id INT)");
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(split_statements("").len(), 0);
        assert_eq!(split_statements("  \n  ").len(), 0);
        assert_eq!(split_statements(";;;").len(), 0);
    }

    // ---- retry primitive (#43) ----

    #[test]
    fn pg_sqlstate_retryable_matches_class_40_and_08() {
        // Class 40 — transaction rollback (serialization failure, deadlock).
        assert!(classify_pg_sqlstate_retryable("40001"));
        assert!(classify_pg_sqlstate_retryable("40P01"));
        // Class 08 — connection exception.
        assert!(classify_pg_sqlstate_retryable("08000"));
        assert!(classify_pg_sqlstate_retryable("08006"));
        // Non-retryable: logical errors (syntax, constraint, etc.).
        assert!(!classify_pg_sqlstate_retryable("23505")); // unique violation
        assert!(!classify_pg_sqlstate_retryable("42P01")); // undefined table
        assert!(!classify_pg_sqlstate_retryable("42601")); // syntax error
        assert!(!classify_pg_sqlstate_retryable(""));
        assert!(!classify_pg_sqlstate_retryable("00000"));
    }

    #[test]
    fn mysql_code_retryable_matches_documented_codes_only() {
        // Documented retryable codes.
        assert!(classify_mysql_code_retryable("1213")); // deadlock
        assert!(classify_mysql_code_retryable("1205")); // lock wait timeout
        assert!(classify_mysql_code_retryable("2006")); // server gone
        assert!(classify_mysql_code_retryable("2013")); // connection lost
        // Logical errors: not retryable.
        assert!(!classify_mysql_code_retryable("1062")); // dup entry
        assert!(!classify_mysql_code_retryable("1146")); // table doesn't exist
        assert!(!classify_mysql_code_retryable("1064")); // syntax error
        // Don't substring-match — 12130 must NOT be confused with 1213.
        assert!(!classify_mysql_code_retryable("12130"));
        assert!(!classify_mysql_code_retryable(""));
    }

    #[test]
    fn mssql_code_retryable_matches_documented_codes_only() {
        assert!(classify_mssql_code_retryable(1205)); // deadlock victim
        assert!(classify_mssql_code_retryable(4060)); // cannot open db (transient)
        assert!(classify_mssql_code_retryable(11001)); // host unreachable
        // Logical errors.
        assert!(!classify_mssql_code_retryable(2627)); // PK violation
        assert!(!classify_mssql_code_retryable(208)); // invalid object name
        assert!(!classify_mssql_code_retryable(0));
    }

    #[test]
    fn retry_delay_follows_schedule_with_jitter_in_bounds() {
        // Backoff base: 100ms / 500ms / 2000ms per the issue spec.
        // Each is allowed ±10% jitter; assert membership in [base*0.9, base*1.1]
        // with a 1ms floor for the smallest tier so 90ms passes.
        for _ in 0..50 {
            let d1 = retry_delay_ms(1);
            let d2 = retry_delay_ms(2);
            let d3 = retry_delay_ms(3);
            let d_overflow = retry_delay_ms(99);
            assert!((90..=110).contains(&d1), "attempt 1 out of band: {d1}");
            assert!((450..=550).contains(&d2), "attempt 2 out of band: {d2}");
            assert!((1800..=2200).contains(&d3), "attempt 3 out of band: {d3}");
            assert!(
                (1800..=2200).contains(&d_overflow),
                "attempts > 3 should saturate at the 2s tier: {d_overflow}"
            );
        }
    }

    #[test]
    fn retry_delay_attempt_zero_is_floor_safe() {
        // Defensive: passing 0 must not panic or wrap. Treat as
        // first attempt (100ms tier).
        let d = retry_delay_ms(0);
        assert!((90..=110).contains(&d), "attempt 0 should hit the 100ms tier, got {d}");
    }

    // ---- run_with_retry behavior (#43) ----
    // Uses `start_paused = true` so the backoff `tokio::time::sleep`
    // calls auto-advance without real-time wall-clock waits — tests
    // stay sub-millisecond even when exhausting the retry budget.

    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retry_helper_retries_until_success() {
        // Fail twice with retryable=true, succeed on the third call.
        // max_retries=3 leaves enough budget; expect 3 invocations
        // and a None error.
        let calls = Arc::new(AtomicU8::new(0));
        let calls_c = calls.clone();
        let outcome = run_with_retry(3, move |_attempt| {
            let calls = calls_c.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err(("transient".to_string(), true))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(outcome.error.is_none(), "expected success after retries");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retry_helper_exhausts_budget_then_surfaces_error() {
        // Always retryable. max_retries=2 → 1 initial + 2 retries = 3
        // total invocations, terminal error after.
        let calls = Arc::new(AtomicU8::new(0));
        let calls_c = calls.clone();
        let outcome = run_with_retry(2, move |_attempt| {
            let calls = calls_c.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(("still transient".to_string(), true))
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3, "1 initial + 2 retries");
        let err = outcome.error.expect("should surface terminal error");
        assert!(err.contains("still transient"), "got: {err}");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retry_helper_does_not_retry_non_retryable() {
        // retryable=false on the first failure: no retry budget
        // consumed, exactly one invocation, error surfaces immediately.
        let calls = Arc::new(AtomicU8::new(0));
        let calls_c = calls.clone();
        let outcome = run_with_retry(5, move |_attempt| {
            let calls = calls_c.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(("logical bug".to_string(), false))
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no retries on non-retryable");
        let err = outcome.error.expect("should surface immediately");
        assert!(err.contains("logical bug"), "got: {err}");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn retry_helper_with_zero_retries_runs_once() {
        // --apply-retries 0 should disable retry entirely: a single
        // attempt, no second chance even for retryable errors.
        let calls = Arc::new(AtomicU8::new(0));
        let calls_c = calls.clone();
        let outcome = run_with_retry(0, move |_attempt| {
            let calls = calls_c.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(("transient".to_string(), true))
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "max_retries=0 means single attempt");
        assert!(outcome.error.is_some());
    }
}
