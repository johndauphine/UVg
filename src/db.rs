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
    concurrency: usize,
) -> Result<IntrospectedSchema> {
    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(pool_size(concurrency))
                .connect(&url)
                .await?;
            let s = introspect::pg::introspect(
                &pool,
                schemas,
                table_filter,
                noviews,
                options,
                concurrency,
            )
            .await;
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
                introspect::mssql::introspect(&mut client, schemas, table_filter, noviews, options)
                    .await?,
            )
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(pool_size(concurrency))
                .connect(&url)
                .await?;
            let s = introspect::mysql::introspect(
                &pool,
                schemas,
                table_filter,
                noviews,
                options,
                concurrency,
            )
            .await;
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

fn pool_size(concurrency: usize) -> u32 {
    concurrency.max(1).min(u32::MAX as usize) as u32
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

/// One statement-level parse error from `parse_check_ddl`.
pub(crate) struct ParseError {
    pub sql: String,
    pub error: String,
}

/// `true` when the dialect supports a per-statement parse probe that
/// won't commit. PG (`BEGIN`/`ROLLBACK`) and MSSQL (`SET PARSEONLY ON`)
/// both qualify. MySQL has no clean parse-only mode (DDL auto-commits
/// and there's no equivalent of `SET PARSEONLY`), and SQLite's
/// `EXPLAIN` doesn't cover most DDL — both skip silently per #44.
pub(crate) fn supports_parse_check(config: &ConnectionConfig) -> bool {
    matches!(
        config,
        ConnectionConfig::Postgres(_) | ConnectionConfig::Mssql { .. }
    )
}

/// Pre-validate every DDL statement by running it through the target
/// dialect's parse-only mode without committing. Returns the list of
/// statements that failed and their errors. Empty list = clean. The
/// `Result` wrapper is for connection-level failures; per-statement
/// failures land in the returned Vec, never in the outer Err.
///
/// Caller decides what to do with parse errors. The apply path aborts
/// with the full list rather than only the first, so the user can
/// fix all issues in one round.
pub(crate) async fn parse_check_ddl(
    config: &ConnectionConfig,
    ddl: &str,
) -> Result<Vec<ParseError>> {
    let statements = split_statements(ddl);
    let mut errors = Vec::new();

    match config {
        ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            // Single outer transaction holds all statements visible to
            // later ones (a CREATE TABLE that references an earlier
            // CREATE TYPE enum must see the enum during parse, or it
            // false-positives "type does not exist"). Each statement
            // runs inside a savepoint so its effects can be reverted
            // on error without poisoning the outer tx — sqlx's nested
            // `Transaction::begin()` lowers to SAVEPOINT/RELEASE.
            // The outer ROLLBACK at the end undoes everything; no
            // change reaches the database.
            let mut outer = match pool.begin().await {
                Ok(t) => t,
                Err(e) => {
                    pool.close().await;
                    return Err(e.into());
                }
            };
            for (i, stmt) in statements.iter().enumerate() {
                let sp = format!("uvg_sp_{i}");
                if let Err(e) = sqlx::query(&format!("SAVEPOINT {sp}"))
                    .execute(&mut *outer)
                    .await
                {
                    // Savepoint creation should only fail at the
                    // connection level; surface and stop probing.
                    let _ = outer.rollback().await;
                    pool.close().await;
                    return Err(e.into());
                }
                match sqlx::query(stmt).execute(&mut *outer).await {
                    Ok(_) => {
                        // RELEASE SAVEPOINT — keep this statement's
                        // effects visible to later probes within the
                        // outer tx (a later CREATE TABLE may reference
                        // a CREATE TYPE just declared).
                        let _ = sqlx::query(&format!("RELEASE SAVEPOINT {sp}"))
                            .execute(&mut *outer)
                            .await;
                    }
                    Err(e) => {
                        errors.push(ParseError {
                            sql: stmt.clone(),
                            error: e.to_string(),
                        });
                        // ROLLBACK TO SAVEPOINT — undo this statement
                        // only; outer tx remains live so the probe
                        // can continue with the next statement.
                        let _ = sqlx::query(&format!("ROLLBACK TO SAVEPOINT {sp}"))
                            .execute(&mut *outer)
                            .await;
                    }
                }
            }
            let _ = outer.rollback().await;
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
            // Switch the session to parse-only mode. Per MS docs,
            // PARSEONLY does pure T-SQL syntax checking — name
            // resolution (missing tables, FK targets, column types)
            // is DEFERRED to execution and is NOT caught here. So
            // this probe catches typos and malformed DDL but not
            // catalog-level errors. The PG probe (savepoint-per-stmt
            // in one outer tx) catches both. SET PARSEONLY itself
            // can't run in PARSEONLY mode, so toggling back is a
            // real execution call.
            if let Err(e) = client.execute("SET PARSEONLY ON".to_string(), &[]).await {
                return Err(e.into());
            }
            for stmt in &statements {
                if let Err(e) = client.execute(stmt.to_string(), &[]).await {
                    errors.push(ParseError {
                        sql: stmt.clone(),
                        error: e.to_string(),
                    });
                }
            }
            // Always reset; if PARSEONLY OFF itself fails the
            // connection is closing anyway and that's the caller's
            // problem to surface.
            let _ = client.execute("SET PARSEONLY OFF".to_string(), &[]).await;
        }
        ConnectionConfig::Mysql(_) | ConnectionConfig::Sqlite(_) => {
            // No parse-only mode. Caller is expected to gate this
            // path with `supports_parse_check` and decide whether to
            // skip silently or emit an info note. The apply path
            // prints a one-line note rather than aborting.
        }
    }

    Ok(errors)
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
            results = execute_sqlx_ddl_statements(
                &pool,
                &statements,
                max_retries,
                &mut on_statement,
                is_retryable_sqlx_pg_error,
            )
            .await;
            pool.close().await;
        }
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(url)
                .await?;
            results = execute_sqlx_ddl_statements(
                &pool,
                &statements,
                max_retries,
                &mut on_statement,
                is_retryable_sqlx_mysql_error,
            )
            .await;
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

async fn execute_sqlx_ddl_statements<DB, F>(
    pool: &sqlx::Pool<DB>,
    statements: &[String],
    max_retries: u8,
    on_statement: &mut F,
    is_retryable: fn(&sqlx::Error) -> bool,
) -> Vec<StmtResult>
where
    DB: sqlx::Database,
    for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    F: FnMut(&StmtResult, usize, usize),
{
    let total = statements.len();
    let mut results = Vec::new();

    for (i, stmt) in statements.iter().enumerate() {
        let result = run_with_retry(max_retries, |_attempt| {
            let stmt = stmt.as_str();
            async move {
                sqlx::query::<DB>(stmt)
                    .execute(pool)
                    .await
                    .map(|_| ())
                    .map_err(|e| (e.to_string(), is_retryable(&e)))
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

    results
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
                return RetryOutcome {
                    error: None,
                    duration: start.elapsed(),
                };
            }
            Err((msg, retryable)) => {
                if !retryable || attempt >= max_retries {
                    return RetryOutcome {
                        error: Some(msg),
                        duration: start.elapsed(),
                    };
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
    if matches!(
        err,
        sqlx::Error::Io(_) | sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut
    ) {
        return true;
    }
    err.as_database_error()
        .and_then(|e| e.code())
        .map(|c| classify_pg_sqlstate_retryable(&c))
        .unwrap_or(false)
}

fn is_retryable_sqlx_mysql_error(err: &sqlx::Error) -> bool {
    if matches!(
        err,
        sqlx::Error::Io(_) | sqlx::Error::PoolClosed | sqlx::Error::PoolTimedOut
    ) {
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
#[path = "db_tests.rs"]
mod tests;
