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
    assert_eq!(
        stmts[0],
        "COMMENT ON TABLE foo IS 'has; semicolons; inside'"
    );
    assert_eq!(stmts[1], "CREATE TABLE bar (id INT)");
}

#[test]
fn test_escaped_single_quotes() {
    let ddl = "COMMENT ON TABLE foo IS 'it''s a test; with quotes';\nSELECT 1;";
    let stmts = split_statements(ddl);
    assert_eq!(stmts.len(), 2);
    assert_eq!(
        stmts[0],
        "COMMENT ON TABLE foo IS 'it''s a test; with quotes'"
    );
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
fn test_risk_comments_skipped() {
    let ddl = "-- RISK: blocking\nALTER TABLE users ADD COLUMN email TEXT;";
    let stmts = split_statements(ddl);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0], "ALTER TABLE users ADD COLUMN email TEXT");
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
    assert!(
        (90..=110).contains(&d),
        "attempt 0 should hit the 100ms tier, got {d}"
    );
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
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "no retries on non-retryable"
    );
    let err = outcome.error.expect("should surface immediately");
    assert!(err.contains("logical bug"), "got: {err}");
}

#[test]
fn supports_parse_check_only_pg_and_mssql() {
    // PG (BEGIN/ROLLBACK) and MSSQL (SET PARSEONLY ON) both have
    // server-side parse-only modes uvg can use. MySQL DDL
    // auto-commits with no PARSEONLY equivalent; SQLite's EXPLAIN
    // doesn't cover most DDL. Caller is expected to skip silently
    // on the latter two.
    assert!(supports_parse_check(&ConnectionConfig::Postgres(
        "postgres://x".to_string()
    )));
    assert!(supports_parse_check(&ConnectionConfig::Mssql {
        host: "x".to_string(),
        port: 1433,
        database: "x".to_string(),
        user: "x".to_string(),
        password: "x".to_string(),
        trust_cert: false,
    }));
    assert!(!supports_parse_check(&ConnectionConfig::Mysql(
        "mysql://x".to_string()
    )));
    assert!(!supports_parse_check(&ConnectionConfig::Sqlite(
        "sqlite::memory:".to_string()
    )));
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
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "max_retries=0 means single attempt"
    );
    assert!(outcome.error.is_some());
}

#[tokio::test]
async fn sqlx_ddl_helper_invokes_callbacks_and_stops_after_failure() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let statements = vec![
        "CREATE TABLE a (id INT)".to_string(),
        "CREATE TABLE a (id INT)".to_string(),
        "CREATE TABLE b (id INT)".to_string(),
    ];
    let mut seen = Vec::new();

    let results = execute_sqlx_ddl_statements(
        &pool,
        &statements,
        0,
        &mut |result, index, total| {
            seen.push((index, total, result.sql.clone(), result.error.is_some()));
        },
        |_| false,
    )
    .await;
    pool.close().await;

    assert_eq!(results.len(), 2);
    assert!(results[0].error.is_none());
    assert!(results[1].error.is_some());
    assert_eq!(
        seen,
        vec![
            (1, 3, "CREATE TABLE a (id INT)".to_string(), false),
            (2, 3, "CREATE TABLE a (id INT)".to_string(), true),
        ]
    );
}
