use super::*;

fn r(sql: &str, ms: u64) -> StmtResult {
    StmtResult {
        sql: sql.to_string(),
        error: None,
        duration: Duration::from_millis(ms),
    }
}

#[test]
fn sql_one_line_collapses_and_keeps_short_text() {
    assert_eq!(sql_one_line("SELECT 1", 60), "SELECT 1");
    assert_eq!(
        sql_one_line("CREATE TABLE\n  \"users\" (id int)", 60),
        "CREATE TABLE \"users\" (id int)"
    );
}

#[test]
fn sql_one_line_truncates_with_ellipsis() {
    let long =
        "CREATE INDEX very_long_name_that_will_definitely_overflow_the_preview_width ON t (col)";
    let out = sql_one_line(long, 20);
    assert_eq!(out.chars().count(), 20);
    assert!(out.ends_with("..."));
    assert!(out.starts_with("CREATE INDEX"));
}

#[test]
fn sql_one_line_respects_char_boundaries() {
    // Multi-byte chars must not be cut mid-byte. 5-char preview
    // limit with a string of multi-byte chars should return chars
    // not bytes.
    let s = "αβγδεζηθικλμνξο";
    let out = sql_one_line(s, 8);
    assert!(out.chars().count() <= 8);
    assert!(out.is_char_boundary(out.len()));
}

#[test]
fn sql_one_line_handles_tiny_max() {
    // Below the ellipsis width (3), degenerate to a plain prefix
    // so the function's contract (≤ max chars out) still holds.
    assert_eq!(sql_one_line("CREATE TABLE foo (id int)", 0), "");
    assert_eq!(sql_one_line("CREATE TABLE foo (id int)", 1), "C");
    assert_eq!(sql_one_line("CREATE TABLE foo (id int)", 2), "CR");
    // Exactly 3: emit just the ellipsis.
    let three = sql_one_line("CREATE TABLE foo (id int)", 3);
    assert_eq!(three.chars().count(), 3);
    assert_eq!(three, "...");
}

#[test]
fn classify_alter_rejects_loose_keyword_matches() {
    // Pre-fix bug: `.contains(" FOREIGN KEY")` matched anywhere in
    // the SQL, including inside identifiers. The fix requires
    // `ADD CONSTRAINT` to be present before classifying as FK/CHECK,
    // so a column named "user CHECK" or a table called
    // `"FOREIGN KEY tbl"` no longer skews the bucket counts.
    assert_eq!(
        classify(r#"ALTER TABLE "FOREIGN KEY tbl" ADD COLUMN x int"#),
        "alters"
    );
    assert_eq!(
        classify(r#"ALTER TABLE foo ADD COLUMN "user CHECK" int"#),
        "alters"
    );
    // Real FK constraint still classifies as FK.
    assert_eq!(
        classify("ALTER TABLE foo ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES b(id)"),
        "FKs"
    );
}

#[test]
fn stats_record_skips_failed_statements() {
    // The summary's count is "successful statements applied" —
    // it must match the apply-summary line, which counts only
    // statements that returned without error.
    let mut stats = ApplyStats::new();
    stats.record(&r("CREATE TABLE users (id int)", 10));
    stats.record(&StmtResult {
        sql: "CREATE TABLE broken (".to_string(),
        error: Some("syntax error".to_string()),
        duration: Duration::from_millis(2),
    });
    let s = stats.render_summary();
    assert!(s.starts_with("Applied 1 statement(s)"), "got: {s}");
    assert!(!s.contains("syntax"), "error text leaked into summary: {s}");
}

#[test]
fn classify_buckets() {
    assert_eq!(classify("CREATE TABLE foo (id int)"), "tables");
    assert_eq!(classify("create table foo (id int)"), "tables");
    assert_eq!(classify("CREATE INDEX ix_foo ON foo(id)"), "indexes");
    assert_eq!(classify("CREATE UNIQUE INDEX uq_foo ON foo(id)"), "indexes");
    assert_eq!(classify("CREATE TYPE color AS ENUM ('r','g','b')"), "types");
    assert_eq!(classify("COMMENT ON TABLE foo IS 'x'"), "comments");
    assert_eq!(
        classify("ALTER TABLE foo ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES b(id)"),
        "FKs"
    );
    assert_eq!(
        classify("ALTER TABLE foo ADD CONSTRAINT ck CHECK (x > 0)"),
        "CHECKs"
    );
    assert_eq!(classify("ALTER TABLE foo ADD COLUMN bar int"), "alters");
    assert_eq!(classify("DROP TABLE foo"), "drops");
    assert_eq!(classify("VACUUM"), "other");
}

#[test]
fn stats_summary_breaks_down_by_class() {
    let mut stats = ApplyStats::new();
    stats.record(&r("CREATE TABLE users (id int)", 10));
    stats.record(&r("CREATE INDEX ix_users_id ON users(id)", 20));
    stats.record(&r("CREATE INDEX ix_users_name ON users(name)", 50));
    stats.record(&r(
        "ALTER TABLE users ADD CONSTRAINT fk FOREIGN KEY (org_id) REFERENCES orgs(id)",
        5,
    ));
    let s = stats.render_summary();
    assert!(s.starts_with("Applied 4 statement(s)"), "got: {s}");
    assert!(s.contains("1 tables"), "got: {s}");
    assert!(s.contains("2 indexes"), "got: {s}");
    assert!(s.contains("1 FKs"), "got: {s}");
    assert!(s.contains("max 50ms"), "got: {s}");
}

#[test]
fn stats_summary_empty_returns_empty_string() {
    let stats = ApplyStats::new();
    assert_eq!(stats.render_summary(), "");
}

#[test]
fn digit_count_basic() {
    assert_eq!(digit_count(0), 1);
    assert_eq!(digit_count(1), 1);
    assert_eq!(digit_count(9), 1);
    assert_eq!(digit_count(10), 2);
    assert_eq!(digit_count(99), 2);
    assert_eq!(digit_count(100), 3);
    assert_eq!(digit_count(999), 3);
    assert_eq!(digit_count(1000), 4);
}

#[test]
fn progress_mode_resolved_respects_explicit_overrides() {
    // Explicit modes never consult the TTY.
    assert!(ProgressMode::On.resolved());
    assert!(!ProgressMode::Off.resolved());
    // Auto's behavior depends on the test runner's stderr — don't
    // assert a specific value; just confirm it returns SOMETHING
    // without panicking.
    let _ = ProgressMode::Auto.resolved();
}
