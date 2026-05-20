use super::{redact_target_url, validate_apply_blob};

#[test]
fn test_redact_target_url_strips_password() {
    assert_eq!(
        redact_target_url("postgres://alice:hunter2@db.example.com:5432/orders"),
        "postgres://***@db.example.com:5432/orders",
    );
}

#[test]
fn test_redact_target_url_strips_username_only() {
    assert_eq!(
        redact_target_url("mysql://root@localhost/mydb"),
        "mysql://***@localhost/mydb",
    );
}

#[test]
fn test_redact_target_url_leaves_credential_free_urls_alone() {
    assert_eq!(
        redact_target_url("sqlite:///tmp/data.db"),
        "sqlite:///tmp/data.db",
    );
    assert_eq!(
        redact_target_url("postgres://db.example.com:5432/orders"),
        "postgres://db.example.com:5432/orders",
    );
}

#[test]
fn test_redact_target_url_passes_through_unparseable() {
    // sqlite:relative form skips url::Url::parse — returned as-is.
    assert_eq!(
        redact_target_url("sqlite:relative.db"),
        "sqlite:relative.db"
    );
}

#[test]
fn test_redact_target_url_preserves_query_and_path() {
    assert_eq!(
        redact_target_url("mysql://u:p@host/db?charset=utf8mb4"),
        "mysql://***@host/db?charset=utf8mb4",
    );
}

#[test]
fn validate_apply_blob_allows_executable_sql() {
    validate_apply_blob("CREATE TABLE users(id INTEGER PRIMARY KEY);", "test").unwrap();
}

#[test]
fn validate_apply_blob_allows_noop_sentinel() {
    validate_apply_blob("-- No schema changes detected.", "test").unwrap();
}

#[test]
fn validate_apply_blob_rejects_unappliable_marker() {
    let err = validate_apply_blob(
        "-- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.\n\
         -- ALTER TABLE users ALTER COLUMN email TYPE TEXT;",
        "test",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("refusing to apply"));
    assert!(err.contains("ALTER COLUMN"));
}

#[test]
fn validate_apply_blob_rejects_mixed_marker_and_sql() {
    let err = validate_apply_blob(
        "ALTER TABLE users ADD COLUMN phone TEXT;\n\
         -- WARNING: SQLite does not support ALTER COLUMN. Table recreation required.",
        "test",
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("refusing to apply"));
    assert!(err.contains("SQLite does not support ALTER COLUMN"));
}

#[test]
fn validate_apply_blob_rejects_comment_only_diff() {
    let err = validate_apply_blob("-- manual follow-up required", "test")
        .unwrap_err()
        .to_string();
    assert!(err.contains("non-executable text"));
}
