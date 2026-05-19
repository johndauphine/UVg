use super::redact_target_url;

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
