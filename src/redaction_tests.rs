use super::redact_connection_url;

#[test]
fn test_redact_connection_url_strips_password() {
    assert_eq!(
        redact_connection_url("postgres://alice:hunter2@db.example.com:5432/orders"),
        "postgres://***@db.example.com:5432/orders",
    );
}

#[test]
fn test_redact_connection_url_strips_username_only() {
    assert_eq!(
        redact_connection_url("mysql://root@localhost/mydb"),
        "mysql://***@localhost/mydb",
    );
}

#[test]
fn test_redact_connection_url_leaves_credential_free_urls_alone() {
    assert_eq!(
        redact_connection_url("sqlite:///tmp/data.db"),
        "sqlite:///tmp/data.db",
    );
    assert_eq!(
        redact_connection_url("postgres://db.example.com:5432/orders"),
        "postgres://db.example.com:5432/orders",
    );
}

#[test]
fn test_redact_connection_url_passes_through_unparseable() {
    // sqlite:relative form skips url::Url::parse and has no network credentials.
    assert_eq!(
        redact_connection_url("sqlite:relative.db"),
        "sqlite:relative.db"
    );
}

#[test]
fn test_redact_connection_url_preserves_safe_query_and_path() {
    assert_eq!(
        redact_connection_url("mysql://u:p@host/db?charset=utf8mb4"),
        "mysql://***@host/db?charset=utf8mb4",
    );
}

#[test]
fn test_redact_connection_url_masks_sensitive_query_params() {
    assert_eq!(
        redact_connection_url(
            "postgres://db.example.com/orders?user=alice&password=hunter2&ssl-key=/tmp/key.pem",
        ),
        "postgres://db.example.com/orders?user=alice&password=***&ssl-key=***",
    );
}

#[test]
fn test_redact_connection_url_preserves_fragment_after_redaction() {
    assert_eq!(
        redact_connection_url("postgres://alice:hunter2@db/orders?sslmode=require#readonly"),
        "postgres://***@db/orders?sslmode=require#readonly",
    );
}
