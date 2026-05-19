use super::*;
use crate::testutil::col;

#[test]
fn test_sqlite_integer() {
    let c = col("id").udt("integer").build();
    assert_eq!(to_canonical(&c), CanonicalType::Integer);
    assert_eq!(from_canonical(&CanonicalType::Integer).sql_type, "INTEGER");
}

#[test]
fn test_sqlite_text() {
    let c = col("name").udt("text").build();
    assert_eq!(to_canonical(&c), CanonicalType::Text);
    assert_eq!(from_canonical(&CanonicalType::Text).sql_type, "TEXT");
}

#[test]
fn test_sqlite_affinity_int() {
    let c = col("n").udt("mediumint").build();
    assert_eq!(to_canonical(&c), CanonicalType::Integer);
}

#[test]
fn test_uuid_to_sqlite() {
    assert_eq!(from_canonical(&CanonicalType::Uuid).sql_type, "TEXT");
}

#[test]
fn test_json_to_sqlite() {
    assert_eq!(from_canonical(&CanonicalType::Json).sql_type, "TEXT");
}

#[test]
fn test_bigint_to_sqlite() {
    assert_eq!(from_canonical(&CanonicalType::BigInt).sql_type, "INTEGER");
}

#[test]
fn test_timestamp_to_sqlite() {
    assert_eq!(
        from_canonical(&CanonicalType::Timestamp {
            with_tz: true,
            precision: None
        })
        .sql_type,
        "DATETIME"
    );
}
