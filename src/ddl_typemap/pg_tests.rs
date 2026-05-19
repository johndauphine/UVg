use super::*;
use crate::testutil::col;

#[test]
fn test_pg_int4() {
    let c = col("id").udt("int4").build();
    let ct = to_canonical(&c);
    assert_eq!(ct, CanonicalType::Integer);
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "INTEGER");
}

#[test]
fn test_pg_varchar_with_length() {
    let mut c = col("name").udt("varchar").build();
    c.character_maximum_length = Some(255);
    let ct = to_canonical(&c);
    assert_eq!(ct, CanonicalType::Varchar { length: Some(255) });
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "VARCHAR(255)");
}

#[test]
fn test_pg_timestamptz() {
    let c = col("ts").udt("timestamptz").build();
    let ct = to_canonical(&c);
    assert_eq!(
        ct,
        CanonicalType::Timestamp {
            with_tz: true,
            precision: None
        }
    );
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "TIMESTAMP WITH TIME ZONE");
}

#[test]
fn test_pg_array() {
    let c = col("tags").udt("_text").build();
    let ct = to_canonical(&c);
    assert!(matches!(ct, CanonicalType::Array { .. }));
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "TEXT[]");
}

#[test]
fn test_pg_uuid() {
    let c = col("uid").udt("uuid").build();
    let ct = to_canonical(&c);
    assert_eq!(ct, CanonicalType::Uuid);
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "UUID");
}

#[test]
fn test_pg_jsonb() {
    let c = col("data").udt("jsonb").build();
    let ct = to_canonical(&c);
    assert_eq!(ct, CanonicalType::Jsonb);
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "JSONB");
}

#[test]
fn test_pg_set_fallback() {
    // #38 — MySQL SET has no PG equivalent. Falls back to VARCHAR sized
    // to fit the worst-case comma-joined value list (with a 255 floor).
    let ct = CanonicalType::Set {
        values: vec!["a".into(), "b".into(), "c".into()],
    };
    let dt = from_canonical(&ct);
    assert!(dt.sql_type.starts_with("VARCHAR("), "got {}", dt.sql_type);
    assert!(dt.is_approximate);
    assert!(dt.warning.as_deref().unwrap().contains("multi-value"));
}

#[test]
fn test_set_varchar_capacity() {
    use super::set_varchar_capacity;
    // 4 values × 6 chars + 3 separators = 27 — but the 255 floor wins.
    let v: Vec<String> = ["billing", "shipping", "mailing", "phys24"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(set_varchar_capacity(&v), 255);
    // Edge case: empty list returns 1 (defensive — shouldn't happen
    // since SET requires at least one value).
    assert_eq!(set_varchar_capacity(&[]), 1);
    // Above the 255 floor: long values force a larger column.
    let big = vec!["x".repeat(300)];
    assert_eq!(set_varchar_capacity(&big), 300);
}

#[test]
fn test_pg_numeric() {
    let mut c = col("price").udt("numeric").build();
    c.numeric_precision = Some(10);
    c.numeric_scale = Some(2);
    let ct = to_canonical(&c);
    assert_eq!(
        ct,
        CanonicalType::Decimal {
            precision: Some(10),
            scale: Some(2)
        }
    );
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "NUMERIC(10, 2)");
}
