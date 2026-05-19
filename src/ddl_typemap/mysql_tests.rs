use super::*;
use crate::testutil::col;

fn mysql_col(udt: &str, column_type: &str) -> ColumnInfo {
    let mut c = col("test").udt(udt).build();
    c.data_type = column_type.to_string();
    c
}

#[test]
fn test_tinyint_bool() {
    let c = mysql_col("tinyint", "tinyint(1)");
    assert_eq!(to_canonical(&c), CanonicalType::Boolean);
}

#[test]
fn test_int() {
    let c = mysql_col("int", "int");
    assert_eq!(to_canonical(&c), CanonicalType::Integer);
    assert_eq!(from_canonical(&CanonicalType::Integer).sql_type, "INT");
}

#[test]
fn test_enum() {
    let c = mysql_col("enum", "enum('a','b','c')");
    let ct = to_canonical(&c);
    assert!(matches!(ct, CanonicalType::Enum { ref values } if values == &["a", "b", "c"]));
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "ENUM('a', 'b', 'c')");
}

#[test]
fn test_json_to_mysql() {
    let dt = from_canonical(&CanonicalType::Json);
    assert_eq!(dt.sql_type, "JSON");
}

#[test]
fn test_uuid_to_mysql() {
    let dt = from_canonical(&CanonicalType::Uuid);
    assert_eq!(dt.sql_type, "CHAR(36)");
}

#[test]
fn test_jsonb_to_mysql() {
    let dt = from_canonical(&CanonicalType::Jsonb);
    assert_eq!(dt.sql_type, "JSON");
}

#[test]
fn test_mysql_set_roundtrip() {
    // #38 — MySQL SET column → CanonicalType::Set with parsed values,
    // re-emits to native SET('a','b','c') on a mysql target.
    let c = mysql_col("set", "set('billing','shipping','physical','mailing')");
    let ct = to_canonical(&c);
    assert!(matches!(
        ct,
        CanonicalType::Set { ref values }
            if values == &["billing", "shipping", "physical", "mailing"]
    ));
    assert_eq!(
        from_canonical(&ct).sql_type,
        "SET('billing', 'shipping', 'physical', 'mailing')"
    );
}

#[test]
fn test_datetime_precision_roundtrip() {
    // #36 — DATETIME(6) source must round-trip with precision intact.
    // Without this, the source's `DEFAULT CURRENT_TIMESTAMP(6)` would
    // hit a plain `DATETIME` column on the target — MySQL rejects the
    // mismatch with "Invalid default value".
    let c = mysql_col("datetime", "datetime(6)");
    let ct = to_canonical(&c);
    assert_eq!(
        ct,
        CanonicalType::Timestamp {
            with_tz: false,
            precision: Some(6)
        }
    );
    let dt = from_canonical(&ct);
    assert_eq!(dt.sql_type, "DATETIME(6)");

    // Plain `datetime` (no precision) round-trips as bare DATETIME.
    let c2 = mysql_col("datetime", "datetime");
    assert_eq!(
        to_canonical(&c2),
        CanonicalType::Timestamp {
            with_tz: false,
            precision: None
        }
    );
    assert_eq!(from_canonical(&to_canonical(&c2)).sql_type, "DATETIME");
}

#[test]
fn test_parse_temporal_precision() {
    assert_eq!(parse_temporal_precision("datetime(6)"), Some(6));
    assert_eq!(parse_temporal_precision("timestamp(3)"), Some(3));
    assert_eq!(parse_temporal_precision("time(0)"), Some(0));
    assert_eq!(parse_temporal_precision("datetime"), None);
    // Out-of-range precisions (>6) are rejected as None — defensive.
    assert_eq!(parse_temporal_precision("datetime(9)"), None);
    // Non-numeric junk inside parens — None.
    assert_eq!(parse_temporal_precision("varchar(N)"), None);
}

#[test]
fn test_interval_to_mysql() {
    let dt = from_canonical(&CanonicalType::Interval);
    assert!(dt.is_approximate);
}

#[test]
fn test_array_to_mysql() {
    let dt = from_canonical(&CanonicalType::Array {
        element: Box::new(CanonicalType::Integer),
    });
    assert_eq!(dt.sql_type, "JSON");
    assert!(dt.is_approximate);
}
