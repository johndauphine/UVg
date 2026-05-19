use super::*;
use crate::testutil::col;

#[test]
fn test_mssql_int() {
    let c = col("id").udt("int").build();
    assert_eq!(to_canonical(&c), CanonicalType::Integer);
}

#[test]
fn test_mssql_uniqueidentifier() {
    let c = col("uid").udt("uniqueidentifier").build();
    assert_eq!(to_canonical(&c), CanonicalType::Uuid);
    assert_eq!(
        from_canonical(&CanonicalType::Uuid).sql_type,
        "UNIQUEIDENTIFIER"
    );
}

#[test]
fn test_mssql_money() {
    let c = col("amount").udt("money").build();
    let ct = to_canonical(&c);
    assert_eq!(
        ct,
        CanonicalType::Decimal {
            precision: Some(19),
            scale: Some(4)
        }
    );
}

#[test]
fn test_mssql_datetimeoffset() {
    let c = col("ts").udt("datetimeoffset").build();
    assert_eq!(
        to_canonical(&c),
        CanonicalType::Timestamp {
            with_tz: true,
            precision: None
        }
    );
    assert_eq!(
        from_canonical(&CanonicalType::Timestamp {
            with_tz: true,
            precision: None
        })
        .sql_type,
        "DATETIMEOFFSET"
    );
}

#[test]
fn test_mssql_bit() {
    let c = col("flag").udt("bit").build();
    assert_eq!(to_canonical(&c), CanonicalType::Boolean);
    assert_eq!(from_canonical(&CanonicalType::Boolean).sql_type, "BIT");
}

#[test]
fn test_mssql_text_to_nvarchar_max() {
    assert_eq!(
        from_canonical(&CanonicalType::Text).sql_type,
        "NVARCHAR(MAX)"
    );
}

#[test]
fn test_json_to_mssql() {
    let dt = from_canonical(&CanonicalType::Json);
    assert_eq!(dt.sql_type, "NVARCHAR(MAX)");
    assert!(dt.is_approximate);
}
