use super::*;
use crate::testutil::test_column;

fn col(udt_name: &str) -> ColumnInfo {
    ColumnInfo {
        udt_name: udt_name.to_string(),
        ..test_column("test")
    }
}

fn col_with_length(udt_name: &str, len: i32) -> ColumnInfo {
    ColumnInfo {
        character_maximum_length: Some(len),
        ..col(udt_name)
    }
}

fn col_with_precision(udt_name: &str, precision: i32, scale: i32) -> ColumnInfo {
    ColumnInfo {
        numeric_precision: Some(precision),
        numeric_scale: Some(scale),
        ..col(udt_name)
    }
}

#[test]
fn test_bool() {
    let m = map_column_type(&col("bool"));
    assert_eq!(m.sa_type, "Boolean");
    assert_eq!(m.python_type, "bool");
}

#[test]
fn test_integer_types() {
    assert_eq!(map_column_type(&col("int2")).sa_type, "SmallInteger");
    assert_eq!(map_column_type(&col("int4")).sa_type, "Integer");
    assert_eq!(map_column_type(&col("int8")).sa_type, "BigInteger");
    assert_eq!(map_column_type(&col("serial")).sa_type, "Integer");
    assert_eq!(map_column_type(&col("bigserial")).sa_type, "BigInteger");
}

#[test]
fn test_float_types() {
    assert_eq!(map_column_type(&col("float4")).sa_type, "Float");
    assert_eq!(map_column_type(&col("float8")).sa_type, "Double");
}

#[test]
fn test_numeric_with_precision() {
    let m = map_column_type(&col_with_precision("numeric", 10, 2));
    assert_eq!(m.sa_type, "Numeric(10, 2)");
    assert_eq!(m.python_type, "decimal.Decimal");
}

#[test]
fn test_string_types() {
    assert_eq!(map_column_type(&col("text")).sa_type, "Text");
    assert_eq!(
        map_column_type(&col_with_length("varchar", 100)).sa_type,
        "String(100)"
    );
    assert_eq!(
        map_column_type(&col_with_length("bpchar", 10)).sa_type,
        "String(10)"
    );
}

#[test]
fn test_datetime_types() {
    assert_eq!(map_column_type(&col("timestamp")).sa_type, "DateTime");
    assert_eq!(
        map_column_type(&col("timestamptz")).sa_type,
        "DateTime(True)"
    );
    assert_eq!(map_column_type(&col("date")).sa_type, "Date");
    assert_eq!(map_column_type(&col("time")).sa_type, "Time");
    assert_eq!(map_column_type(&col("timetz")).sa_type, "Time(True)");
}

#[test]
fn test_dialect_types() {
    let m = map_column_type(&col("uuid"));
    assert_eq!(m.sa_type, "UUID");
    assert_eq!(m.import_module, "sqlalchemy.dialects.postgresql");

    assert_eq!(map_column_type(&col("jsonb")).sa_type, "JSONB");
    assert_eq!(map_column_type(&col("json")).sa_type, "JSON");
    assert_eq!(map_column_type(&col("inet")).sa_type, "INET");
    assert_eq!(map_column_type(&col("cidr")).sa_type, "CIDR");
}

#[test]
fn test_array_type() {
    let m = map_column_type(&col("_int4"));
    assert_eq!(m.sa_type, "ARRAY(Integer)");
    assert_eq!(m.import_name, "ARRAY");
    assert_eq!(
        m.element_import,
        Some(("sqlalchemy".to_string(), "Integer".to_string()))
    );

    let m2 = map_column_type(&col("_text"));
    assert_eq!(m2.sa_type, "ARRAY(Text)");
}

#[test]
fn test_bytea() {
    let m = map_column_type(&col("bytea"));
    assert_eq!(m.sa_type, "LargeBinary");
    assert_eq!(m.python_type, "bytes");
}

#[test]
fn test_interval() {
    let m = map_column_type(&col("interval"));
    assert_eq!(m.sa_type, "Interval");
    assert_eq!(m.python_type, "datetime.timedelta");
}
