use super::*;
use crate::testutil::col;

fn sqlite_col(udt: &str) -> ColumnInfo {
    col("test").udt(udt).build()
}

#[test]
fn test_integer() {
    let m = map_column_type(&sqlite_col("integer"));
    assert_eq!(m.sa_type, "Integer");
    assert_eq!(m.python_type, "int");
}

#[test]
fn test_text() {
    let m = map_column_type(&sqlite_col("text"));
    assert_eq!(m.sa_type, "Text");
    assert_eq!(m.python_type, "str");
}

#[test]
fn test_real() {
    let m = map_column_type(&sqlite_col("real"));
    assert_eq!(m.sa_type, "Float");
    assert_eq!(m.python_type, "float");
}

#[test]
fn test_blob() {
    let m = map_column_type(&sqlite_col("blob"));
    assert_eq!(m.sa_type, "LargeBinary");
    assert_eq!(m.python_type, "bytes");
}

#[test]
fn test_varchar_with_length() {
    let mut c = sqlite_col("varchar");
    c.character_maximum_length = Some(100);
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "String(100)");
}

#[test]
fn test_boolean() {
    let m = map_column_type(&sqlite_col("boolean"));
    assert_eq!(m.sa_type, "Boolean");
    assert_eq!(m.python_type, "bool");
}

#[test]
fn test_datetime() {
    let m = map_column_type(&sqlite_col("datetime"));
    assert_eq!(m.sa_type, "DateTime");
    assert_eq!(m.python_type, "datetime.datetime");
}

#[test]
fn test_json() {
    let m = map_column_type(&sqlite_col("json"));
    assert_eq!(m.sa_type, "JSON");
    assert_eq!(m.python_type, "dict");
}

#[test]
fn test_empty_type() {
    let m = map_column_type(&sqlite_col(""));
    assert_eq!(m.sa_type, "NullType");
}

#[test]
fn test_affinity_int() {
    // "MEDIUMINT" contains "INT" -> Integer affinity
    let m = map_column_type(&sqlite_col("mediumint"));
    assert_eq!(m.sa_type, "Integer");
}

#[test]
fn test_affinity_text() {
    // "LONGTEXT" contains "TEXT" -> Text affinity
    let m = map_column_type(&sqlite_col("longtext"));
    assert_eq!(m.sa_type, "Text");
}

#[test]
fn test_affinity_real() {
    // "DOUBLE PRECISION" contains "DOUB" -> Float affinity
    let m = map_column_type(&sqlite_col("double precision"));
    assert_eq!(m.sa_type, "Float");
}

#[test]
fn test_decimal() {
    let mut c = sqlite_col("decimal");
    c.numeric_precision = Some(10);
    c.numeric_scale = Some(2);
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "Numeric(10, 2)");
}
