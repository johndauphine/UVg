use super::*;
use crate::testutil::test_column;

fn col(udt_name: &str) -> ColumnInfo {
    ColumnInfo {
        udt_name: udt_name.to_string(),
        data_type: udt_name.to_string(),
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
fn test_bit() {
    let m = map_column_type(&col("bit"));
    assert_eq!(m.sa_type, "Boolean");
    assert_eq!(m.python_type, "bool");
}

#[test]
fn test_integer_types() {
    assert_eq!(map_column_type(&col("tinyint")).sa_type, "TINYINT");
    assert_eq!(
        map_column_type(&col("tinyint")).import_module,
        "sqlalchemy.dialects.mssql"
    );
    assert_eq!(map_column_type(&col("smallint")).sa_type, "SmallInteger");
    assert_eq!(map_column_type(&col("int")).sa_type, "Integer");
    assert_eq!(map_column_type(&col("bigint")).sa_type, "BigInteger");
}

#[test]
fn test_float_types() {
    assert_eq!(map_column_type(&col("real")).sa_type, "Float");
    assert_eq!(map_column_type(&col("float")).sa_type, "Double");
}

#[test]
fn test_decimal() {
    let m = map_column_type(&col_with_precision("decimal", 10, 2));
    assert_eq!(m.sa_type, "Numeric(10, 2)");
}

#[test]
fn test_money() {
    assert_eq!(map_column_type(&col("money")).sa_type, "Numeric(19, 4)");
    assert_eq!(
        map_column_type(&col("smallmoney")).sa_type,
        "Numeric(10, 4)"
    );
}

#[test]
fn test_string_types() {
    assert_eq!(
        map_column_type(&col_with_length("varchar", 100)).sa_type,
        "String(100)"
    );
    assert_eq!(
        map_column_type(&col_with_length("nvarchar", 50)).sa_type,
        "Unicode(50)"
    );
    assert_eq!(map_column_type(&col("text")).sa_type, "Text");
    assert_eq!(map_column_type(&col("ntext")).sa_type, "UnicodeText");
}

#[test]
fn test_varchar_max() {
    // varchar(max) has no character_maximum_length
    let m = map_column_type(&col("varchar"));
    assert_eq!(m.sa_type, "String");
}

#[test]
fn test_binary_types() {
    assert_eq!(map_column_type(&col("binary")).sa_type, "LargeBinary");
    assert_eq!(map_column_type(&col("varbinary")).sa_type, "LargeBinary");
    assert_eq!(map_column_type(&col("image")).sa_type, "LargeBinary");
}

#[test]
fn test_datetime_types() {
    assert_eq!(map_column_type(&col("datetime")).sa_type, "DateTime");
    assert_eq!(map_column_type(&col("datetime2")).sa_type, "DateTime");
    assert_eq!(map_column_type(&col("smalldatetime")).sa_type, "DateTime");
    assert_eq!(
        map_column_type(&col("datetimeoffset")).sa_type,
        "DateTime(True)"
    );
    assert_eq!(map_column_type(&col("date")).sa_type, "Date");
    assert_eq!(map_column_type(&col("time")).sa_type, "Time");
}

#[test]
fn test_uniqueidentifier() {
    let m = map_column_type(&col("uniqueidentifier"));
    assert_eq!(m.sa_type, "UNIQUEIDENTIFIER");
    assert_eq!(m.import_module, "sqlalchemy.dialects.mssql");
}

#[test]
fn test_fallback() {
    let m = map_column_type(&col("xml"));
    assert_eq!(m.sa_type, "XML");
}
