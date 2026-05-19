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
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "Boolean");
    assert_eq!(m.python_type, "bool");
}

#[test]
fn test_tinyint_not_bool() {
    let c = mysql_col("tinyint", "tinyint(4)");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "TINYINT");
    assert_eq!(m.import_module, MY);
}

#[test]
fn test_unsigned_int() {
    let c = mysql_col("int", "int unsigned");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "Integer");

    let md = map_column_type_dialect(&c);
    assert_eq!(md.sa_type, "INTEGER(unsigned=True)");
    assert_eq!(md.import_module, MY);
}

#[test]
fn test_varchar() {
    let mut c = mysql_col("varchar", "varchar(255)");
    c.character_maximum_length = Some(255);
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "String(255)");
    assert_eq!(m.python_type, "str");
}

#[test]
fn test_enum_parsing() {
    let c = mysql_col("enum", "enum('active','inactive','pending')");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "Enum('active', 'inactive', 'pending')");
    assert_eq!(m.import_name, "Enum");
}

#[test]
fn test_set_parsing() {
    let c = mysql_col("set", "set('read','write','execute')");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "SET('read', 'write', 'execute')");
    assert_eq!(m.import_module, MY);
}

#[test]
fn test_datetime() {
    let c = mysql_col("datetime", "datetime");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "DateTime");
    assert_eq!(m.python_type, "datetime.datetime");
}

#[test]
fn test_decimal() {
    let mut c = mysql_col("decimal", "decimal(10,2)");
    c.numeric_precision = Some(10);
    c.numeric_scale = Some(2);
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "Numeric(10, 2)");
    assert_eq!(m.python_type, "decimal.Decimal");
}

#[test]
fn test_json() {
    let c = mysql_col("json", "json");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "JSON");
    assert_eq!(m.python_type, "dict");
}

#[test]
fn test_year() {
    let c = mysql_col("year", "year");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "YEAR");
    assert_eq!(m.import_module, MY);
}

#[test]
fn test_mediumtext() {
    let c = mysql_col("mediumtext", "mediumtext");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "MEDIUMTEXT");
    assert_eq!(m.import_module, MY);
}

#[test]
fn test_dialect_tinyint_bool() {
    let c = mysql_col("tinyint", "tinyint(1)");
    let m = map_column_type_dialect(&c);
    assert_eq!(m.sa_type, "TINYINT(display_width=1)");
    assert_eq!(m.import_module, MY);
}

#[test]
fn test_enum_escaped_quotes() {
    let c = mysql_col("enum", "enum('can''t','won''t','ok')");
    let m = map_column_type(&c);
    assert_eq!(m.sa_type, "Enum('can't', 'won't', 'ok')");
}
