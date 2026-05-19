use super::*;
use crate::testutil::col;

fn col_with(udt: &str, data_type: &str) -> ColumnInfo {
    let mut c = col("test").udt(udt).build();
    c.data_type = data_type.to_string();
    c
}

#[test]
fn test_pg_int4_to_mysql() {
    let c = col("id").udt("int4").build();
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mysql);
    assert_eq!(dt.sql_type, "INT");
    assert!(!dt.is_approximate);
}

#[test]
fn test_pg_jsonb_to_mysql() {
    let c = col("data").udt("jsonb").build();
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mysql);
    assert_eq!(dt.sql_type, "JSON");
    assert!(dt.is_approximate);
}

#[test]
fn test_pg_uuid_to_mysql() {
    let c = col("uid").udt("uuid").build();
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mysql);
    assert_eq!(dt.sql_type, "CHAR(36)");
}

#[test]
fn test_pg_uuid_to_mssql() {
    let c = col("uid").udt("uuid").build();
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mssql);
    assert_eq!(dt.sql_type, "UNIQUEIDENTIFIER");
}

#[test]
fn test_pg_timestamptz_to_mysql() {
    let c = col("ts").udt("timestamptz").build();
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mysql);
    assert_eq!(dt.sql_type, "DATETIME");
}

#[test]
fn test_pg_array_to_mysql() {
    let c = col("tags").udt("_text").build();
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mysql);
    assert_eq!(dt.sql_type, "JSON");
    assert!(dt.is_approximate);
}

#[test]
fn test_mysql_tinyint1_to_pg() {
    let c = col_with("tinyint", "tinyint(1)");
    let dt = map_ddl_type(&c, Dialect::Mysql, Dialect::Postgres);
    assert_eq!(dt.sql_type, "BOOLEAN");
}

#[test]
fn test_mssql_uniqueidentifier_to_pg() {
    let c = col("uid").udt("uniqueidentifier").build();
    let dt = map_ddl_type(&c, Dialect::Mssql, Dialect::Postgres);
    assert_eq!(dt.sql_type, "UUID");
}

#[test]
fn test_mssql_money_to_pg() {
    let c = col("amount").udt("money").build();
    let dt = map_ddl_type(&c, Dialect::Mssql, Dialect::Postgres);
    assert_eq!(dt.sql_type, "NUMERIC(19, 4)");
}

#[test]
fn test_same_dialect_passthrough() {
    let mut c = col("name").udt("varchar").build();
    c.character_maximum_length = Some(100);
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Postgres);
    assert_eq!(dt.sql_type, "VARCHAR(100)");
}

#[test]
fn test_mysql_enum_to_pg() {
    let c = col_with("enum", "enum('a','b','c')");
    let dt = map_ddl_type(&c, Dialect::Mysql, Dialect::Postgres);
    // PG needs CREATE TYPE separately; column type is a placeholder
    assert!(dt.sql_type.contains("VARCHAR"));
}

#[test]
fn test_pg_numeric_to_mysql() {
    let mut c = col("price").udt("numeric").build();
    c.numeric_precision = Some(10);
    c.numeric_scale = Some(2);
    let dt = map_ddl_type(&c, Dialect::Postgres, Dialect::Mysql);
    assert_eq!(dt.sql_type, "DECIMAL(10, 2)");
}

#[test]
fn test_sqlite_integer_to_pg() {
    let c = col("id").udt("integer").build();
    let dt = map_ddl_type(&c, Dialect::Sqlite, Dialect::Postgres);
    assert_eq!(dt.sql_type, "INTEGER");
}
