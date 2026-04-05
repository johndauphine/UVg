pub mod mssql;
pub mod mysql;
pub mod pg;
pub mod sqlite;

use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

/// Canonical type representation for cross-dialect type translation.
#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Float,
    Double,
    Decimal {
        precision: Option<i32>,
        scale: Option<i32>,
    },
    Varchar {
        length: Option<i32>,
    },
    Char {
        length: Option<i32>,
    },
    Text,
    Bytes {
        length: Option<i32>,
    },
    Date,
    Time {
        with_tz: bool,
    },
    Timestamp {
        with_tz: bool,
    },
    Interval,
    Uuid,
    Json,
    Jsonb,
    Enum {
        values: Vec<String>,
    },
    Array {
        element: Box<CanonicalType>,
    },
    /// Non-portable type passed through as-is.
    Raw {
        type_name: String,
    },
}

/// Result of mapping a canonical type to a target DDL type string.
#[derive(Debug, Clone)]
pub struct DdlType {
    /// The SQL type string, e.g. "INTEGER", "VARCHAR(100)", "TIMESTAMP WITH TIME ZONE".
    pub sql_type: String,
    /// Whether this was a lossy translation (no exact equivalent in target).
    pub is_approximate: bool,
    /// Optional warning about the translation.
    pub warning: Option<String>,
}

impl DdlType {
    fn exact(sql_type: &str) -> Self {
        DdlType {
            sql_type: sql_type.to_string(),
            is_approximate: false,
            warning: None,
        }
    }

    fn approx(sql_type: &str, warning: &str) -> Self {
        DdlType {
            sql_type: sql_type.to_string(),
            is_approximate: true,
            warning: Some(warning.to_string()),
        }
    }
}

/// Normalize a source column to a canonical type.
pub fn to_canonical(col: &ColumnInfo, dialect: Dialect) -> CanonicalType {
    match dialect {
        Dialect::Postgres => pg::to_canonical(col),
        Dialect::Mysql => mysql::to_canonical(col),
        Dialect::Mssql => mssql::to_canonical(col),
        Dialect::Sqlite => sqlite::to_canonical(col),
    }
}

/// Emit a canonical type as a DDL string for the target dialect.
pub fn from_canonical(ct: &CanonicalType, target: Dialect) -> DdlType {
    match target {
        Dialect::Postgres => pg::from_canonical(ct),
        Dialect::Mysql => mysql::from_canonical(ct),
        Dialect::Mssql => mssql::from_canonical(ct),
        Dialect::Sqlite => sqlite::from_canonical(ct),
    }
}

/// Map a source column type to a target DDL type string.
pub fn map_ddl_type(col: &ColumnInfo, source: Dialect, target: Dialect) -> DdlType {
    let canonical = to_canonical(col, source);
    from_canonical(&canonical, target)
}

#[cfg(test)]
mod tests {
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
}
