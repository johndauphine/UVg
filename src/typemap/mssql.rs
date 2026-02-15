use crate::schema::ColumnInfo;

use super::{simple, MappedType};

/// Map a MSSQL column to its SQLAlchemy type representation.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let dt = col.udt_name.as_str();

    match dt {
        "bit" => simple("Boolean", "bool", "sqlalchemy"),
        "tinyint" => simple("TINYINT", "int", "sqlalchemy.dialects.mssql"),
        "smallint" => simple("SmallInteger", "int", "sqlalchemy"),
        "int" => simple("Integer", "int", "sqlalchemy"),
        "bigint" => simple("BigInteger", "int", "sqlalchemy"),
        "real" => simple("Float", "float", "sqlalchemy"),
        "float" => simple("Double", "float", "sqlalchemy"),
        "decimal" | "numeric" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("Numeric({p}, {s})"),
                (Some(p), None) => format!("Numeric({p})"),
                _ => "Numeric".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "Numeric".to_string(),
                element_import: None,
            }
        }
        "money" => MappedType {
            sa_type: "Numeric(19, 4)".to_string(),
            python_type: "decimal.Decimal".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "Numeric".to_string(),
            element_import: None,
        },
        "smallmoney" => MappedType {
            sa_type: "Numeric(10, 4)".to_string(),
            python_type: "decimal.Decimal".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "Numeric".to_string(),
            element_import: None,
        },
        "varchar" | "char" => {
            let sa_type = format_string_type("String", col.character_maximum_length, col.collation.as_deref());
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "nvarchar" | "nchar" => {
            let sa_type = format_string_type("Unicode", col.character_maximum_length, col.collation.as_deref());
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "Unicode".to_string(),
                element_import: None,
            }
        }
        "text" => simple("Text", "str", "sqlalchemy"),
        "ntext" => simple("UnicodeText", "str", "sqlalchemy"),
        "binary" | "varbinary" | "image" => simple("LargeBinary", "bytes", "sqlalchemy"),
        "datetime" | "datetime2" | "smalldatetime" => {
            simple("DateTime", "datetime.datetime", "sqlalchemy")
        }
        "datetimeoffset" => MappedType {
            sa_type: "DateTime(timezone=True)".to_string(),
            python_type: "datetime.datetime".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "DateTime".to_string(),
            element_import: None,
        },
        "date" => simple("Date", "datetime.date", "sqlalchemy"),
        "time" => simple("Time", "datetime.time", "sqlalchemy"),
        "uniqueidentifier" => {
            simple("UNIQUEIDENTIFIER", "str", "sqlalchemy.dialects.mssql")
        }
        // Fallback: use the data_type as-is, uppercased
        other => MappedType {
            sa_type: other.to_uppercase(),
            python_type: "str".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: other.to_uppercase(),
            element_import: None,
        },
    }
}

/// Format a String/Unicode type expression with optional length and collation.
/// Matches sqlacodegen output: `String(50, 'collation')` or `Unicode(collation='collation')`.
fn format_string_type(base: &str, length: Option<i32>, collation: Option<&str>) -> String {
    match (length, collation) {
        (Some(n), Some(c)) => format!("{base}({n}, '{c}')"),
        (Some(n), None) => format!("{base}({n})"),
        (None, Some(c)) => format!("{base}(collation='{c}')"),
        (None, None) => base.to_string(),
    }
}

#[cfg(test)]
mod tests {
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
            "DateTime(timezone=True)"
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
}
