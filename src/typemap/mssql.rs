use crate::schema::ColumnInfo;

use super::{simple, MappedType};

/// Map a MSSQL column keeping dialect-specific types.
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    let ms = "sqlalchemy.dialects.mssql";
    let dt = col.udt_name.as_str();
    match dt {
        "bit" => simple("BIT", "bool", ms),
        "tinyint" => simple("TINYINT", "int", ms),
        "smallint" => simple("SMALLINT", "int", ms),
        "int" => simple("INTEGER", "int", ms),
        "bigint" => simple("BIGINT", "int", ms),
        "real" => simple("REAL", "float", ms),
        "float" => simple("FLOAT", "float", ms),
        "decimal" | "numeric" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("NUMERIC({p}, {s})"),
                (Some(p), None) => format!("NUMERIC({p})"),
                _ => "NUMERIC".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: ms.to_string(),
                import_name: "NUMERIC".to_string(),
                element_import: None,
            }
        }
        "money" => simple("MONEY", "decimal.Decimal", ms),
        "smallmoney" => simple("SMALLMONEY", "decimal.Decimal", ms),
        "varchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("VARCHAR({n})"),
                None => "VARCHAR".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: ms.to_string(),
                import_name: "VARCHAR".to_string(),
                element_import: None,
            }
        }
        "char" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("CHAR({n})"),
                None => "CHAR".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: ms.to_string(),
                import_name: "CHAR".to_string(),
                element_import: None,
            }
        }
        "nvarchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("NVARCHAR({n})"),
                None => "NVARCHAR".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: ms.to_string(),
                import_name: "NVARCHAR".to_string(),
                element_import: None,
            }
        }
        "nchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("NCHAR({n})"),
                None => "NCHAR".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: ms.to_string(),
                import_name: "NCHAR".to_string(),
                element_import: None,
            }
        }
        "text" => simple("TEXT", "str", ms),
        "ntext" => simple("NTEXT", "str", ms),
        "binary" => simple("BINARY", "bytes", ms),
        "varbinary" => simple("VARBINARY", "bytes", ms),
        "image" => simple("IMAGE", "bytes", ms),
        "datetime" => simple("DATETIME", "datetime.datetime", ms),
        "datetime2" => simple("DATETIME2", "datetime.datetime", ms),
        "smalldatetime" => simple("SMALLDATETIME", "datetime.datetime", ms),
        "datetimeoffset" => simple("DATETIMEOFFSET", "datetime.datetime", ms),
        "date" => simple("DATE", "datetime.date", ms),
        "time" => simple("TIME", "datetime.time", ms),
        "uniqueidentifier" => simple("UNIQUEIDENTIFIER", "str", ms),
        other => MappedType {
            sa_type: other.to_uppercase(),
            python_type: "str".to_string(),
            import_module: ms.to_string(),
            import_name: other.to_uppercase(),
            element_import: None,
        },
    }
}

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
            let sa_type = format_string_type(
                "String",
                col.character_maximum_length,
                col.collation.as_deref(),
            );
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "nvarchar" | "nchar" => {
            let sa_type = format_string_type(
                "Unicode",
                col.character_maximum_length,
                col.collation.as_deref(),
            );
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
            sa_type: "DateTime(True)".to_string(),
            python_type: "datetime.datetime".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "DateTime".to_string(),
            element_import: None,
        },
        "date" => simple("Date", "datetime.date", "sqlalchemy"),
        "time" => simple("Time", "datetime.time", "sqlalchemy"),
        "uniqueidentifier" => simple("UNIQUEIDENTIFIER", "str", "sqlalchemy.dialects.mssql"),
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
#[path = "mssql_tests.rs"]
mod tests;
