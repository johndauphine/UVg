use crate::schema::ColumnInfo;

use super::{simple, MappedType};

const SA: &str = "sqlalchemy";

/// Map a SQLite column to generic SQLAlchemy types.
/// SQLite uses type affinity — declared types are free-form strings.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let dt = col.udt_name.as_str();

    // First try exact matches on common declared types
    match dt {
        "integer" | "int" => simple("Integer", "int", SA),
        "smallint" => simple("SmallInteger", "int", SA),
        "bigint" => simple("BigInteger", "int", SA),
        "real" | "float" | "double" => simple("Float", "float", SA),
        "numeric" | "decimal" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("Numeric({p}, {s})"),
                (Some(p), None) => format!("Numeric({p})"),
                _ => "Numeric".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: SA.to_string(),
                import_name: "Numeric".to_string(),
                element_import: None,
            }
        }
        "text" | "clob" => simple("Text", "str", SA),
        "varchar" | "character varying" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: SA.to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "char" | "character" | "nchar" | "nvarchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: SA.to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "blob" => simple("LargeBinary", "bytes", SA),
        "date" => simple("Date", "datetime.date", SA),
        "datetime" | "timestamp" => simple("DateTime", "datetime.datetime", SA),
        "time" => simple("Time", "datetime.time", SA),
        "boolean" | "bool" => simple("Boolean", "bool", SA),
        "json" => simple("JSON", "dict", SA),
        "" => {
            // No declared type — use NullType
            simple("NullType", "str", "sqlalchemy.sql.sqltypes")
        }
        _ => {
            // Apply SQLite type affinity rules
            map_by_affinity(dt)
        }
    }
}

/// Map a SQLite column keeping dialect-specific types.
/// SQLite has very few dialect-specific types, so this is mostly the same
/// as the generic mapping.
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    // SQLite's dialect types in SA are minimal (DATETIME, DATE, TIME, JSON).
    // For simplicity, use the generic mapping — SA handles SQLite reflection
    // using generic types.
    map_column_type(col)
}

/// Apply SQLite type affinity rules for unknown declared types.
/// See: https://www.sqlite.org/datatype3.html
fn map_by_affinity(dt: &str) -> MappedType {
    let upper = dt.to_uppercase();

    if upper.contains("INT") {
        simple("Integer", "int", SA)
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        simple("Text", "str", SA)
    } else if upper.contains("BLOB") {
        simple("LargeBinary", "bytes", SA)
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        simple("Float", "float", SA)
    } else {
        // NUMERIC affinity — fallback
        simple("Numeric", "decimal.Decimal", SA)
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
