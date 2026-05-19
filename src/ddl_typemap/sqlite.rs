use crate::schema::ColumnInfo;

use super::{CanonicalType, DdlType};

/// Normalize a SQLite column type to canonical form using type affinity rules.
pub fn to_canonical(col: &ColumnInfo) -> CanonicalType {
    let udt = col.udt_name.as_str();

    // Exact matches first
    match udt {
        "integer" | "int" => return CanonicalType::Integer,
        "smallint" => return CanonicalType::SmallInt,
        "bigint" => return CanonicalType::BigInt,
        "real" | "float" => return CanonicalType::Float,
        "double" => return CanonicalType::Double,
        "numeric" | "decimal" => {
            return CanonicalType::Decimal {
                precision: col.numeric_precision,
                scale: col.numeric_scale,
            }
        }
        "text" | "clob" => return CanonicalType::Text,
        "varchar" | "character varying" | "char" | "character" | "nchar" | "nvarchar" => {
            return CanonicalType::Varchar {
                length: col.character_maximum_length,
            }
        }
        "blob" => return CanonicalType::Bytes { length: None },
        "date" => return CanonicalType::Date,
        "datetime" | "timestamp" => {
            return CanonicalType::Timestamp {
                with_tz: false,
                precision: None,
            };
        }
        "time" => {
            return CanonicalType::Time {
                with_tz: false,
                precision: None,
            }
        }
        "boolean" | "bool" => return CanonicalType::Boolean,
        "json" => return CanonicalType::Json,
        "" => return CanonicalType::Text, // No declared type
        _ => {}
    }

    // Affinity rules for unknown types
    let upper = udt.to_uppercase();
    if upper.contains("INT") {
        CanonicalType::Integer
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        CanonicalType::Text
    } else if upper.contains("BLOB") {
        CanonicalType::Bytes { length: None }
    } else if upper.contains("DOUB") {
        CanonicalType::Double
    } else if upper.contains("REAL") || upper.contains("FLOA") {
        CanonicalType::Float
    } else {
        CanonicalType::Decimal {
            precision: None,
            scale: None,
        }
    }
}

/// Emit a canonical type as SQLite DDL.
pub fn from_canonical(ct: &CanonicalType) -> DdlType {
    match ct {
        CanonicalType::Boolean => DdlType::exact("BOOLEAN"),
        CanonicalType::SmallInt => DdlType::exact("INTEGER"),
        CanonicalType::Integer => DdlType::exact("INTEGER"),
        CanonicalType::BigInt => DdlType::exact("INTEGER"),
        CanonicalType::Float | CanonicalType::Double => DdlType::exact("REAL"),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: Some(s),
        } => DdlType::exact(&format!("NUMERIC({p}, {s})")),
        CanonicalType::Decimal { .. } => DdlType::exact("NUMERIC"),
        CanonicalType::Varchar { length: Some(n) } => DdlType::exact(&format!("VARCHAR({n})")),
        CanonicalType::Varchar { length: None } => DdlType::exact("TEXT"),
        CanonicalType::Char { length: Some(n) } => DdlType::exact(&format!("CHAR({n})")),
        CanonicalType::Char { length: None } => DdlType::exact("TEXT"),
        CanonicalType::Text => DdlType::exact("TEXT"),
        CanonicalType::Bytes { .. } => DdlType::exact("BLOB"),
        CanonicalType::Date => DdlType::exact("DATE"),
        CanonicalType::Time { .. } => DdlType::exact("TIME"),
        CanonicalType::Timestamp { .. } => DdlType::exact("DATETIME"),
        CanonicalType::Interval => DdlType::approx("TEXT", "No INTERVAL type in SQLite"),
        CanonicalType::Uuid => DdlType::exact("TEXT"),
        CanonicalType::Json | CanonicalType::Jsonb => DdlType::exact("TEXT"),
        CanonicalType::Enum { .. } => {
            DdlType::approx("TEXT", "No ENUM type in SQLite; consider CHECK constraint")
        }
        CanonicalType::Set { .. } => {
            DdlType::approx("TEXT", "No SET type in SQLite; multi-value semantic lost")
        }
        CanonicalType::Array { .. } => {
            DdlType::approx("TEXT", "No array type in SQLite; using TEXT (JSON)")
        }
        CanonicalType::Raw { type_name } => DdlType::exact(type_name),
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
