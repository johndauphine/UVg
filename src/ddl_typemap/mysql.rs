use crate::schema::ColumnInfo;

use super::{CanonicalType, DdlType};

/// Check if a MySQL tinyint column has display width 1 (boolean).
fn is_tinyint_bool(col: &ColumnInfo) -> bool {
    col.udt_name == "tinyint" && col.data_type.starts_with("tinyint(1)")
}

/// Parse ENUM or SET values from a COLUMN_TYPE string like "enum('a','b','c')".
fn parse_values(column_type: &str) -> Vec<String> {
    let start = match column_type.find('(') {
        Some(i) => i + 1,
        None => return vec![],
    };
    let end = match column_type.rfind(')') {
        Some(i) => i,
        None => return vec![],
    };
    if start >= end {
        return vec![];
    }

    let inner = &column_type[start..end];
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let bytes = inner.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if !in_quote {
            if bytes[i] == b'\'' {
                in_quote = true;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                current.push('\'');
                i += 2;
            } else {
                in_quote = false;
                values.push(current.clone());
                current.clear();
                i += 1;
            }
        } else {
            current.push(bytes[i] as char);
            i += 1;
        }
    }

    values
}

/// Parse the sub-second precision from a MySQL temporal column type string.
/// `column_type` is the full COLUMN_TYPE field — e.g. `datetime(6)`,
/// `timestamp(3)`, `time`. Returns `Some(N)` for `(N)` where N is 0-6,
/// `None` for "no precision specified" (which MySQL stores as 0 implicitly,
/// but we preserve the round-trip distinction). See #36.
fn parse_temporal_precision(column_type: &str) -> Option<u8> {
    let start = column_type.find('(')?;
    let end = column_type.rfind(')')?;
    if start >= end {
        return None;
    }
    let inside = column_type[start + 1..end].trim();
    let n = inside.parse::<u8>().ok()?;
    if n <= 6 {
        Some(n)
    } else {
        None
    }
}

/// Normalize a MySQL column type to canonical form.
pub fn to_canonical(col: &ColumnInfo) -> CanonicalType {
    let udt = col.udt_name.as_str();

    match udt {
        "tinyint" if is_tinyint_bool(col) => CanonicalType::Boolean,
        "tinyint" | "smallint" => CanonicalType::SmallInt,
        "mediumint" | "int" => CanonicalType::Integer,
        "bigint" => CanonicalType::BigInt,
        "float" => CanonicalType::Float,
        "double" => CanonicalType::Double,
        "decimal" | "numeric" => CanonicalType::Decimal {
            precision: col.numeric_precision,
            scale: col.numeric_scale,
        },
        "varchar" => CanonicalType::Varchar {
            length: col.character_maximum_length,
        },
        "char" => CanonicalType::Char {
            length: col.character_maximum_length,
        },
        "text" | "tinytext" | "mediumtext" | "longtext" => CanonicalType::Text,
        "binary" | "varbinary" => CanonicalType::Bytes {
            length: col.character_maximum_length,
        },
        "blob" | "tinyblob" | "mediumblob" | "longblob" => CanonicalType::Bytes { length: None },
        "date" => CanonicalType::Date,
        "time" => CanonicalType::Time {
            with_tz: false,
            precision: parse_temporal_precision(&col.data_type),
        },
        "datetime" | "timestamp" => CanonicalType::Timestamp {
            with_tz: false,
            precision: parse_temporal_precision(&col.data_type),
        },
        "year" => CanonicalType::SmallInt,
        "json" => CanonicalType::Json,
        "enum" => {
            let values = parse_values(&col.data_type);
            CanonicalType::Enum { values }
        }
        "set" => {
            // SET shares parse_values with ENUM — both store comma-separated
            // single-quoted literals inside the type's parens. First-class
            // CanonicalType::Set lets non-mysql targets emit a sized VARCHAR
            // fallback (#38) instead of leaking the verbatim `SET(...)` text.
            let values = parse_values(&col.data_type);
            CanonicalType::Set { values }
        }
        "bit" => {
            // BIT(1) is boolean; BIT(n) preserves width
            if col.numeric_precision.unwrap_or(1) == 1 {
                CanonicalType::Boolean
            } else {
                CanonicalType::Raw {
                    type_name: col.data_type.to_uppercase(),
                }
            }
        }
        "boolean" | "bool" => CanonicalType::Boolean,
        _ => CanonicalType::Raw {
            type_name: udt.to_uppercase(),
        },
    }
}

/// Emit a canonical type as MySQL DDL.
pub fn from_canonical(ct: &CanonicalType) -> DdlType {
    match ct {
        CanonicalType::Boolean => DdlType::exact("TINYINT(1)"),
        CanonicalType::SmallInt => DdlType::exact("SMALLINT"),
        CanonicalType::Integer => DdlType::exact("INT"),
        CanonicalType::BigInt => DdlType::exact("BIGINT"),
        CanonicalType::Float => DdlType::exact("FLOAT"),
        CanonicalType::Double => DdlType::exact("DOUBLE"),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: Some(s),
        } => DdlType::exact(&format!("DECIMAL({p}, {s})")),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: None,
        } => DdlType::exact(&format!("DECIMAL({p})")),
        CanonicalType::Decimal { .. } => DdlType::exact("DECIMAL"),
        CanonicalType::Varchar { length: Some(n) } => DdlType::exact(&format!("VARCHAR({n})")),
        CanonicalType::Varchar { length: None } => DdlType::exact("VARCHAR(255)"),
        CanonicalType::Char { length: Some(n) } => DdlType::exact(&format!("CHAR({n})")),
        CanonicalType::Char { length: None } => DdlType::exact("CHAR(1)"),
        CanonicalType::Text => DdlType::exact("TEXT"),
        CanonicalType::Bytes { length: Some(n) } => DdlType::exact(&format!("VARBINARY({n})")),
        CanonicalType::Bytes { length: None } => DdlType::exact("BLOB"),
        CanonicalType::Date => DdlType::exact("DATE"),
        CanonicalType::Time {
            precision: Some(p), ..
        } => DdlType::exact(&format!("TIME({p})")),
        CanonicalType::Time {
            precision: None, ..
        } => DdlType::exact("TIME"),
        CanonicalType::Timestamp {
            precision: Some(p), ..
        } => DdlType::exact(&format!("DATETIME({p})")),
        CanonicalType::Timestamp {
            precision: None, ..
        } => DdlType::exact("DATETIME"),
        CanonicalType::Interval => DdlType::approx("VARCHAR(255)", "No INTERVAL type in MySQL"),
        CanonicalType::Uuid => DdlType::exact("CHAR(36)"),
        CanonicalType::Json => DdlType::exact("JSON"),
        CanonicalType::Jsonb => {
            DdlType::approx("JSON", "JSONB binary indexing not available in MySQL")
        }
        CanonicalType::Enum { values } => {
            let quoted: Vec<String> = values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            DdlType::exact(&format!("ENUM({})", quoted.join(", ")))
        }
        CanonicalType::Set { values } => {
            let quoted: Vec<String> = values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            DdlType::exact(&format!("SET({})", quoted.join(", ")))
        }
        CanonicalType::Array { .. } => {
            DdlType::approx("JSON", "No array type in MySQL; using JSON")
        }
        CanonicalType::Raw { type_name } => DdlType::exact(type_name),
    }
}

#[cfg(test)]
#[path = "mysql_tests.rs"]
mod tests;
