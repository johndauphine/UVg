use crate::schema::ColumnInfo;

use super::{CanonicalType, DdlType};

/// Check if a MySQL tinyint column has display width 1 (boolean).
fn is_tinyint_bool(col: &ColumnInfo) -> bool {
    col.udt_name == "tinyint" && col.data_type.starts_with("tinyint(1)")
}

/// Check if a MySQL COLUMN_TYPE indicates unsigned.
fn is_unsigned(col: &ColumnInfo) -> bool {
    col.data_type.contains("unsigned")
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
        "time" => CanonicalType::Time { with_tz: false },
        "datetime" => CanonicalType::Timestamp { with_tz: false },
        "timestamp" => CanonicalType::Timestamp { with_tz: false },
        "year" => CanonicalType::SmallInt,
        "json" => CanonicalType::Json,
        "enum" => {
            let values = parse_values(&col.data_type);
            CanonicalType::Enum { values }
        }
        "set" => CanonicalType::Raw {
            type_name: col.data_type.to_uppercase(),
        },
        "bit" => CanonicalType::Boolean,
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
        CanonicalType::Time { .. } => DdlType::exact("TIME"),
        CanonicalType::Timestamp { .. } => DdlType::exact("DATETIME"),
        CanonicalType::Interval => {
            DdlType::approx("VARCHAR(255)", "No INTERVAL type in MySQL")
        }
        CanonicalType::Uuid => DdlType::exact("CHAR(36)"),
        CanonicalType::Json => DdlType::exact("JSON"),
        CanonicalType::Jsonb => DdlType::approx("JSON", "JSONB binary indexing not available in MySQL"),
        CanonicalType::Enum { values } => {
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            DdlType::exact(&format!("ENUM({})", quoted.join(", ")))
        }
        CanonicalType::Array { .. } => {
            DdlType::approx("JSON", "No array type in MySQL; using JSON")
        }
        CanonicalType::Raw { type_name } => DdlType::exact(type_name),
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(to_canonical(&c), CanonicalType::Boolean);
    }

    #[test]
    fn test_int() {
        let c = mysql_col("int", "int");
        assert_eq!(to_canonical(&c), CanonicalType::Integer);
        assert_eq!(from_canonical(&CanonicalType::Integer).sql_type, "INT");
    }

    #[test]
    fn test_enum() {
        let c = mysql_col("enum", "enum('a','b','c')");
        let ct = to_canonical(&c);
        assert!(matches!(ct, CanonicalType::Enum { ref values } if values == &["a", "b", "c"]));
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "ENUM('a', 'b', 'c')");
    }

    #[test]
    fn test_json_to_mysql() {
        let dt = from_canonical(&CanonicalType::Json);
        assert_eq!(dt.sql_type, "JSON");
    }

    #[test]
    fn test_uuid_to_mysql() {
        let dt = from_canonical(&CanonicalType::Uuid);
        assert_eq!(dt.sql_type, "CHAR(36)");
    }

    #[test]
    fn test_jsonb_to_mysql() {
        let dt = from_canonical(&CanonicalType::Jsonb);
        assert_eq!(dt.sql_type, "JSON");
    }

    #[test]
    fn test_interval_to_mysql() {
        let dt = from_canonical(&CanonicalType::Interval);
        assert!(dt.is_approximate);
    }

    #[test]
    fn test_array_to_mysql() {
        let dt = from_canonical(&CanonicalType::Array {
            element: Box::new(CanonicalType::Integer),
        });
        assert_eq!(dt.sql_type, "JSON");
        assert!(dt.is_approximate);
    }
}
