use crate::schema::ColumnInfo;

use super::{CanonicalType, DdlType};

/// Normalize a MSSQL column type to canonical form.
pub fn to_canonical(col: &ColumnInfo) -> CanonicalType {
    let udt = col.udt_name.as_str();

    match udt {
        "bit" => CanonicalType::Boolean,
        "tinyint" => CanonicalType::SmallInt,
        "smallint" => CanonicalType::SmallInt,
        "int" => CanonicalType::Integer,
        "bigint" => CanonicalType::BigInt,
        "real" => CanonicalType::Float,
        "float" => CanonicalType::Double,
        "decimal" | "numeric" => CanonicalType::Decimal {
            precision: col.numeric_precision,
            scale: col.numeric_scale,
        },
        "money" => CanonicalType::Decimal {
            precision: Some(19),
            scale: Some(4),
        },
        "smallmoney" => CanonicalType::Decimal {
            precision: Some(10),
            scale: Some(4),
        },
        "varchar" => CanonicalType::Varchar {
            length: col.character_maximum_length,
        },
        "char" => CanonicalType::Char {
            length: col.character_maximum_length,
        },
        "nvarchar" => CanonicalType::Varchar {
            length: col.character_maximum_length,
        },
        "nchar" => CanonicalType::Char {
            length: col.character_maximum_length,
        },
        "text" | "ntext" => CanonicalType::Text,
        "binary" | "varbinary" | "image" => CanonicalType::Bytes {
            length: col.character_maximum_length,
        },
        "date" => CanonicalType::Date,
        "time" => CanonicalType::Time { with_tz: false },
        "datetime" | "datetime2" | "smalldatetime" => {
            CanonicalType::Timestamp { with_tz: false }
        }
        "datetimeoffset" => CanonicalType::Timestamp { with_tz: true },
        "uniqueidentifier" => CanonicalType::Uuid,
        _ => CanonicalType::Raw {
            type_name: udt.to_uppercase(),
        },
    }
}

/// Emit a canonical type as MSSQL DDL.
pub fn from_canonical(ct: &CanonicalType) -> DdlType {
    match ct {
        CanonicalType::Boolean => DdlType::exact("BIT"),
        CanonicalType::SmallInt => DdlType::exact("SMALLINT"),
        CanonicalType::Integer => DdlType::exact("INT"),
        CanonicalType::BigInt => DdlType::exact("BIGINT"),
        CanonicalType::Float => DdlType::exact("REAL"),
        CanonicalType::Double => DdlType::exact("FLOAT"),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: Some(s),
        } => DdlType::exact(&format!("DECIMAL({p}, {s})")),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: None,
        } => DdlType::exact(&format!("DECIMAL({p})")),
        CanonicalType::Decimal { .. } => DdlType::exact("DECIMAL"),
        CanonicalType::Varchar { length: Some(n) } => {
            DdlType::exact(&format!("NVARCHAR({n})"))
        }
        CanonicalType::Varchar { length: None } => DdlType::exact("NVARCHAR(MAX)"),
        CanonicalType::Char { length: Some(n) } => DdlType::exact(&format!("NCHAR({n})")),
        CanonicalType::Char { length: None } => DdlType::exact("NCHAR(1)"),
        CanonicalType::Text => DdlType::exact("NVARCHAR(MAX)"),
        CanonicalType::Bytes { length: Some(n) } => {
            DdlType::exact(&format!("VARBINARY({n})"))
        }
        CanonicalType::Bytes { length: None } => DdlType::exact("VARBINARY(MAX)"),
        CanonicalType::Date => DdlType::exact("DATE"),
        CanonicalType::Time { .. } => DdlType::exact("TIME"),
        CanonicalType::Timestamp { with_tz: false } => DdlType::exact("DATETIME2"),
        CanonicalType::Timestamp { with_tz: true } => DdlType::exact("DATETIMEOFFSET"),
        CanonicalType::Interval => {
            DdlType::approx("NVARCHAR(255)", "No INTERVAL type in MSSQL")
        }
        CanonicalType::Uuid => DdlType::exact("UNIQUEIDENTIFIER"),
        CanonicalType::Json | CanonicalType::Jsonb => {
            DdlType::approx("NVARCHAR(MAX)", "No native JSON type in MSSQL; using NVARCHAR(MAX)")
        }
        CanonicalType::Enum { .. } => {
            DdlType::approx("NVARCHAR(255)", "No ENUM type in MSSQL; consider CHECK constraint")
        }
        CanonicalType::Array { .. } => {
            DdlType::approx("NVARCHAR(MAX)", "No array type in MSSQL; using NVARCHAR(MAX)")
        }
        CanonicalType::Raw { type_name } => DdlType::exact(type_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::col;

    #[test]
    fn test_mssql_int() {
        let c = col("id").udt("int").build();
        assert_eq!(to_canonical(&c), CanonicalType::Integer);
    }

    #[test]
    fn test_mssql_uniqueidentifier() {
        let c = col("uid").udt("uniqueidentifier").build();
        assert_eq!(to_canonical(&c), CanonicalType::Uuid);
        assert_eq!(from_canonical(&CanonicalType::Uuid).sql_type, "UNIQUEIDENTIFIER");
    }

    #[test]
    fn test_mssql_money() {
        let c = col("amount").udt("money").build();
        let ct = to_canonical(&c);
        assert_eq!(
            ct,
            CanonicalType::Decimal {
                precision: Some(19),
                scale: Some(4)
            }
        );
    }

    #[test]
    fn test_mssql_datetimeoffset() {
        let c = col("ts").udt("datetimeoffset").build();
        assert_eq!(to_canonical(&c), CanonicalType::Timestamp { with_tz: true });
        assert_eq!(
            from_canonical(&CanonicalType::Timestamp { with_tz: true }).sql_type,
            "DATETIMEOFFSET"
        );
    }

    #[test]
    fn test_mssql_bit() {
        let c = col("flag").udt("bit").build();
        assert_eq!(to_canonical(&c), CanonicalType::Boolean);
        assert_eq!(from_canonical(&CanonicalType::Boolean).sql_type, "BIT");
    }

    #[test]
    fn test_mssql_text_to_nvarchar_max() {
        assert_eq!(from_canonical(&CanonicalType::Text).sql_type, "NVARCHAR(MAX)");
    }

    #[test]
    fn test_json_to_mssql() {
        let dt = from_canonical(&CanonicalType::Json);
        assert_eq!(dt.sql_type, "NVARCHAR(MAX)");
        assert!(dt.is_approximate);
    }
}
