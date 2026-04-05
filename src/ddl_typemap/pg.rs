use crate::schema::ColumnInfo;

use super::{CanonicalType, DdlType};

/// Normalize a PostgreSQL column type to canonical form.
pub fn to_canonical(col: &ColumnInfo) -> CanonicalType {
    let udt = col.udt_name.as_str();

    // Handle array types (udt_name starts with '_')
    if let Some(element_udt) = udt.strip_prefix('_') {
        let mut element_col = col.clone();
        element_col.udt_name = element_udt.to_string();
        let element = to_canonical(&element_col);
        return CanonicalType::Array {
            element: Box::new(element),
        };
    }

    match udt {
        "bool" => CanonicalType::Boolean,
        "int2" | "smallserial" => CanonicalType::SmallInt,
        "int4" | "serial" => CanonicalType::Integer,
        "int8" | "bigserial" => CanonicalType::BigInt,
        "float4" | "real" => CanonicalType::Float,
        "float8" | "double precision" => CanonicalType::Double,
        "numeric" | "decimal" => CanonicalType::Decimal {
            precision: col.numeric_precision,
            scale: col.numeric_scale,
        },
        "varchar" | "character varying" => CanonicalType::Varchar {
            length: col.character_maximum_length,
        },
        "char" | "character" | "bpchar" => CanonicalType::Char {
            length: col.character_maximum_length,
        },
        "text" => CanonicalType::Text,
        "bytea" => CanonicalType::Bytes { length: None },
        "date" => CanonicalType::Date,
        "time" | "time without time zone" => CanonicalType::Time { with_tz: false },
        "timetz" | "time with time zone" => CanonicalType::Time { with_tz: true },
        "timestamp" | "timestamp without time zone" => {
            CanonicalType::Timestamp { with_tz: false }
        }
        "timestamptz" | "timestamp with time zone" => CanonicalType::Timestamp { with_tz: true },
        "interval" => CanonicalType::Interval,
        "uuid" => CanonicalType::Uuid,
        "json" => CanonicalType::Json,
        "jsonb" => CanonicalType::Jsonb,
        "inet" | "cidr" | "macaddr" => CanonicalType::Raw {
            type_name: udt.to_uppercase(),
        },
        _ => CanonicalType::Raw {
            type_name: udt.to_uppercase(),
        },
    }
}

/// Emit a canonical type as PostgreSQL DDL.
pub fn from_canonical(ct: &CanonicalType) -> DdlType {
    match ct {
        CanonicalType::Boolean => DdlType::exact("BOOLEAN"),
        CanonicalType::SmallInt => DdlType::exact("SMALLINT"),
        CanonicalType::Integer => DdlType::exact("INTEGER"),
        CanonicalType::BigInt => DdlType::exact("BIGINT"),
        CanonicalType::Float => DdlType::exact("REAL"),
        CanonicalType::Double => DdlType::exact("DOUBLE PRECISION"),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: Some(s),
        } => DdlType::exact(&format!("NUMERIC({p}, {s})")),
        CanonicalType::Decimal {
            precision: Some(p),
            scale: None,
        } => DdlType::exact(&format!("NUMERIC({p})")),
        CanonicalType::Decimal { .. } => DdlType::exact("NUMERIC"),
        CanonicalType::Varchar { length: Some(n) } => DdlType::exact(&format!("VARCHAR({n})")),
        CanonicalType::Varchar { length: None } => DdlType::exact("VARCHAR"),
        CanonicalType::Char { length: Some(n) } => DdlType::exact(&format!("CHAR({n})")),
        CanonicalType::Char { length: None } => DdlType::exact("CHAR"),
        CanonicalType::Text => DdlType::exact("TEXT"),
        CanonicalType::Bytes { .. } => DdlType::exact("BYTEA"),
        CanonicalType::Date => DdlType::exact("DATE"),
        CanonicalType::Time { with_tz: false } => DdlType::exact("TIME"),
        CanonicalType::Time { with_tz: true } => DdlType::exact("TIME WITH TIME ZONE"),
        CanonicalType::Timestamp { with_tz: false } => DdlType::exact("TIMESTAMP"),
        CanonicalType::Timestamp { with_tz: true } => {
            DdlType::exact("TIMESTAMP WITH TIME ZONE")
        }
        CanonicalType::Interval => DdlType::exact("INTERVAL"),
        CanonicalType::Uuid => DdlType::exact("UUID"),
        CanonicalType::Json => DdlType::exact("JSON"),
        CanonicalType::Jsonb => DdlType::exact("JSONB"),
        CanonicalType::Enum { .. } => {
            // Enum CREATE TYPE is handled separately; column uses the type name.
            // For cross-dialect, fall back to VARCHAR.
            DdlType::approx("VARCHAR(255)", "Enum mapped to VARCHAR; use CREATE TYPE for native PG enum")
        }
        CanonicalType::Array { element } => {
            let inner = from_canonical(element);
            DdlType::exact(&format!("{}[]", inner.sql_type))
        }
        CanonicalType::Raw { type_name } => DdlType::exact(type_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::col;

    #[test]
    fn test_pg_int4() {
        let c = col("id").udt("int4").build();
        let ct = to_canonical(&c);
        assert_eq!(ct, CanonicalType::Integer);
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "INTEGER");
    }

    #[test]
    fn test_pg_varchar_with_length() {
        let mut c = col("name").udt("varchar").build();
        c.character_maximum_length = Some(255);
        let ct = to_canonical(&c);
        assert_eq!(ct, CanonicalType::Varchar { length: Some(255) });
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "VARCHAR(255)");
    }

    #[test]
    fn test_pg_timestamptz() {
        let c = col("ts").udt("timestamptz").build();
        let ct = to_canonical(&c);
        assert_eq!(ct, CanonicalType::Timestamp { with_tz: true });
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "TIMESTAMP WITH TIME ZONE");
    }

    #[test]
    fn test_pg_array() {
        let c = col("tags").udt("_text").build();
        let ct = to_canonical(&c);
        assert!(matches!(ct, CanonicalType::Array { .. }));
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "TEXT[]");
    }

    #[test]
    fn test_pg_uuid() {
        let c = col("uid").udt("uuid").build();
        let ct = to_canonical(&c);
        assert_eq!(ct, CanonicalType::Uuid);
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "UUID");
    }

    #[test]
    fn test_pg_jsonb() {
        let c = col("data").udt("jsonb").build();
        let ct = to_canonical(&c);
        assert_eq!(ct, CanonicalType::Jsonb);
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "JSONB");
    }

    #[test]
    fn test_pg_numeric() {
        let mut c = col("price").udt("numeric").build();
        c.numeric_precision = Some(10);
        c.numeric_scale = Some(2);
        let ct = to_canonical(&c);
        assert_eq!(
            ct,
            CanonicalType::Decimal {
                precision: Some(10),
                scale: Some(2)
            }
        );
        let dt = from_canonical(&ct);
        assert_eq!(dt.sql_type, "NUMERIC(10, 2)");
    }
}
