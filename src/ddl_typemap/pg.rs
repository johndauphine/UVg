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
        "time" | "time without time zone" => CanonicalType::Time {
            with_tz: false,
            precision: None,
        },
        "timetz" | "time with time zone" => CanonicalType::Time {
            with_tz: true,
            precision: None,
        },
        "timestamp" | "timestamp without time zone" => CanonicalType::Timestamp {
            with_tz: false,
            precision: None,
        },
        "timestamptz" | "timestamp with time zone" => CanonicalType::Timestamp {
            with_tz: true,
            precision: None,
        },
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
        // PG-side Time/Timestamp emission ignores the canonical precision —
        // PG accepts TIMESTAMP(N) but the precision rarely round-trips
        // meaningfully across dialects (mysql sub-second != pg sub-second
        // semantics on conversion). Drop the precision on emission; the type
        // is still semantically correct.
        CanonicalType::Time { with_tz: false, .. } => DdlType::exact("TIME"),
        CanonicalType::Time { with_tz: true, .. } => DdlType::exact("TIME WITH TIME ZONE"),
        CanonicalType::Timestamp { with_tz: false, .. } => DdlType::exact("TIMESTAMP"),
        CanonicalType::Timestamp { with_tz: true, .. } => {
            DdlType::exact("TIMESTAMP WITH TIME ZONE")
        }
        CanonicalType::Interval => DdlType::exact("INTERVAL"),
        CanonicalType::Uuid => DdlType::exact("UUID"),
        CanonicalType::Json => DdlType::exact("JSON"),
        CanonicalType::Jsonb => DdlType::exact("JSONB"),
        CanonicalType::Enum { .. } => {
            // Enum CREATE TYPE is handled separately; column uses the type name.
            // For cross-dialect, fall back to VARCHAR.
            DdlType::approx(
                "VARCHAR(255)",
                "Enum mapped to VARCHAR; use CREATE TYPE for native PG enum",
            )
        }
        CanonicalType::Set { values } => {
            // PG has no SET type. Fall back to VARCHAR sized to fit the
            // comma-joined value list (worst case: every value present at
            // once). Loses the value-set semantic but at least lets the
            // column accept anything the source could produce. See #38.
            DdlType::approx(
                &format!("VARCHAR({})", set_varchar_capacity(values)),
                "MySQL SET mapped to VARCHAR; multi-value semantic lost",
            )
        }
        CanonicalType::Array { element } => {
            let inner = from_canonical(element);
            let mut ddl = DdlType::exact(&format!("{}[]", inner.sql_type));
            ddl.is_approximate = inner.is_approximate;
            ddl.warning = inner
                .warning
                .map(|warning| format!("array element: {warning}"));
            ddl
        }
        CanonicalType::Raw { type_name } => DdlType::exact(type_name),
    }
}

/// Compute a VARCHAR width sized to fit any concatenation of MySQL SET
/// values: sum of all value lengths plus a comma between each pair.
/// Conservative — accommodates the worst-case "all values selected" row.
/// Used by the PG/MSSQL/SQLite fallback for SET, which has no native form.
pub(super) fn set_varchar_capacity(values: &[String]) -> usize {
    if values.is_empty() {
        return 1;
    }
    let total_chars: usize = values.iter().map(|v| v.chars().count()).sum();
    let separators = values.len().saturating_sub(1);
    let computed = total_chars + separators;
    // Floor at 255 — uvg's existing Enum fallback uses VARCHAR(255), and
    // small sizes look surprising in DDL even when literally correct.
    computed.max(255)
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
        assert_eq!(
            ct,
            CanonicalType::Timestamp {
                with_tz: true,
                precision: None
            }
        );
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
    fn test_pg_set_fallback() {
        // #38 — MySQL SET has no PG equivalent. Falls back to VARCHAR sized
        // to fit the worst-case comma-joined value list (with a 255 floor).
        let ct = CanonicalType::Set {
            values: vec!["a".into(), "b".into(), "c".into()],
        };
        let dt = from_canonical(&ct);
        assert!(dt.sql_type.starts_with("VARCHAR("), "got {}", dt.sql_type);
        assert!(dt.is_approximate);
        assert!(dt.warning.as_deref().unwrap().contains("multi-value"));
    }

    #[test]
    fn test_set_varchar_capacity() {
        use super::set_varchar_capacity;
        // 4 values × 6 chars + 3 separators = 27 — but the 255 floor wins.
        let v: Vec<String> = ["billing", "shipping", "mailing", "phys24"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(set_varchar_capacity(&v), 255);
        // Edge case: empty list returns 1 (defensive — shouldn't happen
        // since SET requires at least one value).
        assert_eq!(set_varchar_capacity(&[]), 1);
        // Above the 255 floor: long values force a larger column.
        let big = vec!["x".repeat(300)];
        assert_eq!(set_varchar_capacity(&big), 300);
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
