use crate::schema::ColumnInfo;

use super::{simple, MappedType};

/// Map a PostgreSQL column to its SQLAlchemy type representation.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let udt = col.udt_name.as_str();

    // Handle array types (udt_name starts with underscore)
    if let Some(element_udt) = udt.strip_prefix('_') {
        let element = map_udt_scalar(element_udt, col);
        return MappedType {
            sa_type: format!("ARRAY({})", element.sa_type),
            python_type: "list".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "ARRAY".to_string(),
            element_import: Some((element.import_module, element.import_name)),
        };
    }

    map_udt_scalar(udt, col)
}

fn map_udt_scalar(udt: &str, col: &ColumnInfo) -> MappedType {
    match udt {
        "bool" => simple("Boolean", "bool", "sqlalchemy"),
        "int2" => simple("SmallInteger", "int", "sqlalchemy"),
        "int4" | "serial" => simple("Integer", "int", "sqlalchemy"),
        "int8" | "bigserial" => simple("BigInteger", "int", "sqlalchemy"),
        "float4" => simple("Float", "float", "sqlalchemy"),
        "float8" => simple("Double", "float", "sqlalchemy"),
        "numeric" => {
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
        "text" => simple("Text", "str", "sqlalchemy"),
        "varchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "char" | "bpchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("String({n})"),
                None => "String".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: "sqlalchemy".to_string(),
                import_name: "String".to_string(),
                element_import: None,
            }
        }
        "bytea" => simple("LargeBinary", "bytes", "sqlalchemy"),
        "timestamp" => simple("DateTime", "datetime.datetime", "sqlalchemy"),
        "timestamptz" => MappedType {
            sa_type: "DateTime(timezone=True)".to_string(),
            python_type: "datetime.datetime".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "DateTime".to_string(),
            element_import: None,
        },
        "date" => simple("Date", "datetime.date", "sqlalchemy"),
        "time" => simple("Time", "datetime.time", "sqlalchemy"),
        "timetz" => MappedType {
            sa_type: "Time(timezone=True)".to_string(),
            python_type: "datetime.time".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: "Time".to_string(),
            element_import: None,
        },
        "interval" => simple("Interval", "datetime.timedelta", "sqlalchemy"),
        "uuid" => simple("UUID", "uuid.UUID", "sqlalchemy.dialects.postgresql"),
        "json" => simple("JSON", "dict", "sqlalchemy.dialects.postgresql"),
        "jsonb" => simple("JSONB", "dict", "sqlalchemy.dialects.postgresql"),
        "inet" => simple("INET", "str", "sqlalchemy.dialects.postgresql"),
        "cidr" => simple("CIDR", "str", "sqlalchemy.dialects.postgresql"),
        // Fallback: use the udt_name as-is, uppercased
        other => MappedType {
            sa_type: other.to_uppercase(),
            python_type: "str".to_string(),
            import_module: "sqlalchemy".to_string(),
            import_name: other.to_uppercase(),
            element_import: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::test_column;

    fn col(udt_name: &str) -> ColumnInfo {
        ColumnInfo {
            udt_name: udt_name.to_string(),
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
    fn test_bool() {
        let m = map_column_type(&col("bool"));
        assert_eq!(m.sa_type, "Boolean");
        assert_eq!(m.python_type, "bool");
    }

    #[test]
    fn test_integer_types() {
        assert_eq!(map_column_type(&col("int2")).sa_type, "SmallInteger");
        assert_eq!(map_column_type(&col("int4")).sa_type, "Integer");
        assert_eq!(map_column_type(&col("int8")).sa_type, "BigInteger");
        assert_eq!(map_column_type(&col("serial")).sa_type, "Integer");
        assert_eq!(map_column_type(&col("bigserial")).sa_type, "BigInteger");
    }

    #[test]
    fn test_float_types() {
        assert_eq!(map_column_type(&col("float4")).sa_type, "Float");
        assert_eq!(map_column_type(&col("float8")).sa_type, "Double");
    }

    #[test]
    fn test_numeric_with_precision() {
        let m = map_column_type(&col_with_precision("numeric", 10, 2));
        assert_eq!(m.sa_type, "Numeric(10, 2)");
        assert_eq!(m.python_type, "decimal.Decimal");
    }

    #[test]
    fn test_string_types() {
        assert_eq!(map_column_type(&col("text")).sa_type, "Text");
        assert_eq!(
            map_column_type(&col_with_length("varchar", 100)).sa_type,
            "String(100)"
        );
        assert_eq!(
            map_column_type(&col_with_length("bpchar", 10)).sa_type,
            "String(10)"
        );
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(map_column_type(&col("timestamp")).sa_type, "DateTime");
        assert_eq!(
            map_column_type(&col("timestamptz")).sa_type,
            "DateTime(timezone=True)"
        );
        assert_eq!(map_column_type(&col("date")).sa_type, "Date");
        assert_eq!(map_column_type(&col("time")).sa_type, "Time");
        assert_eq!(
            map_column_type(&col("timetz")).sa_type,
            "Time(timezone=True)"
        );
    }

    #[test]
    fn test_dialect_types() {
        let m = map_column_type(&col("uuid"));
        assert_eq!(m.sa_type, "UUID");
        assert_eq!(m.import_module, "sqlalchemy.dialects.postgresql");

        assert_eq!(map_column_type(&col("jsonb")).sa_type, "JSONB");
        assert_eq!(map_column_type(&col("json")).sa_type, "JSON");
        assert_eq!(map_column_type(&col("inet")).sa_type, "INET");
        assert_eq!(map_column_type(&col("cidr")).sa_type, "CIDR");
    }

    #[test]
    fn test_array_type() {
        let m = map_column_type(&col("_int4"));
        assert_eq!(m.sa_type, "ARRAY(Integer)");
        assert_eq!(m.import_name, "ARRAY");
        assert_eq!(
            m.element_import,
            Some(("sqlalchemy".to_string(), "Integer".to_string()))
        );

        let m2 = map_column_type(&col("_text"));
        assert_eq!(m2.sa_type, "ARRAY(Text)");
    }

    #[test]
    fn test_bytea() {
        let m = map_column_type(&col("bytea"));
        assert_eq!(m.sa_type, "LargeBinary");
        assert_eq!(m.python_type, "bytes");
    }

    #[test]
    fn test_interval() {
        let m = map_column_type(&col("interval"));
        assert_eq!(m.sa_type, "Interval");
        assert_eq!(m.python_type, "datetime.timedelta");
    }
}
