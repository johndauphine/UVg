use crate::schema::ColumnInfo;

use super::{simple, MappedType};

const SA: &str = "sqlalchemy";
const MY: &str = "sqlalchemy.dialects.mysql";

/// Check if a MySQL COLUMN_TYPE indicates unsigned.
fn is_unsigned(col: &ColumnInfo) -> bool {
    col.data_type.contains("unsigned")
}

/// Check if a MySQL tinyint column has display width 1 (boolean).
fn is_tinyint_bool(col: &ColumnInfo) -> bool {
    col.udt_name == "tinyint" && col.data_type.starts_with("tinyint(1)")
}

/// Parse ENUM or SET values from a COLUMN_TYPE string like "enum('a','b','c')".
fn parse_enum_set_values(column_type: &str) -> Vec<String> {
    // Find the opening paren after "enum" or "set"
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
            // Lookahead for escaped quote ''
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

/// Map a MySQL column to generic SQLAlchemy types.
pub fn map_column_type(col: &ColumnInfo) -> MappedType {
    let dt = col.udt_name.as_str();

    match dt {
        "tinyint" if is_tinyint_bool(col) => simple("Boolean", "bool", SA),
        "tinyint" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "TINYINT(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "TINYINT".to_string(),
                    element_import: None,
                }
            } else {
                simple("TINYINT", "int", MY)
            }
        }
        "smallint" => simple("SmallInteger", "int", SA),
        "mediumint" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "MEDIUMINT(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "MEDIUMINT".to_string(),
                    element_import: None,
                }
            } else {
                simple("MEDIUMINT", "int", MY)
            }
        }
        "int" => simple("Integer", "int", SA),
        "bigint" => simple("BigInteger", "int", SA),
        "float" => simple("Float", "float", SA),
        "double" => simple("Double", "float", SA),
        "decimal" | "numeric" => {
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
        "varchar" => {
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
        "char" => {
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
        "text" => simple("Text", "str", SA),
        "tinytext" => simple("TINYTEXT", "str", MY),
        "mediumtext" => simple("MEDIUMTEXT", "str", MY),
        "longtext" => simple("LONGTEXT", "str", MY),
        "binary" | "varbinary" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("LargeBinary({n})"),
                None => "LargeBinary".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "bytes".to_string(),
                import_module: SA.to_string(),
                import_name: "LargeBinary".to_string(),
                element_import: None,
            }
        }
        "blob" => simple("LargeBinary", "bytes", SA),
        "tinyblob" => simple("TINYBLOB", "bytes", MY),
        "mediumblob" => simple("MEDIUMBLOB", "bytes", MY),
        "longblob" => simple("LONGBLOB", "bytes", MY),
        "date" => simple("Date", "datetime.date", SA),
        "time" => simple("Time", "datetime.time", SA),
        "datetime" => simple("DateTime", "datetime.datetime", SA),
        "timestamp" => simple("TIMESTAMP", "datetime.datetime", MY),
        "year" => simple("YEAR", "int", MY),
        "json" => simple("JSON", "dict", SA),
        "enum" => {
            let values = parse_enum_set_values(&col.data_type);
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            let sa_type = format!("Enum({})", quoted.join(", "));
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: SA.to_string(),
                import_name: "Enum".to_string(),
                element_import: None,
            }
        }
        "set" => {
            let values = parse_enum_set_values(&col.data_type);
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            let sa_type = format!("SET({})", quoted.join(", "));
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: MY.to_string(),
                import_name: "SET".to_string(),
                element_import: None,
            }
        }
        "bit" => simple("BIT", "int", MY),
        "boolean" | "bool" => simple("Boolean", "bool", SA),
        _ => {
            // Fallback: uppercase the type name
            let upper = dt.to_uppercase();
            simple(&upper, "str", SA)
        }
    }
}

/// Map a MySQL column keeping dialect-specific types from sqlalchemy.dialects.mysql.
pub fn map_column_type_dialect(col: &ColumnInfo) -> MappedType {
    let dt = col.udt_name.as_str();

    match dt {
        "tinyint" if is_tinyint_bool(col) => MappedType {
            sa_type: "TINYINT(display_width=1)".to_string(),
            python_type: "int".to_string(),
            import_module: MY.to_string(),
            import_name: "TINYINT".to_string(),
            element_import: None,
        },
        "tinyint" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "TINYINT(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "TINYINT".to_string(),
                    element_import: None,
                }
            } else {
                simple("TINYINT", "int", MY)
            }
        }
        "smallint" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "SMALLINT(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "SMALLINT".to_string(),
                    element_import: None,
                }
            } else {
                simple("SMALLINT", "int", MY)
            }
        }
        "mediumint" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "MEDIUMINT(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "MEDIUMINT".to_string(),
                    element_import: None,
                }
            } else {
                simple("MEDIUMINT", "int", MY)
            }
        }
        "int" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "INTEGER(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "INTEGER".to_string(),
                    element_import: None,
                }
            } else {
                simple("INTEGER", "int", MY)
            }
        }
        "bigint" => {
            if is_unsigned(col) {
                MappedType {
                    sa_type: "BIGINT(unsigned=True)".to_string(),
                    python_type: "int".to_string(),
                    import_module: MY.to_string(),
                    import_name: "BIGINT".to_string(),
                    element_import: None,
                }
            } else {
                simple("BIGINT", "int", MY)
            }
        }
        "float" => simple("FLOAT", "float", MY),
        "double" => simple("DOUBLE", "float", MY),
        "decimal" | "numeric" => {
            let sa_type = match (col.numeric_precision, col.numeric_scale) {
                (Some(p), Some(s)) => format!("DECIMAL({p}, {s})"),
                (Some(p), None) => format!("DECIMAL({p})"),
                _ => "DECIMAL".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "decimal.Decimal".to_string(),
                import_module: MY.to_string(),
                import_name: "DECIMAL".to_string(),
                element_import: None,
            }
        }
        "varchar" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("VARCHAR({n})"),
                None => "VARCHAR".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: MY.to_string(),
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
                import_module: MY.to_string(),
                import_name: "CHAR".to_string(),
                element_import: None,
            }
        }
        "text" => simple("TEXT", "str", MY),
        "tinytext" => simple("TINYTEXT", "str", MY),
        "mediumtext" => simple("MEDIUMTEXT", "str", MY),
        "longtext" => simple("LONGTEXT", "str", MY),
        "binary" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("BINARY({n})"),
                None => "BINARY".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "bytes".to_string(),
                import_module: MY.to_string(),
                import_name: "BINARY".to_string(),
                element_import: None,
            }
        }
        "varbinary" => {
            let sa_type = match col.character_maximum_length {
                Some(n) => format!("VARBINARY({n})"),
                None => "VARBINARY".to_string(),
            };
            MappedType {
                sa_type,
                python_type: "bytes".to_string(),
                import_module: MY.to_string(),
                import_name: "VARBINARY".to_string(),
                element_import: None,
            }
        }
        "blob" => simple("BLOB", "bytes", MY),
        "tinyblob" => simple("TINYBLOB", "bytes", MY),
        "mediumblob" => simple("MEDIUMBLOB", "bytes", MY),
        "longblob" => simple("LONGBLOB", "bytes", MY),
        "date" => simple("DATE", "datetime.date", MY),
        "time" => simple("TIME", "datetime.time", MY),
        "datetime" => simple("DATETIME", "datetime.datetime", MY),
        "timestamp" => simple("TIMESTAMP", "datetime.datetime", MY),
        "year" => simple("YEAR", "int", MY),
        "json" => simple("JSON", "dict", MY),
        "enum" => {
            let values = parse_enum_set_values(&col.data_type);
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            let sa_type = format!("ENUM({})", quoted.join(", "));
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: MY.to_string(),
                import_name: "ENUM".to_string(),
                element_import: None,
            }
        }
        "set" => {
            let values = parse_enum_set_values(&col.data_type);
            let quoted: Vec<String> = values.iter().map(|v| format!("'{v}'")).collect();
            let sa_type = format!("SET({})", quoted.join(", "));
            MappedType {
                sa_type,
                python_type: "str".to_string(),
                import_module: MY.to_string(),
                import_name: "SET".to_string(),
                element_import: None,
            }
        }
        "bit" => simple("BIT", "int", MY),
        "boolean" | "bool" => simple("BOOLEAN", "bool", MY),
        _ => {
            let upper = dt.to_uppercase();
            simple(&upper, "str", MY)
        }
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
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "Boolean");
        assert_eq!(m.python_type, "bool");
    }

    #[test]
    fn test_tinyint_not_bool() {
        let c = mysql_col("tinyint", "tinyint(4)");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "TINYINT");
        assert_eq!(m.import_module, MY);
    }

    #[test]
    fn test_unsigned_int() {
        let c = mysql_col("int", "int unsigned");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "Integer");

        let md = map_column_type_dialect(&c);
        assert_eq!(md.sa_type, "INTEGER(unsigned=True)");
        assert_eq!(md.import_module, MY);
    }

    #[test]
    fn test_varchar() {
        let mut c = mysql_col("varchar", "varchar(255)");
        c.character_maximum_length = Some(255);
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "String(255)");
        assert_eq!(m.python_type, "str");
    }

    #[test]
    fn test_enum_parsing() {
        let c = mysql_col("enum", "enum('active','inactive','pending')");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "Enum('active', 'inactive', 'pending')");
        assert_eq!(m.import_name, "Enum");
    }

    #[test]
    fn test_set_parsing() {
        let c = mysql_col("set", "set('read','write','execute')");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "SET('read', 'write', 'execute')");
        assert_eq!(m.import_module, MY);
    }

    #[test]
    fn test_datetime() {
        let c = mysql_col("datetime", "datetime");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "DateTime");
        assert_eq!(m.python_type, "datetime.datetime");
    }

    #[test]
    fn test_decimal() {
        let mut c = mysql_col("decimal", "decimal(10,2)");
        c.numeric_precision = Some(10);
        c.numeric_scale = Some(2);
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "Numeric(10, 2)");
        assert_eq!(m.python_type, "decimal.Decimal");
    }

    #[test]
    fn test_json() {
        let c = mysql_col("json", "json");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "JSON");
        assert_eq!(m.python_type, "dict");
    }

    #[test]
    fn test_year() {
        let c = mysql_col("year", "year");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "YEAR");
        assert_eq!(m.import_module, MY);
    }

    #[test]
    fn test_mediumtext() {
        let c = mysql_col("mediumtext", "mediumtext");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "MEDIUMTEXT");
        assert_eq!(m.import_module, MY);
    }

    #[test]
    fn test_dialect_tinyint_bool() {
        let c = mysql_col("tinyint", "tinyint(1)");
        let m = map_column_type_dialect(&c);
        assert_eq!(m.sa_type, "TINYINT(display_width=1)");
        assert_eq!(m.import_module, MY);
    }

    #[test]
    fn test_enum_escaped_quotes() {
        let c = mysql_col("enum", "enum('can''t','won''t','ok')");
        let m = map_column_type(&c);
        assert_eq!(m.sa_type, "Enum('can't', 'won't', 'ok')");
    }
}
