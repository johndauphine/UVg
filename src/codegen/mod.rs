pub mod declarative;
pub mod imports;
pub mod tables;

use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::schema::IntrospectedSchema;

/// Trait for code generators.
pub trait Generator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String;
}

/// Format a server_default expression. Wraps raw SQL in text('...').
pub fn format_server_default(default: &str, dialect: Dialect) -> String {
    let cleaned = match dialect {
        Dialect::Postgres => strip_pg_typecast(default),
        Dialect::Mssql => strip_mssql_parens(default),
    };

    format!("text('{cleaned}')")
}

/// Strip PostgreSQL type casts from a default expression.
/// e.g. "'hello'::character varying" -> "'hello'"
/// e.g. "0::integer" -> "0"
fn strip_pg_typecast(expr: &str) -> &str {
    // Find the last :: that's not inside quotes
    if let Some(pos) = find_typecast_pos(expr) {
        expr[..pos].trim()
    } else {
        expr.trim()
    }
}

fn find_typecast_pos(expr: &str) -> Option<usize> {
    let bytes = expr.as_bytes();
    let mut in_quotes = false;
    let mut in_parens = 0u32;
    let mut i = 0;
    let mut last_cast_pos = None;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_quotes = !in_quotes,
            b'(' if !in_quotes => in_parens += 1,
            b')' if !in_quotes => in_parens = in_parens.saturating_sub(1),
            b':' if !in_quotes && in_parens == 0 && i + 1 < bytes.len() && bytes[i + 1] == b':' => {
                last_cast_pos = Some(i);
                i += 1; // skip second ':'
            }
            _ => {}
        }
        i += 1;
    }

    last_cast_pos
}

/// Strip MSSQL wrapping parentheses and leading N from string literals.
/// e.g. "((0))" -> "0"
/// e.g. "(N'hello')" -> "'hello'"
fn strip_mssql_parens(expr: &str) -> &str {
    let mut s = expr.trim();
    // Strip outer parens: MSSQL defaults are often wrapped like ((value))
    while s.starts_with('(') && s.ends_with(')') {
        s = &s[1..s.len() - 1];
    }
    // Strip leading N from N'string' literals
    if s.starts_with("N'") || s.starts_with("N\"") {
        s = &s[1..];
    }
    s.trim()
}

/// Check if a column is part of the primary key.
pub fn is_primary_key_column(
    col_name: &str,
    constraints: &[crate::schema::ConstraintInfo],
) -> bool {
    constraints.iter().any(|c| {
        c.constraint_type == crate::schema::ConstraintType::PrimaryKey
            && c.columns.contains(&col_name.to_string())
    })
}

/// Check if a column has a single-column unique constraint.
pub fn has_unique_constraint(
    col_name: &str,
    constraints: &[crate::schema::ConstraintInfo],
) -> bool {
    constraints.iter().any(|c| {
        c.constraint_type == crate::schema::ConstraintType::Unique
            && c.columns.len() == 1
            && c.columns[0] == col_name
    })
}

/// Get foreign key info for a column, if it has one.
pub fn get_foreign_key_for_column<'a>(
    col_name: &str,
    constraints: &'a [crate::schema::ConstraintInfo],
) -> Option<&'a crate::schema::ConstraintInfo> {
    constraints.iter().find(|c| {
        c.constraint_type == crate::schema::ConstraintType::ForeignKey
            && c.columns.len() == 1
            && c.columns[0] == col_name
    })
}

/// Check if an index is just backing a unique constraint (same columns).
pub fn is_unique_constraint_index(
    index: &crate::schema::IndexInfo,
    constraints: &[crate::schema::ConstraintInfo],
) -> bool {
    if !index.is_unique {
        return false;
    }
    constraints
        .iter()
        .any(|c| c.constraint_type == crate::schema::ConstraintType::Unique && c.columns == index.columns)
}

/// Quote a list of column names for use in constraint arguments.
pub fn quote_constraint_columns(cols: &[String]) -> Vec<String> {
    cols.iter().map(|c| format!("'{c}'")).collect()
}

/// Escape single quotes in a string for Python string literals.
pub fn escape_python_string(s: &str) -> String {
    s.replace('\'', "\\'")
}

/// Check if a column default is a serial/sequence default.
/// PG: starts with `nextval(`; MSSQL: always false (identity columns have NULL defaults).
pub fn is_serial_default(default: &str, dialect: Dialect) -> bool {
    match dialect {
        Dialect::Postgres => default.starts_with("nextval("),
        Dialect::Mssql => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_server_default_pg() {
        assert_eq!(
            format_server_default("now()", Dialect::Postgres),
            "text('now()')"
        );
        assert_eq!(
            format_server_default("0", Dialect::Postgres),
            "text('0')"
        );
    }

    #[test]
    fn test_strip_pg_typecast() {
        assert_eq!(strip_pg_typecast("0::integer"), "0");
        assert_eq!(strip_pg_typecast("'hello'::character varying"), "'hello'");
        assert_eq!(strip_pg_typecast("now()"), "now()");
        assert_eq!(
            strip_pg_typecast("nextval('seq'::regclass)"),
            "nextval('seq'::regclass)"
        );
    }

    #[test]
    fn test_format_server_default_mssql() {
        assert_eq!(
            format_server_default("((0))", Dialect::Mssql),
            "text('0')"
        );
        assert_eq!(
            format_server_default("(N'hello')", Dialect::Mssql),
            "text(''hello'')"
        );
        assert_eq!(
            format_server_default("(getdate())", Dialect::Mssql),
            "text('getdate()')"
        );
    }

    #[test]
    fn test_strip_mssql_parens() {
        assert_eq!(strip_mssql_parens("((0))"), "0");
        assert_eq!(strip_mssql_parens("(N'hello')"), "'hello'");
        assert_eq!(strip_mssql_parens("(getdate())"), "getdate()");
        assert_eq!(strip_mssql_parens("((1))"), "1");
    }

    #[test]
    fn test_is_serial_default() {
        assert!(is_serial_default("nextval('seq'::regclass)", Dialect::Postgres));
        assert!(!is_serial_default("nextval('seq')", Dialect::Mssql));
        assert!(!is_serial_default("((1))", Dialect::Mssql));
    }
}
