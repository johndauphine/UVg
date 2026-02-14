pub mod declarative;
pub mod imports;
pub mod tables;

use crate::cli::GeneratorOptions;
use crate::schema::IntrospectedSchema;

/// Trait for code generators.
pub trait Generator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String;
}

/// Format a server_default expression. Wraps raw SQL in text('...').
pub fn format_server_default(default: &str) -> String {
    // Strip PostgreSQL type casts like ::integer, ::character varying, etc.
    let cleaned = strip_pg_typecast(default);

    // If it looks like a function call or expression, wrap in text()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_server_default() {
        assert_eq!(format_server_default("now()"), "text('now()')");
        assert_eq!(format_server_default("0"), "text('0')");
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
}
