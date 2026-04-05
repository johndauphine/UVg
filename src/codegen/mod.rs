pub mod declarative;
pub mod imports;
pub mod relationships;
pub mod tables;

use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::schema::IntrospectedSchema;

/// Trait for code generators.
pub trait Generator {
    fn generate(&self, schema: &IntrospectedSchema, options: &GeneratorOptions) -> String;
}

/// Format a server_default expression. Wraps raw SQL in text('...').
/// Delegates escaping to format_python_string_literal for proper handling of
/// backslashes, newlines, and quote characters.
pub fn format_server_default(default: &str, dialect: Dialect) -> String {
    let cleaned = match dialect {
        Dialect::Postgres => strip_pg_typecast(default),
        Dialect::Mssql => strip_mssql_parens(default),
    };

    format!("text({})", format_python_string_literal(cleaned))
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

/// Check if a table has any primary key constraint.
pub fn has_primary_key(constraints: &[crate::schema::ConstraintInfo]) -> bool {
    constraints
        .iter()
        .any(|c| c.constraint_type == crate::schema::ConstraintType::PrimaryKey)
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

/// Format FK option kwargs (ondelete, onupdate) for ForeignKeyConstraint.
/// Returns empty string if both rules are NO ACTION (the default).
pub fn format_fk_options(fk: &crate::schema::ForeignKeyInfo) -> String {
    let mut opts = Vec::new();
    if fk.delete_rule != "NO ACTION" {
        opts.push(format!("ondelete='{}'", fk.delete_rule));
    }
    if fk.update_rule != "NO ACTION" {
        opts.push(format!("onupdate='{}'", fk.update_rule));
    }
    if opts.is_empty() {
        String::new()
    } else {
        format!(", {}", opts.join(", "))
    }
}

/// Format a string as a Python string literal, choosing quote style and escaping properly.
/// Uses double quotes if the string contains single quotes (and no double quotes),
/// otherwise uses single quotes with escaping. Newlines are always escaped.
pub fn format_python_string_literal(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\n', "\\n");
    if escaped.contains('\'') && !escaped.contains('"') {
        format!("\"{}\"", escaped)
    } else {
        format!("'{}'", escaped.replace('\'', "\\'"))
    }
}

/// Try to parse an IN-list from a check constraint expression.
/// Returns (column_name, values) if the expression matches `[table.]column IN ('a', 'b', 'c')`.
pub fn parse_check_enum(expression: &str) -> Option<(String, Vec<String>)> {
    // Match pattern: [optional_table.]column IN ('val1', 'val2', ...)
    let expr = expression.trim();

    // Find " IN (" (case-insensitive) using byte-level search to avoid
    // index mismatch from to_uppercase() on non-ASCII input.
    let needle = b" IN (";
    let in_pos = expr
        .as_bytes()
        .windows(needle.len())
        .position(|window| {
            window
                .iter()
                .zip(needle.iter())
                .all(|(b, n)| b.to_ascii_uppercase() == *n)
        })?;
    let col_part = expr[..in_pos].trim();

    // Extract column name (strip optional table prefix)
    let col_name = if let Some(dot_pos) = col_part.rfind('.') {
        col_part[dot_pos + 1..].trim()
    } else {
        col_part
    };

    // Extract the IN list (needle " IN (" is 5 bytes)
    let list_start = in_pos + 4; // skip " IN " (the '(' is checked below)
    let list_str = expr[list_start..].trim();
    if !list_str.starts_with('(') || !list_str.ends_with(')') {
        return None;
    }

    let inner = &list_str[1..list_str.len() - 1];

    // Parse quoted string values
    let mut values = Vec::new();
    for item in inner.split(',') {
        let trimmed = item.trim();
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            // Unescape SQL doubled quotes: '' → '
            let raw = &trimmed[1..trimmed.len() - 1];
            values.push(raw.replace("''", "'"));
        } else {
            // Not a string enum (could be numeric IN list)
            return None;
        }
    }

    if values.is_empty() {
        return None;
    }

    Some((col_name.to_string(), values))
}

/// Generate a Python enum class from an EnumInfo.
/// Returns the class definition string (e.g. "class StatusEnum(str, enum.Enum):\n    ...").
pub fn generate_enum_class(enum_info: &crate::schema::EnumInfo) -> String {
    use heck::ToUpperCamelCase;

    let class_name = enum_info.name.to_upper_camel_case();
    let mut lines = Vec::new();
    lines.push(format!("class {class_name}(str, enum.Enum):"));
    for value in &enum_info.values {
        // Sanitize member name: uppercase, replace non-identifier chars, prefix if starts with digit
        let mut member_name: String = value
            .to_uppercase()
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
            .collect();
        if member_name.starts_with(|c: char| c.is_ascii_digit()) {
            member_name = format!("_{member_name}");
        }
        if member_name.is_empty() {
            member_name = "_".to_string();
        }
        lines.push(format!("    {member_name} = {}", format_python_string_literal(value)));
    }
    lines.join("\n")
}

/// Get the Python class name for an enum.
pub fn enum_class_name(enum_name: &str) -> String {
    use heck::ToUpperCamelCase;
    enum_name.to_upper_camel_case()
}

/// Find the enum info for a column's udt_name in the schema.
pub fn find_enum_for_column<'a>(
    udt_name: &str,
    enums: &'a [crate::schema::EnumInfo],
) -> Option<&'a crate::schema::EnumInfo> {
    enums.iter().find(|e| e.name == udt_name)
}

/// Sort tables in topological order by FK dependencies (Kahn's algorithm).
/// Referenced tables come before referencing tables. Alphabetical tiebreak.
pub fn topo_sort_tables(tables: &[crate::schema::TableInfo]) -> Vec<&crate::schema::TableInfo> {
    use std::collections::{BTreeSet, HashMap};

    // Build name→index map and adjacency / in-degree structures
    let name_to_idx: HashMap<&str, usize> = tables
        .iter()
        .enumerate()
        .map(|(i, t)| (t.name.as_str(), i))
        .collect();

    let n = tables.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n]; // ref_table → [referencing tables]

    for (i, table) in tables.iter().enumerate() {
        for constraint in &table.constraints {
            if constraint.constraint_type == crate::schema::ConstraintType::ForeignKey {
                if let Some(ref fk) = constraint.foreign_key {
                    if let Some(&ref_idx) = name_to_idx.get(fk.ref_table.as_str()) {
                        if ref_idx != i {
                            // self-references don't count
                            in_degree[i] += 1;
                            dependents[ref_idx].push(i);
                        }
                    }
                }
            }
        }
    }

    // Kahn's: start with nodes that have no incoming FK edges, sorted alphabetically
    let mut queue: BTreeSet<(String, usize)> = BTreeSet::new();
    for (i, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            queue.insert((tables[i].name.clone(), i));
        }
    }

    let mut result: Vec<&crate::schema::TableInfo> = Vec::with_capacity(n);
    while let Some((_, idx)) = queue.iter().next().cloned() {
        queue.remove(&(tables[idx].name.clone(), idx));
        result.push(&tables[idx]);
        for &dep in &dependents[idx] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 {
                queue.insert((tables[dep].name.clone(), dep));
            }
        }
    }

    // If there's a cycle, append remaining tables alphabetically
    if result.len() < n {
        let in_result: std::collections::HashSet<usize> =
            result.iter().map(|t| name_to_idx[t.name.as_str()]).collect();
        let mut remaining: Vec<(usize, &str)> = (0..n)
            .filter(|i| !in_result.contains(i))
            .map(|i| (i, tables[i].name.as_str()))
            .collect();
        remaining.sort_by_key(|&(_, name)| name.to_string());
        for (i, _) in remaining {
            result.push(&tables[i]);
        }
    }

    result
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
            "text(\"'hello'\")"
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
