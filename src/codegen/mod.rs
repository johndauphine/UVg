pub mod ddl;
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

    /// Generate split output: one file per table/class.
    /// Returns Vec of (filename, content) pairs.
    /// Default implementation splits the single output by class/table definitions.
    fn generate_split(
        &self,
        schema: &IntrospectedSchema,
        options: &GeneratorOptions,
    ) -> Vec<(String, String)> {
        let full = self.generate(schema, options);
        split_python_output(&full)
    }
}

/// Split generated Python code into per-file chunks.
/// Everything before the first model class/Table() assignment goes into base.py.
/// Each class or t_xxx = Table(...) block becomes its own file.
/// Each model file gets `from .base import *` so it can run standalone.
/// Returns (filename, content) pairs plus base.py and __init__.py.
pub fn split_python_output(full: &str) -> Vec<(String, String)> {
    let blocks = split_python_blocks(full);

    let mut base_parts: Vec<&str> = Vec::new();
    let mut model_files: Vec<(String, String)> = Vec::new();

    for block in &blocks {
        let trimmed = block.trim();
        if trimmed.is_empty() {
            continue;
        }

        if let Some(name) = extract_model_name(trimmed) {
            // Prepend import from base so each file is independently importable
            let content = format!("from .base import *  # noqa\n\n{trimmed}\n");
            model_files.push((format!("{name}.py"), content));
        } else {
            // Everything else (imports, enum classes, Base/metadata) → base.py
            base_parts.push(trimmed);
        }
    }

    let mut files: Vec<(String, String)> = Vec::new();

    // base.py
    let base_content = base_parts.join("\n\n") + "\n";
    files.push(("base.py".to_string(), base_content));

    // Model files
    let model_names: Vec<String> = model_files.iter().map(|(n, _)| n.clone()).collect();
    files.extend(model_files);

    // __init__.py re-exports
    let mut init_lines = vec!["from .base import *  # noqa".to_string()];
    for name in &model_names {
        let module = name.strip_suffix(".py").unwrap_or(name);
        init_lines.push(format!("from .{module} import *  # noqa"));
    }
    init_lines.push(String::new());
    files.push(("__init__.py".to_string(), init_lines.join("\n")));

    files
}

/// Split generated output into logical blocks.
/// Tries triple-newline first (declarative generator), falls back to double-newline
/// (tables generator), picking whichever separator yields more model blocks.
fn split_python_blocks(full: &str) -> Vec<&str> {
    let separators = ["\n\n\n", "\n\n"];
    let mut best_blocks: Vec<&str> = vec![full];
    let mut best_model_count = 0usize;

    for separator in separators {
        let blocks: Vec<&str> = full.split(separator).collect();
        let model_count = blocks
            .iter()
            .map(|block| block.trim())
            .filter(|block| !block.is_empty())
            .filter(|block| extract_model_name(block).is_some())
            .count();

        if model_count > best_model_count {
            best_model_count = model_count;
            best_blocks = blocks;
        }
    }

    best_blocks
}

/// Extract a filename from a code block.
/// Only matches ORM model classes (must have `__tablename__`) or Table() assignments.
/// Enum classes and Base class are NOT matched — they stay in base.py.
fn extract_model_name(block: &str) -> Option<String> {
    let first_line = block.lines().next()?;

    // "class Users(Base):" → "users" (only if block contains __tablename__)
    if first_line.starts_with("class ") && first_line.contains('(') {
        let class_name = first_line
            .strip_prefix("class ")?
            .split('(')
            .next()?
            .trim();
        // Must have __tablename__ to be a model (not an enum class or Base)
        let has_tablename = block.lines().skip(1).any(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("__tablename__") && trimmed.contains('=')
        });
        if has_tablename {
            use heck::ToSnakeCase;
            return Some(class_name.to_snake_case());
        }
        return None;
    }

    // "t_post_tags = Table(" → "t_post_tags"
    if first_line.starts_with("t_") && first_line.contains(" = Table(") {
        let var_name = first_line.split(" = ").next()?.trim();
        return Some(var_name.to_string());
    }

    None
}

/// Format a server_default expression. Wraps raw SQL in text('...').
/// Delegates escaping to format_python_string_literal for proper handling of
/// backslashes, newlines, and quote characters.
pub fn format_server_default(default: &str, dialect: Dialect) -> String {
    let cleaned = match dialect {
        Dialect::Postgres => strip_pg_typecast(default),
        Dialect::Mssql => strip_mssql_parens(default),
        Dialect::Mysql | Dialect::Sqlite => default.trim(),
    };

    format!("text({})", format_python_string_literal(cleaned))
}

/// Strip PostgreSQL type casts from a default expression.
/// e.g. "'hello'::character varying" -> "'hello'"
/// e.g. "0::integer" -> "0"
pub(crate) fn strip_pg_typecast(expr: &str) -> &str {
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
pub(crate) fn strip_mssql_parens(expr: &str) -> &str {
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

/// Format index kwargs as a string of ", key='value'" pairs.
/// Empty values are skipped.
pub fn format_index_kwargs(kwargs: &std::collections::BTreeMap<String, String>) -> String {
    kwargs
        .iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| format!(", {k}={}", format_python_string_literal(v)))
        .collect()
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

/// Check if a check constraint expression represents a boolean column.
/// Returns the column name if the expression matches `[schema.][table.]column IN (0, 1)`.
pub fn parse_check_boolean(expression: &str) -> Option<String> {
    let expr = expression.trim();

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

    // Extract column name (strip optional schema.table prefix)
    let col_name = if let Some(dot_pos) = col_part.rfind('.') {
        col_part[dot_pos + 1..].trim()
    } else {
        col_part
    };

    let list_start = in_pos + 4;
    let list_str = expr[list_start..].trim();
    if !list_str.starts_with('(') || !list_str.ends_with(')') {
        return None;
    }

    let inner = &list_str[1..list_str.len() - 1];
    let items: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();

    // Must be exactly (0, 1) in any order
    if items.len() == 2
        && ((items[0] == "0" && items[1] == "1") || (items[0] == "1" && items[1] == "0"))
    {
        Some(col_name.to_string())
    } else {
        None
    }
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
        Dialect::Mssql | Dialect::Mysql | Dialect::Sqlite => false,
    }
}

/// Extract the sequence name from a nextval default expression.
/// e.g. "nextval('my_seq'::regclass)" → Some("my_seq")
pub fn parse_sequence_name(default: &str) -> Option<String> {
    let s = default.strip_prefix("nextval('")?;
    let end = s.find('\'')?;
    Some(s[..end].to_string())
}

/// Check if a sequence name is "standard" (auto-generated by PG serial).
/// Standard pattern: {table}_{column}_seq
pub fn is_standard_sequence_name(seq_name: &str, table_name: &str, col_name: &str) -> bool {
    seq_name == format!("{table_name}_{col_name}_seq")
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

    #[test]
    fn test_split_python_declarative() {
        let full = "\
from typing import Optional

from sqlalchemy import Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)


class Posts(Base):
    __tablename__ = 'posts'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
";
        let files = split_python_output(full);
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();

        assert!(names.contains(&"base.py"), "missing base.py: {names:?}");
        assert!(names.contains(&"users.py"), "missing users.py: {names:?}");
        assert!(names.contains(&"posts.py"), "missing posts.py: {names:?}");
        assert!(names.contains(&"__init__.py"), "missing __init__.py: {names:?}");

        // base.py should have imports and Base class
        let base = &files.iter().find(|(n, _)| n == "base.py").unwrap().1;
        assert!(base.contains("from sqlalchemy"), "base.py missing imports");
        assert!(base.contains("class Base"), "base.py missing Base class");

        // model files should have from .base import
        let users = &files.iter().find(|(n, _)| n == "users.py").unwrap().1;
        assert!(users.contains("from .base import"), "users.py missing base import");
        assert!(users.contains("__tablename__"), "users.py missing tablename");
    }

    #[test]
    fn test_split_python_enum_stays_in_base() {
        let full = "\
import enum

from sqlalchemy import Enum, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class StatusEnum(str, enum.Enum):
    ACTIVE = 'active'
    INACTIVE = 'inactive'


class Base(DeclarativeBase):
    pass


class Users(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
";
        let files = split_python_output(full);
        let base = &files.iter().find(|(n, _)| n == "base.py").unwrap().1;
        assert!(base.contains("StatusEnum"), "enum should be in base.py");

        // Enum should NOT be split into its own file
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        assert!(!names.contains(&"status_enum.py"), "enum should not be a separate file");
    }

    #[test]
    fn test_split_python_tables_generator() {
        // Tables generator uses double-newline separators
        let full = "\
from sqlalchemy import Column, Integer, MetaData, String, Table

metadata = MetaData()

t_users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True)
)

t_posts = Table(
    'posts', metadata,
    Column('id', Integer, primary_key=True)
)
";
        let files = split_python_output(full);
        let names: Vec<&str> = files.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"t_users.py"), "missing t_users.py: {names:?}");
        assert!(names.contains(&"t_posts.py"), "missing t_posts.py: {names:?}");
    }
}
