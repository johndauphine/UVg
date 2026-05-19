use heck::ToUpperCamelCase;

/// Convert a table name to a Python class name (e.g. "user_profiles" -> "UserProfile").
pub fn table_to_class_name(table_name: &str) -> String {
    table_name.to_upper_camel_case()
}

/// Convert a table name to a variable name for the tables generator (e.g. "users" -> "t_users").
/// Non-identifier characters (hyphens, spaces, etc.) are replaced with underscores.
pub fn table_to_variable_name(table_name: &str) -> String {
    let sanitized: String = table_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("t_{sanitized}")
}

/// Python keywords and builtins that conflict with SQLAlchemy attribute names.
const PYTHON_RESERVED: &[&str] = &[
    // Python keywords
    "False", "None", "True", "and", "as", "assert", "async", "await", "break", "class", "continue",
    "def", "del", "elif", "else", "except", "finally", "for", "from", "global", "if", "import",
    "in", "is", "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try", "while",
    "with", "yield", // SQLAlchemy reserved attribute names
    "metadata", "registry",
];

/// Sanitize a column name into a valid Python attribute name.
/// Returns the sanitized name. If it differs from the input, the caller should
/// emit the original column name as an explicit first argument to mapped_column().
pub fn column_to_attr_name(col_name: &str) -> String {
    let trimmed = col_name.trim();

    // Replace non-identifier chars with underscores
    let mut sanitized: String = trimmed
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Fallback for empty/whitespace-only names
    if sanitized.is_empty() {
        return "_".to_string();
    }

    // Prefix leading digits with underscore
    if sanitized.starts_with(|c: char| c.is_ascii_digit()) {
        sanitized = format!("_{sanitized}");
    }

    // Append underscore for Python reserved words and SQLAlchemy conflicts
    if PYTHON_RESERVED.contains(&sanitized.as_str()) {
        sanitized.push('_');
    }

    sanitized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_to_class_name() {
        assert_eq!(table_to_class_name("users"), "Users");
        assert_eq!(table_to_class_name("user_profiles"), "UserProfiles");
        assert_eq!(table_to_class_name("order_items"), "OrderItems");
        assert_eq!(table_to_class_name("a"), "A");
    }

    #[test]
    fn test_table_to_variable_name() {
        assert_eq!(table_to_variable_name("users"), "t_users");
        assert_eq!(table_to_variable_name("order_items"), "t_order_items");
    }
}
