use heck::ToUpperCamelCase;

/// Convert a table name to a Python class name (e.g. "user_profiles" -> "UserProfile").
pub fn table_to_class_name(table_name: &str) -> String {
    table_name.to_upper_camel_case()
}

/// Convert a table name to a variable name for the tables generator (e.g. "users" -> "t_users").
pub fn table_to_variable_name(table_name: &str) -> String {
    format!("t_{table_name}")
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
