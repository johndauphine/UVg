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
