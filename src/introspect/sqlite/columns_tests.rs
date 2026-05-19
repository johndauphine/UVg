use super::*;

#[test]
fn test_parse_declared_type_integer() {
    let (base, len, prec, scale) = parse_declared_type("INTEGER");
    assert_eq!(base, "integer");
    assert_eq!(len, None);
    assert_eq!(prec, None);
    assert_eq!(scale, None);
}

#[test]
fn test_parse_declared_type_varchar() {
    let (base, len, prec, scale) = parse_declared_type("VARCHAR(255)");
    assert_eq!(base, "varchar");
    assert_eq!(len, Some(255));
    assert_eq!(prec, None);
    assert_eq!(scale, None);
}

#[test]
fn test_parse_declared_type_decimal() {
    let (base, len, prec, scale) = parse_declared_type("DECIMAL(10,2)");
    assert_eq!(base, "decimal");
    assert_eq!(len, None);
    assert_eq!(prec, Some(10));
    assert_eq!(scale, Some(2));
}

#[test]
fn test_parse_declared_type_empty() {
    let (base, len, prec, scale) = parse_declared_type("");
    assert_eq!(base, "");
    assert_eq!(len, None);
    assert_eq!(prec, None);
    assert_eq!(scale, None);
}

#[test]
fn test_detect_autoincrement() {
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)";
    assert!(detect_autoincrement(sql, "id"));
    assert!(!detect_autoincrement(sql, "name"));
}

#[test]
fn test_detect_autoincrement_no_keyword() {
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)";
    assert!(!detect_autoincrement(sql, "id"));
}
