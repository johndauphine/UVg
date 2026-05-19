use super::*;

#[test]
fn test_parse_check_constraints() {
    let sql = "CREATE TABLE t (id INTEGER, status TEXT, CHECK(status IN ('a', 'b')))";
    let checks = parse_check_constraints(sql);
    assert_eq!(checks.len(), 1);
    assert_eq!(
        checks[0].check_expression.as_deref(),
        Some("status IN ('a', 'b')")
    );
}

#[test]
fn test_parse_check_no_checks() {
    let sql = "CREATE TABLE t (id INTEGER, name TEXT)";
    let checks = parse_check_constraints(sql);
    assert!(checks.is_empty());
}

#[test]
fn test_parse_check_column_level() {
    let sql = "CREATE TABLE t (id INTEGER, status TEXT CHECK(status IN ('active', 'inactive')))";
    let checks = parse_check_constraints(sql);
    assert_eq!(checks.len(), 1);
    assert_eq!(
        checks[0].check_expression.as_deref(),
        Some("status IN ('active', 'inactive')")
    );
}

#[test]
fn test_parse_check_mixed_levels() {
    let sql = "CREATE TABLE t (id INTEGER, val INTEGER CHECK(val > 0), CHECK(id > 0))";
    let checks = parse_check_constraints(sql);
    assert_eq!(checks.len(), 2);
}

#[test]
fn test_normalize_fk_rule() {
    assert_eq!(normalize_fk_rule("CASCADE"), "CASCADE");
    assert_eq!(normalize_fk_rule("NO ACTION"), "NO ACTION");
    assert_eq!(normalize_fk_rule("SET NULL"), "SET NULL");
    assert_eq!(normalize_fk_rule(""), "NO ACTION");
}

#[test]
fn test_split_respecting_parens_with_quoted_comma() {
    // Commas inside string literals should not split
    let result = split_respecting_parens("a TEXT DEFAULT 'x,y', b INTEGER");
    assert_eq!(result.len(), 2);
    assert!(result[0].contains("'x,y'"));
    assert!(result[1].contains("b INTEGER"));
}

#[test]
fn test_parse_check_with_default_containing_comma() {
    let sql = "CREATE TABLE t (id INTEGER, label TEXT DEFAULT 'a,b', CHECK(id > 0))";
    let checks = parse_check_constraints(sql);
    assert_eq!(checks.len(), 1);
    assert_eq!(checks[0].check_expression.as_deref(), Some("id > 0"));
}
