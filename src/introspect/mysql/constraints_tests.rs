use super::normalize_mysql_check_clause;

#[test]
fn strips_charset_prefix() {
    // Single charset prefix. After normalization, `\'individual\'` is
    // un-escaped to `'individual'` (the original SQL string-literal
    // form) and the leading `_latin1` is stripped.
    assert_eq!(
        normalize_mysql_check_clause("`status` = _latin1\\'active\\'"),
        "`status` = 'active'"
    );
    // Multiple in one predicate.
    assert_eq!(
        normalize_mysql_check_clause("`type` in (_latin1\\'company\\',_latin1\\'government\\')"),
        "`type` in ('company','government')"
    );
    // Different charsets — utf8mb4, cp1251 etc.
    assert_eq!(
        normalize_mysql_check_clause("`x` = _utf8mb4\\'a\\'"),
        "`x` = 'a'"
    );
}

#[test]
fn passes_through_when_no_quirks() {
    // Predicates that don't have charset prefix or backslash escapes
    // pass through unchanged.
    let predicate = "(`is_active` in (0,1))";
    assert_eq!(normalize_mysql_check_clause(predicate), predicate);
}

#[test]
fn does_not_strip_underscore_in_identifier() {
    // `_internal` is part of a column name, not a charset prefix —
    // there's no quote after it. Should pass through.
    let predicate = "(`row_internal_state` >= 0)";
    assert_eq!(normalize_mysql_check_clause(predicate), predicate);
}

#[test]
fn converts_backslash_escape_only() {
    // No charset prefix, but backslash-escaped quote: un-escape to
    // plain quote (the original delimiter).
    assert_eq!(normalize_mysql_check_clause("`x` = \\'a\\'"), "`x` = 'a'");
}
