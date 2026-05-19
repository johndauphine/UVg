use super::strip_check_wrapper;

#[test]
fn strips_check_wrapper() {
    assert_eq!(strip_check_wrapper("CHECK (x > 0)"), "x > 0");
    assert_eq!(strip_check_wrapper("CHECK ((x > 0))"), "(x > 0)");
    assert_eq!(
        strip_check_wrapper("  CHECK (a IS NOT NULL)  "),
        "a IS NOT NULL"
    );
    // Defensive: unrecognized format returns input unchanged.
    assert_eq!(strip_check_wrapper("(x > 0)"), "(x > 0)");
}

#[test]
fn strips_check_wrapper_with_trailing_modifiers() {
    // PG's pg_get_constraintdef emits trailing NOT VALID / NO INHERIT
    // for constraints created with those clauses. Without stripping,
    // the wrapper match would miss and the emitter would double-wrap
    // as `CHECK (CHECK (...) NOT VALID)`. Per Copilot review on PR #37.
    assert_eq!(strip_check_wrapper("CHECK (x > 0) NOT VALID"), "x > 0");
    assert_eq!(strip_check_wrapper("CHECK ((x > 0)) NO INHERIT"), "(x > 0)");
    assert_eq!(
        strip_check_wrapper("CHECK (a IS NOT NULL) NOT VALID NO INHERIT"),
        "a IS NOT NULL"
    );
    // Order independence — NO INHERIT can come before NOT VALID too.
    assert_eq!(
        strip_check_wrapper("CHECK (a IS NOT NULL) NO INHERIT NOT VALID"),
        "a IS NOT NULL"
    );
}
