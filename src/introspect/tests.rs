use super::*;

#[test]
fn restore_original_order_sorts_by_ordinal() {
    let restored = restore_original_order(vec![(2, "c"), (0, "a"), (1, "b")]);

    assert_eq!(restored, vec!["a", "b", "c"]);
}
