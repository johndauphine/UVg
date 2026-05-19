pub mod mssql;
pub mod mysql;
pub mod pg;
pub mod sqlite;

pub(crate) fn restore_original_order<T>(mut items: Vec<(usize, T)>) -> Vec<T> {
    items.sort_by_key(|(ordinal, _)| *ordinal);
    items.into_iter().map(|(_, item)| item).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_original_order_sorts_by_ordinal() {
        let restored = restore_original_order(vec![(2, "c"), (0, "a"), (1, "b")]);

        assert_eq!(restored, vec!["a", "b", "c"]);
    }
}
