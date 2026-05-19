mod grouping;

pub mod mssql;
pub mod mysql;
pub mod pg;
pub mod sqlite;

pub(crate) fn restore_original_order<T>(mut items: Vec<(usize, T)>) -> Vec<T> {
    items.sort_by_key(|(ordinal, _)| *ordinal);
    items.into_iter().map(|(_, item)| item).collect()
}

#[cfg(test)]
#[path = "grouping_tests.rs"]
mod grouping_tests;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
