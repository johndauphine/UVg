use std::future::Future;
use std::sync::Arc;

use futures::stream::{self, StreamExt, TryStreamExt};

use crate::error::UvgError;
use crate::schema::TableInfo;

mod grouping;

pub mod mssql;
pub mod mysql;
pub mod pg;
pub mod sqlite;

pub(crate) fn restore_original_order<T>(mut items: Vec<(usize, T)>) -> Vec<T> {
    items.sort_by_key(|(ordinal, _)| *ordinal);
    items.into_iter().map(|(_, item)| item).collect()
}

pub(crate) async fn populate_tables_concurrently<F, Fut>(
    tables: Vec<TableInfo>,
    concurrency: usize,
    populate: F,
) -> Result<Vec<TableInfo>, UvgError>
where
    F: Fn(TableInfo) -> Fut,
    Fut: Future<Output = Result<TableInfo, UvgError>>,
{
    let populate = Arc::new(populate);
    let populated = stream::iter(tables.into_iter().enumerate())
        .map(|(ordinal, table)| {
            let populate = Arc::clone(&populate);
            async move { populate(table).await.map(|table| (ordinal, table)) }
        })
        .buffer_unordered(concurrency.max(1))
        .try_collect::<Vec<_>>()
        .await?;

    Ok(restore_original_order(populated))
}

#[cfg(test)]
#[path = "grouping_tests.rs"]
mod grouping_tests;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
