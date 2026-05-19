mod columns;
mod constraints;
mod indexes;
mod tables;

use futures::stream::{self, StreamExt, TryStreamExt};
use sqlx::MySqlPool;

use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::error::UvgError;
use crate::schema::IntrospectedSchema;
use crate::table_filter::TableFilter;

/// Introspect a MySQL database and return the full schema metadata.
pub async fn introspect(
    pool: &MySqlPool,
    schemas: &[String],
    table_filter: &TableFilter,
    noviews: bool,
    _options: &GeneratorOptions,
    concurrency: usize,
) -> Result<IntrospectedSchema, UvgError> {
    let mut all_tables = Vec::new();
    let concurrency = concurrency.max(1);

    for schema in schemas {
        let mut schema_tables = tables::query_tables(pool, schema, noviews).await?;

        schema_tables.retain(|t| table_filter.matches(&t.name));

        let schema_tables = stream::iter(schema_tables.into_iter().enumerate())
            .map(|(ordinal, mut table)| async move {
                table.columns = columns::query_columns(pool, &table.schema, &table.name).await?;
                table.constraints =
                    constraints::query_constraints(pool, &table.schema, &table.name).await?;
                table.indexes = indexes::query_indexes(pool, &table.schema, &table.name).await?;
                Ok::<_, UvgError>((ordinal, table))
            })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?;

        all_tables.extend(crate::introspect::restore_original_order(schema_tables));
    }

    // Sort alphabetically to match sqlacodegen output
    all_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(IntrospectedSchema {
        dialect: Dialect::Mysql,
        tables: all_tables,
        enums: vec![],
        domains: vec![],
    })
}
