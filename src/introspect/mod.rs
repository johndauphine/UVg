mod columns;
mod constraints;
mod indexes;
mod tables;

use sqlx::PgPool;

use crate::cli::GeneratorOptions;
use crate::error::UvgError;
use crate::schema::IntrospectedSchema;

/// Introspect the database and return the full schema metadata.
pub async fn introspect(
    pool: &PgPool,
    schemas: &[String],
    table_filter: &[String],
    noviews: bool,
    _options: &GeneratorOptions,
) -> Result<IntrospectedSchema, UvgError> {
    let mut all_tables = Vec::new();

    for schema in schemas {
        let mut schema_tables = tables::query_tables(pool, schema, noviews).await?;

        // Apply table filter if specified
        if !table_filter.is_empty() {
            schema_tables.retain(|t| table_filter.contains(&t.name));
        }

        // Populate columns, constraints, and indexes for each table
        for table in &mut schema_tables {
            table.columns = columns::query_columns(pool, &table.schema, &table.name).await?;
            table.constraints =
                constraints::query_constraints(pool, &table.schema, &table.name).await?;
            table.indexes = indexes::query_indexes(pool, &table.schema, &table.name).await?;
        }

        all_tables.extend(schema_tables);
    }

    Ok(IntrospectedSchema { tables: all_tables })
}
