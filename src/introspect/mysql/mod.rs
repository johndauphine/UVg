mod columns;
mod constraints;
mod indexes;
mod tables;

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
) -> Result<IntrospectedSchema, UvgError> {
    let mut all_tables = Vec::new();

    for schema in schemas {
        let mut schema_tables = tables::query_tables(pool, schema, noviews).await?;

        schema_tables.retain(|t| table_filter.matches(&t.name));

        for table in &mut schema_tables {
            table.columns = columns::query_columns(pool, &table.schema, &table.name).await?;
            table.constraints =
                constraints::query_constraints(pool, &table.schema, &table.name).await?;
            table.indexes = indexes::query_indexes(pool, &table.schema, &table.name).await?;
        }

        all_tables.extend(schema_tables);
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
