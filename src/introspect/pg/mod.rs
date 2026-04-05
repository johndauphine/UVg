mod columns;
mod constraints;
mod indexes;
mod tables;

use sqlx::PgPool;

use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::error::UvgError;
use crate::schema::{EnumInfo, IntrospectedSchema};

/// Introspect a PostgreSQL database and return the full schema metadata.
pub async fn introspect(
    pool: &PgPool,
    schemas: &[String],
    table_filter: &[String],
    noviews: bool,
    _options: &GeneratorOptions,
) -> Result<IntrospectedSchema, UvgError> {
    let mut all_tables = Vec::new();
    let mut all_enums = Vec::new();

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

        // Query enum types for this schema
        let enums = query_enums(pool, schema).await?;
        all_enums.extend(enums);
    }

    Ok(IntrospectedSchema {
        dialect: Dialect::Postgres,
        tables: all_tables,
        enums: all_enums,
        domains: vec![],
    })
}

/// Query PostgreSQL enum types from pg_catalog.
async fn query_enums(pool: &PgPool, schema: &str) -> Result<Vec<EnumInfo>, UvgError> {
    let rows = sqlx::query_as::<_, EnumRow>(
        r#"
        SELECT t.typname AS enum_name, n.nspname AS enum_schema,
               array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = $1
        GROUP BY t.typname, n.nspname
        ORDER BY t.typname
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EnumInfo {
            name: r.enum_name,
            schema: Some(r.enum_schema),
            values: r.enum_values,
        })
        .collect())
}

#[derive(sqlx::FromRow)]
struct EnumRow {
    enum_name: String,
    enum_schema: String,
    enum_values: Vec<String>,
}
