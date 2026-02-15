use sqlx::PgPool;

use crate::error::UvgError;
use crate::schema::IndexInfo;

pub async fn query_indexes(
    pool: &PgPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<IndexInfo>, UvgError> {
    let rows = sqlx::query_as::<_, IndexRow>(
        r#"
        SELECT i.relname AS index_name, ix.indisunique AS is_unique,
               array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns
        FROM pg_index ix
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = $1 AND t.relname = $2 AND NOT ix.indisprimary
        GROUP BY i.relname, ix.indisunique
        ORDER BY i.relname
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let indexes = rows
        .into_iter()
        .map(|row| IndexInfo {
            name: row.index_name,
            is_unique: row.is_unique,
            columns: row.columns,
        })
        .collect();

    Ok(indexes)
}

#[derive(sqlx::FromRow)]
struct IndexRow {
    index_name: String,
    is_unique: bool,
    columns: Vec<String>,
}
