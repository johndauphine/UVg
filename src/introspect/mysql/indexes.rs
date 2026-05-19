use sqlx::MySqlPool;

use crate::error::UvgError;
use crate::introspect::grouping::{grouped_indexes, IndexColumn};
use crate::schema::IndexInfo;

pub async fn query_indexes(
    pool: &MySqlPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<IndexInfo>, UvgError> {
    let rows = sqlx::query_as::<_, IndexRow>(
        r#"
        SELECT
            CAST(INDEX_NAME AS CHAR) AS INDEX_NAME,
            NON_UNIQUE,
            CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME,
            SEQ_IN_INDEX
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME = ?
          AND INDEX_NAME != 'PRIMARY'
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let indexes = grouped_indexes(rows.into_iter().map(|row| IndexColumn {
        index_name: row.index_name,
        is_unique: !row.non_unique,
        // COLUMN_NAME is NULL for functional/expression indexes (MySQL 8+);
        // skip those columns rather than crashing.
        column: row.column_name,
    }));

    Ok(indexes)
}

#[derive(sqlx::FromRow)]
struct IndexRow {
    #[sqlx(rename = "INDEX_NAME")]
    index_name: String,
    #[sqlx(rename = "NON_UNIQUE")]
    non_unique: bool,
    #[sqlx(rename = "COLUMN_NAME")]
    column_name: Option<String>,
    #[sqlx(rename = "SEQ_IN_INDEX")]
    _seq_in_index: u32,
}
