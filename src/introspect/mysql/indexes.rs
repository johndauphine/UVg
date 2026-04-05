use std::collections::BTreeMap;

use sqlx::MySqlPool;

use crate::error::UvgError;
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

    let mut index_map: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
    for row in rows {
        let entry = index_map
            .entry(row.index_name)
            .or_insert_with(|| (!row.non_unique, Vec::new()));
        // COLUMN_NAME is NULL for functional/expression indexes (MySQL 8+);
        // skip those columns rather than crashing.
        if let Some(col) = row.column_name {
            entry.1.push(col);
        }
    }

    // Filter out indexes that ended up with no columns (purely expression-based)
    let indexes = index_map
        .into_iter()
        .filter(|(_, (_, cols))| !cols.is_empty())
        .map(|(name, (is_unique, columns))| IndexInfo {
            name,
            is_unique,
            columns,
            kwargs: BTreeMap::new(),
        })
        .collect();

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
