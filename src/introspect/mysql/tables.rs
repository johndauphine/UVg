use sqlx::MySqlPool;

use crate::error::UvgError;
use crate::schema::{TableInfo, TableType};

pub async fn query_tables(
    pool: &MySqlPool,
    schema: &str,
    noviews: bool,
) -> Result<Vec<TableInfo>, UvgError> {
    let rows = sqlx::query_as::<_, TableRow>(
        r#"
        SELECT
            CAST(TABLE_SCHEMA AS CHAR) AS TABLE_SCHEMA,
            CAST(TABLE_NAME AS CHAR) AS TABLE_NAME,
            CAST(TABLE_TYPE AS CHAR) AS TABLE_TYPE,
            CAST(TABLE_COMMENT AS CHAR) AS TABLE_COMMENT
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = ?
          AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        ORDER BY TABLE_NAME
        "#,
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    let tables = rows
        .into_iter()
        .filter_map(|row| {
            let table_type = match row.table_type.as_str() {
                "BASE TABLE" => TableType::Table,
                "VIEW" => {
                    if noviews {
                        return None;
                    }
                    TableType::View
                }
                _ => return None,
            };
            // MySQL returns empty string for no comment
            let comment = if row.table_comment.is_empty() {
                None
            } else {
                Some(row.table_comment)
            };
            Some(TableInfo {
                schema: row.table_schema,
                name: row.table_name,
                table_type,
                comment,
                columns: Vec::new(),
                constraints: Vec::new(),
                indexes: Vec::new(),
            })
        })
        .collect();

    Ok(tables)
}

#[derive(sqlx::FromRow)]
struct TableRow {
    #[sqlx(rename = "TABLE_SCHEMA")]
    table_schema: String,
    #[sqlx(rename = "TABLE_NAME")]
    table_name: String,
    #[sqlx(rename = "TABLE_TYPE")]
    table_type: String,
    #[sqlx(rename = "TABLE_COMMENT")]
    table_comment: String,
}
