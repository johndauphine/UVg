use sqlx::SqlitePool;

use crate::error::UvgError;
use crate::schema::{TableInfo, TableType};

pub async fn query_tables(pool: &SqlitePool, noviews: bool) -> Result<Vec<TableInfo>, UvgError> {
    let rows = sqlx::query_as::<_, TableRow>(
        r#"
        SELECT name, type
        FROM sqlite_master
        WHERE type IN ('table', 'view')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        "#,
    )
    .fetch_all(pool)
    .await?;

    let tables = rows
        .into_iter()
        .filter_map(|row| {
            let table_type = match row.type_.as_str() {
                "table" => TableType::Table,
                "view" => {
                    if noviews {
                        return None;
                    }
                    TableType::View
                }
                _ => return None,
            };
            Some(TableInfo {
                schema: "main".to_string(),
                name: row.name,
                table_type,
                comment: None, // SQLite has no table comments
                columns: Vec::new(),
                constraints: Vec::new(),
                indexes: Vec::new(),
            })
        })
        .collect();

    Ok(tables)
}

/// Get the CREATE TABLE SQL for a table from sqlite_master.
/// Used for AUTOINCREMENT detection and CHECK constraint parsing.
pub async fn query_create_sql(pool: &SqlitePool, table_name: &str) -> Result<String, UvgError> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table_name)
            .fetch_optional(pool)
            .await?;

    Ok(row.map(|r| r.0).unwrap_or_default())
}

#[derive(sqlx::FromRow)]
struct TableRow {
    name: String,
    #[sqlx(rename = "type")]
    type_: String,
}
