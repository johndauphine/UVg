use sqlx::PgPool;

use crate::error::UvgError;
use crate::schema::{TableInfo, TableType};

pub async fn query_tables(
    pool: &PgPool,
    schema: &str,
    noviews: bool,
) -> Result<Vec<TableInfo>, UvgError> {
    let rows = sqlx::query_as::<_, TableRow>(
        r#"
        SELECT t.table_schema, t.table_name, t.table_type,
               obj_description(
                   (quote_ident(t.table_schema) || '.' || quote_ident(t.table_name))::regclass
               ) AS comment
        FROM information_schema.tables t
        WHERE t.table_schema = $1
          AND t.table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY t.table_name
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
            Some(
                TableInfo::new(row.table_schema, row.table_name, table_type)
                    .with_comment(row.comment),
            )
        })
        .collect();

    Ok(tables)
}

#[derive(sqlx::FromRow)]
struct TableRow {
    table_schema: String,
    table_name: String,
    table_type: String,
    comment: Option<String>,
}
