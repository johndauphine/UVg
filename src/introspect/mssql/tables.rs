use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tiberius::Client;

use crate::error::UvgError;
use crate::schema::{TableInfo, TableType};

pub async fn query_tables(
    client: &mut Client<Compat<TcpStream>>,
    schema: &str,
    noviews: bool,
) -> Result<Vec<TableInfo>, UvgError> {
    let query = r#"
        SELECT
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            t.TABLE_TYPE,
            CAST(ep.value AS NVARCHAR(MAX)) AS comment
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN sys.extended_properties ep
            ON ep.major_id = OBJECT_ID(QUOTENAME(t.TABLE_SCHEMA) + '.' + QUOTENAME(t.TABLE_NAME))
            AND ep.minor_id = 0
            AND ep.name = 'MS_Description'
        WHERE t.TABLE_SCHEMA = @P1
          AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        ORDER BY t.TABLE_NAME
    "#;

    let stream = client.query(query, &[&schema]).await?;
    let rows = stream.into_first_result().await?;

    let mut tables = Vec::new();
    for row in rows {
        let table_type_str: &str = row.get::<&str, _>("TABLE_TYPE").unwrap_or("BASE TABLE");
        let table_type = match table_type_str {
            "BASE TABLE" => TableType::Table,
            "VIEW" => {
                if noviews {
                    continue;
                }
                TableType::View
            }
            _ => continue,
        };

        tables.push(TableInfo {
            schema: row.get::<&str, _>("TABLE_SCHEMA").unwrap_or("").to_string(),
            name: row.get::<&str, _>("TABLE_NAME").unwrap_or("").to_string(),
            table_type,
            comment: row.get::<&str, _>("comment").map(|s| s.to_string()),
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
        });
    }

    Ok(tables)
}
