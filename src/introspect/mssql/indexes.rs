use std::collections::BTreeMap;

use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tiberius::Client;

use crate::error::UvgError;
use crate::schema::IndexInfo;

pub async fn query_indexes(
    client: &mut Client<Compat<TcpStream>>,
    schema: &str,
    table_name: &str,
) -> Result<Vec<IndexInfo>, UvgError> {
    let query = r#"
        SELECT
            i.name AS index_name,
            i.is_unique,
            COL_NAME(ic.object_id, ic.column_id) AS column_name,
            ic.key_ordinal
        FROM sys.indexes i
        JOIN sys.index_columns ic
            ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.object_id = OBJECT_ID(QUOTENAME(@P1) + '.' + QUOTENAME(@P2))
          AND i.is_primary_key = 0
          AND i.type <> 0
          AND ic.key_ordinal > 0
        ORDER BY i.name, ic.key_ordinal
    "#;

    let stream = client.query(query, &[&schema, &table_name]).await?;
    let rows = stream.into_first_result().await?;

    // Group rows by index name (MSSQL returns one row per column, unlike PG's array_agg)
    let mut index_map: BTreeMap<String, (bool, Vec<String>)> = BTreeMap::new();
    for row in rows {
        let name: String = row
            .get::<&str, _>("index_name")
            .unwrap_or("")
            .to_string();
        let is_unique: bool = row.get::<bool, _>("is_unique").unwrap_or(false);
        let col: String = row
            .get::<&str, _>("column_name")
            .unwrap_or("")
            .to_string();

        index_map
            .entry(name)
            .or_insert_with(|| (is_unique, Vec::new()))
            .1
            .push(col);
    }

    let indexes = index_map
        .into_iter()
        .map(|(name, (is_unique, columns))| IndexInfo {
            name,
            is_unique,
            columns,
        })
        .collect();

    Ok(indexes)
}
