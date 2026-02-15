use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tiberius::Client;

use crate::error::UvgError;
use crate::schema::{ColumnInfo, IdentityInfo};

pub async fn query_columns(
    client: &mut Client<Compat<TcpStream>>,
    schema: &str,
    table_name: &str,
) -> Result<Vec<ColumnInfo>, UvgError> {
    let query = r#"
        SELECT
            c.COLUMN_NAME,
            c.ORDINAL_POSITION,
            CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS is_nullable,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.COLUMN_DEFAULT,
            COLUMNPROPERTY(OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)), c.COLUMN_NAME, 'IsIdentity') AS is_identity,
            CAST(ic.seed_value AS BIGINT) AS seed_value,
            CAST(ic.increment_value AS BIGINT) AS increment_value,
            CAST(ep.value AS NVARCHAR(MAX)) AS comment,
            c.COLLATION_NAME
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.identity_columns ic
            ON ic.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
            AND ic.name = c.COLUMN_NAME
        LEFT JOIN sys.columns sc
            ON sc.object_id = OBJECT_ID(QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME))
            AND sc.name = c.COLUMN_NAME
        LEFT JOIN sys.extended_properties ep
            ON ep.major_id = sc.object_id
            AND ep.minor_id = sc.column_id
            AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = @P1 AND c.TABLE_NAME = @P2
        ORDER BY c.ORDINAL_POSITION
    "#;

    let stream = client.query(query, &[&schema, &table_name]).await?;
    let rows = stream.into_first_result().await?;

    let mut columns = Vec::new();
    for row in rows {
        let is_identity_val: i32 = row.get::<i32, _>("is_identity").unwrap_or(0);
        let is_identity = is_identity_val == 1;

        let data_type: String = row
            .get::<&str, _>("DATA_TYPE")
            .unwrap_or("")
            .to_lowercase();

        // CHARACTER_MAXIMUM_LENGTH is -1 for varchar(max)/nvarchar(max) — map to None
        let char_max_len: Option<i32> = row.get::<i32, _>("CHARACTER_MAXIMUM_LENGTH");
        let character_maximum_length = char_max_len.filter(|&n| n > 0);

        let numeric_precision: Option<i32> = row
            .get::<u8, _>("NUMERIC_PRECISION")
            .map(|v| v as i32);
        let numeric_scale: Option<i32> = row.get::<i32, _>("NUMERIC_SCALE");

        let identity = if is_identity {
            let seed: i64 = row.get::<i64, _>("seed_value").unwrap_or(1);
            let incr: i64 = row.get::<i64, _>("increment_value").unwrap_or(1);
            Some(IdentityInfo {
                start: seed,
                increment: incr,
                min_value: 0,
                max_value: 0,
                cycle: false,
                cache: 0,
            })
        } else {
            None
        };

        columns.push(ColumnInfo {
            name: row
                .get::<&str, _>("COLUMN_NAME")
                .unwrap_or("")
                .to_string(),
            ordinal_position: row.get::<i32, _>("ORDINAL_POSITION").unwrap_or(0),
            is_nullable: row.get::<i32, _>("is_nullable").unwrap_or(0) == 1,
            data_type: data_type.clone(),
            udt_name: data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            column_default: row
                .get::<&str, _>("COLUMN_DEFAULT")
                .map(|s| s.to_string()),
            is_identity,
            identity_generation: if is_identity {
                Some("ALWAYS".to_string())
            } else {
                None
            },
            identity,
            comment: row.get::<&str, _>("comment").map(|s| s.to_string()),
            collation: row.get::<&str, _>("COLLATION_NAME").map(|s| s.to_string()),
        });
    }

    Ok(columns)
}

