use sqlx::MySqlPool;

use crate::error::UvgError;
use crate::schema::ColumnInfo;

pub async fn query_columns(
    pool: &MySqlPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<ColumnInfo>, UvgError> {
    let rows = sqlx::query_as::<_, ColumnRow>(
        r#"
        SELECT
            CAST(c.COLUMN_NAME AS CHAR) AS COLUMN_NAME,
            c.ORDINAL_POSITION,
            c.IS_NULLABLE = 'YES' AS is_nullable,
            CAST(c.DATA_TYPE AS CHAR) AS DATA_TYPE,
            CAST(c.COLUMN_TYPE AS CHAR) AS COLUMN_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            CAST(c.COLUMN_DEFAULT AS CHAR) AS COLUMN_DEFAULT,
            CAST(c.EXTRA AS CHAR) AS EXTRA,
            CAST(c.COLUMN_COMMENT AS CHAR) AS COLUMN_COMMENT,
            CAST(c.COLLATION_NAME AS CHAR) AS COLLATION_NAME
        FROM information_schema.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let columns = rows
        .into_iter()
        .map(|row| {
            let is_auto_increment = row.extra.contains("auto_increment");
            let comment = if row.column_comment.is_empty() {
                None
            } else {
                Some(row.column_comment)
            };

            ColumnInfo {
                name: row.column_name,
                ordinal_position: row.ordinal_position as i32,
                is_nullable: row.is_nullable,
                // Store COLUMN_TYPE (e.g. "int unsigned", "enum('a','b')") for type mapper
                data_type: row.column_type,
                // Store DATA_TYPE (base type name) for matching
                udt_name: row.data_type.to_lowercase(),
                character_maximum_length: row.character_maximum_length.map(|v| v as i32),
                numeric_precision: row.numeric_precision.map(|v| v as i32),
                numeric_scale: row.numeric_scale.map(|v| v as i32),
                column_default: row.column_default,
                is_identity: is_auto_increment,
                identity_generation: None,
                identity: None,
                comment,
                collation: row.collation_name,
                autoincrement: if is_auto_increment {
                    Some(true)
                } else {
                    None
                },
            }
        })
        .collect();

    Ok(columns)
}

#[derive(sqlx::FromRow)]
struct ColumnRow {
    #[sqlx(rename = "COLUMN_NAME")]
    column_name: String,
    #[sqlx(rename = "ORDINAL_POSITION")]
    ordinal_position: u32,
    is_nullable: bool,
    #[sqlx(rename = "DATA_TYPE")]
    data_type: String,
    #[sqlx(rename = "COLUMN_TYPE")]
    column_type: String,
    #[sqlx(rename = "CHARACTER_MAXIMUM_LENGTH")]
    character_maximum_length: Option<i64>,
    #[sqlx(rename = "NUMERIC_PRECISION")]
    numeric_precision: Option<u32>,
    #[sqlx(rename = "NUMERIC_SCALE")]
    numeric_scale: Option<u32>,
    #[sqlx(rename = "COLUMN_DEFAULT")]
    column_default: Option<String>,
    #[sqlx(rename = "EXTRA")]
    extra: String,
    #[sqlx(rename = "COLUMN_COMMENT")]
    column_comment: String,
    #[sqlx(rename = "COLLATION_NAME")]
    collation_name: Option<String>,
}
