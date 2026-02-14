use sqlx::PgPool;

use crate::error::UvgError;
use crate::schema::ColumnInfo;

pub async fn query_columns(
    pool: &PgPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<ColumnInfo>, UvgError> {
    let rows = sqlx::query_as::<_, ColumnRow>(
        r#"
        SELECT c.column_name, c.ordinal_position::int4, c.is_nullable = 'YES' AS is_nullable,
               c.data_type, c.udt_name, c.character_maximum_length::int4,
               c.numeric_precision::int4, c.numeric_scale::int4, c.column_default,
               c.is_identity = 'YES' AS is_identity, c.identity_generation,
               col_description(
                   (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass,
                   c.ordinal_position
               ) AS comment
        FROM information_schema.columns c
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let columns = rows
        .into_iter()
        .map(|row| ColumnInfo {
            name: row.column_name,
            ordinal_position: row.ordinal_position,
            is_nullable: row.is_nullable,
            data_type: row.data_type,
            udt_name: row.udt_name,
            character_maximum_length: row.character_maximum_length,
            numeric_precision: row.numeric_precision,
            numeric_scale: row.numeric_scale,
            column_default: row.column_default,
            is_identity: row.is_identity,
            identity_generation: row.identity_generation,
            comment: row.comment,
        })
        .collect();

    Ok(columns)
}

#[derive(sqlx::FromRow)]
struct ColumnRow {
    column_name: String,
    ordinal_position: i32,
    is_nullable: bool,
    data_type: String,
    udt_name: String,
    character_maximum_length: Option<i32>,
    numeric_precision: Option<i32>,
    numeric_scale: Option<i32>,
    column_default: Option<String>,
    is_identity: bool,
    identity_generation: Option<String>,
    comment: Option<String>,
}
