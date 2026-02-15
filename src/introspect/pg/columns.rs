use sqlx::PgPool;

use crate::error::UvgError;
use crate::schema::{ColumnInfo, IdentityInfo};

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

    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let identity = if row.is_identity {
            query_identity_info(pool, schema, table_name, &row.column_name).await?
        } else {
            None
        };
        columns.push(ColumnInfo {
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
            identity,
            comment: row.comment,
            collation: None,
        });
    }

    Ok(columns)
}

/// Query identity sequence parameters for an identity column.
async fn query_identity_info(
    pool: &PgPool,
    schema: &str,
    table_name: &str,
    column_name: &str,
) -> Result<Option<IdentityInfo>, UvgError> {
    let qualified = format!("{schema}.{table_name}");
    let row = sqlx::query_as::<_, IdentityRow>(
        r#"
        SELECT s.seqstart, s.seqincrement, s.seqmin, s.seqmax, s.seqcycle, s.seqcache
        FROM pg_sequence s
        JOIN pg_class c ON c.oid = s.seqrelid
        WHERE c.oid = pg_get_serial_sequence($1, $2)::regclass
        "#,
    )
    .bind(&qualified)
    .bind(column_name)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| IdentityInfo {
        start: r.seqstart,
        increment: r.seqincrement,
        min_value: r.seqmin,
        max_value: r.seqmax,
        cycle: r.seqcycle,
        cache: r.seqcache,
    }))
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

#[derive(sqlx::FromRow)]
struct IdentityRow {
    seqstart: i64,
    seqincrement: i64,
    seqmin: i64,
    seqmax: i64,
    seqcycle: bool,
    seqcache: i64,
}
