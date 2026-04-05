use std::collections::BTreeMap;

use sqlx::MySqlPool;

use crate::error::UvgError;
use crate::schema::{ConstraintInfo, ConstraintType, ForeignKeyInfo};

pub async fn query_constraints(
    pool: &MySqlPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<ConstraintInfo>, UvgError> {
    let mut constraints: Vec<ConstraintInfo> = Vec::new();

    // Primary keys and unique constraints
    let pk_uq_rows = sqlx::query_as::<_, PkUqRow>(
        r#"
        SELECT
            CAST(tc.CONSTRAINT_NAME AS CHAR) AS CONSTRAINT_NAME,
            CAST(tc.CONSTRAINT_TYPE AS CHAR) AS CONSTRAINT_TYPE,
            CAST(kcu.COLUMN_NAME AS CHAR) AS COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM information_schema.TABLE_CONSTRAINTS tc
        JOIN information_schema.KEY_COLUMN_USAGE kcu
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
            AND kcu.TABLE_NAME = tc.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = ?
          AND tc.TABLE_NAME = ?
          AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut pk_uq_map: BTreeMap<String, (ConstraintType, Vec<String>)> = BTreeMap::new();
    for row in pk_uq_rows {
        let ct = match row.constraint_type.as_str() {
            "PRIMARY KEY" => ConstraintType::PrimaryKey,
            "UNIQUE" => ConstraintType::Unique,
            _ => continue,
        };
        pk_uq_map
            .entry(row.constraint_name)
            .or_insert_with(|| (ct, Vec::new()))
            .1
            .push(row.column_name);
    }
    for (name, (ct, columns)) in pk_uq_map {
        constraints.push(ConstraintInfo {
            name,
            constraint_type: ct,
            columns,
            foreign_key: None,
            check_expression: None,
        });
    }

    // Foreign keys
    let fk_rows = sqlx::query_as::<_, FkRow>(
        r#"
        SELECT
            CAST(kcu.CONSTRAINT_NAME AS CHAR) AS CONSTRAINT_NAME,
            CAST(kcu.COLUMN_NAME AS CHAR) AS COLUMN_NAME,
            CAST(kcu.REFERENCED_TABLE_SCHEMA AS CHAR) AS REFERENCED_TABLE_SCHEMA,
            CAST(kcu.REFERENCED_TABLE_NAME AS CHAR) AS REFERENCED_TABLE_NAME,
            CAST(kcu.REFERENCED_COLUMN_NAME AS CHAR) AS REFERENCED_COLUMN_NAME,
            CAST(rc.UPDATE_RULE AS CHAR) AS UPDATE_RULE,
            CAST(rc.DELETE_RULE AS CHAR) AS DELETE_RULE
        FROM information_schema.KEY_COLUMN_USAGE kcu
        JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
            ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
        WHERE kcu.TABLE_SCHEMA = ?
          AND kcu.TABLE_NAME = ?
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut fk_map: BTreeMap<String, FkAccumulator> = BTreeMap::new();
    for row in fk_rows {
        let acc = fk_map
            .entry(row.constraint_name.clone())
            .or_insert_with(|| FkAccumulator {
                columns: Vec::new(),
                ref_schema: row.ref_schema.clone(),
                ref_table: row.ref_table.clone(),
                ref_columns: Vec::new(),
                update_rule: row.update_rule.clone(),
                delete_rule: row.delete_rule.clone(),
            });
        if !acc.columns.contains(&row.column_name) {
            acc.columns.push(row.column_name);
        }
        if !acc.ref_columns.contains(&row.ref_column) {
            acc.ref_columns.push(row.ref_column);
        }
    }
    for (name, acc) in fk_map {
        constraints.push(ConstraintInfo {
            name,
            constraint_type: ConstraintType::ForeignKey,
            columns: acc.columns,
            foreign_key: Some(ForeignKeyInfo {
                ref_schema: acc.ref_schema,
                ref_table: acc.ref_table,
                ref_columns: acc.ref_columns,
                update_rule: acc.update_rule,
                delete_rule: acc.delete_rule,
            }),
            check_expression: None,
        });
    }

    // Check constraints (MySQL 8.0+; older versions lack CHECK_CONSTRAINTS table)
    let check_rows = match sqlx::query_as::<_, CheckRow>(
        r#"
        SELECT
            CAST(cc.CONSTRAINT_NAME AS CHAR) AS CONSTRAINT_NAME,
            CAST(cc.CHECK_CLAUSE AS CHAR) AS CHECK_CLAUSE
        FROM information_schema.CHECK_CONSTRAINTS cc
        JOIN information_schema.TABLE_CONSTRAINTS tc
            ON tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME
            AND tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA
        WHERE tc.TABLE_SCHEMA = ?
          AND tc.TABLE_NAME = ?
          AND tc.CONSTRAINT_TYPE = 'CHECK'
        ORDER BY cc.CONSTRAINT_NAME
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            let msg = e.to_string();
            // MySQL < 8.0.16 does not have CHECK_CONSTRAINTS table.
            // Only suppress "table doesn't exist" errors (error 1109/1146);
            // propagate other errors (permissions, connectivity, etc.).
            if msg.contains("1109") || msg.contains("1146") || msg.contains("doesn't exist") {
                tracing::debug!(
                    "Skipping CHECK constraints for {}.{} (table not available): {}",
                    schema,
                    table_name,
                    e
                );
                vec![]
            } else {
                return Err(e.into());
            }
        }
    };

    for row in check_rows {
        constraints.push(ConstraintInfo {
            name: row.constraint_name,
            constraint_type: ConstraintType::Check,
            columns: vec![],
            foreign_key: None,
            check_expression: Some(row.check_clause),
        });
    }

    Ok(constraints)
}

struct FkAccumulator {
    columns: Vec<String>,
    ref_schema: String,
    ref_table: String,
    ref_columns: Vec<String>,
    update_rule: String,
    delete_rule: String,
}

#[derive(sqlx::FromRow)]
struct PkUqRow {
    #[sqlx(rename = "CONSTRAINT_NAME")]
    constraint_name: String,
    #[sqlx(rename = "CONSTRAINT_TYPE")]
    constraint_type: String,
    #[sqlx(rename = "COLUMN_NAME")]
    column_name: String,
    #[sqlx(rename = "ORDINAL_POSITION")]
    _ordinal_position: u32,
}

#[derive(sqlx::FromRow)]
struct FkRow {
    #[sqlx(rename = "CONSTRAINT_NAME")]
    constraint_name: String,
    #[sqlx(rename = "COLUMN_NAME")]
    column_name: String,
    #[sqlx(rename = "REFERENCED_TABLE_SCHEMA")]
    ref_schema: String,
    #[sqlx(rename = "REFERENCED_TABLE_NAME")]
    ref_table: String,
    #[sqlx(rename = "REFERENCED_COLUMN_NAME")]
    ref_column: String,
    #[sqlx(rename = "UPDATE_RULE")]
    update_rule: String,
    #[sqlx(rename = "DELETE_RULE")]
    delete_rule: String,
}

#[derive(sqlx::FromRow)]
struct CheckRow {
    #[sqlx(rename = "CONSTRAINT_NAME")]
    constraint_name: String,
    #[sqlx(rename = "CHECK_CLAUSE")]
    check_clause: String,
}
