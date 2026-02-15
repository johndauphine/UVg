use std::collections::BTreeMap;

use tokio::net::TcpStream;
use tokio_util::compat::Compat;
use tiberius::Client;

use crate::error::UvgError;
use crate::schema::{ConstraintInfo, ConstraintType, ForeignKeyInfo};

pub async fn query_constraints(
    client: &mut Client<Compat<TcpStream>>,
    schema: &str,
    table_name: &str,
) -> Result<Vec<ConstraintInfo>, UvgError> {
    let mut constraints: Vec<ConstraintInfo> = Vec::new();

    // Primary keys and unique constraints via INFORMATION_SCHEMA
    let pk_uq_query = r#"
        SELECT
            tc.CONSTRAINT_NAME,
            tc.CONSTRAINT_TYPE,
            kcu.COLUMN_NAME,
            kcu.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
            AND kcu.TABLE_NAME = tc.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = @P1
          AND tc.TABLE_NAME = @P2
          AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
    "#;

    let stream = client.query(pk_uq_query, &[&schema, &table_name]).await?;
    let rows = stream.into_first_result().await?;

    let mut pk_uq_map: BTreeMap<String, (ConstraintType, Vec<String>)> = BTreeMap::new();
    for row in rows {
        let name: String = row
            .get::<&str, _>("CONSTRAINT_NAME")
            .unwrap_or("")
            .to_string();
        let ctype_str: &str = row.get::<&str, _>("CONSTRAINT_TYPE").unwrap_or("");
        let col: String = row
            .get::<&str, _>("COLUMN_NAME")
            .unwrap_or("")
            .to_string();

        let ctype = match ctype_str {
            "PRIMARY KEY" => ConstraintType::PrimaryKey,
            "UNIQUE" => ConstraintType::Unique,
            _ => continue,
        };

        pk_uq_map
            .entry(name)
            .or_insert_with(|| (ctype, Vec::new()))
            .1
            .push(col);
    }

    for (name, (ctype, columns)) in pk_uq_map {
        constraints.push(ConstraintInfo {
            name,
            constraint_type: ctype,
            columns,
            foreign_key: None,
        });
    }

    // Foreign keys via sys.foreign_keys + sys.foreign_key_columns
    let fk_query = r#"
        SELECT
            fk.name AS constraint_name,
            COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
            SCHEMA_NAME(ref_t.schema_id) AS ref_schema,
            ref_t.name AS ref_table,
            COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column,
            fk.update_referential_action_desc AS update_rule,
            fk.delete_referential_action_desc AS delete_rule
        FROM sys.foreign_keys fk
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.tables ref_t ON ref_t.object_id = fk.referenced_object_id
        WHERE fk.parent_object_id = OBJECT_ID(QUOTENAME(@P1) + '.' + QUOTENAME(@P2))
        ORDER BY fk.name, fkc.constraint_column_id
    "#;

    let stream = client.query(fk_query, &[&schema, &table_name]).await?;
    let fk_rows = stream.into_first_result().await?;

    let mut fk_map: BTreeMap<String, FkAccumulator> = BTreeMap::new();
    for row in fk_rows {
        let name: String = row
            .get::<&str, _>("constraint_name")
            .unwrap_or("")
            .to_string();
        let col: String = row
            .get::<&str, _>("column_name")
            .unwrap_or("")
            .to_string();
        let ref_schema: String = row
            .get::<&str, _>("ref_schema")
            .unwrap_or("")
            .to_string();
        let ref_table: String = row
            .get::<&str, _>("ref_table")
            .unwrap_or("")
            .to_string();
        let ref_col: String = row
            .get::<&str, _>("ref_column")
            .unwrap_or("")
            .to_string();
        // MSSQL uses underscores in action names: NO_ACTION -> NO ACTION
        let update_rule: String = row
            .get::<&str, _>("update_rule")
            .unwrap_or("NO_ACTION")
            .replace('_', " ");
        let delete_rule: String = row
            .get::<&str, _>("delete_rule")
            .unwrap_or("NO_ACTION")
            .replace('_', " ");

        let acc = fk_map.entry(name).or_insert_with(|| FkAccumulator {
            columns: Vec::new(),
            ref_schema,
            ref_table,
            ref_columns: Vec::new(),
            update_rule,
            delete_rule,
        });
        if !acc.columns.contains(&col) {
            acc.columns.push(col);
        }
        if !acc.ref_columns.contains(&ref_col) {
            acc.ref_columns.push(ref_col);
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
