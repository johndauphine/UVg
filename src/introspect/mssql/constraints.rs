use tiberius::Client;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use crate::error::UvgError;
use crate::introspect::grouping::{
    foreign_key_constraints, typed_column_constraints, ForeignKeyColumn,
};
use crate::schema::{ConstraintInfo, ConstraintType};

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

    constraints.extend(typed_column_constraints(rows, |row| {
        let name: String = row
            .get::<&str, _>("CONSTRAINT_NAME")
            .unwrap_or("")
            .to_string();
        let ctype_str: &str = row.get::<&str, _>("CONSTRAINT_TYPE").unwrap_or("");
        let col: String = row.get::<&str, _>("COLUMN_NAME").unwrap_or("").to_string();

        let ctype = match ctype_str {
            "PRIMARY KEY" => ConstraintType::PrimaryKey,
            "UNIQUE" => ConstraintType::Unique,
            _ => return None,
        };

        Some((name, ctype, col))
    }));

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

    constraints.extend(foreign_key_constraints(fk_rows.into_iter().map(|row| {
        let name: String = row
            .get::<&str, _>("constraint_name")
            .unwrap_or("")
            .to_string();
        let col: String = row.get::<&str, _>("column_name").unwrap_or("").to_string();
        let ref_schema: String = row.get::<&str, _>("ref_schema").unwrap_or("").to_string();
        let ref_table: String = row.get::<&str, _>("ref_table").unwrap_or("").to_string();
        let ref_col: String = row.get::<&str, _>("ref_column").unwrap_or("").to_string();
        // MSSQL uses underscores in action names: NO_ACTION -> NO ACTION
        let update_rule: String = row
            .get::<&str, _>("update_rule")
            .unwrap_or("NO_ACTION")
            .replace('_', " ");
        let delete_rule: String = row
            .get::<&str, _>("delete_rule")
            .unwrap_or("NO_ACTION")
            .replace('_', " ");

        ForeignKeyColumn {
            constraint_name: name,
            column: col,
            ref_schema,
            ref_table,
            ref_column: ref_col,
            update_rule,
            delete_rule,
        }
    })));

    // CHECK constraints via sys.check_constraints. The `definition` column
    // carries the predicate text MSSQL stores after creation — typically
    // wrapped in parens like `([is_active]=(1) OR [is_active]=(0))`. We
    // pass it through verbatim; the codegen emitter wraps it in `CHECK (..)`.
    // See #33.
    let chk_query = r#"
        SELECT
            cc.name AS constraint_name,
            cc.definition AS predicate
        FROM sys.check_constraints cc
        JOIN sys.tables t ON t.object_id = cc.parent_object_id
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = @P1 AND t.name = @P2
        ORDER BY cc.name
    "#;

    let stream = client.query(chk_query, &[&schema, &table_name]).await?;
    let chk_rows = stream.into_first_result().await?;

    for row in chk_rows {
        let name: String = row
            .get::<&str, _>("constraint_name")
            .unwrap_or("")
            .to_string();
        let predicate: String = row.get::<&str, _>("predicate").unwrap_or("").to_string();
        if name.is_empty() || predicate.is_empty() {
            continue;
        }
        constraints.push(ConstraintInfo::check(name, predicate));
    }

    Ok(constraints)
}
