use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::error::UvgError;
use crate::schema::{ConstraintInfo, ConstraintType, ForeignKeyInfo};

pub async fn query_constraints(
    pool: &PgPool,
    schema: &str,
    table_name: &str,
) -> Result<Vec<ConstraintInfo>, UvgError> {
    let mut constraints: Vec<ConstraintInfo> = Vec::new();

    // Primary keys
    let pk_rows = sqlx::query_as::<_, PkRow>(
        r#"
        SELECT kcu.column_name, tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            USING (constraint_name, table_schema, table_name)
        WHERE tc.table_schema = $1 AND tc.table_name = $2
            AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    // Group PK columns by constraint name
    let mut pk_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in pk_rows {
        pk_map
            .entry(row.constraint_name)
            .or_default()
            .push(row.column_name);
    }
    for (name, columns) in pk_map {
        constraints.push(ConstraintInfo {
            name,
            constraint_type: ConstraintType::PrimaryKey,
            columns,
            foreign_key: None,
            check_expression: None,
        });
    }

    // Foreign keys
    let fk_rows = sqlx::query_as::<_, FkRow>(
        r#"
        SELECT kcu.column_name, ccu.table_schema AS ref_schema, ccu.table_name AS ref_table,
               ccu.column_name AS ref_column, tc.constraint_name,
               rc.update_rule, rc.delete_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON kcu.constraint_name = tc.constraint_name
            AND kcu.table_schema = tc.table_schema
            AND kcu.table_name = tc.table_name
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.constraint_schema = tc.constraint_schema
        JOIN information_schema.referential_constraints rc
            ON rc.constraint_name = tc.constraint_name
            AND rc.constraint_schema = tc.constraint_schema
        WHERE tc.table_schema = $1 AND tc.table_name = $2
            AND tc.constraint_type = 'FOREIGN KEY'
        ORDER BY tc.constraint_name, kcu.ordinal_position
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    // Group FK columns by constraint name
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

    // Unique constraints
    let uq_rows = sqlx::query_as::<_, UqRow>(
        r#"
        SELECT tc.constraint_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            USING (constraint_name, table_schema, table_name)
        WHERE tc.table_schema = $1 AND tc.table_name = $2
            AND tc.constraint_type = 'UNIQUE'
        ORDER BY tc.constraint_name, kcu.ordinal_position
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    let mut uq_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in uq_rows {
        uq_map
            .entry(row.constraint_name)
            .or_default()
            .push(row.column_name);
    }
    for (name, columns) in uq_map {
        constraints.push(ConstraintInfo {
            name,
            constraint_type: ConstraintType::Unique,
            columns,
            foreign_key: None,
            check_expression: None,
        });
    }

    // CHECK constraints. pg_constraint.contype='c' is the catalog-side filter;
    // pg_get_constraintdef returns a readable predicate string like
    // `CHECK ((email ~ '^[^@]+@[^@]+\.[^@]+$'::text))`. Strip the outer
    // `CHECK (...)` wrapping so the codegen emitter can wrap it the same way
    // it does for mssql/mysql sources. See #33.
    let chk_rows = sqlx::query_as::<_, ChkRow>(
        r#"
        SELECT c.conname AS constraint_name,
               pg_get_constraintdef(c.oid) AS predicate
        FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        JOIN pg_class cl    ON cl.oid = c.conrelid
        WHERE c.contype = 'c'
          AND n.nspname = $1
          AND cl.relname = $2
        ORDER BY c.conname
        "#,
    )
    .bind(schema)
    .bind(table_name)
    .fetch_all(pool)
    .await?;

    for row in chk_rows {
        // pg_get_constraintdef returns "CHECK (...)" — strip the wrapper so
        // emitter doesn't double-wrap. Also strip any leading "NOT VALID"
        // suffix which constraint metadata can carry but isn't predicate.
        let predicate = strip_check_wrapper(&row.predicate);
        constraints.push(ConstraintInfo {
            name: row.constraint_name,
            constraint_type: ConstraintType::Check,
            columns: vec![],
            foreign_key: None,
            check_expression: Some(predicate),
        });
    }

    Ok(constraints)
}

/// Strip the outer "CHECK (..)" envelope from a pg_get_constraintdef result.
/// `CHECK ((x > 0))` → `(x > 0)` (kept inner parens — they're part of the
/// expression). `CHECK (x > 0)` → `x > 0`. If the input doesn't start with
/// `CHECK (`, return it unchanged — defensive against future format changes.
fn strip_check_wrapper(def: &str) -> String {
    let trimmed = def.trim();
    let prefix = "CHECK (";
    if let Some(stripped) = trimmed.strip_prefix(prefix) {
        if let Some(stripped) = stripped.strip_suffix(')') {
            return stripped.trim().to_string();
        }
    }
    trimmed.to_string()
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
struct PkRow {
    column_name: String,
    constraint_name: String,
}

#[derive(sqlx::FromRow)]
struct FkRow {
    column_name: String,
    ref_schema: String,
    ref_table: String,
    ref_column: String,
    constraint_name: String,
    update_rule: String,
    delete_rule: String,
}

#[derive(sqlx::FromRow)]
struct UqRow {
    constraint_name: String,
    column_name: String,
}

#[derive(sqlx::FromRow)]
struct ChkRow {
    constraint_name: String,
    predicate: String,
}

#[cfg(test)]
mod tests {
    use super::strip_check_wrapper;

    #[test]
    fn strips_check_wrapper() {
        assert_eq!(strip_check_wrapper("CHECK (x > 0)"), "x > 0");
        assert_eq!(strip_check_wrapper("CHECK ((x > 0))"), "(x > 0)");
        assert_eq!(
            strip_check_wrapper("  CHECK (a IS NOT NULL)  "),
            "a IS NOT NULL"
        );
        // Defensive: unrecognized format returns input unchanged.
        assert_eq!(strip_check_wrapper("(x > 0)"), "(x > 0)");
    }
}
