use crate::dialect::Dialect;
use crate::schema::TableInfo;

use super::ident::{qualified_table_name, quote_identifier};

/// Generate COMMENT ON statements (PG only; MySQL is inline).
pub(super) fn generate_comments(
    table: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Vec<String> {
    // Only PG uses separate COMMENT ON statements
    if target_dialect != Dialect::Postgres {
        return vec![];
    }

    let mut stmts = Vec::new();
    let tname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);

    if let Some(ref comment) = table.comment {
        stmts.push(format!(
            "COMMENT ON TABLE {tname} IS '{}';",
            comment.replace('\'', "''")
        ));
    }

    for col in &table.columns {
        if let Some(ref comment) = col.comment {
            stmts.push(format!(
                "COMMENT ON COLUMN {tname}.{} IS '{}';",
                quote_identifier(&col.name, target_dialect),
                comment.replace('\'', "''")
            ));
        }
    }

    stmts
}
