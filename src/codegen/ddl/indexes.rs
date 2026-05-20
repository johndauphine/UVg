use crate::codegen::is_unique_constraint_index;
use crate::dialect::Dialect;
use crate::schema::TableInfo;

use super::ident::{qualified_table_name, quote_identifier};

/// Generate CREATE INDEX statements for a table.
pub(in crate::codegen) fn generate_indexes(
    table: &TableInfo,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> Vec<String> {
    let mut stmts = Vec::new();
    let tname = qualified_table_name(&table.schema, &table.name, source_dialect, target_dialect);

    for idx in &table.indexes {
        if is_unique_constraint_index(idx, &table.constraints) {
            continue;
        }

        let unique = if idx.is_unique { "UNIQUE " } else { "" };
        let cols: Vec<String> = idx
            .columns
            .iter()
            .map(|c| quote_identifier(c, target_dialect))
            .collect();
        stmts.push(format!(
            "CREATE {unique}INDEX {} ON {tname} ({});",
            quote_identifier(&idx.name, target_dialect),
            cols.join(", ")
        ));
    }

    stmts
}
