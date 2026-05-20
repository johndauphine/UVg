use crate::cli::GeneratorOptions;
use crate::codegen::imports::ImportCollector;
use crate::codegen::relationships::find_inline_fk;
use crate::dialect::Dialect;
use crate::naming::table_to_variable_name;
use crate::schema::TableInfo;
use crate::typemap::{map_column_type, map_column_type_dialect};

/// Generate a Table() for M2M association tables.
/// Columns use ForeignKey() inline (not ForeignKeyConstraint).
pub(super) fn generate_association_table(
    table: &TableInfo,
    imports: &mut ImportCollector,
    options: &GeneratorOptions,
    dialect: Dialect,
    metadata_ref: &str,
) -> String {
    let var_name = table_to_variable_name(&table.name);
    let mut lines: Vec<String> = Vec::new();

    lines.push(format!("{var_name} = Table("));
    lines.push(format!("    '{}', {metadata_ref},", table.name));

    let mut body_items: Vec<String> = Vec::new();

    for col_info in &table.columns {
        let fk = find_inline_fk(&col_info.name, &table.constraints);
        if let Some(fk_constraint) = fk {
            if let Some(ref fk_info) = fk_constraint.foreign_key {
                imports.add("sqlalchemy", "ForeignKey");
                let target = if fk_info.ref_schema != dialect.default_schema() {
                    format!(
                        "{}.{}.{}",
                        fk_info.ref_schema, fk_info.ref_table, fk_info.ref_columns[0]
                    )
                } else {
                    format!("{}.{}", fk_info.ref_table, fk_info.ref_columns[0])
                };
                body_items.push(format!(
                    "Column('{}', ForeignKey('{}'))",
                    col_info.name, target
                ));
            }
        } else {
            let mapped = if options.keep_dialect_types {
                map_column_type_dialect(col_info, dialect)
            } else {
                map_column_type(col_info, dialect)
            };
            imports.add(&mapped.import_module, &mapped.import_name);
            body_items.push(format!("Column('{}', {})", col_info.name, mapped.sa_type));
        }
    }

    if table.schema != dialect.default_schema() {
        body_items.push(format!("schema='{}'", table.schema));
    }

    let last = body_items.len().saturating_sub(1);
    for (i, item) in body_items.iter().enumerate() {
        if i < last {
            lines.push(format!("    {item},"));
        } else {
            lines.push(format!("    {item}"));
        }
    }

    lines.push(")".to_string());
    lines.join("\n")
}
