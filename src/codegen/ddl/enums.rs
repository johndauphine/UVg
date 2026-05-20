use crate::dialect::Dialect;
use crate::schema::IntrospectedSchema;

use super::ident::quote_identifier;

/// Generate CREATE TYPE statements for enums (PG target only).
pub(super) fn generate_enum_types(
    schema: &IntrospectedSchema,
    target_dialect: Dialect,
) -> Vec<String> {
    if target_dialect != Dialect::Postgres || schema.enums.is_empty() {
        return vec![];
    }

    schema
        .enums
        .iter()
        .map(|e| {
            let qname = quote_identifier(&e.name, target_dialect);
            let values: Vec<String> = e
                .values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect();
            format!("CREATE TYPE {qname} AS ENUM ({});", values.join(", "))
        })
        .collect()
}
