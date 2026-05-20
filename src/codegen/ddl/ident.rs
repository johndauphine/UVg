use crate::dialect::Dialect;

/// Quote an identifier for the target dialect.
pub(in crate::codegen) fn quote_identifier(name: &str, dialect: Dialect) -> String {
    match dialect {
        Dialect::Postgres | Dialect::Sqlite => format!("\"{}\"", name.replace('"', "\"\"")),
        Dialect::Mysql => format!("`{}`", name.replace('`', "``")),
        Dialect::Mssql => format!("[{}]", name.replace(']', "]]")),
    }
}

/// Generate a qualified table name with schema prefix if non-default.
/// Maps source default schemas to target default schemas (e.g. PG "public" -> MSSQL "dbo").
pub(in crate::codegen) fn qualified_table_name(
    schema: &str,
    table: &str,
    source_dialect: Dialect,
    target_dialect: Dialect,
) -> String {
    let default_schema = target_dialect.default_schema();

    // Suppress schema if it matches the target's default
    if schema.is_empty() || schema == default_schema {
        return quote_identifier(table, target_dialect);
    }

    // Map other dialects' default schemas to the target's default.
    // This covers PG "public" -> MSSQL (use dbo), MSSQL "dbo" -> PG (use public),
    // and SQLite "main".
    let is_source_default_schema = schema == "public" || schema == "dbo" || schema == "main";

    // For MySQL targets, the schema IS the database name — always suppress it
    // since the target connection already specifies the database.
    let suppress_for_mysql_target = target_dialect == Dialect::Mysql;

    // For MySQL SOURCE on a non-MySQL target, the source's "schema" is also
    // the database name (MySQL conflates the two). The user's target DB
    // doesn't have a same-named PG/MSSQL schema, and qualifying tables with
    // it produces `ERROR: schema "crm_mysql" does not exist`. Drop the
    // qualification cross-dialect from MySQL. Same-dialect (mysql->mysql)
    // preserves it via the `target_dialect == Mysql` branch above. See #40.
    let suppress_for_mysql_source =
        source_dialect == Dialect::Mysql && target_dialect != Dialect::Mysql;

    if is_source_default_schema || suppress_for_mysql_target || suppress_for_mysql_source {
        return quote_identifier(table, target_dialect);
    }

    format!(
        "{}.{}",
        quote_identifier(schema, target_dialect),
        quote_identifier(table, target_dialect)
    )
}
