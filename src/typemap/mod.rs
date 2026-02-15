pub mod mssql;
pub mod pg;

use crate::dialect::Dialect;
use crate::schema::ColumnInfo;

/// The result of mapping a database type to its SQLAlchemy representation.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedType {
    /// The SQLAlchemy type expression (e.g. "Integer", "String(100)", "JSONB").
    pub sa_type: String,
    /// The Python type annotation for Mapped[] (e.g. "int", "str", "datetime.datetime").
    pub python_type: String,
    /// The module to import the type from (e.g. "sqlalchemy" or "sqlalchemy.dialects.postgresql").
    pub import_module: String,
    /// The type name to import (e.g. "Integer", "JSONB"). For parameterized types, just the base name.
    pub import_name: String,
    /// For ARRAY types, the element type import info.
    pub element_import: Option<(String, String)>,
}

/// Map a column to its SQLAlchemy type representation, dispatching by dialect.
pub fn map_column_type(col: &ColumnInfo, dialect: Dialect) -> MappedType {
    match dialect {
        Dialect::Postgres => pg::map_column_type(col),
        Dialect::Mssql => mssql::map_column_type(col),
    }
}

/// Helper to create a simple MappedType with no parameters or element imports.
pub fn simple(sa_type: &str, python_type: &str, import_module: &str) -> MappedType {
    MappedType {
        sa_type: sa_type.to_string(),
        python_type: python_type.to_string(),
        import_module: import_module.to_string(),
        import_name: sa_type.to_string(),
        element_import: None,
    }
}
