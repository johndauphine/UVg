use crate::dialect::Dialect;

/// Represents an introspected database schema containing all tables and their metadata.
#[derive(Debug, Clone)]
pub struct IntrospectedSchema {
    pub dialect: Dialect,
    pub tables: Vec<TableInfo>,
}

/// Metadata for a single table or view.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub table_type: TableType,
    pub comment: Option<String>,
    pub columns: Vec<ColumnInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub indexes: Vec<IndexInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableType {
    Table,
    View,
}

/// Metadata for a single column.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnInfo {
    pub name: String,
    pub ordinal_position: i32,
    pub is_nullable: bool,
    pub data_type: String,
    pub udt_name: String,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub column_default: Option<String>,
    pub is_identity: bool,
    pub identity_generation: Option<String>,
    pub identity: Option<IdentityInfo>,
    pub comment: Option<String>,
    pub collation: Option<String>,
}

/// Parameters for an identity column's underlying sequence.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct IdentityInfo {
    pub start: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
    pub cache: i64,
}

/// Metadata for a constraint (PK, FK, Unique, Check).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub columns: Vec<String>,
    /// For foreign keys: the referenced schema, table, and columns.
    pub foreign_key: Option<ForeignKeyInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    Unique,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ForeignKeyInfo {
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub update_rule: String,
    pub delete_rule: String,
}

/// Metadata for a database index.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub is_unique: bool,
    pub columns: Vec<String>,
}
