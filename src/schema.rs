use crate::dialect::Dialect;
use serde::{Deserialize, Serialize};

/// Represents an introspected database schema containing all tables and their metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectedSchema {
    pub dialect: Dialect,
    pub tables: Vec<TableInfo>,
    /// Named enum types defined in the database.
    pub enums: Vec<EnumInfo>,
    /// Domain types defined in the database.
    pub domains: Vec<DomainInfo>,
}

/// A PostgreSQL domain type wrapping a base type with constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainInfo {
    pub name: String,
    pub schema: Option<String>,
    pub base_type: String,
    pub constraint_name: Option<String>,
    pub not_null: bool,
    pub check_expression: Option<String>,
}

/// A named enum type in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumInfo {
    pub name: String,
    pub schema: Option<String>,
    pub values: Vec<String>,
}

/// Metadata for a single table or view.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl TableInfo {
    pub fn new(schema: impl Into<String>, name: impl Into<String>, table_type: TableType) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
            table_type,
            comment: None,
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
        }
    }

    pub fn with_comment(mut self, comment: Option<impl Into<String>>) -> Self {
        self.comment = comment.map(Into::into);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableType {
    Table,
    View,
}

/// Metadata for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub autoincrement: Option<bool>,
}

impl ColumnInfo {
    pub fn new(
        name: impl Into<String>,
        ordinal_position: i32,
        is_nullable: bool,
        data_type: impl Into<String>,
        udt_name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            ordinal_position,
            is_nullable,
            data_type: data_type.into(),
            udt_name: udt_name.into(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            column_default: None,
            is_identity: false,
            identity_generation: None,
            identity: None,
            comment: None,
            collation: None,
            autoincrement: None,
        }
    }
}

/// Parameters for an identity column's underlying sequence.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct IdentityInfo {
    pub start: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
    pub cache: i64,
}

impl IdentityInfo {
    pub fn new(
        start: i64,
        increment: i64,
        min_value: i64,
        max_value: i64,
        cycle: bool,
        cache: i64,
    ) -> Self {
        Self {
            start,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
        }
    }
}

/// Metadata for a constraint (PK, FK, Unique, Check).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: ConstraintType,
    pub columns: Vec<String>,
    /// For foreign keys: the referenced schema, table, and columns.
    pub foreign_key: Option<ForeignKeyInfo>,
    /// For check constraints: the SQL expression.
    pub check_expression: Option<String>,
}

impl ConstraintInfo {
    pub fn primary_key(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::simple(name, ConstraintType::PrimaryKey, columns)
    }

    pub fn unique(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::simple(name, ConstraintType::Unique, columns)
    }

    pub fn foreign_key(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
        foreign_key: ForeignKeyInfo,
    ) -> Self {
        Self {
            name: name.into(),
            constraint_type: ConstraintType::ForeignKey,
            columns: collect_strings(columns),
            foreign_key: Some(foreign_key),
            check_expression: None,
        }
    }

    pub fn check(name: impl Into<String>, expression: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            constraint_type: ConstraintType::Check,
            columns: Vec::new(),
            foreign_key: None,
            check_expression: Some(expression.into()),
        }
    }

    fn simple(
        name: impl Into<String>,
        constraint_type: ConstraintType,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            constraint_type,
            columns: collect_strings(columns),
            foreign_key: None,
            check_expression: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    Unique,
    Check,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct ForeignKeyInfo {
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub update_rule: String,
    pub delete_rule: String,
}

impl ForeignKeyInfo {
    pub fn new(
        ref_schema: impl Into<String>,
        ref_table: impl Into<String>,
        ref_columns: impl IntoIterator<Item = impl Into<String>>,
        update_rule: impl Into<String>,
        delete_rule: impl Into<String>,
    ) -> Self {
        Self {
            ref_schema: ref_schema.into(),
            ref_table: ref_table.into(),
            ref_columns: collect_strings(ref_columns),
            update_rule: update_rule.into(),
            delete_rule: delete_rule.into(),
        }
    }
}

/// Metadata for a database index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub is_unique: bool,
    pub columns: Vec<String>,
    /// Dialect-specific index kwargs (e.g. postgresql_using, mysql_length).
    pub kwargs: std::collections::BTreeMap<String, String>,
}

impl IndexInfo {
    pub fn new(
        name: impl Into<String>,
        is_unique: bool,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            is_unique,
            columns: collect_strings(columns),
            kwargs: std::collections::BTreeMap::new(),
        }
    }
}

fn collect_strings(values: impl IntoIterator<Item = impl Into<String>>) -> Vec<String> {
    values.into_iter().map(Into::into).collect()
}
