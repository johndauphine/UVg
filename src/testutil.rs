use crate::dialect::Dialect;
use crate::schema::*;

/// Create a ColumnInfo with sensible defaults for testing.
/// Returns a non-nullable int4 column with no defaults, no identity, and no comment.
pub fn test_column(name: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        ordinal_position: 1,
        is_nullable: false,
        data_type: String::new(),
        udt_name: "int4".to_string(),
        character_maximum_length: None,
        numeric_precision: None,
        numeric_scale: None,
        column_default: None,
        is_identity: false,
        identity_generation: None,
        identity: None,
        comment: None,
        collation: None,
    }
}

/// Builder for constructing ColumnInfo in tests.
pub struct ColumnInfoBuilder {
    inner: ColumnInfo,
}

impl ColumnInfoBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            inner: test_column(name),
        }
    }

    pub fn udt(mut self, udt: &str) -> Self {
        self.inner.udt_name = udt.to_string();
        self
    }

    pub fn nullable(mut self) -> Self {
        self.inner.is_nullable = true;
        self
    }

    pub fn not_null(mut self) -> Self {
        self.inner.is_nullable = false;
        self
    }

    pub fn max_length(mut self, len: i32) -> Self {
        self.inner.character_maximum_length = Some(len);
        self
    }

    pub fn precision(mut self, p: i32, s: i32) -> Self {
        self.inner.numeric_precision = Some(p);
        self.inner.numeric_scale = Some(s);
        self
    }

    pub fn default_val(mut self, d: &str) -> Self {
        self.inner.column_default = Some(d.to_string());
        self
    }

    pub fn identity(mut self) -> Self {
        self.inner.is_identity = true;
        self.inner.identity_generation = Some("ALWAYS".to_string());
        self
    }

    pub fn identity_info(mut self, info: IdentityInfo) -> Self {
        self.inner.is_identity = true;
        self.inner.identity_generation = Some("ALWAYS".to_string());
        self.inner.identity = Some(info);
        self
    }

    pub fn comment(mut self, c: &str) -> Self {
        self.inner.comment = Some(c.to_string());
        self
    }

    pub fn collation(mut self, c: &str) -> Self {
        self.inner.collation = Some(c.to_string());
        self
    }

    pub fn build(self) -> ColumnInfo {
        self.inner
    }
}

/// Builder for constructing TableInfo in tests.
pub struct TableInfoBuilder {
    inner: TableInfo,
    next_ordinal: i32,
}

impl TableInfoBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            inner: TableInfo {
                schema: "public".to_string(),
                name: name.to_string(),
                table_type: TableType::Table,
                comment: None,
                columns: vec![],
                constraints: vec![],
                indexes: vec![],
            },
            next_ordinal: 1,
        }
    }

    pub fn schema(mut self, s: &str) -> Self {
        self.inner.schema = s.to_string();
        self
    }

    pub fn column(mut self, mut col: ColumnInfo) -> Self {
        col.ordinal_position = self.next_ordinal;
        self.next_ordinal += 1;
        self.inner.columns.push(col);
        self
    }

    pub fn pk(mut self, name: &str, cols: &[&str]) -> Self {
        self.inner.constraints.push(ConstraintInfo {
            name: name.to_string(),
            constraint_type: ConstraintType::PrimaryKey,
            columns: cols.iter().map(|s| s.to_string()).collect(),
            foreign_key: None,
        });
        self
    }

    pub fn unique(mut self, name: &str, cols: &[&str]) -> Self {
        self.inner.constraints.push(ConstraintInfo {
            name: name.to_string(),
            constraint_type: ConstraintType::Unique,
            columns: cols.iter().map(|s| s.to_string()).collect(),
            foreign_key: None,
        });
        self
    }

    pub fn fk(
        mut self,
        name: &str,
        local_cols: &[&str],
        ref_table: &str,
        ref_cols: &[&str],
    ) -> Self {
        self.inner.constraints.push(ConstraintInfo {
            name: name.to_string(),
            constraint_type: ConstraintType::ForeignKey,
            columns: local_cols.iter().map(|s| s.to_string()).collect(),
            foreign_key: Some(ForeignKeyInfo {
                ref_schema: "public".to_string(),
                ref_table: ref_table.to_string(),
                ref_columns: ref_cols.iter().map(|s| s.to_string()).collect(),
                update_rule: "NO ACTION".to_string(),
                delete_rule: "NO ACTION".to_string(),
            }),
        });
        self
    }

    pub fn fk_full(
        mut self,
        name: &str,
        local_cols: &[&str],
        ref_schema: &str,
        ref_table: &str,
        ref_cols: &[&str],
        update_rule: &str,
        delete_rule: &str,
    ) -> Self {
        self.inner.constraints.push(ConstraintInfo {
            name: name.to_string(),
            constraint_type: ConstraintType::ForeignKey,
            columns: local_cols.iter().map(|s| s.to_string()).collect(),
            foreign_key: Some(ForeignKeyInfo {
                ref_schema: ref_schema.to_string(),
                ref_table: ref_table.to_string(),
                ref_columns: ref_cols.iter().map(|s| s.to_string()).collect(),
                update_rule: update_rule.to_string(),
                delete_rule: delete_rule.to_string(),
            }),
        });
        self
    }

    pub fn index(mut self, name: &str, cols: &[&str], unique: bool) -> Self {
        self.inner.indexes.push(IndexInfo {
            name: name.to_string(),
            is_unique: unique,
            columns: cols.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    pub fn comment(mut self, c: &str) -> Self {
        self.inner.comment = Some(c.to_string());
        self
    }

    pub fn table_type(mut self, tt: TableType) -> Self {
        self.inner.table_type = tt;
        self
    }

    pub fn build(self) -> TableInfo {
        self.inner
    }
}

/// Shorthand for creating an IntrospectedSchema with Postgres dialect.
pub fn schema_pg(tables: Vec<TableInfo>) -> IntrospectedSchema {
    IntrospectedSchema {
        dialect: Dialect::Postgres,
        tables,
    }
}

/// Shorthand for creating an IntrospectedSchema with MSSQL dialect.
pub fn schema_mssql(tables: Vec<TableInfo>) -> IntrospectedSchema {
    IntrospectedSchema {
        dialect: Dialect::Mssql,
        tables,
    }
}

/// Shorthand constructors for column builder.
pub fn col(name: &str) -> ColumnInfoBuilder {
    ColumnInfoBuilder::new(name)
}

/// Shorthand constructor for table builder.
pub fn table(name: &str) -> TableInfoBuilder {
    TableInfoBuilder::new(name)
}
