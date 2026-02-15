/// Supported database backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mssql,
}

impl Dialect {
    /// Return the default schema name for this dialect.
    pub fn default_schema(&self) -> &'static str {
        match self {
            Dialect::Postgres => "public",
            Dialect::Mssql => "dbo",
        }
    }
}
