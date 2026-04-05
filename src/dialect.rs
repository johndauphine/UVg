use std::fmt;
use std::str::FromStr;

/// Supported database backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mssql,
    Mysql,
    Sqlite,
}

impl Dialect {
    /// Return the default schema name for this dialect.
    /// For MySQL the real default is the database name (dynamic); callers
    /// should use `ConnectionConfig::database_name()` instead.
    pub fn default_schema(&self) -> &'static str {
        match self {
            Dialect::Postgres => "public",
            Dialect::Mssql => "dbo",
            Dialect::Mysql => "",
            Dialect::Sqlite => "main",
        }
    }
}

impl FromStr for Dialect {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "postgres" | "postgresql" | "pg" => Ok(Dialect::Postgres),
            "mysql" | "mariadb" => Ok(Dialect::Mysql),
            "sqlite" => Ok(Dialect::Sqlite),
            "mssql" | "sqlserver" => Ok(Dialect::Mssql),
            _ => Err(format!(
                "Unknown dialect '{s}'. Expected: postgres, mysql, sqlite, mssql"
            )),
        }
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Dialect::Postgres => write!(f, "postgres"),
            Dialect::Mssql => write!(f, "mssql"),
            Dialect::Mysql => write!(f, "mysql"),
            Dialect::Sqlite => write!(f, "sqlite"),
        }
    }
}
