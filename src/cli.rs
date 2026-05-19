use std::path::PathBuf;

use clap::Parser;

use crate::dialect::Dialect;

/// Generate SQLAlchemy model code from an existing database.
///
/// Drop-in compatible reimplementation of sqlacodegen in Rust.
#[derive(Parser, Debug)]
#[command(name = "uvg", version, about)]
pub struct Cli {
    /// Source database URL (e.g. postgresql://, mysql://, sqlite:///path, mssql://)
    pub url: String,

    /// Target database URL for DDL generation/migration (optional)
    pub target_url: Option<String>,

    /// Code generator to use (declarative, tables, ddl)
    #[arg(long, default_value = "declarative")]
    pub generator: String,

    /// Target SQL dialect for DDL output (postgres, mysql, sqlite, mssql)
    #[arg(long)]
    pub target_dialect: Option<String>,

    /// Output one file per table into the outfile directory
    #[arg(long)]
    pub split_tables: bool,

    /// Execute generated DDL against the target database after rendering it.
    /// Requires a target URL. Combines naturally with `--out-dir`: the
    /// per-table files are written first, then applied in manifest order.
    /// Exits non-zero on the first failed statement.
    #[arg(long)]
    pub apply: bool,

    /// Per-statement progress reporting on `--apply`. Default `auto`
    /// emits when stderr is a terminal and stays silent when redirected.
    #[arg(long, value_enum, default_value_t = crate::apply_progress::ProgressMode::Auto)]
    pub progress: crate::apply_progress::ProgressMode,

    /// Maximum retry attempts per statement on `--apply` for transient
    /// errors (deadlock, lock-wait timeout, brief connection drops).
    /// Logical errors (constraint, syntax, missing column) fail
    /// immediately regardless. `0` disables retry; default `3`.
    /// Backoff is 100ms / 500ms / 2s with jitter.
    #[arg(long, default_value_t = 3)]
    pub apply_retries: u8,

    /// Tables to process (comma-delimited). Each item is a glob pattern
    /// (`*`, `?`, `[abc]`); bare names with no metacharacters match
    /// exactly. Default: all tables.
    #[arg(long)]
    pub tables: Option<String>,

    /// Tables to exclude (comma-delimited), evaluated after `--tables`.
    /// Same glob syntax as `--tables`.
    #[arg(long)]
    pub exclude_tables: Option<String>,

    /// Schemas to load (comma-delimited)
    #[arg(long)]
    pub schemas: Option<String>,

    /// Ignore views
    #[arg(long)]
    pub noviews: bool,

    /// Generator options (comma-delimited): noindexes, noconstraints, nocomments, nobidi, nofknames, noidsuffix, nosyntheticenums, nonativeenums, keep_dialect_types
    #[arg(long)]
    pub options: Option<String>,

    /// Output file or directory (default: stdout)
    #[arg(long)]
    pub outfile: Option<String>,

    /// Write per-table DDL diff into this directory. One subdir per
    /// modified table plus `_schema/` for non-table-scoped DDL and
    /// `_runs/` for the manifest. Empty diffs write nothing.
    /// Only meaningful for the `ddl` generator with a target URL.
    /// `--outfile` takes precedence if both are set.
    #[arg(long)]
    pub out_dir: Option<PathBuf>,

    /// Slug used in `--out-dir` filenames. Defaults to
    /// `<source>_to_<target>` (e.g. `postgres_to_mysql`).
    #[arg(long)]
    pub name: Option<String>,

    /// Trust the server certificate (MSSQL only)
    #[arg(long)]
    pub trust_cert: bool,

    /// Launch interactive TUI for DDL diff and apply
    #[arg(long, short = 'i')]
    pub interactive: bool,
}

#[derive(Debug, Default)]
pub struct GeneratorOptions {
    pub noindexes: bool,
    pub noconstraints: bool,
    pub nocomments: bool,
    pub nobidi: bool,
    pub nofknames: bool,
    pub noidsuffix: bool,
    pub nosyntheticenums: bool,
    pub nonativeenums: bool,
    pub keep_dialect_types: bool,
}

/// Options specific to the DDL generator.
#[derive(Debug)]
pub struct DdlOptions {
    pub target_dialect: Dialect,
    pub split_tables: bool,
    pub apply: bool,
    pub noindexes: bool,
    pub noconstraints: bool,
    pub nocomments: bool,
}

/// Parsed connection configuration.
#[derive(Debug)]
pub enum ConnectionConfig {
    Postgres(String),
    Mssql {
        host: String,
        port: u16,
        database: String,
        user: String,
        password: String,
        trust_cert: bool,
    },
    Mysql(String),
    Sqlite(String),
}

impl ConnectionConfig {
    pub fn dialect(&self) -> Dialect {
        match self {
            ConnectionConfig::Postgres(_) => Dialect::Postgres,
            ConnectionConfig::Mssql { .. } => Dialect::Mssql,
            ConnectionConfig::Mysql(_) => Dialect::Mysql,
            ConnectionConfig::Sqlite(_) => Dialect::Sqlite,
        }
    }

    /// Extract the database name from a MySQL connection URL.
    /// Returns `None` if the URL has no database path or it is empty.
    pub fn database_name(&self) -> Option<String> {
        match self {
            ConnectionConfig::Mysql(url) => url::Url::parse(url).ok().and_then(|u| {
                let database = u.path().trim_start_matches('/').to_string();
                if database.is_empty() {
                    None
                } else {
                    Some(database)
                }
            }),
            _ => None,
        }
    }
}

/// Split a comma-delimited CLI value, trimming whitespace and dropping
/// empty entries. `None` / empty string produce an empty vec.
fn split_csv(raw: Option<&str>) -> Vec<String> {
    let Some(s) = raw else { return Vec::new() };
    s.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

/// Ensure a MySQL URL includes `charset=utf8mb4` so that `information_schema`
/// returns proper VARCHAR columns instead of VARBINARY.
fn ensure_mysql_charset(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return url.to_string();
    };

    let has_charset = parsed.query_pairs().any(|(key, _)| key == "charset");
    if !has_charset {
        parsed.query_pairs_mut().append_pair("charset", "utf8mb4");
    }

    parsed.into()
}

impl Cli {
    /// Parse the comma-delimited --tables flag into a Vec of glob patterns.
    /// Bare names with no metacharacters degenerate to exact-match (back-compat
    /// with the original exact-name behavior). Empty / missing flag → empty vec.
    pub fn table_list(&self) -> Vec<String> {
        split_csv(self.tables.as_deref())
    }

    /// Parse the comma-delimited --exclude-tables flag into a Vec of glob
    /// patterns. Same syntax and degeneration rule as `table_list`.
    pub fn exclude_table_list(&self) -> Vec<String> {
        split_csv(self.exclude_tables.as_deref())
    }

    /// Build a `TableFilter` from `--tables` and `--exclude-tables`.
    /// Validates every glob pattern up front so bad input surfaces
    /// before any DB connection is opened.
    pub fn table_filter(&self) -> Result<crate::table_filter::TableFilter, crate::error::UvgError> {
        crate::table_filter::TableFilter::new(&self.table_list(), &self.exclude_table_list())
    }

    /// Parse the comma-delimited --schemas flag, falling back to the given default.
    pub fn schema_list_or(&self, default: &str) -> Vec<String> {
        let raw = self.schemas.as_deref().unwrap_or(default);
        raw.split(',').map(|s| s.trim().to_string()).collect()
    }

    /// Parse the comma-delimited --options flag into structured options.
    pub fn generator_options(&self) -> GeneratorOptions {
        let mut opts = GeneratorOptions::default();
        if let Some(ref options_str) = self.options {
            for opt in options_str.split(',').map(|s| s.trim()) {
                match opt {
                    "noindexes" => opts.noindexes = true,
                    "noconstraints" => opts.noconstraints = true,
                    "nocomments" => opts.nocomments = true,
                    "nobidi" => opts.nobidi = true,
                    "nofknames" => opts.nofknames = true,
                    "noidsuffix" => opts.noidsuffix = true,
                    "nosyntheticenums" => opts.nosyntheticenums = true,
                    "nonativeenums" => opts.nonativeenums = true,
                    "keep_dialect_types" => opts.keep_dialect_types = true,
                    _ => tracing::warn!("Unknown generator option: {}", opt),
                }
            }
        }
        opts
    }

    /// Build DDL-specific options. `source_dialect` is used as the default target
    /// when neither `--target-dialect` nor a target URL is provided.
    pub fn ddl_options(
        &self,
        source_dialect: Dialect,
    ) -> Result<DdlOptions, crate::error::UvgError> {
        let target_dialect = if let Some(ref td) = self.target_dialect {
            td.parse::<Dialect>()
                .map_err(|e| crate::error::UvgError::InvalidDialect(e))?
        } else if let Some(ref target_url) = self.target_url {
            // Infer dialect from target URL scheme
            let cli = Cli {
                url: target_url.clone(),
                target_url: None,
                generator: String::new(),
                target_dialect: None,
                split_tables: false,
                apply: false,
                progress: crate::apply_progress::ProgressMode::Auto,
                apply_retries: 3,
                tables: None,
                exclude_tables: None,
                schemas: None,
                noviews: false,
                options: None,
                outfile: None,
                out_dir: None,
                name: None,
                trust_cert: self.trust_cert,
                interactive: false,
            };
            cli.parse_connection()?.dialect()
        } else {
            source_dialect
        };

        let gen_opts = self.generator_options();
        Ok(DdlOptions {
            target_dialect,
            split_tables: self.split_tables,
            apply: self.apply,
            noindexes: gen_opts.noindexes,
            noconstraints: gen_opts.noconstraints,
            nocomments: gen_opts.nocomments,
        })
    }

    /// Parse a target URL into a `ConnectionConfig`.
    pub fn parse_target_connection(
        &self,
        target_url: &str,
    ) -> Result<ConnectionConfig, crate::error::UvgError> {
        let cli = Cli {
            url: target_url.to_string(),
            target_url: None,
            generator: String::new(),
            target_dialect: None,
            split_tables: false,
            apply: false,
            progress: crate::apply_progress::ProgressMode::Auto,
            apply_retries: 3,
            tables: None,
            exclude_tables: None,
            schemas: None,
            noviews: false,
            options: None,
            outfile: None,
            out_dir: None,
            name: None,
            trust_cert: self.trust_cert,
            interactive: false,
        };
        cli.parse_connection()
    }

    /// Parse the URL into a `ConnectionConfig`.
    pub fn parse_connection(&self) -> Result<ConnectionConfig, crate::error::UvgError> {
        let url = &self.url;

        // PostgreSQL schemes
        if let Some(rest) = url
            .strip_prefix("postgresql+psycopg2://")
            .or_else(|| url.strip_prefix("postgresql+asyncpg://"))
            .or_else(|| url.strip_prefix("postgresql+psycopg://"))
        {
            return Ok(ConnectionConfig::Postgres(format!("postgres://{rest}")));
        }
        if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            return Ok(ConnectionConfig::Postgres(url.clone()));
        }

        // MSSQL schemes
        if url.starts_with("mssql://")
            || url.starts_with("mssql+pytds://")
            || url.starts_with("mssql+pyodbc://")
            || url.starts_with("mssql+pymssql://")
        {
            return self.parse_mssql_url(url);
        }

        // MySQL schemes
        if let Some(rest) = url
            .strip_prefix("mysql+pymysql://")
            .or_else(|| url.strip_prefix("mysql+mysqldb://"))
            .or_else(|| url.strip_prefix("mysql+aiomysql://"))
            .or_else(|| url.strip_prefix("mysql+asyncmy://"))
        {
            return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(&format!(
                "mysql://{rest}"
            ))));
        }
        if let Some(rest) = url
            .strip_prefix("mariadb+pymysql://")
            .or_else(|| url.strip_prefix("mariadb+mysqldb://"))
        {
            return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(&format!(
                "mysql://{rest}"
            ))));
        }
        if let Some(rest) = url.strip_prefix("mariadb://") {
            return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(&format!(
                "mysql://{rest}"
            ))));
        }
        if url.starts_with("mysql://") {
            return Ok(ConnectionConfig::Mysql(ensure_mysql_charset(url)));
        }

        // SQLite schemes
        if let Some(rest) = url.strip_prefix("sqlite:///") {
            // sqlacodegen format: sqlite:///relative or sqlite:////absolute
            // sqlx format: sqlite:relative or sqlite:///absolute
            if rest.starts_with('/') {
                // sqlite:////absolute/path -> sqlite:///absolute/path
                return Ok(ConnectionConfig::Sqlite(format!("sqlite://{rest}")));
            }
            if rest == ":memory:" {
                return Ok(ConnectionConfig::Sqlite("sqlite::memory:".to_string()));
            }
            // sqlite:///relative/path -> sqlite:relative/path
            return Ok(ConnectionConfig::Sqlite(format!("sqlite:{rest}")));
        }

        Err(crate::error::UvgError::UnsupportedScheme(
            url.split("://").next().unwrap_or("unknown").to_string(),
        ))
    }

    fn parse_mssql_url(&self, raw: &str) -> Result<ConnectionConfig, crate::error::UvgError> {
        // Normalize scheme to a url-crate-parseable form
        let normalized = if let Some(rest) = raw.strip_prefix("mssql+pytds://") {
            format!("mssql://{rest}")
        } else if let Some(rest) = raw.strip_prefix("mssql+pyodbc://") {
            format!("mssql://{rest}")
        } else if let Some(rest) = raw.strip_prefix("mssql+pymssql://") {
            format!("mssql://{rest}")
        } else {
            raw.to_string()
        };

        let parsed = url::Url::parse(&normalized)
            .map_err(|e| crate::error::UvgError::Connection(format!("Invalid MSSQL URL: {e}")))?;

        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(1433);
        let database = parsed.path().trim_start_matches('/').to_string();
        if database.is_empty() {
            return Err(crate::error::UvgError::Connection(
                "MSSQL URL must include a database name".to_string(),
            ));
        }
        let user = percent_encoding::percent_decode_str(parsed.username())
            .decode_utf8_lossy()
            .into_owned();
        let password = parsed
            .password()
            .map(|p| {
                percent_encoding::percent_decode_str(p)
                    .decode_utf8_lossy()
                    .into_owned()
            })
            .unwrap_or_default();

        Ok(ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert: self.trust_cert,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_with_url(url: &str) -> Cli {
        Cli {
            url: url.to_string(),
            target_url: None,
            generator: "declarative".to_string(),
            target_dialect: None,
            split_tables: false,
            apply: false,
            progress: crate::apply_progress::ProgressMode::Auto,
            apply_retries: 3,
            tables: None,
            exclude_tables: None,
            schemas: None,
            noviews: false,
            options: None,
            outfile: None,
            out_dir: None,
            name: None,
            trust_cert: false,
            interactive: false,
        }
    }

    #[test]
    fn test_mysql_url() {
        let cli = cli_with_url("mysql://user:pass@localhost/mydb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Mysql);
        assert!(matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4"));
    }

    #[test]
    fn test_mysql_pymysql_url() {
        let cli = cli_with_url("mysql+pymysql://user:pass@localhost/mydb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Mysql);
        assert!(matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4"));
    }

    #[test]
    fn test_mariadb_url() {
        let cli = cli_with_url("mariadb://user:pass@localhost/mydb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Mysql);
        assert!(matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4"));
    }

    #[test]
    fn test_mariadb_pymysql_url() {
        let cli = cli_with_url("mariadb+pymysql://user:pass@localhost/mydb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Mysql);
        assert!(matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4"));
    }

    #[test]
    fn test_mysql_preserves_existing_charset() {
        let cli = cli_with_url("mysql://user:pass@localhost/mydb?charset=latin1");
        let config = cli.parse_connection().unwrap();
        assert!(matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=latin1"));
    }

    #[test]
    fn test_mysql_database_name() {
        let cli = cli_with_url("mysql://user:pass@localhost/testdb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.database_name(), Some("testdb".to_string()));
    }

    #[test]
    fn test_sqlite_relative_path() {
        let cli = cli_with_url("sqlite:///test.db");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Sqlite);
        assert!(matches!(config, ConnectionConfig::Sqlite(ref u) if u == "sqlite:test.db"));
    }

    #[test]
    fn test_sqlite_absolute_path() {
        let cli = cli_with_url("sqlite:////tmp/test.db");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Sqlite);
        assert!(matches!(config, ConnectionConfig::Sqlite(ref u) if u == "sqlite:///tmp/test.db"));
    }

    #[test]
    fn test_sqlite_memory() {
        let cli = cli_with_url("sqlite:///:memory:");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Sqlite);
        assert!(matches!(config, ConnectionConfig::Sqlite(ref u) if u == "sqlite::memory:"));
    }

    #[test]
    fn test_postgres_url_unchanged() {
        let cli = cli_with_url("postgresql://user:pass@localhost/mydb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.dialect(), Dialect::Postgres);
    }

    #[test]
    fn test_unsupported_scheme() {
        let cli = cli_with_url("oracle://user:pass@localhost/mydb");
        let result = cli.parse_connection();
        assert!(result.is_err());
    }

    #[test]
    fn test_non_mysql_database_name() {
        let cli = cli_with_url("postgresql://user:pass@localhost/testdb");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.database_name(), None);
    }

    #[test]
    fn test_mysql_empty_database_name() {
        let cli = cli_with_url("mysql://user:pass@host/");
        let config = cli.parse_connection().unwrap();
        assert_eq!(config.database_name(), None);
    }
}
