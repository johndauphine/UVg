use clap::Parser;

use crate::dialect::Dialect;

/// Generate SQLAlchemy model code from an existing database.
///
/// Drop-in compatible reimplementation of sqlacodegen in Rust.
#[derive(Parser, Debug)]
#[command(name = "uvg", version, about)]
pub struct Cli {
    /// SQLAlchemy-style database URL (e.g. postgresql://user:pass@localhost/mydb)
    pub url: String,

    /// Code generator to use
    #[arg(long, default_value = "declarative")]
    pub generator: String,

    /// Tables to process (comma-delimited)
    #[arg(long)]
    pub tables: Option<String>,

    /// Schemas to load (comma-delimited)
    #[arg(long)]
    pub schemas: Option<String>,

    /// Ignore views
    #[arg(long)]
    pub noviews: bool,

    /// Generator options (comma-delimited): noindexes, noconstraints, nocomments
    #[arg(long)]
    pub options: Option<String>,

    /// Output file (default: stdout)
    #[arg(long)]
    pub outfile: Option<String>,

    /// Trust the server certificate (MSSQL only)
    #[arg(long)]
    pub trust_cert: bool,
}

#[derive(Debug, Default)]
pub struct GeneratorOptions {
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
}

impl ConnectionConfig {
    pub fn dialect(&self) -> Dialect {
        match self {
            ConnectionConfig::Postgres(_) => Dialect::Postgres,
            ConnectionConfig::Mssql { .. } => Dialect::Mssql,
        }
    }
}

impl Cli {
    /// Parse the comma-delimited --tables flag into a Vec of table names.
    pub fn table_list(&self) -> Vec<String> {
        self.tables
            .as_deref()
            .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
            .unwrap_or_default()
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
                    _ => tracing::warn!("Unknown generator option: {}", opt),
                }
            }
        }
        opts
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
