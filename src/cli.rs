use clap::Parser;

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
    #[arg(long, default_value = "public")]
    pub schemas: String,

    /// Ignore views
    #[arg(long)]
    pub noviews: bool,

    /// Generator options (comma-delimited): noindexes, noconstraints, nocomments, use_inflect, nojoined, nobidi
    #[arg(long)]
    pub options: Option<String>,

    /// Output file (default: stdout)
    #[arg(long)]
    pub outfile: Option<String>,
}

#[derive(Debug, Default)]
pub struct GeneratorOptions {
    pub noindexes: bool,
    pub noconstraints: bool,
    pub nocomments: bool,
    pub use_inflect: bool,
    pub nojoined: bool,
    pub nobidi: bool,
}

impl Cli {
    /// Parse the comma-delimited --tables flag into a Vec of table names.
    pub fn table_list(&self) -> Vec<String> {
        self.tables
            .as_deref()
            .map(|s| s.split(',').map(|t| t.trim().to_string()).collect())
            .unwrap_or_default()
    }

    /// Parse the comma-delimited --schemas flag into a Vec of schema names.
    pub fn schema_list(&self) -> Vec<String> {
        self.schemas
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
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
                    "use_inflect" => opts.use_inflect = true,
                    "nojoined" => opts.nojoined = true,
                    "nobidi" => opts.nobidi = true,
                    _ => tracing::warn!("Unknown generator option: {}", opt),
                }
            }
        }
        opts
    }

    /// Convert the SQLAlchemy-style URL to a native connection string.
    /// Handles postgresql:// and postgresql+psycopg2:// style URLs.
    pub fn connection_url(&self) -> Result<String, crate::error::UvgError> {
        let url = &self.url;
        // Accept postgresql://, postgresql+psycopg2://, postgres://
        if let Some(rest) = url
            .strip_prefix("postgresql+psycopg2://")
            .or_else(|| url.strip_prefix("postgresql+asyncpg://"))
            .or_else(|| url.strip_prefix("postgresql+psycopg://"))
        {
            Ok(format!("postgres://{rest}"))
        } else if url.starts_with("postgresql://") || url.starts_with("postgres://") {
            Ok(url.clone())
        } else {
            Err(crate::error::UvgError::UnsupportedScheme(
                url.split("://").next().unwrap_or("unknown").to_string(),
            ))
        }
    }
}
