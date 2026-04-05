use thiserror::Error;

#[derive(Error, Debug)]
pub enum UvgError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("MSSQL error: {0}")]
    Mssql(#[from] tiberius::error::Error),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Unsupported URL scheme: {0}")]
    UnsupportedScheme(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unknown generator: {0}")]
    UnknownGenerator(String),

    #[error("Invalid dialect: {0}")]
    InvalidDialect(String),

    #[error("--{0} is only valid with --generator ddl")]
    DdlOnlyOption(String),
}
