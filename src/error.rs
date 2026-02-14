use thiserror::Error;

#[derive(Error, Debug)]
pub enum UvgError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Unsupported URL scheme: {0}")]
    UnsupportedScheme(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unknown generator: {0}")]
    UnknownGenerator(String),
}
