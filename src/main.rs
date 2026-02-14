mod cli;
mod codegen;
mod error;
mod introspect;
mod naming;
mod schema;
mod typemap;

use std::fs;

use anyhow::Result;
use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;
use crate::codegen::declarative::DeclarativeGenerator;
use crate::codegen::tables::TablesGenerator;
use crate::codegen::Generator;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let conn_url = cli.connection_url()?;
    let schemas = cli.schema_list();
    let table_filter = cli.table_list();
    let options = cli.generator_options();

    tracing::debug!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&conn_url)
        .await?;

    tracing::debug!("Introspecting schema...");
    let schema =
        introspect::introspect(&pool, &schemas, &table_filter, cli.noviews, &options).await?;

    tracing::debug!("Found {} tables/views", schema.tables.len());

    let output = match cli.generator.as_str() {
        "tables" => {
            let gen = TablesGenerator;
            gen.generate(&schema, &options)
        }
        "declarative" => {
            let gen = DeclarativeGenerator;
            gen.generate(&schema, &options)
        }
        other => {
            return Err(error::UvgError::UnknownGenerator(other.to_string()).into());
        }
    };

    match cli.outfile {
        Some(ref path) => {
            fs::write(path, &output)?;
            tracing::info!("Output written to {path}");
        }
        None => {
            print!("{output}");
        }
    }

    pool.close().await;
    Ok(())
}
