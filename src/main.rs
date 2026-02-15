mod cli;
mod codegen;
mod dialect;
mod error;
mod introspect;
mod naming;
mod schema;
#[cfg(test)]
mod testutil;
mod typemap;

use std::fs;

use anyhow::Result;
use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use crate::cli::{Cli, ConnectionConfig};
use crate::codegen::declarative::DeclarativeGenerator;
use crate::codegen::tables::TablesGenerator;
use crate::codegen::Generator;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();

    let config = cli.parse_connection()?;
    let dialect = config.dialect();
    let schemas = cli.schema_list_or(dialect.default_schema());
    let table_filter = cli.table_list();
    let options = cli.generator_options();

    tracing::debug!("Connecting to database...");

    let schema = match config {
        ConnectionConfig::Postgres(url) => {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::pg::introspect(
                &pool,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
        ConnectionConfig::Mssql {
            host,
            port,
            database,
            user,
            password,
            trust_cert,
        } => {
            let mut client =
                introspect::mssql::connect(&host, port, &database, &user, &password, trust_cert)
                    .await?;
            tracing::debug!("Introspecting schema...");
            introspect::mssql::introspect(
                &mut client,
                &schemas,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await?
        }
    };

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

    Ok(())
}
