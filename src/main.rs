mod cli;
mod codegen;
mod ddl_typemap;
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
    // MySQL default schema = database name from URL; others use static defaults.
    let schemas = if let Some(db) = config.database_name() {
        cli.schema_list_or(&db)
    } else {
        cli.schema_list_or(dialect.default_schema())
    };
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
        ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::mysql::introspect(
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
        ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            tracing::debug!("Introspecting schema...");
            let s = introspect::sqlite::introspect(
                &pool,
                &table_filter,
                cli.noviews,
                &options,
            )
            .await;
            pool.close().await;
            s?
        }
    };

    tracing::debug!("Found {} tables/views", schema.tables.len());

    match cli.generator.as_str() {
        "tables" => {
            let output = TablesGenerator.generate(&schema, &options);
            write_output(&output, &cli.outfile)?;
        }
        "declarative" => {
            let output = DeclarativeGenerator.generate(&schema, &options);
            write_output(&output, &cli.outfile)?;
        }
        "ddl" => {
            use crate::codegen::ddl::{DdlGenerator, DdlOutput};

            let ddl_opts = cli.ddl_options(dialect)?;

            // If a target URL is provided, introspect it for diff
            let target_schema = if let Some(ref target_url) = cli.target_url {
                let target_config = cli.parse_target_connection(target_url)?;
                let target_dialect = target_config.dialect();
                let target_schemas = if let Some(db) = target_config.database_name() {
                    cli.schema_list_or(&db)
                } else {
                    cli.schema_list_or(target_dialect.default_schema())
                };
                Some(
                    introspect_with_config(
                        target_config,
                        &target_schemas,
                        &table_filter,
                        cli.noviews,
                        &options,
                    )
                    .await?,
                )
            } else {
                None
            };

            let gen = DdlGenerator;
            let ddl_output = gen.generate(&schema, target_schema.as_ref(), &ddl_opts);

            match ddl_output {
                DdlOutput::Single(content) => {
                    write_output(&content, &cli.outfile)?;
                }
                DdlOutput::Split(files) => {
                    match cli.outfile {
                        Some(ref dir) => {
                            fs::create_dir_all(dir)?;
                            for (filename, content) in &files {
                                let path = format!("{dir}/{filename}");
                                fs::write(&path, content)?;
                                tracing::info!("Written {path}");
                            }
                        }
                        None => {
                            for (filename, content) in &files {
                                println!("-- File: {filename}");
                                print!("{content}\n");
                            }
                        }
                    }
                }
            }
        }
        other => {
            return Err(error::UvgError::UnknownGenerator(other.to_string()).into());
        }
    };

    Ok(())
}

fn write_output(output: &str, outfile: &Option<String>) -> anyhow::Result<()> {
    match outfile {
        Some(ref path) => {
            fs::write(path, output)?;
            tracing::info!("Output written to {path}");
        }
        None => {
            print!("{output}");
        }
    }
    Ok(())
}

/// Introspect a database given a ConnectionConfig (used for target DB in DDL diff).
async fn introspect_with_config(
    config: cli::ConnectionConfig,
    schemas: &[String],
    table_filter: &[String],
    noviews: bool,
    options: &cli::GeneratorOptions,
) -> anyhow::Result<schema::IntrospectedSchema> {
    match config {
        cli::ConnectionConfig::Postgres(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s = introspect::pg::introspect(&pool, schemas, table_filter, noviews, options)
                .await;
            pool.close().await;
            Ok(s?)
        }
        cli::ConnectionConfig::Mssql {
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
            Ok(
                introspect::mssql::introspect(
                    &mut client,
                    schemas,
                    table_filter,
                    noviews,
                    options,
                )
                .await?,
            )
        }
        cli::ConnectionConfig::Mysql(url) => {
            let pool = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s =
                introspect::mysql::introspect(&pool, schemas, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
        cli::ConnectionConfig::Sqlite(url) => {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await?;
            let s = introspect::sqlite::introspect(&pool, table_filter, noviews, options).await;
            pool.close().await;
            Ok(s?)
        }
    }
}
