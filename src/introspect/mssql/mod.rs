mod columns;
mod constraints;
mod indexes;
mod tables;

use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tiberius::{Client, Config, EncryptionLevel};

use crate::cli::GeneratorOptions;
use crate::dialect::Dialect;
use crate::error::UvgError;
use crate::schema::IntrospectedSchema;

/// Establish a connection to a MSSQL server.
pub async fn connect(
    host: &str,
    port: u16,
    database: &str,
    user: &str,
    password: &str,
    trust_cert: bool,
) -> Result<Client<Compat<TcpStream>>, UvgError> {
    let mut config = Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(tiberius::AuthMethod::sql_server(user, password));
    config.encryption(EncryptionLevel::Required);
    if trust_cert {
        config.trust_cert();
    }

    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| UvgError::Connection(format!("TCP connection to {host}:{port} failed: {e}")))?;
    tcp.set_nodelay(true)
        .map_err(|e| UvgError::Connection(format!("Failed to set TCP_NODELAY: {e}")))?;

    let client = Client::connect(config, tcp.compat_write()).await?;
    Ok(client)
}

/// Introspect a MSSQL database and return the full schema metadata.
pub async fn introspect(
    client: &mut Client<Compat<TcpStream>>,
    schemas: &[String],
    table_filter: &[String],
    noviews: bool,
    _options: &GeneratorOptions,
) -> Result<IntrospectedSchema, UvgError> {
    let mut all_tables = Vec::new();

    for schema in schemas {
        let mut schema_tables = tables::query_tables(client, schema, noviews).await?;

        if !table_filter.is_empty() {
            schema_tables.retain(|t| table_filter.contains(&t.name));
        }

        for table in &mut schema_tables {
            table.columns = columns::query_columns(client, &table.schema, &table.name).await?;
            table.constraints =
                constraints::query_constraints(client, &table.schema, &table.name).await?;
            table.indexes = indexes::query_indexes(client, &table.schema, &table.name).await?;
        }

        all_tables.extend(schema_tables);
    }

    // Sort by byte order (case-sensitive) to match sqlacodegen's Python sort
    all_tables.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(IntrospectedSchema {
        dialect: Dialect::Mssql,
        tables: all_tables,
    })
}
