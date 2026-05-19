use super::*;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn cli_with_url(url: &str) -> Cli {
    Cli {
        command: None,
        profile: None,
        url: Some(url.to_string()),
        target_url: None,
        generator: "declarative".to_string(),
        target_dialect: None,
        split_tables: false,
        apply: false,
        progress: crate::apply_progress::ProgressMode::Auto,
        apply_retries: 3,
        no_parse_check: false,
        risk_classify: false,
        introspect_concurrency: DEFAULT_INTROSPECT_CONCURRENCY,
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
fn introspect_concurrency_defaults_to_eight() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::remove_var("UVG_INTROSPECT_CONCURRENCY");

    let cli = Cli::try_parse_from(["uvg", "sqlite:///tmp.db"]).unwrap();

    assert_eq!(cli.introspect_concurrency, DEFAULT_INTROSPECT_CONCURRENCY);
}

#[test]
fn introspect_concurrency_flag_overrides_default() {
    let cli =
        Cli::try_parse_from(["uvg", "--introspect-concurrency", "3", "sqlite:///tmp.db"]).unwrap();

    assert_eq!(cli.introspect_concurrency, 3);
}

#[test]
fn introspect_concurrency_env_is_supported() {
    let _guard = ENV_LOCK.lock().unwrap();
    std::env::set_var("UVG_INTROSPECT_CONCURRENCY", "5");
    let cli = Cli::try_parse_from(["uvg", "sqlite:///tmp.db"]).unwrap();
    std::env::remove_var("UVG_INTROSPECT_CONCURRENCY");

    assert_eq!(cli.introspect_concurrency, 5);
}

#[test]
fn introspect_concurrency_rejects_zero() {
    let err = Cli::try_parse_from(["uvg", "--introspect-concurrency", "0", "sqlite:///tmp.db"])
        .unwrap_err();

    assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
}

#[test]
fn risk_classify_flag_parses() {
    let cli = Cli::try_parse_from(["uvg", "--risk-classify", "sqlite:///tmp.db"]).unwrap();

    assert!(cli.risk_classify);
}

#[test]
fn test_mysql_url() {
    let cli = cli_with_url("mysql://user:pass@localhost/mydb");
    let config = cli.parse_connection().unwrap();
    assert_eq!(config.dialect(), Dialect::Mysql);
    assert!(
        matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4")
    );
}

#[test]
fn test_mysql_pymysql_url() {
    let cli = cli_with_url("mysql+pymysql://user:pass@localhost/mydb");
    let config = cli.parse_connection().unwrap();
    assert_eq!(config.dialect(), Dialect::Mysql);
    assert!(
        matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4")
    );
}

#[test]
fn test_mariadb_url() {
    let cli = cli_with_url("mariadb://user:pass@localhost/mydb");
    let config = cli.parse_connection().unwrap();
    assert_eq!(config.dialect(), Dialect::Mysql);
    assert!(
        matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4")
    );
}

#[test]
fn test_mariadb_pymysql_url() {
    let cli = cli_with_url("mariadb+pymysql://user:pass@localhost/mydb");
    let config = cli.parse_connection().unwrap();
    assert_eq!(config.dialect(), Dialect::Mysql);
    assert!(
        matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=utf8mb4")
    );
}

#[test]
fn test_mysql_preserves_existing_charset() {
    let cli = cli_with_url("mysql://user:pass@localhost/mydb?charset=latin1");
    let config = cli.parse_connection().unwrap();
    assert!(
        matches!(config, ConnectionConfig::Mysql(ref u) if u == "mysql://user:pass@localhost/mydb?charset=latin1")
    );
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
