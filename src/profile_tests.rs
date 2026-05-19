use super::*;
use crate::apply_progress::ProgressMode;

fn default_cli(profile: &str) -> Cli {
    Cli {
        command: None,
        profile: Some(profile.to_string()),
        url: None,
        target_url: None,
        generator: "declarative".to_string(),
        target_dialect: None,
        split_tables: false,
        apply: false,
        progress: ProgressMode::Auto,
        apply_retries: 3,
        no_parse_check: false,
        risk_classify: false,
        introspect_concurrency: crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
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

fn temp_profile_path(name: &str) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("uvg-profile-test-{}-{nonce}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn write_profile(contents: &str) -> PathBuf {
    let path = temp_profile_path("profiles.yaml");
    fs::write(&path, contents).unwrap();
    path
}

#[test]
fn profile_fills_empty_cli_fields() {
    let path = write_profile(
        r#"
profiles:
  prod:
    source: postgresql://src/db
    target: mysql://target/db
    generator: ddl
    target_dialect: postgres
    schemas: [public, audit]
    exclude_tables: ["__*"]
    noviews: true
"#,
    );
    let mut cli = default_cli("prod");

    apply_requested_profile_from_path(&mut cli, &ProfileValueSources::default(), &path).unwrap();

    assert_eq!(cli.url.as_deref(), Some("postgresql://src/db"));
    assert_eq!(cli.target_url.as_deref(), Some("mysql://target/db"));
    assert_eq!(cli.generator, "ddl");
    assert_eq!(cli.target_dialect.as_deref(), Some("postgres"));
    assert_eq!(cli.schemas.as_deref(), Some("public,audit"));
    assert_eq!(cli.exclude_tables.as_deref(), Some("__*"));
    assert!(cli.noviews);
}

#[test]
fn command_line_values_override_profile() {
    let path = write_profile(
        r#"
profiles:
  prod:
    source: postgresql://profile/db
    generator: ddl
    schemas: [profile]
"#,
    );
    let mut cli = default_cli("prod");
    cli.url = Some("postgresql://cli/db".to_string());
    cli.generator = "declarative".to_string();
    let mut sources = ProfileValueSources::default();
    sources.command_line.insert("url");
    sources.command_line.insert("generator");

    apply_requested_profile_from_path(&mut cli, &sources, &path).unwrap();

    assert_eq!(cli.url.as_deref(), Some("postgresql://cli/db"));
    assert_eq!(cli.generator, "declarative");
    assert_eq!(cli.schemas.as_deref(), Some("profile"));
}

#[test]
fn missing_profile_file_reports_path() {
    let mut cli = default_cli("prod");
    let path = temp_profile_path("missing.yaml");

    let err = apply_requested_profile_from_path(&mut cli, &ProfileValueSources::default(), &path)
        .unwrap_err()
        .to_string();

    assert!(err.contains("profile `prod` requested"));
    assert!(err.contains(path.to_str().unwrap()));
}

#[test]
fn unknown_profile_lists_available_profiles() {
    let path = write_profile(
        r#"
profiles:
  prod:
    source: postgresql://prod/db
  staging:
    source: postgresql://staging/db
"#,
    );
    let mut cli = default_cli("qa");

    let err = apply_requested_profile_from_path(&mut cli, &ProfileValueSources::default(), &path)
        .unwrap_err()
        .to_string();

    assert!(err.contains("unknown profile `qa`"));
    assert!(err.contains("prod, staging"));
}
