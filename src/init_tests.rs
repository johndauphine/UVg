use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn tmpdir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "uvg-init-test-{label}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn command(root: &Path) -> InitCommand {
    InitCommand {
        migrations_dir: root.join("migrations"),
        config: root.join("uvg.toml"),
    }
}

#[test]
fn test_init_project_creates_expected_artifacts() {
    let root = tmpdir("create");
    let args = command(&root);
    let report = init_project(&args).unwrap();

    assert_eq!(report.created.len(), 4);
    assert!(args.migrations_dir.is_dir());
    assert!(args.migrations_dir.join(BASELINE_FILENAME).is_file());
    assert!(args.migrations_dir.join(META_FILENAME).is_file());
    assert!(args.config.is_file());

    let baseline = fs::read_to_string(args.migrations_dir.join(BASELINE_FILENAME)).unwrap();
    assert!(baseline.contains("-- uvg revision: 00000000_000000"));
    assert!(baseline.contains("-- UP"));

    let meta = fs::read_to_string(args.migrations_dir.join(META_FILENAME)).unwrap();
    assert!(meta.contains("head: '00000000_000000'"));

    let config = fs::read_to_string(&args.config).unwrap();
    assert!(config.contains("[migrations]"));
    assert!(config.contains("version_table = \"uvg_version\""));

    fs::remove_dir_all(&root).ok();
}

#[test]
fn test_init_project_is_idempotent_and_preserves_existing_files() {
    let root = tmpdir("idempotent");
    let args = command(&root);
    init_project(&args).unwrap();
    fs::write(&args.config, "custom = true\n").unwrap();

    let report = init_project(&args).unwrap();
    assert!(report.created.is_empty());
    assert_eq!(report.existing.len(), 4);
    assert_eq!(fs::read_to_string(&args.config).unwrap(), "custom = true\n");

    fs::remove_dir_all(&root).ok();
}

#[test]
fn test_init_project_errors_when_migrations_path_is_file() {
    let root = tmpdir("file-conflict");
    let args = command(&root);
    fs::write(&args.migrations_dir, "not a directory").unwrap();

    let err = init_project(&args).unwrap_err();
    assert!(err.to_string().contains("non-directory"));

    fs::remove_dir_all(&root).ok();
}

#[test]
fn test_config_uses_custom_migrations_dir() {
    let body = config_toml(Path::new("db/migrations"));
    assert!(body.contains("directory = \"db/migrations\""));
}
