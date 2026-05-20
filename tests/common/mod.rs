#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Allocate a unique tmpdir for this test invocation.
pub(crate) fn tmpdir(label: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("uvg-cli-test-{label}-{pid}-{nanos}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(crate) async fn exec_sql(db_path: &Path, sql: &str) {
    let url = format!("sqlite://{}?mode=rwc", db_path.display());
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("sqlite connect");
    for stmt in sql.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await.expect("exec");
    }
    pool.close().await;
}

/// Snapshot of a directory: sorted list of `(relpath, byte-content)`
/// for every regular file. Used to assert that a no-op uvg run leaves
/// the directory byte-identical.
pub(crate) fn snapshot_dir(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = Vec::new();
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                let bytes = std::fs::read(&path).unwrap();
                out.push((rel, bytes));
            }
        }
    }
    if root.exists() {
        walk(root, root, &mut out);
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn uvg_bin() -> PathBuf {
    // cargo populates this env var with the path to the built `uvg`
    // binary for integration tests; no need to find it manually.
    PathBuf::from(env!("CARGO_BIN_EXE_uvg"))
}

pub(crate) fn run_uvg(args: &[&str]) -> std::process::Output {
    std::process::Command::new(uvg_bin())
        .args(args)
        .output()
        .expect("spawn uvg")
}

pub(crate) fn run_uvg_without_env(args: &[&str], key: &str) -> std::process::Output {
    std::process::Command::new(uvg_bin())
        .args(args)
        .env_remove(key)
        .output()
        .expect("spawn uvg")
}

pub(crate) fn run_uvg_with_env(args: &[&str], key: &str, value: &Path) -> std::process::Output {
    std::process::Command::new(uvg_bin())
        .args(args)
        .env(key, value)
        .output()
        .expect("spawn uvg")
}
