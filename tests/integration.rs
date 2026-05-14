/// Integration tests require a live database.
/// Set the appropriate env var to run these tests:
///   DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration -- --ignored
///   MYSQL_URL=mysql://user:pass@localhost/testdb cargo test --test integration -- --ignored
#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn test_introspect_live_pg() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("Failed to connect");

        // Just verify we can connect and query information_schema
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM information_schema.tables")
            .fetch_one(&pool)
            .await
            .expect("Failed to query");

        assert!(
            row.0 > 0,
            "Expected at least one table in information_schema"
        );
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "requires MYSQL_URL"]
    async fn test_introspect_live_mysql() {
        let url = std::env::var("MYSQL_URL").expect("MYSQL_URL must be set");
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("Failed to connect to MySQL");

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM information_schema.TABLES")
            .fetch_one(&pool)
            .await
            .expect("Failed to query");

        assert!(
            row.0 > 0,
            "Expected at least one table in information_schema"
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn test_introspect_sqlite_in_memory() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("Failed to connect to SQLite");

        // Create a test table
        sqlx::query(
            "CREATE TABLE test_table (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                value REAL,
                data BLOB
            )",
        )
        .execute(&pool)
        .await
        .expect("Failed to create table");

        // Verify table exists in sqlite_master
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'test_table'",
        )
        .fetch_one(&pool)
        .await
        .expect("Failed to query");

        assert_eq!(row.0, 1);

        // Verify columns
        let cols: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pragma_table_info('test_table') ORDER BY cid")
                .fetch_all(&pool)
                .await
                .expect("Failed to query columns");

        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0].0, "id");
        assert_eq!(cols[1].0, "name");
        assert_eq!(cols[2].0, "value");
        assert_eq!(cols[3].0, "data");

        pool.close().await;
    }

    // -------- per-table --out-dir end-to-end tests --------

    use std::path::{Path, PathBuf};

    /// Allocate a unique tmpdir for this test invocation.
    fn tmpdir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let dir =
            std::env::temp_dir().join(format!("uvg-cli-test-{label}-{pid}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn exec_sql(db_path: &Path, sql: &str) {
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
    fn snapshot_dir(root: &Path) -> Vec<(String, Vec<u8>)> {
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    walk(root, &path, out);
                } else {
                    let rel = path.strip_prefix(root).unwrap().to_string_lossy().to_string();
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

    fn run_uvg(args: &[&str]) -> std::process::Output {
        std::process::Command::new(uvg_bin())
            .args(args)
            .output()
            .expect("spawn uvg")
    }

    #[tokio::test]
    async fn test_out_dir_first_run_then_noop() {
        let dir = tmpdir("outdir-first-then-noop");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        let migrations = dir.join("migrations");

        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        // Touch the target so the file exists. uvg still sees an empty
        // schema and emits CREATE TABLEs for every source table.
        exec_sql(&target, "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;").await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        // First run — should produce per-table layout.
        let out = run_uvg(&[
            "--generator", "ddl",
            "--out-dir", &mig_str,
            "--name", "initial",
            &src_url, &tgt_url,
        ]);
        assert!(out.status.success(), "first run failed: {}", String::from_utf8_lossy(&out.stderr));

        assert!(migrations.join("users").is_dir(), "users/ subdir missing");
        assert!(migrations.join("posts").is_dir(), "posts/ subdir missing");
        assert!(migrations.join("_runs").is_dir(), "_runs/ missing");

        // One .sql per table, and the manifest names both.
        let users_sql: Vec<_> = std::fs::read_dir(migrations.join("users"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(users_sql.len(), 1, "expected one users sql, got {users_sql:?}");
        assert!(users_sql[0].ends_with("__initial.sql"));

        let body = std::fs::read_to_string(migrations.join("users").join(&users_sql[0])).unwrap();
        assert!(body.contains("-- Generated by uvg"), "missing provenance header: {body}");
        assert!(body.contains("CREATE TABLE \"users\""), "missing CREATE: {body}");

        // Snapshot the directory before the second (no-op) run.
        // To make the no-op meaningful, apply the migrations to the
        // target first so the schemas match.
        let mut migration_files: Vec<PathBuf> = Vec::new();
        for sub in ["users", "posts"] {
            for entry in std::fs::read_dir(migrations.join(sub)).unwrap() {
                migration_files.push(entry.unwrap().path());
            }
        }
        migration_files.sort();
        for path in &migration_files {
            let sql = std::fs::read_to_string(path).unwrap();
            // Strip comment-only header lines (they aren't valid SQL inside
            // executescript on some SQLite builds; sqlx tolerates them but
            // we filter anyway for clarity).
            let stripped: String = sql
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            exec_sql(&target, &stripped).await;
        }

        let before = snapshot_dir(&migrations);

        // Second run with identical source/target — must write nothing.
        let out2 = run_uvg(&[
            "--generator", "ddl",
            "--out-dir", &mig_str,
            "--name", "should-not-appear",
            &src_url, &tgt_url,
        ]);
        assert!(out2.status.success(), "noop run failed: {}", String::from_utf8_lossy(&out2.stderr));
        let stderr = String::from_utf8_lossy(&out2.stderr);
        assert!(stderr.contains("no schema changes"), "expected no-op message, got: {stderr}");

        let after = snapshot_dir(&migrations);
        assert_eq!(
            before, after,
            "no-op run must leave the out-dir byte-identical"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_out_dir_requires_target_url() {
        let dir = tmpdir("outdir-no-target");
        let source = dir.join("source.db");
        let migrations = dir.join("migrations");

        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        let src_url = format!("sqlite:///{}", source.display());
        let mig_str = migrations.display().to_string();

        // No target URL — uvg should refuse and explain.
        let out = run_uvg(&[
            "--generator", "ddl",
            "--out-dir", &mig_str,
            &src_url,
        ]);
        assert!(!out.status.success(), "expected non-zero exit");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("target database URL"),
            "expected helpful error about missing target URL, got: {stderr}"
        );
        assert!(
            !migrations.exists() || snapshot_dir(&migrations).is_empty(),
            "no files should be written when --out-dir is misconfigured"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
