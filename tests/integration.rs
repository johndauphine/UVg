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
        let dir = std::env::temp_dir().join(format!("uvg-cli-test-{label}-{pid}-{nanos}"));
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

    fn run_uvg(args: &[&str]) -> std::process::Output {
        std::process::Command::new(uvg_bin())
            .args(args)
            .output()
            .expect("spawn uvg")
    }

    fn run_uvg_without_env(args: &[&str], key: &str) -> std::process::Output {
        std::process::Command::new(uvg_bin())
            .args(args)
            .env_remove(key)
            .output()
            .expect("spawn uvg")
    }

    fn run_uvg_with_env(args: &[&str], key: &str, value: &Path) -> std::process::Output {
        std::process::Command::new(uvg_bin())
            .args(args)
            .env(key, value)
            .output()
            .expect("spawn uvg")
    }

    #[tokio::test]
    async fn test_profile_cli_fills_required_fields() {
        let dir = tmpdir("profile-cli");
        let source = dir.join("source.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .await;
        let config_home = dir.join("config");
        let profile_dir = config_home.join("uvg");
        std::fs::create_dir_all(&profile_dir).unwrap();
        std::fs::write(
            profile_dir.join("profiles.yaml"),
            format!(
                "profiles:\n  prod:\n    source: sqlite:///{}\n    generator: ddl\n    target_dialect: sqlite\n",
                source.display()
            ),
        )
        .unwrap();

        let out = run_uvg_with_env(&["--profile", "prod"], "XDG_CONFIG_HOME", &config_home);
        assert!(
            out.status.success(),
            "profile run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains("CREATE TABLE \"users\""),
            "missing users DDL: {stdout}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_snapshot_cli_round_trip_and_diff_inputs() {
        let dir = tmpdir("snapshot-round-trip");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        let snapshot = dir.join("source.yaml");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let snapshot_path = snapshot.display().to_string();
        let snapshot_ref = format!("@{snapshot_path}");

        let out = run_uvg(&["snapshot", &src_url, "-o", &snapshot_path]);
        assert!(
            out.status.success(),
            "snapshot failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let raw = std::fs::read_to_string(&snapshot).unwrap();
        assert!(
            raw.contains("format_version: 1"),
            "snapshot header missing: {raw}"
        );
        assert!(raw.contains("uvg_version:"), "uvg_version missing: {raw}");
        assert!(raw.contains("captured_at:"), "captured_at missing: {raw}");
        assert!(raw.contains("dialect: sqlite"), "dialect missing: {raw}");

        let live = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            live.status.success(),
            "live diff failed: {}",
            String::from_utf8_lossy(&live.stderr)
        );
        let snap_source = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &snapshot_ref,
            &tgt_url,
        ]);
        assert!(
            snap_source.status.success(),
            "snapshot source diff failed: {}",
            String::from_utf8_lossy(&snap_source.stderr)
        );
        assert_eq!(live.stdout, snap_source.stdout);

        let live_target = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &tgt_url,
            &src_url,
        ]);
        assert!(
            live_target.status.success(),
            "live target diff failed: {}",
            String::from_utf8_lossy(&live_target.stderr)
        );
        let snap_target = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            &tgt_url,
            &snapshot_ref,
        ]);
        assert!(
            snap_target.status.success(),
            "snapshot target diff failed: {}",
            String::from_utf8_lossy(&snap_target.stderr)
        );
        assert_eq!(live_target.stdout, snap_target.stdout);

        let same = run_uvg(&["--generator", "ddl", &snapshot_ref, &src_url]);
        assert!(
            same.status.success(),
            "same-db snapshot diff failed: {}",
            String::from_utf8_lossy(&same.stderr)
        );
        let same_stdout = String::from_utf8_lossy(&same.stdout);
        assert!(
            same_stdout.contains("-- No schema changes detected."),
            "same-db diff was not empty: {same_stdout}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_snapshot_format_mismatch_is_clear() {
        let dir = tmpdir("snapshot-format-mismatch");
        let target = dir.join("target.db");
        let bad_snapshot = dir.join("bad.yaml");
        exec_sql(&target, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        std::fs::write(
            &bad_snapshot,
            "format_version: 999\nuvg_version: 1.5.0\ndialect: sqlite\ntables: []\nenums: []\ndomains: []\n",
        )
        .unwrap();
        let bad_ref = format!("@{}", bad_snapshot.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", &bad_ref, &tgt_url]);
        assert!(!out.status.success(), "expected format mismatch to fail");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("unsupported snapshot format_version 999"),
            "missing clear format error: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_risk_classify_requires_anthropic_key() {
        let dir = tmpdir("risk-classify-missing-key");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT);",
        )
        .await;
        exec_sql(&target, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg_without_env(
            &["--generator", "ddl", "--risk-classify", &src_url, &tgt_url],
            "ANTHROPIC_API_KEY",
        );
        assert!(!out.status.success(), "expected missing API key to fail");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("ANTHROPIC_API_KEY is required"),
            "missing clear API key error: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    async fn sqlite_table_exists(db_path: &Path, table: &str) -> bool {
        let url = format!("sqlite:///{}", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .expect("sqlite table lookup");
        pool.close().await;
        count > 0
    }

    async fn postgres_table_exists(url: &str, table: &str) -> bool {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .expect("postgres connect");
        let qualified = format!("public.{table}");
        let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(qualified)
            .fetch_one(&pool)
            .await
            .expect("postgres table lookup");
        pool.close().await;
        exists.is_some()
    }

    fn write_create_users_migration(migrations: &Path) {
        std::fs::create_dir_all(migrations).unwrap();
        std::fs::write(
            migrations.join("20260513_193000_create_users.sql"),
            "-- uvg revision: 20260513_193000\n\
             -- parent:\n\
             -- description: create users\n\n\
             -- UP\n\
             CREATE TABLE users(id INTEGER PRIMARY KEY);\n\n\
             -- DOWN\n\
             DROP TABLE users;\n",
        )
        .unwrap();
    }

    #[tokio::test]
    async fn test_versioned_migration_upgrade_downgrade_round_trip_cli() {
        let dir = tmpdir("versioned-round-trip");
        let target = dir.join("target.db");
        std::fs::File::create(&target).unwrap();
        let migrations = dir.join("migrations");
        write_create_users_migration(&migrations);

        let target_url = format!("sqlite:///{}", target.display());
        let migrations_arg = migrations.display().to_string();

        let out = run_uvg(&["upgrade", &target_url, "--migrations-dir", &migrations_arg]);
        assert!(
            out.status.success(),
            "upgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(sqlite_table_exists(&target, "users").await);

        let out = run_uvg(&[
            "downgrade",
            &target_url,
            "base",
            "--migrations-dir",
            &migrations_arg,
        ]);
        assert!(
            out.status.success(),
            "downgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!sqlite_table_exists(&target, "users").await);

        let out = run_uvg(&["upgrade", &target_url, "--migrations-dir", &migrations_arg]);
        assert!(
            out.status.success(),
            "second upgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(sqlite_table_exists(&target, "users").await);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_versioned_migration_parse_check_skipped_note_on_sqlite() {
        let dir = tmpdir("versioned-parse-skip");
        let target = dir.join("target.db");
        std::fs::File::create(&target).unwrap();
        let migrations = dir.join("migrations");
        write_create_users_migration(&migrations);

        let target_url = format!("sqlite:///{}", target.display());
        let migrations_arg = migrations.display().to_string();

        let out = run_uvg(&["upgrade", &target_url, "--migrations-dir", &migrations_arg]);
        assert!(
            out.status.success(),
            "upgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("migration parse-check skipped"),
            "expected versioned migration skip note on sqlite: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_versioned_migration_no_parse_check_suppresses_skip_note() {
        let dir = tmpdir("versioned-parse-suppressed");
        let target = dir.join("target.db");
        std::fs::File::create(&target).unwrap();
        let migrations = dir.join("migrations");
        write_create_users_migration(&migrations);

        let target_url = format!("sqlite:///{}", target.display());
        let migrations_arg = migrations.display().to_string();

        let out = run_uvg(&[
            "--no-parse-check",
            "upgrade",
            &target_url,
            "--migrations-dir",
            &migrations_arg,
        ]);
        assert!(
            out.status.success(),
            "upgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("migration parse-check skipped"),
            "--no-parse-check should suppress sqlite skip note: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    #[ignore = "requires UVG_DISPOSABLE_PG_URL pointing at a disposable PostgreSQL database"]
    async fn test_versioned_migration_live_postgres_workflow_cli() {
        let url =
            std::env::var("UVG_DISPOSABLE_PG_URL").expect("UVG_DISPOSABLE_PG_URL must be set");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("postgres connect");
        sqlx::query("DROP TABLE IF EXISTS uvg_live_migration_users")
            .execute(&pool)
            .await
            .expect("drop live test table");
        sqlx::query("DROP TABLE IF EXISTS uvg_version")
            .execute(&pool)
            .await
            .expect("drop version table");
        pool.close().await;

        let dir = tmpdir("versioned-live-pg");
        let migrations = dir.join("migrations");
        std::fs::create_dir_all(&migrations).unwrap();
        std::fs::write(
            migrations.join("20260513_193000_create_live_users.sql"),
            "-- uvg revision: 20260513_193000\n\
             -- parent:\n\
             -- description: create live users\n\n\
             -- UP\n\
             CREATE TABLE uvg_live_migration_users(id integer PRIMARY KEY);\n\n\
             -- DOWN\n\
             DROP TABLE uvg_live_migration_users;\n",
        )
        .unwrap();
        let migrations_arg = migrations.display().to_string();

        let out = run_uvg(&["upgrade", &url, "--migrations-dir", &migrations_arg]);
        assert!(
            out.status.success(),
            "upgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(postgres_table_exists(&url, "uvg_live_migration_users").await);

        let out = run_uvg(&["current", &url]);
        assert!(
            out.status.success(),
            "current failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "20260513_193000"
        );

        let out = run_uvg(&["history", &url, "--migrations-dir", &migrations_arg]);
        assert!(
            out.status.success(),
            "history failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let history = String::from_utf8_lossy(&out.stdout);
        assert!(history.contains("[applied, current, head]"), "{history}");

        let out = run_uvg(&[
            "downgrade",
            &url,
            "base",
            "--migrations-dir",
            &migrations_arg,
        ]);
        assert!(
            out.status.success(),
            "downgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!postgres_table_exists(&url, "uvg_live_migration_users").await);

        let out = run_uvg(&["current", &url]);
        assert!(
            out.status.success(),
            "current after downgrade failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "");

        let out = run_uvg(&[
            "stamp",
            &url,
            "20260513_193000",
            "--yes",
            "--migrations-dir",
            &migrations_arg,
        ]);
        assert!(
            out.status.success(),
            "stamp failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let out = run_uvg(&["current", &url]);
        assert!(
            out.status.success(),
            "current after stamp failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "20260513_193000"
        );

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("postgres reconnect");
        sqlx::query("DROP TABLE IF EXISTS uvg_live_migration_users")
            .execute(&pool)
            .await
            .expect("cleanup live test table");
        sqlx::query("DROP TABLE IF EXISTS uvg_version")
            .execute(&pool)
            .await
            .expect("cleanup version table");
        pool.close().await;

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_versioned_merge_writes_multi_parent_revision_cli() {
        let dir = tmpdir("versioned-merge");
        let migrations = dir.join("migrations");
        std::fs::create_dir_all(&migrations).unwrap();
        std::fs::write(
            migrations.join("20260513_193000_branch_a.sql"),
            "-- uvg revision: 20260513_193000\n\
             -- parent:\n\
             -- description: branch a\n\n\
             -- UP\n\
             -- empty\n\n\
             -- DOWN\n\
             -- empty\n",
        )
        .unwrap();
        std::fs::write(
            migrations.join("20260513_194000_branch_b.sql"),
            "-- uvg revision: 20260513_194000\n\
             -- parent:\n\
             -- description: branch b\n\n\
             -- UP\n\
             -- empty\n\n\
             -- DOWN\n\
             -- empty\n",
        )
        .unwrap();

        let migrations_arg = migrations.display().to_string();
        let out = run_uvg(&[
            "merge",
            "--message",
            "merge branches",
            "--migrations-dir",
            &migrations_arg,
        ]);
        assert!(
            out.status.success(),
            "merge failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        let mut merge_files = std::fs::read_dir(&migrations)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains("merge-branches"))
            })
            .collect::<Vec<_>>();
        merge_files.sort();
        assert_eq!(merge_files.len(), 1);
        let body = std::fs::read_to_string(&merge_files[0]).unwrap();
        assert!(body.contains("-- parents: 20260513_193000, 20260513_194000"));
        assert!(body.contains("-- DOWN"));

        std::fs::remove_dir_all(&dir).ok();
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
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        // First run — should produce per-table layout.
        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--out-dir",
            &mig_str,
            "--name",
            "initial",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "first run failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );

        assert!(migrations.join("users").is_dir(), "users/ subdir missing");
        assert!(migrations.join("posts").is_dir(), "posts/ subdir missing");
        assert!(migrations.join("_runs").is_dir(), "_runs/ missing");

        // One .sql per table, and the manifest names both.
        let users_sql: Vec<_> = std::fs::read_dir(migrations.join("users"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            users_sql.len(),
            1,
            "expected one users sql, got {users_sql:?}"
        );
        assert!(users_sql[0].ends_with("__initial.sql"));

        let body = std::fs::read_to_string(migrations.join("users").join(&users_sql[0])).unwrap();
        assert!(
            body.contains("-- Generated by uvg"),
            "missing provenance header: {body}"
        );
        assert!(
            body.contains("CREATE TABLE \"users\""),
            "missing CREATE: {body}"
        );

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
            "--generator",
            "ddl",
            "--out-dir",
            &mig_str,
            "--name",
            "should-not-appear",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out2.status.success(),
            "noop run failed: {}",
            String::from_utf8_lossy(&out2.stderr)
        );
        let stderr = String::from_utf8_lossy(&out2.stderr);
        assert!(
            stderr.contains("no schema changes"),
            "expected no-op message, got: {stderr}"
        );

        let after = snapshot_dir(&migrations);
        assert_eq!(
            before, after,
            "no-op run must leave the out-dir byte-identical"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Read every table name from a SQLite DB. Used to verify that an
    /// `--apply` run actually created the source's tables on the target.
    async fn list_tables(db_path: &Path) -> Vec<String> {
        let url = format!("sqlite://{}?mode=rwc", db_path.display());
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .expect("sqlite connect");
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .expect("list tables");
        pool.close().await;
        rows.into_iter().map(|(n,)| n).collect()
    }

    #[tokio::test]
    async fn test_apply_inline_creates_target_tables() {
        // --apply (no --out-dir) should generate the diff and execute it
        // against the target in one shot.
        let dir = tmpdir("apply-inline");
        let source = dir.join("source.db");
        let target = dir.join("target.db");

        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        // Bring the target file into existence with an empty schema.
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        // Sanity: target starts empty.
        assert!(
            list_tables(&target).await.is_empty(),
            "target should start empty"
        );

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out.status.success(),
            "apply run failed: stderr={}, stdout={}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout),
        );

        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("applied") && stderr.contains("statement"),
            "expected applied-summary on stderr, got: {stderr}"
        );

        // Target should now match the source's tables.
        let tables = list_tables(&target).await;
        assert_eq!(tables, vec!["posts".to_string(), "users".to_string()]);

        // Second run is a no-op: zero applied statements, "no schema changes" on stderr.
        let out2 = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out2.status.success(),
            "noop apply failed: {}",
            String::from_utf8_lossy(&out2.stderr)
        );
        let stderr2 = String::from_utf8_lossy(&out2.stderr);
        assert!(
            stderr2.contains("no schema changes"),
            "expected no-schema-changes message, got: {stderr2}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_outdir_writes_and_applies() {
        // --apply with --out-dir should write per-table files AND execute
        // them in manifest order. The FK from posts -> users means the
        // apply only succeeds if users is created before posts.
        let dir = tmpdir("apply-outdir");
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
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;

        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());
        let mig_str = migrations.display().to_string();

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--out-dir",
            &mig_str,
            "--name",
            "initial",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "apply+outdir run failed: stderr={}",
            String::from_utf8_lossy(&out.stderr),
        );

        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("wrote"),
            "expected write summary, got: {stderr}"
        );
        assert!(
            stderr.contains("applied") && stderr.contains("file"),
            "expected apply summary, got: {stderr}"
        );

        // Files were written under per-table layout.
        assert!(migrations.join("users").is_dir(), "users/ missing");
        assert!(migrations.join("posts").is_dir(), "posts/ missing");

        // Target now matches the source.
        assert_eq!(
            list_tables(&target).await,
            vec!["posts".to_string(), "users".to_string()]
        );

        // Re-run is a no-op: nothing applied, nothing written.
        let before = snapshot_dir(&migrations);
        let out2 = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--out-dir",
            &mig_str,
            "--name",
            "should-not-appear",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out2.status.success(),
            "noop apply+outdir failed: {}",
            String::from_utf8_lossy(&out2.stderr),
        );
        let stderr2 = String::from_utf8_lossy(&out2.stderr);
        assert!(
            stderr2.contains("no schema changes"),
            "expected no-op message, got: {stderr2}"
        );
        let after = snapshot_dir(&migrations);
        assert_eq!(before, after, "no-op run must not touch the migrations dir");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_tables_glob_filters_to_matching_names() {
        // End-to-end: `--tables 'users_*'` should keep only users_active
        // and users_archive, dropping orders/invoices.
        let dir = tmpdir("tables-glob");
        let source = dir.join("source.db");
        exec_sql(
            &source,
            "CREATE TABLE users_active(id INTEGER PRIMARY KEY);
             CREATE TABLE users_archive(id INTEGER PRIMARY KEY);
             CREATE TABLE orders(id INTEGER PRIMARY KEY);
             CREATE TABLE invoices(id INTEGER PRIMARY KEY);",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            "--tables",
            "users_*",
            &src_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);

        assert!(
            stdout.contains("users_active"),
            "missing users_active: {stdout}"
        );
        assert!(
            stdout.contains("users_archive"),
            "missing users_archive: {stdout}"
        );
        assert!(
            !stdout.contains("orders"),
            "orders leaked through filter: {stdout}"
        );
        assert!(
            !stdout.contains("invoices"),
            "invoices leaked through filter: {stdout}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_exclude_tables_drops_matches() {
        // End-to-end: with no `--tables`, `--exclude-tables '__*'` should
        // emit every table except those whose name starts with `__`.
        let dir = tmpdir("exclude-tables");
        let source = dir.join("source.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY);
             CREATE TABLE __migrations(version TEXT PRIMARY KEY);
             CREATE TABLE __schema_log(id INTEGER PRIMARY KEY);",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            "--exclude-tables",
            "__*",
            &src_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);

        assert!(stdout.contains("users"), "missing users: {stdout}");
        assert!(
            !stdout.contains("__migrations"),
            "excluded table leaked: {stdout}"
        );
        assert!(
            !stdout.contains("__schema_log"),
            "excluded table leaked: {stdout}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_invalid_tables_glob_errors_before_connection() {
        // A malformed glob pattern must fail with a clean error before any
        // attempt to connect to the (here, nonexistent) database.
        let bogus_url = "sqlite:///definitely/does/not/exist/db.db";
        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--target-dialect",
            "sqlite",
            "--tables",
            "[unclosed",
            bogus_url,
        ]);
        assert!(!out.status.success(), "expected non-zero exit");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("tables") && stderr.contains("[unclosed"),
            "expected error to mention flag and pattern, got: {stderr}"
        );
        // And the error must NOT be a connection error — it must surface
        // before any DB work happens.
        assert!(
            !stderr.to_lowercase().contains("unable to open")
                && !stderr.to_lowercase().contains("connection"),
            "validation should run before connecting, got: {stderr}"
        );
    }

    #[tokio::test]
    async fn test_apply_progress_emits_per_statement_lines_with_on() {
        // --progress=on must emit one `[i/total] <preview>  <ms>ms`
        // line per executed statement plus a class-breakdown summary.
        let dir = tmpdir("apply-progress-on");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(
            &source,
            "CREATE TABLE users(id INTEGER PRIMARY KEY, email TEXT NOT NULL);
             CREATE TABLE posts(id INTEGER PRIMARY KEY, user_id INTEGER, body TEXT,
                                FOREIGN KEY(user_id) REFERENCES users(id));",
        )
        .await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--progress",
            "on",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr),
        );

        let stderr = String::from_utf8_lossy(&out.stderr);
        // At least one per-statement line.
        let stmt_lines: Vec<_> = stderr.lines().filter(|l| l.starts_with('[')).collect();
        assert!(
            stmt_lines.len() >= 2,
            "expected ≥2 per-statement progress lines, got {}: {stderr}",
            stmt_lines.len(),
        );
        // Each per-statement line carries `[i/total]` and `ms`.
        for line in &stmt_lines {
            assert!(line.contains('/'), "missing /: {line}");
            assert!(line.contains("ms"), "missing ms: {line}");
        }
        // Final summary line with class breakdown.
        assert!(
            stderr.contains("Applied") && stderr.contains("tables"),
            "missing class-breakdown summary: {stderr}",
        );
        // Progress is on stderr, not stdout.
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            !stdout.lines().any(|l| l.starts_with('[')),
            "progress leaked into stdout: {stdout}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_progress_off_is_silent() {
        // --progress=off must suppress per-statement lines and the
        // class-breakdown summary, while keeping the standard
        // "uvg: applied N statement(s)..." one-liner.
        let dir = tmpdir("apply-progress-off");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--progress",
            "off",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);

        assert!(
            !stderr.lines().any(|l| l.starts_with('[')),
            "per-statement line leaked with --progress=off: {stderr}",
        );
        assert!(
            !stderr.contains("Applied 1 statement(s) in"),
            "class-breakdown summary leaked with --progress=off: {stderr}",
        );
        // Standard apply-summary still present.
        assert!(
            stderr.contains("uvg: applied 1"),
            "missing apply summary: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_parse_check_skipped_note_on_sqlite() {
        // SQLite has no parse-only mode, so `run_parse_check` emits a
        // one-line skip note on stderr (rather than aborting the
        // apply). Default --apply enables parse-check, so the note
        // should appear on a normal apply run.
        let dir = tmpdir("parse-check-sqlite-skip");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("parse-check skipped"),
            "expected parse-check-skipped note on sqlite default apply: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_no_parse_check_suppresses_skip_note() {
        // --no-parse-check must avoid even the skip note, because the
        // user explicitly turned the phase off. Verifies the flag is
        // wired through end-to-end.
        let dir = tmpdir("parse-check-sqlite-suppressed");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&[
            "--generator",
            "ddl",
            "--apply",
            "--no-parse-check",
            &src_url,
            &tgt_url,
        ]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.contains("parse-check skipped"),
            "skip note must not appear with --no-parse-check: {stderr}"
        );
        // Sanity: the apply itself still happened.
        assert!(
            stderr.contains("uvg: applied"),
            "apply summary missing: {stderr}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // No E2E for the failed-statement progress path: forcing uvg to
    // emit a statement that fails-on-apply requires either custom
    // injection beyond the public CLI's surface (the diff engine
    // skips emit when source and target match) or a race-prone
    // "pre-create-after-introspection" trick. The FAIL suffix and
    // record-skipping-on-failure contracts are covered by unit tests
    // in `src/apply_progress.rs` (`stats_record_skips_failed_statements`).

    #[tokio::test]
    async fn test_apply_progress_auto_is_silent_when_stderr_redirected() {
        // Default --progress=auto consults stderr.is_terminal(); the
        // subprocess we spawn has a piped stderr (not a TTY), so auto
        // should resolve to off. Same assertion as --progress=off.
        let dir = tmpdir("apply-progress-auto");
        let source = dir.join("source.db");
        let target = dir.join("target.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        exec_sql(
            &target,
            "CREATE TABLE _bootstrap(id INTEGER); DROP TABLE _bootstrap;",
        )
        .await;
        let src_url = format!("sqlite:///{}", source.display());
        let tgt_url = format!("sqlite:///{}", target.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url, &tgt_url]);
        assert!(
            out.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !stderr.lines().any(|l| l.starts_with('[')),
            "auto should suppress when stderr isn't a TTY: {stderr}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_apply_requires_target_url() {
        // --apply without a target URL must exit non-zero with a helpful message.
        let dir = tmpdir("apply-no-target");
        let source = dir.join("source.db");
        exec_sql(&source, "CREATE TABLE users(id INTEGER PRIMARY KEY);").await;
        let src_url = format!("sqlite:///{}", source.display());

        let out = run_uvg(&["--generator", "ddl", "--apply", &src_url]);
        assert!(!out.status.success(), "expected non-zero exit");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("target database URL"),
            "expected helpful error, got: {stderr}"
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
        let out = run_uvg(&["--generator", "ddl", "--out-dir", &mig_str, &src_url]);
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
