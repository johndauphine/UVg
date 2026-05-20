mod common;

#[cfg(test)]
mod tests {
    use super::common::{run_uvg, tmpdir};
    use std::path::Path;

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
}
