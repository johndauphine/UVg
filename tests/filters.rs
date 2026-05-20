mod common;

#[cfg(test)]
mod tests {
    use super::common::{exec_sql, run_uvg, tmpdir};

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
        // And the error must NOT be a connection error - it must surface
        // before any DB work happens.
        assert!(
            !stderr.to_lowercase().contains("unable to open")
                && !stderr.to_lowercase().contains("connection"),
            "validation should run before connecting, got: {stderr}"
        );
    }
}
