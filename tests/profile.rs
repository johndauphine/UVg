mod common;

#[cfg(test)]
mod tests {
    use super::common::{exec_sql, run_uvg_with_env, tmpdir};

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
}
