mod common;

#[cfg(test)]
mod tests {
    use super::common::{exec_sql, run_uvg_without_env, tmpdir};

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
}
