/// Integration tests require a live PostgreSQL database.
/// Set DATABASE_URL to run these tests:
///   DATABASE_URL=postgresql://user:pass@localhost/testdb cargo test --test integration
#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "requires DATABASE_URL"]
    async fn test_introspect_live_db() {
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
}
