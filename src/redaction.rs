/// Best-effort redaction for connection URLs before they are printed.
///
/// Strips URL userinfo (`scheme://user:pass@host/db`) and masks common
/// query-parameter credential keys supported by database drivers. Values
/// that are not parseable as URLs are returned unchanged; in UVG those are
/// expected to be SQLite forms that do not carry network credentials.
pub(crate) fn redact_connection_url(raw: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(raw) else {
        return raw.to_string();
    };

    let mut changed = false;

    if !parsed.username().is_empty() || parsed.password().is_some() {
        let _ = parsed.set_username("***");
        let _ = parsed.set_password(None);
        changed = true;
    }

    let has_sensitive_query = parsed.query().is_some_and(|_| {
        parsed
            .query_pairs()
            .any(|(key, _)| is_sensitive_query_key(&key))
    });

    if has_sensitive_query {
        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(key, value)| {
                let value = if is_sensitive_query_key(&key) {
                    "***".to_string()
                } else {
                    value.into_owned()
                };
                (key.into_owned(), value)
            })
            .collect();

        let mut query = parsed.query_pairs_mut();
        query.clear();
        for (key, value) in pairs {
            query.append_pair(&key, &value);
        }
        changed = true;
    }

    if changed {
        parsed.into()
    } else {
        raw.to_string()
    }
}

fn is_sensitive_query_key(key: &str) -> bool {
    const SENSITIVE_KEYS: &[&str] = &[
        "password",
        "pass",
        "pwd",
        "token",
        "access_token",
        "auth_token",
        "secret",
        "client_secret",
        "sslkey",
        "ssl-key",
        "ssl_key",
    ];

    SENSITIVE_KEYS
        .iter()
        .any(|candidate| key.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
#[path = "redaction_tests.rs"]
mod tests;
