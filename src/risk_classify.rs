use anyhow::{bail, Context, Result};
use serde::{Deserialize, Deserializer, Serialize};

use crate::output::Change;

const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MODEL: &str = "claude-haiku-4-5-20251001";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RiskClass {
    Safe,
    Blocking,
    Rebuild,
    DataLossRisk,
}

impl RiskClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            RiskClass::Safe => "safe",
            RiskClass::Blocking => "blocking",
            RiskClass::Rebuild => "rebuild",
            RiskClass::DataLossRisk => "data-loss-risk",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "safe" => Ok(RiskClass::Safe),
            "blocking" => Ok(RiskClass::Blocking),
            "rebuild" => Ok(RiskClass::Rebuild),
            "data-loss-risk" => Ok(RiskClass::DataLossRisk),
            other => bail!(
                "unknown risk class `{other}`; expected safe, blocking, rebuild, data-loss-risk"
            ),
        }
    }
}

impl<'de> Deserialize<'de> for RiskClass {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        RiskClass::parse(&raw).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug)]
pub(crate) struct AnthropicConfig {
    api_key: String,
    model: String,
}

impl AnthropicConfig {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_api_key(std::env::var("ANTHROPIC_API_KEY").ok())
    }

    fn from_api_key(api_key: Option<String>) -> Result<Self> {
        let Some(api_key) = api_key.filter(|v| !v.trim().is_empty()) else {
            bail!("ANTHROPIC_API_KEY is required when --risk-classify is set");
        };
        Ok(Self {
            api_key,
            model: DEFAULT_MODEL.to_string(),
        })
    }
}

pub(crate) async fn classify_changes(
    config: &AnthropicConfig,
    changes: &[Change],
) -> Result<Vec<RiskClass>> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

    let client = reqwest::Client::new();
    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &config.api_key)
        .header("anthropic-version", ANTHROPIC_VERSION)
        .json(&MessagesRequest::new(&config.model, changes))
        .send()
        .await
        .context("Anthropic request failed")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read Anthropic response")?;
    if !status.is_success() {
        bail!(
            "Anthropic request failed with {}: {}",
            status,
            truncate(&body)
        );
    }

    let parsed: MessagesResponse =
        serde_json::from_str(&body).context("Anthropic response was not valid JSON")?;
    let text = parsed
        .content
        .into_iter()
        .find_map(|block| block.text)
        .context("Anthropic response did not include text content")?;
    parse_classification_text(&text, changes.len())
}

pub(crate) fn annotate_changes(changes: &[Change], risks: &[RiskClass]) -> Result<Vec<Change>> {
    if changes.len() != risks.len() {
        bail!(
            "risk classifier returned {} result(s) for {} change(s)",
            risks.len(),
            changes.len()
        );
    }

    Ok(changes
        .iter()
        .zip(risks)
        .map(|(change, risk)| Change {
            table_schema: change.table_schema.clone(),
            table_name: change.table_name.clone(),
            sql: format!("-- RISK: {}\n{}", risk.as_str(), change.sql),
        })
        .collect())
}

fn parse_classification_text(raw: &str, expected_len: usize) -> Result<Vec<RiskClass>> {
    let json = strip_json_fence(raw.trim());
    let parsed: ClassificationResponse =
        serde_json::from_str(json).context("risk classification response was not valid JSON")?;
    if parsed.risks.len() != expected_len {
        bail!(
            "risk classifier returned {} result(s) for {} change(s)",
            parsed.risks.len(),
            expected_len
        );
    }
    let mut risks = vec![RiskClass::Safe; expected_len];
    let mut seen = vec![false; expected_len];
    for item in parsed.risks {
        if item.index >= expected_len {
            bail!("risk classifier returned out-of-range index {}", item.index);
        }
        if seen[item.index] {
            bail!("risk classifier returned duplicate index {}", item.index);
        }
        seen[item.index] = true;
        risks[item.index] = item.risk;
    }
    if seen.iter().any(|v| !v) {
        bail!("risk classifier did not return every change index");
    }
    Ok(risks)
}

fn strip_json_fence(raw: &str) -> &str {
    raw.strip_prefix("```json")
        .or_else(|| raw.strip_prefix("```"))
        .and_then(|s| s.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(raw)
}

fn truncate(raw: &str) -> String {
    const MAX: usize = 400;
    if raw.chars().count() <= MAX {
        return raw.to_string();
    }
    let mut out = raw.chars().take(MAX).collect::<String>();
    out.push_str("...");
    out
}

#[derive(Serialize)]
struct MessagesRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    temperature: f32,
    system: &'a str,
    messages: Vec<Message>,
}

impl<'a> MessagesRequest<'a> {
    fn new(model: &'a str, changes: &[Change]) -> Self {
        Self {
            model,
            max_tokens: 1200,
            temperature: 0.0,
            system: "Classify database schema migration DDL risk. Return only JSON of the form {\"risks\":[{\"index\":0,\"risk\":\"safe\"}]}. Valid risk values are safe, blocking, rebuild, data-loss-risk.",
            messages: vec![Message {
                role: "user",
                content: prompt(changes),
            }],
        }
    }
}

#[derive(Serialize)]
struct Message {
    role: &'static str,
    content: String,
}

fn prompt(changes: &[Change]) -> String {
    let mut out = String::from(
        "Classify each SQL change. Use exactly one risk per index.\n\nRisk definitions:\n- safe: metadata-only or short non-blocking change\n- blocking: may take locks or block concurrent writes/reads\n- rebuild: likely rewrites/rebuilds a table or large index\n- data-loss-risk: drops, truncates, narrows, or may destroy data\n\nChanges:\n",
    );
    for (idx, change) in changes.iter().enumerate() {
        out.push_str(&format!("\n[{idx}]\n{}\n", change.sql));
    }
    out
}

#[derive(Deserialize)]
struct MessagesResponse {
    content: Vec<ContentBlock>,
}

#[derive(Deserialize)]
struct ContentBlock {
    text: Option<String>,
}

#[derive(Deserialize)]
struct ClassificationResponse {
    risks: Vec<RiskItem>,
}

#[derive(Deserialize)]
struct RiskItem {
    index: usize,
    risk: RiskClass,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(sql: &str) -> Change {
        Change {
            table_schema: "main".to_string(),
            table_name: Some("users".to_string()),
            sql: sql.to_string(),
        }
    }

    #[test]
    fn risk_classes_parse_and_render() {
        let raw = r#"{"risks":[
            {"index":0,"risk":"safe"},
            {"index":1,"risk":"blocking"},
            {"index":2,"risk":"rebuild"},
            {"index":3,"risk":"data-loss-risk"}
        ]}"#;

        let risks = parse_classification_text(raw, 4).unwrap();

        assert_eq!(
            risks.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
            vec!["safe", "blocking", "rebuild", "data-loss-risk"]
        );
    }

    #[test]
    fn annotate_changes_adds_one_risk_comment_per_change() {
        let changes = vec![
            change("ALTER TABLE users ADD COLUMN email TEXT;"),
            change("DROP TABLE old_users;"),
        ];

        let annotated =
            annotate_changes(&changes, &[RiskClass::Safe, RiskClass::DataLossRisk]).unwrap();

        assert_eq!(annotated[0].sql.matches("-- RISK:").count(), 1);
        assert!(annotated[0].sql.starts_with("-- RISK: safe\nALTER TABLE"));
        assert_eq!(annotated[1].sql.matches("-- RISK:").count(), 1);
        assert!(annotated[1]
            .sql
            .starts_with("-- RISK: data-loss-risk\nDROP TABLE"));
        assert!(!changes[0].sql.contains("-- RISK:"));
    }

    #[test]
    fn invalid_classification_count_errors_without_mutating_changes() {
        let changes = vec![change("ALTER TABLE users ADD COLUMN email TEXT;")];

        let err = annotate_changes(&changes, &[]).unwrap_err().to_string();

        assert!(err.contains("0 result(s) for 1 change(s)"));
        assert_eq!(changes[0].sql, "ALTER TABLE users ADD COLUMN email TEXT;");
    }

    #[test]
    fn missing_api_key_is_clear() {
        let err = AnthropicConfig::from_api_key(None).unwrap_err().to_string();

        assert!(err.contains("ANTHROPIC_API_KEY is required"));
    }
}
