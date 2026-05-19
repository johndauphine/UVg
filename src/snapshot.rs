use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::dialect::Dialect;
use crate::output::format_utc_iso8601;
use crate::schema::{DomainInfo, EnumInfo, IntrospectedSchema, TableInfo};

const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SnapshotFile {
    pub format_version: u32,
    pub uvg_version: String,
    pub captured_at: String,
    pub dialect: Dialect,
    pub tables: Vec<TableInfo>,
    pub enums: Vec<EnumInfo>,
    pub domains: Vec<DomainInfo>,
}

#[derive(Debug, Deserialize)]
struct SnapshotHeader {
    format_version: Option<u32>,
}

impl SnapshotFile {
    fn from_schema(schema: &IntrospectedSchema) -> Self {
        Self {
            format_version: FORMAT_VERSION,
            uvg_version: env!("CARGO_PKG_VERSION").to_string(),
            captured_at: now_iso8601(),
            dialect: schema.dialect,
            tables: schema.tables.clone(),
            enums: schema.enums.clone(),
            domains: schema.domains.clone(),
        }
    }

    fn into_schema(self) -> IntrospectedSchema {
        IntrospectedSchema {
            dialect: self.dialect,
            tables: self.tables,
            enums: self.enums,
            domains: self.domains,
        }
    }
}

pub(crate) fn write(path: &Path, schema: &IntrospectedSchema) -> Result<()> {
    let snapshot = SnapshotFile::from_schema(schema);
    let raw = serde_yaml::to_string(&snapshot).context("failed to serialize snapshot YAML")?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create snapshot directory {}", parent.display()))?;
    }
    fs::write(path, raw).with_context(|| format!("failed to write snapshot {}", path.display()))
}

pub(crate) fn load(path: &Path) -> Result<IntrospectedSchema> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read snapshot {}", path.display()))?;
    load_str(&raw).with_context(|| format!("failed to load snapshot {}", path.display()))
}

fn load_str(raw: &str) -> Result<IntrospectedSchema> {
    let header: SnapshotHeader = serde_yaml::from_str(raw).context("snapshot is not valid YAML")?;
    match header.format_version {
        Some(FORMAT_VERSION) => {}
        Some(other) => {
            bail!("unsupported snapshot format_version {other}; expected {FORMAT_VERSION}")
        }
        None => bail!("unsupported snapshot format: missing format_version"),
    }

    let snapshot: SnapshotFile =
        serde_yaml::from_str(raw).context("snapshot does not match uvg snapshot schema")?;
    Ok(snapshot.into_schema())
}

fn now_iso8601() -> String {
    let epoch_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format_utc_iso8601(epoch_secs)
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
