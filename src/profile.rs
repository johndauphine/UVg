use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::parser::ValueSource;
use clap::ArgMatches;
use serde::Deserialize;

use crate::cli::Cli;

const PROFILED_ARGS: &[&str] = &[
    "url",
    "target_url",
    "generator",
    "target_dialect",
    "split_tables",
    "apply",
    "no_parse_check",
    "tables",
    "exclude_tables",
    "schemas",
    "noviews",
    "options",
    "outfile",
    "out_dir",
    "name",
    "trust_cert",
];

#[derive(Debug, Deserialize)]
struct ProfilesFile {
    profiles: BTreeMap<String, ProfileDefaults>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
struct ProfileDefaults {
    source: Option<String>,
    target: Option<String>,
    generator: Option<String>,
    target_dialect: Option<String>,
    tables: Option<Vec<String>>,
    exclude_tables: Option<Vec<String>>,
    schemas: Option<Vec<String>>,
    options: Option<Vec<String>>,
    outfile: Option<String>,
    out_dir: Option<PathBuf>,
    name: Option<String>,
    split_tables: Option<bool>,
    apply: Option<bool>,
    no_parse_check: Option<bool>,
    noviews: Option<bool>,
    trust_cert: Option<bool>,
}

#[derive(Debug, Default)]
struct ProfileValueSources {
    command_line: HashSet<&'static str>,
}

impl ProfileValueSources {
    fn from_matches(matches: &ArgMatches) -> Self {
        let mut sources = Self::default();
        for &id in PROFILED_ARGS {
            if matches.value_source(id) == Some(ValueSource::CommandLine) {
                sources.command_line.insert(id);
            }
        }
        sources
    }

    fn explicit(&self, id: &'static str) -> bool {
        self.command_line.contains(id)
    }
}

pub(crate) fn apply_requested_profile(cli: &mut Cli, matches: &ArgMatches) -> Result<()> {
    if cli.profile.is_none() {
        return Ok(());
    }

    let sources = ProfileValueSources::from_matches(matches);
    let path = default_profiles_path()?;
    apply_requested_profile_from_path(cli, &sources, &path)
}

fn default_profiles_path() -> Result<PathBuf> {
    if let Some(config_home) = std::env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(config_home).join("uvg").join("profiles.yaml"));
    }

    let home = std::env::var_os("HOME").map(PathBuf::from).context(
        "UVG_PROFILE was set, but HOME is not available to locate ~/.config/uvg/profiles.yaml",
    )?;
    Ok(home.join(".config").join("uvg").join("profiles.yaml"))
}

fn apply_requested_profile_from_path(
    cli: &mut Cli,
    sources: &ProfileValueSources,
    path: &Path,
) -> Result<()> {
    let Some(profile_name) = cli.profile.clone() else {
        return Ok(());
    };

    if !path.exists() {
        bail!(
            "profile `{}` requested but profile file not found at {}",
            profile_name,
            path.display()
        );
    }

    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read profile file {}", path.display()))?;
    let profiles: ProfilesFile = serde_yaml::from_str(&raw)
        .with_context(|| format!("failed to parse profile file {}", path.display()))?;

    let Some(profile) = profiles.profiles.get(&profile_name) else {
        let available = profiles
            .profiles
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        let available = if available.is_empty() {
            "(none)".to_string()
        } else {
            available
        };
        bail!(
            "unknown profile `{}` in {}; available profiles: {}",
            profile_name,
            path.display(),
            available
        );
    };

    fill_option(&mut cli.url, profile.source.clone(), sources, "url");
    fill_option(
        &mut cli.target_url,
        profile.target.clone(),
        sources,
        "target_url",
    );
    fill_string(
        &mut cli.generator,
        profile.generator.clone(),
        sources,
        "generator",
    );
    fill_option(
        &mut cli.target_dialect,
        profile.target_dialect.clone(),
        sources,
        "target_dialect",
    );
    fill_option(
        &mut cli.tables,
        profile.tables.as_deref().map(csv),
        sources,
        "tables",
    );
    fill_option(
        &mut cli.exclude_tables,
        profile.exclude_tables.as_deref().map(csv),
        sources,
        "exclude_tables",
    );
    fill_option(
        &mut cli.schemas,
        profile.schemas.as_deref().map(csv),
        sources,
        "schemas",
    );
    fill_option(
        &mut cli.options,
        profile.options.as_deref().map(csv),
        sources,
        "options",
    );
    fill_option(
        &mut cli.outfile,
        profile.outfile.clone(),
        sources,
        "outfile",
    );
    fill_option(
        &mut cli.out_dir,
        profile.out_dir.clone(),
        sources,
        "out_dir",
    );
    fill_option(&mut cli.name, profile.name.clone(), sources, "name");
    fill_bool(
        &mut cli.split_tables,
        profile.split_tables,
        sources,
        "split_tables",
    );
    fill_bool(&mut cli.apply, profile.apply, sources, "apply");
    fill_bool(
        &mut cli.no_parse_check,
        profile.no_parse_check,
        sources,
        "no_parse_check",
    );
    fill_bool(&mut cli.noviews, profile.noviews, sources, "noviews");
    fill_bool(
        &mut cli.trust_cert,
        profile.trust_cert,
        sources,
        "trust_cert",
    );

    Ok(())
}

fn fill_option<T>(
    slot: &mut Option<T>,
    profile_value: Option<T>,
    sources: &ProfileValueSources,
    arg_id: &'static str,
) {
    if !sources.explicit(arg_id) && slot.is_none() {
        *slot = profile_value;
    }
}

fn fill_string(
    slot: &mut String,
    profile_value: Option<String>,
    sources: &ProfileValueSources,
    arg_id: &'static str,
) {
    if !sources.explicit(arg_id) {
        if let Some(value) = profile_value {
            *slot = value;
        }
    }
}

fn fill_bool(
    slot: &mut bool,
    profile_value: Option<bool>,
    sources: &ProfileValueSources,
    arg_id: &'static str,
) {
    if !sources.explicit(arg_id) {
        if let Some(value) = profile_value {
            *slot = value;
        }
    }
}

fn csv(values: &[String]) -> String {
    values.join(",")
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;
