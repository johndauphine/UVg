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
        profile.tables.as_ref().map(csv),
        sources,
        "tables",
    );
    fill_option(
        &mut cli.exclude_tables,
        profile.exclude_tables.as_ref().map(csv),
        sources,
        "exclude_tables",
    );
    fill_option(
        &mut cli.schemas,
        profile.schemas.as_ref().map(csv),
        sources,
        "schemas",
    );
    fill_option(
        &mut cli.options,
        profile.options.as_ref().map(csv),
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

fn csv(values: &Vec<String>) -> String {
    values.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apply_progress::ProgressMode;

    fn default_cli(profile: &str) -> Cli {
        Cli {
            command: None,
            profile: Some(profile.to_string()),
            url: None,
            target_url: None,
            generator: "declarative".to_string(),
            target_dialect: None,
            split_tables: false,
            apply: false,
            progress: ProgressMode::Auto,
            apply_retries: 3,
            no_parse_check: false,
            risk_classify: false,
            introspect_concurrency: crate::cli::DEFAULT_INTROSPECT_CONCURRENCY,
            tables: None,
            exclude_tables: None,
            schemas: None,
            noviews: false,
            options: None,
            outfile: None,
            out_dir: None,
            name: None,
            trust_cert: false,
            interactive: false,
        }
    }

    fn temp_profile_path(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("uvg-profile-test-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn write_profile(contents: &str) -> PathBuf {
        let path = temp_profile_path("profiles.yaml");
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn profile_fills_empty_cli_fields() {
        let path = write_profile(
            r#"
profiles:
  prod:
    source: postgresql://src/db
    target: mysql://target/db
    generator: ddl
    target_dialect: postgres
    schemas: [public, audit]
    exclude_tables: ["__*"]
    noviews: true
"#,
        );
        let mut cli = default_cli("prod");

        apply_requested_profile_from_path(&mut cli, &ProfileValueSources::default(), &path)
            .unwrap();

        assert_eq!(cli.url.as_deref(), Some("postgresql://src/db"));
        assert_eq!(cli.target_url.as_deref(), Some("mysql://target/db"));
        assert_eq!(cli.generator, "ddl");
        assert_eq!(cli.target_dialect.as_deref(), Some("postgres"));
        assert_eq!(cli.schemas.as_deref(), Some("public,audit"));
        assert_eq!(cli.exclude_tables.as_deref(), Some("__*"));
        assert!(cli.noviews);
    }

    #[test]
    fn command_line_values_override_profile() {
        let path = write_profile(
            r#"
profiles:
  prod:
    source: postgresql://profile/db
    generator: ddl
    schemas: [profile]
"#,
        );
        let mut cli = default_cli("prod");
        cli.url = Some("postgresql://cli/db".to_string());
        cli.generator = "declarative".to_string();
        let mut sources = ProfileValueSources::default();
        sources.command_line.insert("url");
        sources.command_line.insert("generator");

        apply_requested_profile_from_path(&mut cli, &sources, &path).unwrap();

        assert_eq!(cli.url.as_deref(), Some("postgresql://cli/db"));
        assert_eq!(cli.generator, "declarative");
        assert_eq!(cli.schemas.as_deref(), Some("profile"));
    }

    #[test]
    fn missing_profile_file_reports_path() {
        let mut cli = default_cli("prod");
        let path = temp_profile_path("missing.yaml");

        let err =
            apply_requested_profile_from_path(&mut cli, &ProfileValueSources::default(), &path)
                .unwrap_err()
                .to_string();

        assert!(err.contains("profile `prod` requested"));
        assert!(err.contains(path.to_str().unwrap()));
    }

    #[test]
    fn unknown_profile_lists_available_profiles() {
        let path = write_profile(
            r#"
profiles:
  prod:
    source: postgresql://prod/db
  staging:
    source: postgresql://staging/db
"#,
        );
        let mut cli = default_cli("qa");

        let err =
            apply_requested_profile_from_path(&mut cli, &ProfileValueSources::default(), &path)
                .unwrap_err()
                .to_string();

        assert!(err.contains("unknown profile `qa`"));
        assert!(err.contains("prod, staging"));
    }
}
