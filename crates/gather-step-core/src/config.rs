use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatherStepConfig {
    #[serde(default)]
    pub allow_listed_repos: Vec<String>,
    pub repos: Vec<RepoConfig>,
    #[serde(default)]
    pub github: Option<GithubConfig>,
    #[serde(default)]
    pub jira: Option<JiraConfig>,
    #[serde(default)]
    pub indexing: IndexingConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepoConfig {
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub depth: Option<DepthLevel>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GithubConfig {
    pub owner: String,
    #[serde(default)]
    pub api_base_url: Option<String>,
    #[serde(default)]
    pub token_env: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JiraConfig {
    pub project_key: String,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub token_env: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexingConfig {
    #[serde(default = "default_exclude")]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub language_excludes: Vec<LanguageExcludeConfig>,
    #[serde(default)]
    pub include_languages: Vec<String>,
    #[serde(default)]
    pub include_dotfiles: bool,
    #[serde(default)]
    pub min_file_size: Option<String>,
    #[serde(default = "default_max_file_size")]
    pub max_file_size: String,
    #[serde(default)]
    pub workspace_concurrency: Option<usize>,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            exclude: default_exclude(),
            language_excludes: Vec::new(),
            include_languages: Vec::new(),
            include_dotfiles: false,
            min_file_size: None,
            max_file_size: default_max_file_size(),
            workspace_concurrency: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LanguageExcludeConfig {
    pub language: String,
    #[serde(default)]
    pub patterns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum DepthLevel {
    #[serde(alias = "1")]
    Level1,
    #[serde(alias = "2")]
    Level2,
    #[serde(alias = "3")]
    Level3,
    Full,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config from {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse YAML config from {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_norway::Error,
    },
    #[error("invalid config in {path}: {reason}")]
    Validation { path: String, reason: String },
}

/// Maximum byte length accepted for a workspace config file.  Files larger
/// than this are rejected before being handed to the YAML parser; YAML anchor
/// expansion can cause unbounded memory growth on adversarially crafted input.
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;

impl GatherStepConfig {
    pub fn from_yaml_str(input: &str) -> Result<Self, ConfigError> {
        let config: Self = serde_norway::from_str(input).map_err(|source| ConfigError::Parse {
            path: "<inline>".to_owned(),
            source,
        })?;
        config.validate_with_path("<inline>")?;
        Ok(config)
    }

    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let path = path_ref.display().to_string();

        // Reject files larger than 1 MiB before handing them to the YAML
        // parser.  serde_norway internally expands YAML anchors, so an
        // adversarially crafted config (billion-laughs style) could otherwise
        // cause unbounded memory growth.
        let file_size = fs::metadata(path_ref)
            .map_err(|source| ConfigError::Read {
                path: path.clone(),
                source,
            })?
            .len();
        if file_size > MAX_CONFIG_BYTES {
            return Err(ConfigError::Validation {
                path: path.clone(),
                reason: format!("config file exceeds 1 MiB safety limit ({file_size} bytes)"),
            });
        }

        let raw = fs::read_to_string(path_ref).map_err(|source| ConfigError::Read {
            path: path.clone(),
            source,
        })?;

        let config: Self = serde_norway::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.clone(),
            source,
        })?;
        config.validate_with_path(&path)?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        self.validate_with_path("<inline>")
    }

    pub fn validate_repo_roots_against_config_root(
        &self,
        config_root: &Path,
    ) -> Result<(), ConfigError> {
        let root_display = config_root.display().to_string();
        let canonical_root = fs::canonicalize(config_root).map_err(|source| ConfigError::Read {
            path: root_display.clone(),
            source,
        })?;

        for repo in &self.repos {
            let repo_root = config_root.join(&repo.path);
            let metadata = match fs::symlink_metadata(&repo_root) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(ConfigError::Validation {
                        path: root_display.clone(),
                        reason: format!(
                            "repo `{}` path does not exist: {}",
                            repo.name,
                            repo_root.display()
                        ),
                    });
                }
                Err(source) => {
                    return Err(ConfigError::Read {
                        path: repo_root.display().to_string(),
                        source,
                    });
                }
            };

            if metadata.file_type().is_symlink() {
                return Err(ConfigError::Validation {
                    path: root_display.clone(),
                    reason: format!(
                        "repo `{}` path resolves through a symlinked repo root",
                        repo.name
                    ),
                });
            }

            let canonical_repo =
                fs::canonicalize(&repo_root).map_err(|source| ConfigError::Read {
                    path: repo_root.display().to_string(),
                    source,
                })?;
            if !canonical_repo.starts_with(&canonical_root) {
                return Err(ConfigError::Validation {
                    path: root_display.clone(),
                    reason: format!("repo `{}` path resolves outside the config root", repo.name),
                });
            }
        }

        Ok(())
    }

    fn validate_with_path(&self, path: &str) -> Result<(), ConfigError> {
        if self.repos.is_empty() {
            return Err(ConfigError::Validation {
                path: path.to_owned(),
                reason: "at least one repo must be configured".to_owned(),
            });
        }

        let mut repo_names = BTreeSet::new();
        let mut repo_paths = Vec::<(&str, PathBuf)>::new();
        for repo in &self.repos {
            if repo.name.trim().is_empty() {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: "repo names must not be empty".to_owned(),
                });
            }
            if repo
                .name
                .chars()
                .any(|ch| matches!(ch, '/' | '\\' | '\n' | '\r'))
                || repo.name == "."
                || repo.name == ".."
            {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!(
                        "repo `{}` name must not contain path separators or dot segments",
                        repo.name
                    ),
                });
            }
            if repo.name.contains('\0') || repo.path.contains('\0') {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!(
                        "repo `{}` contains an embedded NUL byte in its name or path",
                        repo.name
                    ),
                });
            }
            let repo_path = Path::new(&repo.path);
            if repo_path.is_absolute() {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!(
                        "repo `{}` path must be relative to the config root",
                        repo.name
                    ),
                });
            }
            if repo_path
                .components()
                .any(|component| matches!(component, Component::ParentDir))
            {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!("repo `{}` path must not escape the config root", repo.name),
                });
            }
            if !repo_names.insert(repo.name.as_str()) {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!("duplicate repo name `{}`", repo.name),
                });
            }
            repo_paths.push((repo.name.as_str(), normalize_repo_path(repo_path)));
        }

        for (index, (left_name, left_path)) in repo_paths.iter().enumerate() {
            for (right_name, right_path) in repo_paths.iter().skip(index + 1) {
                if left_path.starts_with(right_path) || right_path.starts_with(left_path) {
                    return Err(ConfigError::Validation {
                        path: path.to_owned(),
                        reason: format!(
                            "repo paths for `{left_name}` and `{right_name}` must not overlap"
                        ),
                    });
                }
            }
        }

        let configured_repo_names: BTreeSet<&str> =
            self.repos.iter().map(|repo| repo.name.as_str()).collect();
        let mut allow_listed = BTreeSet::new();
        for repo_name in &self.allow_listed_repos {
            if !configured_repo_names.contains(repo_name.as_str()) {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!("allow_listed_repos references unknown repo `{repo_name}`"),
                });
            }
            if !allow_listed.insert(repo_name.as_str()) {
                return Err(ConfigError::Validation {
                    path: path.to_owned(),
                    reason: format!("duplicate allow-listed repo `{repo_name}`"),
                });
            }
        }

        Ok(())
    }
}

fn normalize_repo_path(path: &Path) -> PathBuf {
    path.components()
        .filter(|component| !matches!(component, Component::CurDir))
        .fold(PathBuf::new(), |mut normalized, component| {
            normalized.push(component.as_os_str());
            normalized
        })
}

fn default_exclude() -> Vec<String> {
    vec![
        "node_modules".to_owned(),
        "dist".to_owned(),
        "*.min.js".to_owned(),
        "*.map".to_owned(),
        "*.lock".to_owned(),
        "*.d.ts".to_owned(),
    ]
}

fn default_max_file_size() -> String {
    "1MB".to_owned()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use pretty_assertions::assert_eq;

    use super::{ConfigError, DepthLevel, GatherStepConfig};

    #[test]
    fn parses_workspace_yaml_config() {
        let config = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: ./repos/service-a
    depth: full
  - name: service-b
    path: ./repos/service-b
github:
  owner: acme-corp
  token_env: GITHUB_TOKEN
indexing:
  include_dotfiles: false
  max_file_size: 2MB
  min_file_size: 1KB
  include_languages:
    - typescript
    - python
  exclude:
    - node_modules
    - dist
  language_excludes:
    - language: typescript
      patterns:
        - '*.d.ts'
allow_listed_repos:
  - service-a
",
        )
        .expect("yaml should parse");

        assert_eq!(config.repos.len(), 2);
        assert_eq!(config.repos[0].depth, Some(DepthLevel::Full));
        assert_eq!(
            config.github.as_ref().map(|cfg| cfg.owner.as_str()),
            Some("acme-corp")
        );
        assert_eq!(config.indexing.max_file_size, "2MB");
        assert_eq!(config.indexing.min_file_size.as_deref(), Some("1KB"));
        assert_eq!(config.indexing.include_languages.len(), 2);
        assert_eq!(config.indexing.language_excludes.len(), 1);
        assert_eq!(config.allow_listed_repos, vec!["service-a".to_owned()]);
    }

    #[test]
    fn indexing_config_defaults_apply() {
        let config = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: ./repos/service-a
",
        )
        .expect("yaml should parse");

        assert_eq!(config.indexing.include_dotfiles, false);
        assert_eq!(config.indexing.max_file_size, "1MB");
        assert_eq!(config.indexing.min_file_size, None);
        assert!(
            config
                .indexing
                .exclude
                .iter()
                .any(|entry| entry == "node_modules")
        );
        assert!(config.allow_listed_repos.is_empty());
    }

    #[test]
    fn rejects_unknown_fields() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: ./repos/service-a
indexing:
  unknown_flag: true
",
        )
        .expect_err("unknown fields should fail");

        assert!(matches!(error, ConfigError::Parse { .. }));
    }

    #[test]
    fn rejects_duplicate_repo_names() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: ./repos/service-a
  - name: service-a
    path: ./repos/service-a-copy
",
        )
        .expect_err("duplicate repo names should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
    }

    #[test]
    fn rejects_overlapping_repo_paths() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: backend_standard
    path: repos/backend
  - name: shared_contracts
    path: repos/backend/shared
",
        )
        .expect_err("overlapping repo paths should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
    }

    #[test]
    fn rejects_unknown_allow_listed_repo() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: ./repos/service-a
allow_listed_repos:
  - billing
",
        )
        .expect_err("unknown allow-listed repo should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
    }

    #[test]
    fn rejects_absolute_repo_paths() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: /tmp/service-a
",
        )
        .expect_err("absolute repo paths should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
    }

    #[test]
    fn rejects_parent_dir_repo_paths() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: ../service-a
",
        )
        .expect_err("parent-dir repo paths should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
    }

    #[test]
    fn rejects_repo_names_with_path_separators() {
        let error = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: ../../escape
    path: repos/service-a
",
        )
        .expect_err("repo names with path separators should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
    }

    #[test]
    #[cfg(unix)]
    fn rejects_symlink_repo_roots_outside_config_root() {
        use std::os::unix::fs::symlink;
        use std::time::{SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should work")
            .as_nanos();
        let temp = std::env::temp_dir().join(format!("gather-step-config-symlink-{unique}"));
        let workspace_root = temp.join("workspace");
        let external_root = temp.join("external");
        fs::create_dir_all(workspace_root.join("repos")).expect("workspace repos");
        fs::create_dir_all(&external_root).expect("external root");
        symlink(&external_root, workspace_root.join("repos/link")).expect("repo root symlink");

        let config = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: backend_standard
    path: repos/link
",
        )
        .expect("config should parse");

        let error = config
            .validate_repo_roots_against_config_root(&workspace_root)
            .expect_err("symlink repo root should fail");

        assert!(matches!(error, ConfigError::Validation { .. }));
        let _ = fs::remove_dir_all(&temp);
    }

    #[test]
    fn rejects_missing_repo_roots_with_stable_validation_error() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should work")
            .as_nanos();
        let temp = std::env::temp_dir().join(format!("gather-step-config-missing-{unique}"));
        let workspace_root = temp.join("workspace");
        fs::create_dir_all(&workspace_root).expect("workspace root");

        let config = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: backend_standard
    path: repos/missing
",
        )
        .expect("config should parse");

        let error = config
            .validate_repo_roots_against_config_root(&workspace_root)
            .expect_err("missing repo root should fail");

        let ConfigError::Validation { reason, .. } = error else {
            panic!("expected validation error for missing repo root");
        };
        assert!(reason.contains("repo `backend_standard` path does not exist"));
        let _ = fs::remove_dir_all(&temp);
    }
}
