use std::{
    collections::BTreeMap,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Serialize, de::DeserializeOwned};
use serde_norway::Value;
use thiserror::Error;

use crate::{ConfigError, DepthLevel, GatherStepConfig};

const REGISTRY_VERSION: u32 = 1;
const REGISTRY_DIR_NAME: &str = ".gather-step";
const REGISTRY_FILE_NAME: &str = "registry.json";

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WorkspaceRegistry {
    #[serde(default = "registry_version")]
    pub version: u32,
    #[serde(default)]
    pub repos: BTreeMap<String, RegisteredRepo>,
}

impl Default for WorkspaceRegistry {
    fn default() -> Self {
        Self {
            version: REGISTRY_VERSION,
            repos: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredRepo {
    pub path: PathBuf,
    #[serde(default)]
    pub last_indexed_at: Option<String>,
    #[serde(default)]
    pub file_count: u64,
    #[serde(default)]
    pub symbol_count: u64,
    #[serde(default)]
    pub frameworks: Vec<String>,
    #[serde(default = "default_depth_level")]
    pub depth_level: DepthLevel,
    #[serde(default)]
    pub cursors: BTreeMap<RegistrySource, CursorState>,
}

impl RegisteredRepo {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>, depth_level: DepthLevel) -> Self {
        Self {
            path: path.into(),
            last_indexed_at: None,
            file_count: 0,
            symbol_count: 0,
            frameworks: Vec::new(),
            depth_level,
            cursors: BTreeMap::new(),
        }
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RegistrySource {
    Git,
    GithubPr,
    Jira,
    Blame,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CursorState {
    pub depth_level: DepthLevel,
    pub value: Value,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoIndexMetadata {
    pub last_indexed_at: Option<String>,
    pub file_count: u64,
    pub symbol_count: u64,
    pub frameworks: Vec<String>,
    pub depth_level: DepthLevel,
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("failed to read registry from {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write registry to {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse registry from {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_norway::Error,
    },
    #[error("failed to serialize registry data for {context}: {source}")]
    ValueSerialize {
        context: String,
        #[source]
        source: serde_norway::Error,
    },
    #[error("failed to deserialize registry data for {context}: {source}")]
    ValueDeserialize {
        context: String,
        #[source]
        source: serde_norway::Error,
    },
    #[error("registry at {path} uses unsupported version {version}")]
    UnsupportedVersion { path: String, version: u32 },
    #[error("config error: {0}")]
    Config(#[from] ConfigError),
    #[error("repo `{repo}` is not registered")]
    UnknownRepo { repo: String },
    #[error("repo name must not be empty")]
    EmptyRepoName,
    #[error("HOME is not set; cannot resolve default registry location")]
    HomeDirectoryUnavailable,
    #[error("failed to render registry JSON: {reason}")]
    JsonRender { reason: String },
}

pub struct RegistryStore {
    path: PathBuf,
    registry: WorkspaceRegistry,
}

impl RegistryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let path = path.as_ref().to_path_buf();
        let registry = WorkspaceRegistry::load_or_default_from_path(&path)?;
        Ok(Self { path, registry })
    }

    pub fn open_default() -> Result<Self, RegistryError> {
        Self::open(WorkspaceRegistry::default_path()?)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn registry(&self) -> &WorkspaceRegistry {
        &self.registry
    }

    #[must_use]
    pub fn into_registry(self) -> WorkspaceRegistry {
        self.registry
    }

    pub fn register_repo(
        &mut self,
        name: impl Into<String>,
        path: impl Into<PathBuf>,
        depth_level: Option<DepthLevel>,
    ) -> Result<(), RegistryError> {
        self.registry.register_repo(name, path, depth_level)?;
        self.save()
    }

    pub fn unregister_repo(&mut self, name: &str) -> Result<bool, RegistryError> {
        let removed = self.registry.unregister_repo(name);
        self.save()?;
        Ok(removed)
    }

    pub fn register_from_config(
        &mut self,
        config: &GatherStepConfig,
        config_root: impl AsRef<Path>,
    ) -> Result<(), RegistryError> {
        self.registry
            .register_from_config(config, config_root.as_ref())?;
        self.save()
    }

    pub fn register_from_config_file(
        &mut self,
        config_path: impl AsRef<Path>,
    ) -> Result<(), RegistryError> {
        let config_path = config_path.as_ref();
        let config = GatherStepConfig::from_yaml_file(config_path)?;
        let config_root = config_path.parent().unwrap_or_else(|| Path::new("."));
        self.register_from_config(&config, config_root)
    }

    pub fn update_repo_metadata(
        &mut self,
        repo: &str,
        metadata: RepoIndexMetadata,
    ) -> Result<(), RegistryError> {
        self.registry.update_repo_metadata(repo, metadata)?;
        self.save()
    }

    pub fn set_cursor<T: Serialize>(
        &mut self,
        repo: &str,
        source: RegistrySource,
        depth_level: DepthLevel,
        value: &T,
        updated_at: Option<String>,
    ) -> Result<(), RegistryError> {
        self.registry
            .set_cursor(repo, source, depth_level, value, updated_at)?;
        self.save()
    }

    pub fn get_cursor<T: DeserializeOwned>(
        &self,
        repo: &str,
        source: RegistrySource,
    ) -> Result<Option<T>, RegistryError> {
        self.registry.cursor(repo, source)
    }

    pub fn save(&self) -> Result<(), RegistryError> {
        self.registry.save_to_path(&self.path)
    }
}

impl WorkspaceRegistry {
    pub fn default_path() -> Result<PathBuf, RegistryError> {
        let home = env::var_os("HOME").ok_or(RegistryError::HomeDirectoryUnavailable)?;
        Ok(PathBuf::from(home)
            .join(REGISTRY_DIR_NAME)
            .join(REGISTRY_FILE_NAME))
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let path = path.as_ref();
        let path_display = path.display().to_string();
        let raw = fs::read_to_string(path).map_err(|source| RegistryError::Read {
            path: path_display.clone(),
            source,
        })?;
        let registry =
            serde_norway::from_str::<Self>(&raw).map_err(|source| RegistryError::Parse {
                path: path_display.clone(),
                source,
            })?;
        if registry.version != REGISTRY_VERSION {
            return Err(RegistryError::UnsupportedVersion {
                path: path_display,
                version: registry.version,
            });
        }
        Ok(registry)
    }

    pub fn load_or_default_from_path(path: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let path = path.as_ref();
        if path.exists() {
            Self::load_from_path(path)
        } else {
            Ok(Self::default())
        }
    }

    pub fn from_config_file(path: impl AsRef<Path>) -> Result<Self, RegistryError> {
        let path = path.as_ref();
        let config = GatherStepConfig::from_yaml_file(path)?;
        let config_root = path.parent().unwrap_or_else(|| Path::new("."));
        Self::from_config(&config, config_root)
    }

    pub fn from_config(
        config: &GatherStepConfig,
        config_root: impl AsRef<Path>,
    ) -> Result<Self, RegistryError> {
        let mut registry = Self::default();
        registry.register_from_config(config, config_root)?;
        Ok(registry)
    }

    pub fn save_to_path(&self, path: impl AsRef<Path>) -> Result<(), RegistryError> {
        let path = path.as_ref();
        let path_display = path.display().to_string();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| RegistryError::Write {
                path: path_display.clone(),
                source,
            })?;
        }

        let rendered = self.to_json_string()?;
        let temp_path = temporary_path_for(path);

        {
            let mut temp_file =
                fs::File::create(&temp_path).map_err(|source| RegistryError::Write {
                    path: temp_path.display().to_string(),
                    source,
                })?;
            temp_file
                .write_all(rendered.as_bytes())
                .map_err(|source| RegistryError::Write {
                    path: temp_path.display().to_string(),
                    source,
                })?;
            temp_file
                .sync_all()
                .map_err(|source| RegistryError::Write {
                    path: temp_path.display().to_string(),
                    source,
                })?;
        }

        // Persist via same-directory temp file + rename so readers never observe a partially
        // written registry file.
        fs::rename(&temp_path, path).map_err(|source| RegistryError::Write {
            path: path_display.clone(),
            source,
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
                RegistryError::Write {
                    path: path_display.clone(),
                    source,
                }
            })?;
        }
        if let Some(parent) = path.parent() {
            fs::File::open(parent)
                .and_then(|dir| dir.sync_all())
                .map_err(|source| RegistryError::Write {
                    path: parent.display().to_string(),
                    source,
                })?;
        }
        Ok(())
    }

    pub fn register_from_config(
        &mut self,
        config: &GatherStepConfig,
        config_root: impl AsRef<Path>,
    ) -> Result<(), RegistryError> {
        let config_root = config_root.as_ref();
        for repo in &config.repos {
            self.register_repo(
                repo.name.clone(),
                resolve_repo_path(config_root, &repo.path),
                repo.depth,
            )?;
        }
        Ok(())
    }

    pub fn register_repo(
        &mut self,
        name: impl Into<String>,
        path: impl Into<PathBuf>,
        depth_level: Option<DepthLevel>,
    ) -> Result<(), RegistryError> {
        let name = normalize_repo_name(name.into())?;
        let path = path.into();
        let depth_level = depth_level.unwrap_or(default_depth_level());

        self.repos
            .entry(name)
            .and_modify(|repo| {
                repo.path.clone_from(&path);
                repo.depth_level = depth_level;
            })
            .or_insert_with(|| RegisteredRepo::new(path, depth_level));

        Ok(())
    }

    #[must_use]
    pub fn unregister_repo(&mut self, name: &str) -> bool {
        self.repos.remove(name).is_some()
    }

    #[must_use]
    pub fn repo(&self, name: &str) -> Option<&RegisteredRepo> {
        self.repos.get(name)
    }

    pub fn update_repo_metadata(
        &mut self,
        repo: &str,
        metadata: RepoIndexMetadata,
    ) -> Result<(), RegistryError> {
        let entry = self
            .repos
            .get_mut(repo)
            .ok_or_else(|| RegistryError::UnknownRepo {
                repo: repo.to_owned(),
            })?;

        entry.last_indexed_at = metadata.last_indexed_at;
        entry.file_count = metadata.file_count;
        entry.symbol_count = metadata.symbol_count;
        entry.frameworks = metadata.frameworks;
        entry.depth_level = metadata.depth_level;

        Ok(())
    }

    pub fn set_cursor<T: Serialize>(
        &mut self,
        repo: &str,
        source: RegistrySource,
        depth_level: DepthLevel,
        value: &T,
        updated_at: Option<String>,
    ) -> Result<(), RegistryError> {
        let entry = self
            .repos
            .get_mut(repo)
            .ok_or_else(|| RegistryError::UnknownRepo {
                repo: repo.to_owned(),
            })?;

        let value =
            serde_norway::to_value(value).map_err(|source| RegistryError::ValueSerialize {
                context: format!("cursor `{source:?}` for repo `{repo}`"),
                source,
            })?;

        entry.cursors.insert(
            source,
            CursorState {
                depth_level,
                value,
                updated_at,
            },
        );

        Ok(())
    }

    pub fn cursor<T: DeserializeOwned>(
        &self,
        repo: &str,
        source: RegistrySource,
    ) -> Result<Option<T>, RegistryError> {
        let Some(repo) = self.repos.get(repo) else {
            return Err(RegistryError::UnknownRepo {
                repo: repo.to_owned(),
            });
        };

        let Some(cursor) = repo.cursors.get(&source) else {
            return Ok(None);
        };

        let value = serde_norway::from_value(cursor.value.clone()).map_err(|error| {
            RegistryError::ValueDeserialize {
                context: format!("cursor `{source:?}`"),
                source: error,
            }
        })?;

        Ok(Some(value))
    }

    fn to_json_string(&self) -> Result<String, RegistryError> {
        let value =
            serde_norway::to_value(self).map_err(|source| RegistryError::ValueSerialize {
                context: "workspace registry".to_owned(),
                source,
            })?;

        let mut output = String::new();
        write_json_value(&mut output, &value, 0)
            .map_err(|reason| RegistryError::JsonRender { reason })?;
        output.push('\n');
        Ok(output)
    }
}

fn registry_version() -> u32 {
    REGISTRY_VERSION
}

fn default_depth_level() -> DepthLevel {
    DepthLevel::Level1
}

fn normalize_repo_name(name: String) -> Result<String, RegistryError> {
    if name.trim().is_empty() {
        return Err(RegistryError::EmptyRepoName);
    }

    Ok(name)
}

fn resolve_repo_path(config_root: &Path, repo_path: &str) -> PathBuf {
    let repo_path = PathBuf::from(repo_path);
    if repo_path.is_absolute() {
        repo_path
    } else {
        config_root.join(repo_path)
    }
}

fn temporary_path_for(path: &Path) -> PathBuf {
    let mut name = path.file_name().map_or_else(
        || "registry".to_owned(),
        |name| name.to_string_lossy().into_owned(),
    );
    name.push_str(".tmp");
    path.with_file_name(name)
}

fn write_json_value(output: &mut String, value: &Value, indent: usize) -> Result<(), String> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(boolean) => output.push_str(if *boolean { "true" } else { "false" }),
        Value::Number(number) => output.push_str(&number.to_string()),
        Value::String(string) => write_json_string(output, string),
        Value::Sequence(items) => {
            if items.is_empty() {
                output.push_str("[]");
                return Ok(());
            }

            output.push('[');
            output.push('\n');
            for (index, item) in items.iter().enumerate() {
                write_indent(output, indent + 2);
                write_json_value(output, item, indent + 2)?;
                if index + 1 != items.len() {
                    output.push(',');
                }
                output.push('\n');
            }
            write_indent(output, indent);
            output.push(']');
        }
        Value::Mapping(map) => {
            if map.is_empty() {
                output.push_str("{}");
                return Ok(());
            }

            let mut entries = Vec::with_capacity(map.len());
            for (key, value) in map {
                let Value::String(key) = key else {
                    return Err("registry JSON keys must be strings".to_owned());
                };
                entries.push((key.as_str(), value));
            }
            entries.sort_by(|left, right| left.0.cmp(right.0));

            output.push('{');
            output.push('\n');
            for (index, (key, value)) in entries.iter().enumerate() {
                write_indent(output, indent + 2);
                write_json_string(output, key);
                output.push_str(": ");
                write_json_value(output, value, indent + 2)?;
                if index + 1 != entries.len() {
                    output.push(',');
                }
                output.push('\n');
            }
            write_indent(output, indent);
            output.push('}');
        }
        Value::Tagged(tagged) => write_json_value(output, &tagged.value, indent)?,
    }

    Ok(())
}

fn write_json_string(output: &mut String, string: &str) {
    output.push('"');
    for character in string.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0C}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character <= '\u{1F}' => {
                let escaped = format!("\\u{:04X}", u32::from(character));
                output.push_str(&escaped);
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

fn write_indent(output: &mut String, indent: usize) {
    for _ in 0..indent {
        output.push(' ');
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::{RegistrySource, RegistryStore, RepoIndexMetadata, WorkspaceRegistry};
    use crate::DepthLevel;

    static TEMP_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct GithubPrCursor {
        last_pr: u64,
        etag: String,
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let counter = TEMP_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-core-registry-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test directory should be created");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn register_repo_persists_to_registry_json() {
        let temp_dir = TestDir::new("register");
        let registry_path = temp_dir.path().join("registry.json");
        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");

        registry
            .register_repo(
                "service-a",
                temp_dir.path().join("repos/service-a"),
                Some(DepthLevel::Full),
            )
            .expect("repo should register");

        let raw = fs::read_to_string(&registry_path).expect("registry should be written");
        assert!(raw.contains("\"service-a\""));

        let reopened = WorkspaceRegistry::load_from_path(&registry_path).expect("registry loads");
        let repo = reopened.repo("service-a").expect("repo should exist");
        assert_eq!(repo.depth_level, DepthLevel::Full);
        assert_eq!(repo.path, temp_dir.path().join("repos/service-a"));
    }

    #[test]
    fn unregister_repo_removes_it_from_registry() {
        let temp_dir = TestDir::new("unregister");
        let registry_path = temp_dir.path().join("registry.json");
        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");

        registry
            .register_repo("service-a", temp_dir.path().join("repos/service-a"), None)
            .expect("repo should register");
        let removed = registry
            .unregister_repo("service-a")
            .expect("repo should unregister");

        assert!(removed);

        let raw = fs::read_to_string(&registry_path).expect("registry should be written");
        assert!(!raw.contains("\"service-a\""));
        let reopened = WorkspaceRegistry::load_from_path(&registry_path).expect("registry loads");
        assert!(reopened.repo("service-a").is_none());
    }

    #[test]
    fn builds_registry_from_multi_repo_workspace_config() {
        let temp_dir = TestDir::new("config");
        let config_path = temp_dir.path().join("gather-step.config.yaml");
        fs::write(
            &config_path,
            r"
repos:
  - name: service-a
    path: ./repos/service-a
    depth: full
  - name: service-b
    path: ./repos/service-b
github:
  owner: acme-corp
indexing:
  include_dotfiles: true
",
        )
        .expect("config should be written");

        let registry =
            WorkspaceRegistry::from_config_file(&config_path).expect("config should parse");

        assert_eq!(registry.repos.len(), 2);
        assert_eq!(
            registry.repo("service-a").map(|repo| repo.depth_level),
            Some(DepthLevel::Full)
        );
        assert_eq!(
            registry.repo("service-b").map(|repo| repo.depth_level),
            Some(DepthLevel::Level1)
        );
        assert_eq!(
            registry.repo("service-a").map(|repo| repo.path.clone()),
            Some(temp_dir.path().join("repos/service-a"))
        );
    }

    #[test]
    fn cursor_state_can_be_stored_and_retrieved() {
        let temp_dir = TestDir::new("cursor");
        let registry_path = temp_dir.path().join("registry.json");
        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");

        registry
            .register_repo("service-a", temp_dir.path().join("repos/service-a"), None)
            .expect("repo should register");
        registry
            .set_cursor(
                "service-a",
                RegistrySource::GithubPr,
                DepthLevel::Level2,
                &GithubPrCursor {
                    last_pr: 1_897,
                    etag: "W/abc".to_owned(),
                },
                Some("2026-04-13T08:45:00Z".to_owned()),
            )
            .expect("cursor should be stored");

        let reopened = RegistryStore::open(&registry_path).expect("registry should reopen");
        let cursor = reopened
            .get_cursor::<GithubPrCursor>("service-a", RegistrySource::GithubPr)
            .expect("cursor should deserialize")
            .expect("cursor should exist");

        assert_eq!(
            cursor,
            GithubPrCursor {
                last_pr: 1_897,
                etag: "W/abc".to_owned(),
            }
        );
    }

    #[test]
    fn repo_metadata_can_be_updated() {
        let mut registry = WorkspaceRegistry::default();
        registry
            .register_repo("service-a", "/tmp/service-a", Some(DepthLevel::Level1))
            .expect("repo should register");

        registry
            .update_repo_metadata(
                "service-a",
                RepoIndexMetadata {
                    last_indexed_at: Some("2026-04-13T09:00:00Z".to_owned()),
                    file_count: 42,
                    symbol_count: 314,
                    frameworks: vec!["nestjs".to_owned()],
                    depth_level: DepthLevel::Level3,
                },
            )
            .expect("metadata should update");

        let repo = registry.repo("service-a").expect("repo should exist");
        assert_eq!(
            repo.last_indexed_at.as_deref(),
            Some("2026-04-13T09:00:00Z")
        );
        assert_eq!(repo.file_count, 42);
        assert_eq!(repo.symbol_count, 314);
        assert_eq!(repo.frameworks, vec!["nestjs".to_owned()]);
        assert_eq!(repo.depth_level, DepthLevel::Level3);
    }

    #[test]
    fn registry_load_ignores_unknown_fields_in_persisted_json() {
        let temp_dir = TestDir::new("unknown-fields");
        let registry_path = temp_dir.path().join("registry.json");
        fs::write(
            &registry_path,
            r#"{
  "future_registry_field": true,
  "repos": {
    "service-a": {
      "cursors": {
        "github_pr": {
          "depth_level": "level2",
          "future_cursor_field": "kept by newer builds",
          "updated_at": "2026-04-13T08:45:00Z",
          "value": {
            "etag": "W/abc",
            "future_value_field": 99,
            "last_pr": 1897
          }
        }
      },
      "depth_level": "full",
      "file_count": 42,
      "future_repo_field": "newer schema",
      "last_indexed_at": "2026-04-13T09:00:00Z",
      "path": "/tmp/service-a",
      "symbol_count": 314
    }
  },
  "version": 1
}
"#,
        )
        .expect("registry fixture should be written");

        let registry = WorkspaceRegistry::load_from_path(&registry_path)
            .expect("registry should ignore unknown persisted fields");
        let repo = registry.repo("service-a").expect("repo should exist");
        assert_eq!(repo.depth_level, DepthLevel::Full);
        assert_eq!(repo.file_count, 42);
        assert_eq!(repo.symbol_count, 314);
        assert_eq!(
            repo.last_indexed_at.as_deref(),
            Some("2026-04-13T09:00:00Z")
        );

        let cursor: GithubPrCursor = registry
            .cursor("service-a", RegistrySource::GithubPr)
            .expect("cursor lookup should succeed")
            .expect("cursor should exist");
        assert_eq!(
            cursor,
            GithubPrCursor {
                last_pr: 1_897,
                etag: "W/abc".to_owned(),
            }
        );
    }
}
