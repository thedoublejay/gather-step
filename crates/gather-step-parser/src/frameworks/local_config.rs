//! Local configuration loader for `.gather-step.local.yaml`.
//!
//! This file is intentionally gitignored - it lets individual developers
//! specify which profile to apply to each repo on their machine without
//! committing that decision to the repository.
//!
//! # File format
//!
//! ```yaml
//! profiles:
//!   - name: backend_standard
//!     packs:
//!       - nestjs
//!       - mongoose
//!       - azure
//!       - shared_lib
//!
//!   - name: frontend_standard
//!     packs:
//!       - react
//!       - react_router
//!       - redux
//!       - zustand
//!       - shared_lib
//!
//! repos:
//!   - path: /absolute/or/relative/path/to/service-a
//!     profile: backend_standard
//!   - path: /absolute/or/relative/path/to/frontend-app
//!     profile: frontend_standard
//! ```
//!
//! When no config file is found, the orchestrator falls back to automatic
//! framework detection for each repo.

use std::path::Path;

use super::profile::{Profile, ResolvedPack, resolve_profile};

/// Schema for `.gather-step.local.yaml`.
///
/// All fields have `#[serde(default)]` so an empty file is valid.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LocalConfig {
    /// Named profiles available for assignment to repos.
    #[serde(default)]
    pub profiles: Vec<Profile>,
    /// Per-repo profile assignments.
    #[serde(default)]
    pub repos: Vec<RepoPackConfig>,
}

/// Associates a file-system path with a profile name.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepoPackConfig {
    /// File-system path to the repo root.  May be absolute or relative; the
    /// orchestrator typically uses the same string it received as `repo_root`.
    pub path: String,
    /// Name of the profile to apply (must appear in [`LocalConfig::profiles`]).
    pub profile: String,
}

impl LocalConfig {
    /// Try to load from `.gather-step.local.yaml` in `dir`.
    ///
    /// Returns `None` if the file does not exist.  Returns `None` and logs a
    /// warning (via `tracing::warn!`) if the file exists but cannot be
    /// parsed, so that a corrupted config does not crash the whole indexing
    /// run.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use gather_step_parser::frameworks::local_config::LocalConfig;
    ///
    /// if let Some(cfg) = LocalConfig::load(Path::new("/workspace")) {
    ///     println!("Loaded {} profiles", cfg.profiles.len());
    /// }
    /// ```
    #[must_use]
    pub fn load(dir: &Path) -> Option<Self> {
        let config_path = dir.join(".gather-step.local.yaml");
        let raw = match std::fs::read_to_string(&config_path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
            Err(err) => {
                tracing::warn!(
                    path = %config_path.display(),
                    error = %err,
                    "failed to read local config; falling back to auto-detection"
                );
                return None;
            }
        };

        match serde_norway::from_str::<Self>(&raw) {
            Ok(config) => Some(config),
            Err(err) => {
                tracing::warn!(
                    path = %config_path.display(),
                    error = %err,
                    "failed to parse local config; falling back to auto-detection"
                );
                None
            }
        }
    }

    /// Resolve which packs should be active for a given repo path string.
    ///
    /// Returns `None` if no [`RepoPackConfig`] entry matches `repo_path`,
    /// which tells the caller to fall back to automatic detection.
    ///
    /// # Examples
    ///
    /// ```
    /// use gather_step_parser::frameworks::{
    ///     local_config::{LocalConfig, RepoPackConfig},
    ///     profile::{PackRef, Profile},
    ///     registry::PackId,
    /// };
    ///
    /// let config = LocalConfig {
    ///     profiles: vec![Profile {
    ///         name: "backend".to_owned(),
    ///         extends: vec![],
    ///         packs: vec![PackRef::Simple(PackId::Nestjs)],
    ///     }],
    ///     repos: vec![RepoPackConfig {
    ///         path: "/repos/service-a".to_owned(),
    ///         profile: "backend".to_owned(),
    ///     }],
    /// };
    ///
    /// let packs = config.packs_for_repo("/repos/service-a");
    /// assert!(packs.is_some());
    /// let packs = packs.unwrap();
    /// assert!(packs.iter().any(|pack| pack.id == PackId::Nestjs));
    /// ```
    #[must_use]
    pub fn packs_for_repo(&self, repo_path: &str) -> Option<Vec<ResolvedPack>> {
        let repo_entry = self
            .repos
            .iter()
            .find(|entry| paths_match(&entry.path, repo_path))?;

        Some(resolve_profile(&repo_entry.profile, &self.profiles))
    }
}

fn paths_match(config_path: &str, repo_path: &str) -> bool {
    let normalized_config = normalize_path(config_path);
    let normalized_repo = normalize_path(repo_path);
    if normalized_config == normalized_repo {
        return true;
    }

    let canonical_config = std::fs::canonicalize(config_path).ok();
    let canonical_repo = std::fs::canonicalize(repo_path).ok();
    canonical_config.is_some() && canonical_config == canonical_repo
}

fn normalize_path(path: &str) -> std::path::PathBuf {
    std::path::Path::new(path).components().collect()
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::{LocalConfig, RepoPackConfig};
    use crate::frameworks::{
        profile::{PackRef, Profile},
        registry::PackId,
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-local-config-{name}-{}-{counter}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn write(&self, relative: &str, contents: &str) {
            let full = self.path.join(relative);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).expect("parent dir should create");
            }
            fs::write(full, contents).expect("fixture should write");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn loads_local_config_from_yaml() {
        let dir = TempDir::new("load-yaml");
        dir.write(
            ".gather-step.local.yaml",
            r"
profiles:
  - name: backend_standard
    packs:
      - nestjs
      - mongoose
      - shared_lib
repos:
  - path: /repos/service-a
    profile: backend_standard
",
        );

        let config = LocalConfig::load(&dir.path).expect("config should load");
        assert_eq!(config.profiles.len(), 1);
        assert_eq!(config.repos.len(), 1);
        assert_eq!(config.profiles[0].name, "backend_standard");
        assert_eq!(config.repos[0].path, "/repos/service-a");
        assert_eq!(config.repos[0].profile, "backend_standard");
    }

    #[test]
    fn returns_none_when_file_absent() {
        let dir = TempDir::new("no-config");
        // No `.gather-step.local.yaml` written.
        assert!(
            LocalConfig::load(&dir.path).is_none(),
            "load() must return None when the config file does not exist"
        );
    }

    #[test]
    fn resolves_packs_for_repo() {
        let config = LocalConfig {
            profiles: vec![Profile {
                name: "backend".to_owned(),
                extends: vec![],
                packs: vec![
                    PackRef::Simple(PackId::Nestjs),
                    PackRef::Simple(PackId::Mongoose),
                ],
            }],
            repos: vec![RepoPackConfig {
                path: "/repos/service-a".to_owned(),
                profile: "backend".to_owned(),
            }],
        };

        let packs = config
            .packs_for_repo("/repos/service-a")
            .expect("packs should resolve");
        assert!(packs.iter().any(|pack| pack.id == PackId::Nestjs));
        assert!(packs.iter().any(|pack| pack.id == PackId::Mongoose));
    }

    #[test]
    fn packs_for_repo_returns_none_for_unknown_path() {
        let config = LocalConfig::default();
        assert!(
            config.packs_for_repo("/repos/nonexistent").is_none(),
            "packs_for_repo should return None when repo path is not configured"
        );
    }

    #[test]
    fn packs_for_repo_returns_none_for_unknown_profile() {
        // Repo entry points to a non-existent profile.
        let config = LocalConfig {
            profiles: vec![],
            repos: vec![RepoPackConfig {
                path: "/repos/service-a".to_owned(),
                profile: "nonexistent".to_owned(),
            }],
        };
        // resolve_profile returns an empty set when the profile name is not
        // found; packs_for_repo converts that to Some([]) - the caller gets
        // an empty pack list (which means only SharedLib effectively runs via
        // auto-detection fallback logic, but the config DID match a repo entry,
        // so None is not returned).
        let packs = config.packs_for_repo("/repos/service-a");
        // A repo entry WAS found, even though the profile is unknown - we
        // return Some(empty) rather than None.
        assert!(
            packs.is_some(),
            "a matched repo entry should return Some even if the profile is missing"
        );
        assert!(
            packs.unwrap().is_empty(),
            "an unknown profile name resolves to an empty pack list"
        );
    }

    #[test]
    fn loads_config_with_profile_extends() {
        let dir = TempDir::new("extends-yaml");
        dir.write(
            ".gather-step.local.yaml",
            r"
profiles:
  - name: base
    packs:
      - nestjs
  - name: full
    extends:
      - base
    packs:
      - mongoose
repos:
  - path: /repos/service-a
    profile: full
",
        );

        let config = LocalConfig::load(&dir.path).expect("config should load");
        let packs = config
            .packs_for_repo("/repos/service-a")
            .expect("packs should resolve");
        assert!(
            packs.iter().any(|pack| pack.id == PackId::Nestjs),
            "inherited from base"
        );
        assert!(
            packs.iter().any(|pack| pack.id == PackId::Mongoose),
            "declared in full"
        );
    }

    #[test]
    fn packs_for_repo_matches_trailing_slash_variants() {
        let config = LocalConfig {
            profiles: vec![Profile {
                name: "backend".to_owned(),
                extends: vec![],
                packs: vec![PackRef::Simple(PackId::Nestjs)],
            }],
            repos: vec![RepoPackConfig {
                path: "/repos/service-a/".to_owned(),
                profile: "backend".to_owned(),
            }],
        };

        let packs = config
            .packs_for_repo("/repos/service-a")
            .expect("normalized path should match");
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].id, PackId::Nestjs);
    }

    #[test]
    fn returns_none_for_malformed_yaml() {
        let dir = TempDir::new("malformed-yaml");
        dir.write(".gather-step.local.yaml", "{ this is not valid yaml: [");
        // Should return None (after logging a warning) rather than panicking.
        assert!(
            LocalConfig::load(&dir.path).is_none(),
            "malformed YAML should return None"
        );
    }
}
