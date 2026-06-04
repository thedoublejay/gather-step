use std::path::Path;

use gather_step_core::RegistryStore;
use gather_step_git::{GitHistoryIndexer, GitRepoSource, IndexFreshness};
use gather_step_storage::{MetadataStore, MetadataStoreDb};
use serde::Serialize;
use tracing::warn;

#[derive(Debug, Clone, Serialize)]
pub struct RepoFreshness {
    pub repo: String,
    pub freshness: String,
}

#[must_use]
pub fn freshness_label(freshness: &IndexFreshness) -> &'static str {
    match freshness {
        IndexFreshness::Fresh { .. } => "fresh",
        IndexFreshness::Stale { .. } => "stale",
        IndexFreshness::NeverIndexed { .. } => "never_indexed",
    }
}

#[must_use]
pub fn repo_freshness(repo: &str, path: &Path, indexed_sha: Option<&str>) -> String {
    let indexer = GitHistoryIndexer::new(GitRepoSource::from_path(path), repo);
    match indexer.index_freshness(indexed_sha) {
        Ok(freshness) => freshness_label(&freshness).to_owned(),
        Err(error) => {
            warn!(
                repo = %repo,
                path = %path.display(),
                %error,
                "failed to determine git freshness for registered repo"
            );
            "unknown".to_owned()
        }
    }
}

#[must_use]
pub fn workspace_freshness(
    registry: &RegistryStore,
    metadata: &MetadataStoreDb,
) -> Vec<RepoFreshness> {
    registry
        .registry()
        .repos
        .iter()
        .map(|(repo, registered)| {
            let indexed_sha = match metadata.get_last_commit_sha(repo) {
                Ok(indexed_sha) => indexed_sha,
                Err(error) => {
                    warn!(
                        repo = %repo,
                        path = %registered.path.display(),
                        %error,
                        "failed to read indexed commit SHA while computing freshness"
                    );
                    return RepoFreshness {
                        repo: repo.clone(),
                        freshness: "unknown".to_owned(),
                    };
                }
            };
            RepoFreshness {
                repo: repo.clone(),
                freshness: repo_freshness(repo, &registered.path, indexed_sha.as_deref()),
            }
        })
        .collect()
}

/// Best-effort workspace freshness read straight from on-disk registry +
/// metadata, without opening the (lockable) graph store. Returns an empty vec
/// when the workspace is unindexed. When the registry is readable but metadata
/// is corrupt or otherwise unavailable, repos are returned with `unknown`
/// freshness so callers can distinguish "present but unreadable" from absent
/// stores.
#[must_use]
pub fn freshness_from_paths(registry_path: &Path, metadata_path: &Path) -> Vec<RepoFreshness> {
    if !registry_path.exists() || !metadata_path.exists() {
        return Vec::new();
    }

    let registry = match RegistryStore::open(registry_path) {
        Ok(registry) => registry,
        Err(error) => {
            warn!(
                path = %registry_path.display(),
                %error,
                "failed to read workspace registry while computing freshness"
            );
            return Vec::new();
        }
    };
    let metadata = match MetadataStoreDb::open(metadata_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            warn!(
                path = %metadata_path.display(),
                %error,
                "failed to read metadata store while computing freshness"
            );
            return unknown_freshness(&registry);
        }
    };
    workspace_freshness(&registry, &metadata)
}

fn unknown_freshness(registry: &RegistryStore) -> Vec<RepoFreshness> {
    registry
        .registry()
        .repos
        .keys()
        .map(|repo| RepoFreshness {
            repo: repo.clone(),
            freshness: "unknown".to_owned(),
        })
        .collect()
}

/// Repos whose index is `stale` relative to the working tree's HEAD.
#[must_use]
pub fn stale_repos(freshness: &[RepoFreshness]) -> Vec<&str> {
    freshness
        .iter()
        .filter(|entry| entry.freshness == "stale")
        .map(|entry| entry.repo.as_str())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use gather_step_core::{GatherStepConfig, RegistryStore};
    use gather_step_git::IndexFreshness;

    use super::{RepoFreshness, freshness_from_paths, freshness_label, stale_repos};

    #[test]
    fn freshness_label_maps_every_variant() {
        assert_eq!(
            freshness_label(&IndexFreshness::Fresh {
                head_sha: "abc".to_owned()
            }),
            "fresh"
        );
        assert_eq!(
            freshness_label(&IndexFreshness::Stale {
                indexed_sha: "old".to_owned(),
                head_sha: "new".to_owned()
            }),
            "stale"
        );
        assert_eq!(
            freshness_label(&IndexFreshness::NeverIndexed {
                head_sha: "abc".to_owned()
            }),
            "never_indexed"
        );
    }

    #[test]
    fn stale_repos_filters_only_stale_entries() {
        let entries = vec![
            RepoFreshness {
                repo: "a".to_owned(),
                freshness: "fresh".to_owned(),
            },
            RepoFreshness {
                repo: "b".to_owned(),
                freshness: "stale".to_owned(),
            },
            RepoFreshness {
                repo: "c".to_owned(),
                freshness: "unknown".to_owned(),
            },
        ];
        assert_eq!(stale_repos(&entries), vec!["b"]);
    }

    #[test]
    fn freshness_from_paths_is_empty_for_unindexed_workspace() {
        let dir = std::env::temp_dir().join(format!(
            "gather-step-freshness-empty-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        assert!(
            freshness_from_paths(&dir.join("registry.json"), &dir.join("metadata.sqlite"))
                .is_empty()
        );
        assert!(
            !dir.join("metadata.sqlite").exists(),
            "freshness checks for absent stores must not create metadata files"
        );
    }

    #[test]
    fn freshness_from_paths_reports_unknown_for_unreadable_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "gather-step-freshness-corrupt-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        let workspace = dir.join("workspace");
        fs::create_dir_all(workspace.join("repo_a")).expect("repo dir");

        let registry_path = dir.join("registry.json");
        let metadata_path = dir.join("metadata.sqlite");
        let config = GatherStepConfig::from_yaml_str(
            "repos:\n  - name: repo_a\n    path: repo_a\nindexing:\n  workspace_concurrency: 1\n",
        )
        .expect("config should parse");
        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_from_config(&config, &workspace)
            .expect("registry should register repo");
        fs::write(&metadata_path, b"not a sqlite database").expect("corrupt metadata");

        let freshness = freshness_from_paths(&registry_path, &metadata_path);

        assert_eq!(freshness.len(), 1);
        assert_eq!(freshness[0].repo, "repo_a");
        assert_eq!(freshness[0].freshness, "unknown");

        let _ = fs::remove_dir_all(&dir);
    }
}
