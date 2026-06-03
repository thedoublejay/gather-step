use std::path::Path;

use gather_step_core::RegistryStore;
use gather_step_git::{GitHistoryIndexer, GitRepoSource, IndexFreshness};
use gather_step_storage::{MetadataStore, MetadataStoreDb};
use serde::Serialize;

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
        Err(_) => "unknown".to_owned(),
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
            let indexed_sha = metadata.get_last_commit_sha(repo).ok().flatten();
            RepoFreshness {
                repo: repo.clone(),
                freshness: repo_freshness(repo, &registered.path, indexed_sha.as_deref()),
            }
        })
        .collect()
}

/// Best-effort workspace freshness read straight from on-disk registry +
/// metadata, without opening the (lockable) graph store. Returns an empty vec
/// when the workspace is unindexed or the stores cannot be read.
#[must_use]
pub fn freshness_from_paths(registry_path: &Path, metadata_path: &Path) -> Vec<RepoFreshness> {
    let Ok(registry) = RegistryStore::open(registry_path) else {
        return Vec::new();
    };
    let Ok(metadata) = MetadataStoreDb::open(metadata_path) else {
        return Vec::new();
    };
    workspace_freshness(&registry, &metadata)
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
        assert!(
            freshness_from_paths(&dir.join("registry.json"), &dir.join("metadata.sqlite"))
                .is_empty()
        );
    }
}
