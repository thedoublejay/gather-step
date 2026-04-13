use std::path::Path;

use rayon::prelude::*;
use thiserror::Error;

use crate::{
    DepthLevel, GatherStepConfig, RegistryError, RegistryStore, RepoConfig, RepoIndexMetadata,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkspaceStats {
    pub total_repos: usize,
    pub indexed_repos: usize,
    pub total_files: u64,
    pub total_symbols: u64,
    pub total_edges: u64,
    pub cross_repo_edges: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceRepoResult {
    pub repo: String,
    pub last_indexed_at: Option<String>,
    pub file_count: u64,
    pub symbol_count: u64,
    pub edge_count: u64,
    pub frameworks: Vec<String>,
    pub depth_level: DepthLevel,
}

pub trait WorkspaceIndexDelegate {
    type Error;

    fn index_repo(
        &self,
        repo: &RepoConfig,
        repo_root: &Path,
    ) -> Result<WorkspaceRepoResult, Self::Error>;

    fn finalize_workspace(&self, _repos: &[WorkspaceRepoResult]) -> Result<u64, Self::Error> {
        Ok(0)
    }
}

#[derive(Debug, Error)]
pub enum WorkspaceIndexError<E> {
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error("repo `{repo}` failed to index: {source}")]
    Repo { repo: String, source: E },
    #[error("failed to build workspace thread pool: {0}")]
    Build(String),
    #[error("workspace finalization failed: {0}")]
    Finalize(E),
}

pub fn index_workspace<D: WorkspaceIndexDelegate + Sync>(
    config: &GatherStepConfig,
    config_root: &Path,
    registry: &mut RegistryStore,
    delegate: &D,
) -> Result<WorkspaceStats, WorkspaceIndexError<D::Error>>
where
    D::Error: Send,
{
    config
        .validate_repo_roots_against_config_root(config_root)
        .map_err(RegistryError::Config)?;
    registry.register_from_config(config, config_root)?;

    let concurrency = config
        .indexing
        .workspace_concurrency
        .filter(|threads| *threads > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, usize::from));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(concurrency)
        .build()
        .map_err(|error| WorkspaceIndexError::Build(error.to_string()))?;
    let results = pool.install(|| {
        config
            .repos
            .par_iter()
            .map(|repo| {
                let repo_root = config_root.join(&repo.path);
                delegate
                    .index_repo(repo, &repo_root)
                    .map_err(|source| WorkspaceIndexError::Repo {
                        repo: repo.name.clone(),
                        source,
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    })?;

    let mut stats = WorkspaceStats {
        total_repos: config.repos.len(),
        indexed_repos: results.len(),
        ..WorkspaceStats::default()
    };

    for result in &results {
        registry.update_repo_metadata(
            &result.repo,
            RepoIndexMetadata {
                last_indexed_at: result.last_indexed_at.clone(),
                file_count: result.file_count,
                symbol_count: result.symbol_count,
                frameworks: result.frameworks.clone(),
                depth_level: result.depth_level,
            },
        )?;

        stats.total_files += result.file_count;
        stats.total_symbols += result.symbol_count;
        stats.total_edges += result.edge_count;
    }

    stats.cross_repo_edges = delegate
        .finalize_workspace(&results)
        .map_err(WorkspaceIndexError::Finalize)?;

    Ok(stats)
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

    use super::{WorkspaceIndexDelegate, WorkspaceRepoResult, index_workspace};
    use crate::{DepthLevel, GatherStepConfig, RegistryStore, RepoConfig};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-workspace-{name}-{}-{id}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir should create");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct FakeDelegate;

    impl WorkspaceIndexDelegate for FakeDelegate {
        type Error = &'static str;

        fn index_repo(
            &self,
            repo: &RepoConfig,
            _repo_root: &Path,
        ) -> Result<WorkspaceRepoResult, Self::Error> {
            Ok(WorkspaceRepoResult {
                repo: repo.name.clone(),
                last_indexed_at: Some("2026-04-14T00:00:00Z".to_owned()),
                file_count: 3,
                symbol_count: 7,
                edge_count: 11,
                frameworks: vec!["nestjs".to_owned()],
                depth_level: repo.depth.unwrap_or(DepthLevel::Full),
            })
        }

        fn finalize_workspace(&self, repos: &[WorkspaceRepoResult]) -> Result<u64, Self::Error> {
            Ok(u64::try_from(repos.len()).expect("repo count should fit"))
        }
    }

    #[test]
    fn indexes_workspace_and_updates_registry() {
        let root = TempDir::new("config");
        fs::create_dir_all(root.path().join("repos/a")).expect("repo a should exist");
        fs::create_dir_all(root.path().join("repos/b")).expect("repo b should exist");
        let config = GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: service-a
    path: repos/a
    depth: full
  - name: service-b
    path: repos/b
",
        )
        .expect("config should parse");
        let registry_path = root.path().join("registry.json");
        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");

        let stats = index_workspace(&config, root.path(), &mut registry, &FakeDelegate)
            .expect("workspace should index");

        assert_eq!(stats.total_repos, 2);
        assert_eq!(stats.indexed_repos, 2);
        assert_eq!(stats.total_files, 6);
        assert_eq!(stats.total_symbols, 14);
        assert_eq!(stats.total_edges, 22);
        assert_eq!(stats.cross_repo_edges, 2);
        assert_eq!(registry.registry().repos.len(), 2);
    }
}
