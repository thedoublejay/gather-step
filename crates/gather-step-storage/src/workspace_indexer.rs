use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use gather_step_core::{
    GatherStepConfig, RegistryStore, RepoConfig, WorkspaceIndexDelegate, WorkspaceIndexError,
    WorkspaceRepoResult, WorkspaceStats, index_workspace,
};
use gather_step_parser::frameworks::{Framework, detect_frameworks};

use crate::{IndexingOptions, RepoIndexer, RepoIndexerError};

pub struct StorageWorkspaceIndexDelegate {
    indexer: Arc<RepoIndexer>,
    run_timestamp: String,
}

impl StorageWorkspaceIndexDelegate {
    /// Opens a single shared `RepoIndexer` (and thus a single
    /// `StorageCoordinator`) that is reused across all repos in the workspace.
    ///
    /// Enables Tantivy deferred-commit mode for the duration of the workspace
    /// run — per-repo `replace_by_files` calls stage documents without
    /// committing. A single final commit in `finalize_workspace` replaces
    /// ~250 segment flushes with 1.
    pub fn new(
        storage_root: impl AsRef<Path>,
        options: IndexingOptions,
    ) -> Result<Self, RepoIndexerError> {
        let indexer = RepoIndexer::open(storage_root, options)?;
        indexer.storage().search().set_deferred_commit(true);
        Ok(Self {
            indexer: Arc::new(indexer),
            run_timestamp: current_unix_timestamp_string(),
        })
    }
}

impl WorkspaceIndexDelegate for StorageWorkspaceIndexDelegate {
    type Error = RepoIndexerError;

    fn index_repo(
        &self,
        repo: &RepoConfig,
        repo_root: &Path,
    ) -> Result<WorkspaceRepoResult, Self::Error> {
        let detected_frameworks = detect_frameworks(repo_root).into_iter().collect::<Vec<_>>();
        let mut frameworks = detected_frameworks
            .iter()
            .copied()
            .map(framework_label)
            .collect::<Vec<_>>();
        frameworks.sort();

        let stats = self.indexer.index_repo_with_frameworks(
            &repo.name,
            repo_root,
            &detected_frameworks,
            None,
        )?;

        Ok(WorkspaceRepoResult {
            repo: repo.name.clone(),
            last_indexed_at: Some(self.run_timestamp.clone()),
            file_count: u64::try_from(stats.files_parsed).unwrap_or(u64::MAX),
            symbol_count: u64::try_from(stats.nodes_created).unwrap_or(u64::MAX),
            edge_count: u64::try_from(stats.edges_created).unwrap_or(u64::MAX),
            frameworks,
            depth_level: repo.depth.unwrap_or(gather_step_core::DepthLevel::Full),
        })
    }

    fn finalize_workspace(&self, _repos: &[WorkspaceRepoResult]) -> Result<u64, Self::Error> {
        // Flush the Tantivy writer once, after all repos have staged their
        // documents, turning ~250 per-batch commits into 1. Safe to call
        // regardless of deferred mode.
        self.indexer
            .storage()
            .search()
            .flush()
            .map_err(crate::StorageCoordinatorError::Search)?;

        // Compact the metadata SQLite database: checkpoint the WAL back into
        // the main file and VACUUM freed pages.  Both are best-effort; the
        // database remains consistent even if they are skipped.
        self.indexer.storage().metadata().finalize();

        // Count cross-repo edges via a single EDGES-table scan + a small
        // in-memory node_id→repo_id cache. Replaces ~270K read transactions
        // with one.
        self.indexer
            .storage()
            .graph()
            .count_cross_repo_edges()
            .map_err(RepoIndexerError::Graph)
    }
}

fn framework_label(framework: Framework) -> String {
    match framework {
        Framework::NestJs => "nestjs",
        Framework::Mongoose => "mongoose",
        Framework::NextJs => "nextjs",
        Framework::Tailwind => "tailwind",
        Framework::Prisma => "prisma",
        Framework::Drizzle => "drizzle",
        Framework::React => "react",
        Framework::ReactRouter => "react_router",
        Framework::ReactHookForm => "react_hook_form",
        Framework::Storybook => "storybook",
        Framework::Azure => "azure",
        Framework::Redux => "redux",
        Framework::Zustand => "zustand",
        Framework::LaunchDarkly => "launchdarkly",
        Framework::FrontendHooks => "frontend_hooks",
    }
    .to_owned()
}

pub fn index_workspace_with_storage(
    config: &GatherStepConfig,
    config_root: &Path,
    registry: &mut RegistryStore,
    storage_root: impl AsRef<Path>,
    options: IndexingOptions,
) -> Result<WorkspaceStats, Box<WorkspaceIndexError<RepoIndexerError>>> {
    let delegate = StorageWorkspaceIndexDelegate::new(storage_root, options)
        .map_err(|error| Box::new(WorkspaceIndexError::Build(error.to_string())))?;
    index_workspace(config, config_root, registry, &delegate).map_err(Box::new)
}

fn current_unix_timestamp_string() -> String {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or_else(
        |_| "0".to_owned(),
        |duration| duration.as_secs().to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::RegistryStore;
    use pretty_assertions::assert_eq;

    use crate::{GraphStore, GraphStoreDb};

    use super::{StorageWorkspaceIndexDelegate, index_workspace_with_storage};
    use crate::IndexingOptions;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-workspace-storage-{name}-{}-{id}",
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

    #[test]
    fn indexes_workspace_with_real_storage_delegate() {
        let root = TempDir::new("config");
        let storage = TempDir::new("storage");
        fs::create_dir_all(root.path().join("repos/producer/src")).expect("producer dir");
        fs::create_dir_all(root.path().join("repos/consumer/src")).expect("consumer dir");
        fs::write(
            root.path().join("repos/producer/package.json"),
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0", "@workspace/shared-contracts": "2.3.1" } }"#,
        )
        .expect("producer package");
        fs::write(
            root.path().join("repos/consumer/package.json"),
            r#"{ "dependencies": { "@nestjs/core": "^11.0.0", "@workspace/shared-contracts": "2.4.0" } }"#,
        )
        .expect("consumer package");
        fs::write(
            root.path().join("repos/producer/src/events.ts"),
            r"
import { Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

@Controller()
export class EventController {
  @MessagePattern(['order.created'])
  handleCreated() {
    return {};
  }
}
",
        )
        .expect("producer source");
        fs::write(
            root.path().join("repos/consumer/src/controller.ts"),
            r"
import { Controller, Get } from '@nestjs/common';

@Controller('orders')
export class OrderController {
  @Get()
  list() {
    return [];
  }
}
",
        )
        .expect("consumer source");

        let config = gather_step_core::GatherStepConfig::from_yaml_str(
            r"
repos:
  - name: producer
    path: repos/producer
  - name: consumer
    path: repos/consumer
indexing:
  workspace_concurrency: 1
",
        )
        .expect("config should parse");
        let registry_path = root.path().join("registry.json");
        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");

        let stats = index_workspace_with_storage(
            &config,
            root.path(),
            &mut registry,
            storage.path(),
            IndexingOptions::default(),
        )
        .expect("workspace should index");

        assert_eq!(stats.total_repos, 2);
        assert_eq!(stats.indexed_repos, 2);
        assert!(stats.cross_repo_edges > 0);

        let graph =
            GraphStoreDb::open(storage.path().join("graph.redb")).expect("graph should open");
        assert_eq!(
            graph
                .nodes_by_repo("producer")
                .expect("producer nodes")
                .is_empty(),
            false
        );
        assert_eq!(
            graph
                .nodes_by_repo("consumer")
                .expect("consumer nodes")
                .is_empty(),
            false
        );
        assert!(
            graph
                .nodes_by_type(gather_step_core::NodeKind::SharedSymbol)
                .expect("shared symbols")
                .iter()
                .any(|node| node.external_id.as_deref()
                    == Some("__shared__@workspace/shared-contracts@2.3.1__package"))
        );

        let _delegate = StorageWorkspaceIndexDelegate::new(
            storage.path().join("extra"),
            IndexingOptions::default(),
        )
        .expect("delegate should open");
    }
}
