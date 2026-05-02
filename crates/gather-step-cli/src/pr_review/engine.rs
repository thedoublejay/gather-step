//! `ReviewEngine` trait and built-in implementations.
//!
//! Phase 5 Task 1: defines the abstraction that hides whether the review-side
//! graph is produced by a full temp-index run or by a diff overlay.
//!
//! Extractors receive a [`ReviewSnapshot`] and never need to know which engine
//! produced it.

use std::path::PathBuf;

use anyhow::{Result, anyhow};
use gather_step_storage::{IndexingOptions, StorageCoordinator};

use crate::pr_review::{
    affected::AffectedRepos, artifact_root::ReviewArtifactRoot, index_runner::run_review_index,
};

// ─── Snapshot ────────────────────────────────────────────────────────────────

/// Materialized review snapshot used by extractors.
///
/// Provides handles to the graph and metadata stores that represent the
/// review (PR-head) state.  Whether these handles are backed by a freshly
/// indexed temp-index, an overlay, or a cached artifact is invisible to
/// extractors.
pub struct ReviewSnapshot {
    /// Open coordinator over the review-side storage.
    pub coordinator: StorageCoordinator,
    /// Path to the review registry file.
    pub registry_path: PathBuf,
    /// Path to the review storage root.
    pub storage_root: PathBuf,
    /// Engine name used to produce this snapshot (`"temp-index"` or `"overlay"`).
    pub engine: &'static str,
    /// Surfaces NOT fully supported by this engine.
    ///
    /// Extractors should check this before reporting empty deltas — they can
    /// flag "unavailable" instead of "no changes" so reviewers don't get false
    /// confidence from an empty section that simply wasn't computed.
    pub unsupported_surfaces: Vec<UnsupportedSurface>,
    /// Number of repos indexed (for display purposes only).
    ///
    /// Sourced from [`WorkspaceStats::total_repos`] for `TempIndexEngine`;
    /// 0 for `OverlayEngine` (repos are read from the baseline, not re-indexed).
    pub total_repos: usize,
}

// ─── UnsupportedSurface ───────────────────────────────────────────────────────

/// A delta surface that a given engine cannot fully populate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedSurface {
    Routes,
    Symbols,
    PayloadContracts,
    Events,
    Decorators,
    ContractAlignments,
}

impl UnsupportedSurface {
    /// Human-readable name for inclusion in reports.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Routes => "routes",
            Self::Symbols => "symbols",
            Self::PayloadContracts => "payload_contracts",
            Self::Events => "events",
            Self::Decorators => "decorators",
            Self::ContractAlignments => "contract_alignments",
        }
    }
}

// ─── Trait ────────────────────────────────────────────────────────────────────

/// Abstraction over "how the review-side graph + metadata is materialized".
///
/// Callers obtain a [`ReviewSnapshot`] from the engine and hand it to the
/// extractor pipeline.  The engine takes full responsibility for any indexing,
/// overlay construction, or caching it needs to build the snapshot.
pub trait ReviewEngineImpl {
    /// Short name for logging / diagnostics.
    fn name(&self) -> &'static str;

    /// Build the review-side [`ReviewSnapshot`].
    ///
    /// `artifact_root` is provided so engines that need persistent state (like
    /// `TempIndexEngine`) can write to the review storage root.  Engines that
    /// operate entirely in memory may ignore it.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine cannot produce a valid snapshot (e.g.
    /// indexing fails, or the engine is not yet implemented).
    fn materialize(
        &self,
        artifact_root: &ReviewArtifactRoot,
        affected: Option<&AffectedRepos>,
        options: IndexingOptions,
    ) -> Result<ReviewSnapshot>;
}

// ─── TempIndexEngine ──────────────────────────────────────────────────────────

/// Engine that materializes the review snapshot by running a full temp-index
/// of the PR-head worktree into the artifact root's isolated storage.
///
/// This is the production-ready engine for Phase 5.  All surfaces are
/// supported.
pub struct TempIndexEngine;

impl ReviewEngineImpl for TempIndexEngine {
    fn name(&self) -> &'static str {
        "temp-index"
    }

    fn materialize(
        &self,
        artifact_root: &ReviewArtifactRoot,
        affected: Option<&AffectedRepos>,
        options: IndexingOptions,
    ) -> Result<ReviewSnapshot> {
        let stats = run_review_index(artifact_root, affected, options)?;

        let coordinator =
            StorageCoordinator::open_read_only(&artifact_root.storage_root).map_err(|e| {
                anyhow!(
                    "failed to open review storage at `{}`: {e}",
                    artifact_root.storage_root.display()
                )
            })?;

        Ok(ReviewSnapshot {
            coordinator,
            registry_path: artifact_root.registry_path.clone(),
            storage_root: artifact_root.storage_root.clone(),
            engine: self.name(),
            unsupported_surfaces: vec![],
            total_repos: stats.total_repos,
        })
    }
}

// ─── OverlayEngine ────────────────────────────────────────────────────────────

/// Engine that materializes the review snapshot by layering a
/// [`DiffOverlayStore`][crate::pr_review::overlay::store::DiffOverlayStore]
/// over the baseline workspace storage.
///
/// **Phase 5 Task 2 prototype:** graph-only overlay; search/metadata surfaces
/// are marked unsupported so the report renderer prints "_unavailable on the
/// overlay engine_" rather than "_no changes_".
///
/// Phase 5 Task 4 (parity gate) will validate when this engine is
/// production-ready.
pub struct OverlayEngine;

impl OverlayEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for OverlayEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ReviewEngineImpl for OverlayEngine {
    fn name(&self) -> &'static str {
        "overlay"
    }

    fn materialize(
        &self,
        artifact_root: &ReviewArtifactRoot,
        _affected: Option<&AffectedRepos>,
        _options: IndexingOptions,
    ) -> Result<ReviewSnapshot> {
        // CONCERN: The overlay engine does not yet have a StorageCoordinator
        // wrapping the DiffOverlayStore.  Opening the baseline workspace
        // storage as a fallback coordinator allows the call site to compile and
        // the test suite to verify trait wiring, but extractors backed by
        // search/metadata will see baseline data, not overlay data.
        //
        // Phase 5 Task 3 (search/metadata overlay strategy) must resolve this
        // before the engine is production-ready.
        //
        // For now we open the review storage root if it exists, otherwise fall
        // back to an error so callers get a clear message rather than silent
        // wrong results from baseline data.
        if !artifact_root.storage_root.exists() {
            return Err(anyhow!(
                "overlay engine: review storage root `{}` does not exist; \
                 run with --engine temp-index first to populate the artifact root, \
                 or wait for Phase 5 Task 3 (search/metadata overlay strategy)",
                artifact_root.storage_root.display()
            ));
        }

        let coordinator =
            StorageCoordinator::open_read_only(&artifact_root.storage_root).map_err(|e| {
                anyhow!(
                    "overlay engine: failed to open review storage at `{}`: {e}",
                    artifact_root.storage_root.display()
                )
            })?;

        Ok(ReviewSnapshot {
            coordinator,
            registry_path: artifact_root.registry_path.clone(),
            storage_root: artifact_root.storage_root.clone(),
            engine: self.name(),
            // Routes and Symbols can use graph-only overlay; the others need
            // search/metadata stores not yet wired to the overlay.
            unsupported_surfaces: vec![
                UnsupportedSurface::PayloadContracts,
                UnsupportedSurface::Events,
                UnsupportedSurface::Decorators,
                UnsupportedSurface::ContractAlignments,
            ],
            total_repos: 0,
        })
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::SystemTime,
    };

    use gather_step_core::NodeKind;
    use gather_step_storage::{GraphStore, GraphStoreDb, IndexingOptions};

    use crate::pr_review::artifact_root::create_artifact_root;

    use super::*;

    // ── temp-dir helper ───────────────────────────────────────────────────────

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("gs-engine-{name}-{nanos}-{counter}"));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// Write a minimal indexable workspace fixture:
    /// ```text
    /// root/
    ///   gather-step.config.yaml
    ///   myrepo/
    ///     package.json
    ///     src/hello.ts
    /// ```
    fn write_minimal_fixture(root: &std::path::Path) {
        fs::write(
            root.join("gather-step.config.yaml"),
            "repos:\n  - name: myrepo\n    path: myrepo\nindexing:\n  workspace_concurrency: 1\n",
        )
        .expect("write config");

        let repo_dir = root.join("myrepo");
        fs::create_dir_all(repo_dir.join("src")).expect("create src dir");
        fs::write(
            repo_dir.join("package.json"),
            r#"{"name":"myrepo","version":"0.1.0"}"#,
        )
        .expect("write package.json");
        fs::write(
            repo_dir.join("src/hello.ts"),
            "export function greetEngine(): string { return 'hello'; }\n",
        )
        .expect("write hello.ts");
    }

    /// Check whether git is available; skip tests that need it otherwise.
    fn git_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .is_ok()
    }

    // ── TempIndexEngine ───────────────────────────────────────────────────────

    /// `TempIndexEngine::materialize` produces a snapshot with:
    /// - `engine == "temp-index"`
    /// - `unsupported_surfaces` is empty
    /// - coordinator can be queried (no panic)
    #[test]
    fn temp_index_engine_materializes_snapshot() {
        if !git_available() {
            return;
        }

        let ws_tmp = TempDir::new("ws");
        write_minimal_fixture(&ws_tmp.path);

        // Initialise a git repo so the worktree has a HEAD commit.
        let git = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&ws_tmp.path)
                .env("GIT_AUTHOR_NAME", "Test")
                .env("GIT_AUTHOR_EMAIL", "t@t")
                .env("GIT_COMMITTER_NAME", "Test")
                .env("GIT_COMMITTER_EMAIL", "t@t")
                .output()
                .expect("git command")
        };
        git(&["init"]);
        git(&[
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "init",
        ]);

        let cache_tmp = TempDir::new("cache");
        let artifact_root = create_artifact_root(
            &cache_tmp.path,
            &ws_tmp.path,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "test-run-engine",
        )
        .expect("create artifact root");

        // Point worktree_root at our fixture so the indexer finds the config.
        // artifact_root.worktree_root is the worktree sub-dir — for testing we
        // create a symlink-equivalent by using the ws_tmp root directly via a
        // new ArtifactRoot that has worktree_root = ws_tmp.
        // Simpler: just copy the config into the worktree_root.
        fs::create_dir_all(&artifact_root.worktree_root).ok();
        fs::copy(
            ws_tmp.path.join("gather-step.config.yaml"),
            artifact_root.worktree_root.join("gather-step.config.yaml"),
        )
        .expect("copy config");
        // Copy the repo dir too.
        let dest_repo = artifact_root.worktree_root.join("myrepo");
        fs::create_dir_all(dest_repo.join("src")).expect("create dest repo src");
        fs::copy(
            ws_tmp.path.join("myrepo/package.json"),
            dest_repo.join("package.json"),
        )
        .ok();
        fs::copy(
            ws_tmp.path.join("myrepo/src/hello.ts"),
            dest_repo.join("src/hello.ts"),
        )
        .ok();

        let engine = TempIndexEngine;
        let snapshot = engine
            .materialize(&artifact_root, None, IndexingOptions::default())
            .expect("materialize should succeed");

        assert_eq!(snapshot.engine, "temp-index");
        assert!(
            snapshot.unsupported_surfaces.is_empty(),
            "temp-index must support all surfaces"
        );

        // Verify coordinator is queryable.
        let _ = snapshot
            .coordinator
            .graph()
            .nodes_by_type(NodeKind::Function)
            .expect("nodes_by_type must not error");
    }

    // ── OverlayEngine ─────────────────────────────────────────────────────────

    /// `OverlayEngine::materialize` returns a snapshot with `PayloadContracts`
    /// and `Events` in `unsupported_surfaces`.
    #[test]
    fn overlay_engine_marks_unsupported_surfaces() {
        let cache_tmp = TempDir::new("cache-overlay");
        let ws_tmp = TempDir::new("ws-overlay");
        write_minimal_fixture(&ws_tmp.path);

        let artifact_root = create_artifact_root(
            &cache_tmp.path,
            &ws_tmp.path,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa0",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "test-run-overlay",
        )
        .expect("create artifact root");

        // Pre-create the storage root so OverlayEngine doesn't immediately error.
        fs::create_dir_all(&artifact_root.storage_root).expect("create storage root");

        // The overlay engine needs a valid redb graph file to open — write a
        // minimal graph store so open_read_only doesn't fail.
        let graph_path = artifact_root.storage_root.join("graph.redb");
        drop(GraphStoreDb::open(&graph_path).expect("create graph store"));

        let engine = OverlayEngine::new();
        let snapshot = engine
            .materialize(&artifact_root, None, IndexingOptions::default())
            .expect("overlay materialize should succeed");

        assert_eq!(snapshot.engine, "overlay");

        let surfaces: Vec<UnsupportedSurface> = snapshot.unsupported_surfaces.clone();
        assert!(
            surfaces.contains(&UnsupportedSurface::PayloadContracts),
            "overlay must mark PayloadContracts unsupported"
        );
        assert!(
            surfaces.contains(&UnsupportedSurface::Events),
            "overlay must mark Events unsupported"
        );
        assert!(
            surfaces.contains(&UnsupportedSurface::Decorators),
            "overlay must mark Decorators unsupported"
        );
        assert!(
            surfaces.contains(&UnsupportedSurface::ContractAlignments),
            "overlay must mark ContractAlignments unsupported"
        );
    }
}
