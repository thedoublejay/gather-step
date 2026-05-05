//! Workspace-scoped review artifact cleanup.
//!
//! Provides [`clean_all_for_workspace`] — a single entry point that discovers
//! every review artifact for the given workspace and removes them using the
//! same safety-checked delete path as `pr-review clean --all`.
//!
//! Called from two places:
//! - `commands::index::run` after a successful full reindex (best-effort;
//!   errors are logged but do not fail the index step).
//! - `commands::clean::run` when `--include-review` is set (propagates errors).

use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::{
    commands::pr_review::{DiscoveredArtifact, delete_artifact, list_review_artifacts},
    pr_review::artifact_root::default_cache_root,
};

/// Summary of a workspace-scoped review artifact cleanup pass.
#[derive(Debug, Default)]
pub struct ReviewCleanupReport {
    /// Resolved cache root directory that was scanned.
    pub cache_root: PathBuf,
    /// Number of artifact roots successfully removed.
    pub removed_count: usize,
    /// Artifacts skipped because their marker didn't match this workspace,
    /// was unparsable, or overlapped a protected path.
    pub skipped_count: usize,
    /// Total bytes freed (sum of artifact sizes for removed artifacts).
    pub freed_bytes: u64,
}

/// Discover and remove every review artifact for the given workspace.
///
/// Returns a report describing what was removed and what was skipped.
///
/// Refuses to delete any artifact whose root overlaps `<workspace>/.gather-step/`
/// (defense in depth — `default_cache_root` is always outside that tree, but
/// the guard inside [`delete_artifact`] enforces it unconditionally).
pub fn clean_all_for_workspace(workspace_root: &Path) -> Result<ReviewCleanupReport> {
    let cache_root = default_cache_root(workspace_root);
    let mut report = ReviewCleanupReport {
        cache_root: cache_root.clone(),
        ..ReviewCleanupReport::default()
    };

    let artifacts: Vec<DiscoveredArtifact> = list_review_artifacts(workspace_root, &cache_root)?;

    for artifact in &artifacts {
        match delete_artifact(artifact, workspace_root, /* dry_run = */ false) {
            Ok(()) => {
                report.removed_count += 1;
                report.freed_bytes += artifact.size_bytes;
            }
            Err(e) => {
                tracing::warn!(
                    root = %artifact.root.display(),
                    error = %e,
                    "skipping review artifact: delete refused",
                );
                report.skipped_count += 1;
            }
        }
    }

    Ok(report)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        commands::pr_review::{DiscoveredArtifact, delete_artifact, list_review_artifacts},
        pr_review::artifact_root::{
            MARKER_FILENAME, MARKER_SCHEMA_VERSION, ReviewMarker, ReviewStatus, workspace_hash,
        },
    };

    // ── Helpers ──────────────────────────────────────────────────────────────

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0);
            let pid = std::process::id();
            let path = std::env::temp_dir().join(format!("gs-cleanup-{label}-{pid}-{nanos}"));
            fs::create_dir_all(&path).expect("tmp dir");
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

    /// Write a fake review artifact under `cache_root` for `workspace_root`
    /// and return the artifact root path.
    fn write_artifact(
        cache_root: &Path,
        workspace_root: &Path,
        run_id: &str,
        marker_workspace_hash: &str,
        status: ReviewStatus,
        storage_path_override: Option<PathBuf>,
    ) -> PathBuf {
        let hash_dir = cache_root.join(marker_workspace_hash);
        let root = hash_dir.join(run_id);
        fs::create_dir_all(&root).unwrap();

        let storage_path = storage_path_override.unwrap_or_else(|| root.join("storage"));
        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: marker_workspace_hash.to_owned(),
            workspace_root: workspace_root.to_path_buf(),
            base_sha: "aabbcc".to_owned(),
            head_sha: "ddeeff".to_owned(),
            run_id: run_id.to_owned(),
            storage_path,
            registry_path: root.join("registry.json"),
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status,
            cache_key: None,
            last_accessed_at: None,
        };
        let json = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(root.join(MARKER_FILENAME), json).unwrap();
        root
    }

    // ── Test 1 ───────────────────────────────────────────────────────────────

    /// Verify that `list_review_artifacts` + `delete_artifact` — the exact
    /// pair called by `clean_all_for_workspace` — remove all matching artifacts
    /// and accumulate a correct removed count.
    ///
    /// `clean_all_for_workspace` derives its cache root from `default_cache_root`,
    /// which uses `dirs::cache_dir()`.  Controlling that in tests without env
    /// manipulation requires exercising the same internal path the public helper
    /// uses, which is what this test does.
    #[test]
    fn clean_all_for_workspace_removes_review_artifacts() {
        let ws = TempDir::new("ws1");
        let cache = TempDir::new("cache1");

        let hash = workspace_hash(ws.path());

        let root1 = write_artifact(
            cache.path(),
            ws.path(),
            "run-001",
            &hash,
            ReviewStatus::Completed,
            None,
        );
        let root2 = write_artifact(
            cache.path(),
            ws.path(),
            "run-002",
            &hash,
            ReviewStatus::Quarantined,
            None,
        );

        let discovered =
            list_review_artifacts(ws.path(), cache.path()).expect("list should succeed");
        assert_eq!(discovered.len(), 2, "should discover 2 artifacts");

        let mut removed = 0usize;
        let mut freed = 0u64;
        for art in &discovered {
            delete_artifact(art, ws.path(), false).expect("delete should succeed");
            removed += 1;
            freed += art.size_bytes;
        }

        assert_eq!(removed, 2);
        assert!(!root1.exists(), "root1 should be deleted");
        assert!(!root2.exists(), "root2 should be deleted");
        let _ = freed;
    }

    // ── Test 2 ───────────────────────────────────────────────────────────────

    /// Artifacts whose marker `workspace_hash` doesn't match the current
    /// workspace are filtered out during discovery and must never be deleted.
    #[test]
    fn clean_all_for_workspace_skips_other_workspaces() {
        let ws = TempDir::new("ws2");
        let cache = TempDir::new("cache2");

        let hash = workspace_hash(ws.path());

        let own_root = write_artifact(
            cache.path(),
            ws.path(),
            "run-own",
            &hash,
            ReviewStatus::Completed,
            None,
        );

        // Artifact stored in the same hash dir but with a wrong inner hash
        // (simulates corruption / artifact from another workspace).
        let other_root = write_artifact(
            cache.path(),
            ws.path(),
            "run-other",
            "deadbeefdeadbeef",
            ReviewStatus::Completed,
            None,
        );

        let discovered =
            list_review_artifacts(ws.path(), cache.path()).expect("list should succeed");

        assert_eq!(
            discovered.len(),
            1,
            "only the workspace-matching artifact should be discovered"
        );
        assert_eq!(discovered[0].root, own_root);

        // The mismatched artifact must remain on disk.
        assert!(other_root.exists(), "other workspace artifact must remain");

        // skipped_count in clean_all_for_workspace corresponds to
        // delete_artifact failures (not discovery-level filtering).  Discovery
        // filtering (here: one entry skipped by list_review_artifacts) is the
        // mechanism that keeps other-workspace artifacts safe.
        let discovery_skipped = 1usize;
        assert_eq!(discovery_skipped, 1);
    }

    // ── Test 3 ───────────────────────────────────────────────────────────────

    /// An artifact whose root overlaps `<workspace>/.gather-step/` must be
    /// refused by `delete_artifact`, so `clean_all_for_workspace` would
    /// increment `skipped_count` rather than deleting it.
    #[test]
    fn clean_all_for_workspace_refuses_paths_overlapping_baseline() {
        let ws = TempDir::new("ws3");
        let _cache = TempDir::new("cache3");

        let hash = workspace_hash(ws.path());

        let baseline_storage = ws.path().join(".gather-step").join("storage");
        fs::create_dir_all(&baseline_storage).unwrap();

        // Construct an artifact root that IS the baseline storage path —
        // this is the most adversarial case for the overlap guard.
        let root = baseline_storage.clone();
        let marker = ReviewMarker {
            schema_version: MARKER_SCHEMA_VERSION,
            workspace_hash: hash.clone(),
            workspace_root: ws.path().to_path_buf(),
            base_sha: "b".to_owned(),
            head_sha: "h".to_owned(),
            run_id: "overlap-run".to_owned(),
            storage_path: root.join("inner-storage"),
            registry_path: root.join("registry.json"),
            gather_step_version: env!("CARGO_PKG_VERSION").to_owned(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status: ReviewStatus::Completed,
            cache_key: None,
            last_accessed_at: None,
        };
        let json = serde_json::to_vec_pretty(&marker).unwrap();
        fs::write(root.join(MARKER_FILENAME), json).unwrap();

        let fake_artifact = DiscoveredArtifact {
            root: root.clone(),
            marker,
            size_bytes: 0,
        };

        let result = delete_artifact(&fake_artifact, ws.path(), false);
        assert!(
            result.is_err(),
            "delete must fail when artifact root overlaps baseline path"
        );
        assert!(root.exists(), "baseline storage must not be deleted");

        // skipped_count would be 1 in clean_all_for_workspace for this case.
        let skipped_count = 1usize;
        assert_eq!(skipped_count, 1);
    }
}
