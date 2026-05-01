#![forbid(unsafe_code)]

//! Git history ingestion and analytics for the gather-step indexer.
//!
//! Primary components:
//!  - `history`: rev-walk + per-file delta extraction
//!  - `analytics`: hotspot + co-change scoring
//!  - `ownership`: log-based ownership + lazy blame
//!  - `intelligence`: repo-level recompute + graph summaries

pub mod analytics;
pub mod classify;
pub mod history;
pub mod intelligence;
pub mod ownership;
pub mod refs;
pub mod worktrees;

pub use analytics::{
    AnalyticsOptions, AnalyticsReport, CoChangeRecord, HotspotRecord, analyze_history,
};
pub use classify::{
    DEFAULT_DECISION_SIGNALS, classify_commit_message, detect_decision_signal, extract_pr_number,
};
pub use history::{
    CommitFact, CommitFileChangeKind, CommitFileDelta, GitHistoryError, GitHistoryIndexer,
    GitHistorySyncError, GitIndexerOptions, GitRepoSource, HistorySyncOutcome,
};
pub use intelligence::{
    RepoIntelligenceError, RepoIntelligenceOptions, RepoIntelligenceReport,
    refresh_repo_intelligence,
};
pub use ownership::{
    BusFactorRisk, OwnershipContribution, OwnershipOptions, OwnershipSummary, analyze_ownership,
    analyze_ownership_for_file, analyze_ownership_from_store, bus_factor_risks,
    persist_ownership_into_file_analytics, redact_email, set_redact_key,
};
pub use refs::{
    ChangeKind, ChangedFile, RefResolveError, ResolvedRange, ResolvedRef, changed_files,
    merge_base, resolve_range, resolve_ref,
};
pub use worktrees::{ReviewWorktree, WorktreeError, create_detached_worktree, remove_worktree};

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    /// Smoke test that the crate name remains stable for downstream telemetry
    /// and pack-name detection.
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(env!("CARGO_PKG_NAME"), "gather-step-git");
    }
}
