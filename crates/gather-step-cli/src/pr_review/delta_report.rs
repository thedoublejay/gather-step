//! Delta report — MVP output struct for `gather-step pr-review`.
//!
//! The `added_routes`, `added_symbols`, and `added_payload_contracts` arrays
//! are intentionally empty in Phase 1 (MVP).  Phase 2 populates them by running
//! diff extraction against the review index.
//!
//! Phase 1 Task 5 of the PR review mode plan.

use std::{fmt::Write as _, path::PathBuf};

use serde::Serialize;

// ─── Public types ─────────────────────────────────────────────────────────────

/// Top-level output struct for `gather-step pr-review`.
#[derive(Debug, Clone, Serialize)]
pub struct DeltaReport {
    pub schema_version: u32,
    pub metadata: ReviewMetadata,
    pub safety: SafetyMetadata,
    /// Paths of files that changed between base and head.
    pub changed_files: Vec<String>,
    /// `true` if the list was truncated at 200 entries.
    pub changed_files_truncated: bool,
    // ── Phase 2 placeholders ────────────────────────────────────────────────
    // These arrays are empty in Phase 1 (MVP).  Phase 2 populates them by
    // running diff extraction against the review index.
    pub added_routes: Vec<serde_json::Value>,
    pub added_symbols: Vec<serde_json::Value>,
    pub added_payload_contracts: Vec<serde_json::Value>,
    pub suggested_followups: Vec<SuggestedCommand>,
}

/// Review run metadata emitted in every delta report.
#[derive(Debug, Clone, Serialize)]
pub struct ReviewMetadata {
    pub workspace: PathBuf,
    /// The base ref as supplied by the user (branch name, SHA, …).
    pub base_input: String,
    /// Resolved 40-char hex SHA for the base commit.
    pub base_sha: String,
    /// The head ref as supplied by the user.
    pub head_input: String,
    /// Resolved 40-char hex SHA for the head commit.
    pub head_sha: String,
    /// Checkout mode used to materialize the review worktree.
    /// Always `"head"` in Phase 1; Phase 6 may add `"synthetic-merge"`.
    pub checkout_mode: String,
    /// Config-registered repos whose paths overlap with the changed files.
    /// If a changed file does not match any configured repo path, it is
    /// grouped under the synthetic `"<workspace>"` entry.
    pub changed_repos: Vec<String>,
    /// Repos actually indexed in this review run (may differ from
    /// `changed_repos` if, for example, a repo has no indexable sources).
    pub indexed_repos: Vec<String>,
    /// Wall-clock milliseconds spent running the review indexer.
    pub elapsed_ms: u64,
}

/// Paths and identifiers describing the review artifact set.
#[derive(Debug, Clone, Serialize)]
pub struct SafetyMetadata {
    /// `.gather-step/registry.json` in the source workspace.
    pub baseline_registry_path: PathBuf,
    /// `.gather-step/storage` in the source workspace.
    pub baseline_storage_path: PathBuf,
    /// `<review_root>/registry.json`
    pub review_registry_path: PathBuf,
    /// `<review_root>/storage`
    pub review_storage_path: PathBuf,
    /// `<cache_root>/<workspace_hash>/<run_id>/`
    pub review_root: PathBuf,
    pub run_id: String,
    pub cleanup_policy: CleanupPolicy,
    /// Composite key that uniquely identifies this review state.
    /// Format: `"<workspace_hash>:<base_sha>:<head_sha>"`.
    pub cache_key: String,
}

/// Whether the review artifact root is kept or removed after the run.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupPolicy {
    RemoveOnExit,
    KeepCache,
}

/// A suggested `gather-step` invocation the reviewer can run against the
/// review artifact root.
#[derive(Debug, Clone, Serialize)]
pub struct SuggestedCommand {
    pub label: String,
    /// Shell-formatted command string.
    pub command: String,
    /// `true` if the command requires `--keep-cache` to have been set
    /// (because the artifact root must exist when the command runs).
    pub requires_keep_cache: bool,
}

// ─── Renderer ─────────────────────────────────────────────────────────────────

const MAX_CHANGED_FILES_DISPLAY: usize = 200;

impl DeltaReport {
    /// Render the report as a JSON string (one line, compact).
    pub fn render_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Render the report as a human-readable Markdown string.
    pub fn render_markdown(&self) -> String {
        let m = &self.metadata;
        let s = &self.safety;

        let mut buf = String::new();

        buf.push_str("# gather-step pr-review\n\n");

        buf.push_str("## Review metadata\n\n");
        let _ = writeln!(buf, "- **workspace:** `{}`", m.workspace.display());
        let _ = writeln!(buf, "- **base:** `{}` → `{}`", m.base_input, m.base_sha);
        let _ = writeln!(buf, "- **head:** `{}` → `{}`", m.head_input, m.head_sha);
        let _ = writeln!(buf, "- **checkout mode:** `{}`", m.checkout_mode);
        let changed_repos_str = if m.changed_repos.is_empty() {
            "(none)".to_owned()
        } else {
            m.changed_repos.join(", ")
        };
        let _ = writeln!(buf, "- **changed repos:** {changed_repos_str}");
        let indexed_repos_str = if m.indexed_repos.is_empty() {
            "(none)".to_owned()
        } else {
            m.indexed_repos.join(", ")
        };
        let _ = writeln!(buf, "- **indexed repos:** {indexed_repos_str}");
        let _ = writeln!(buf, "- **elapsed:** {}ms", m.elapsed_ms);

        buf.push_str("\n## Safety metadata\n\n");
        let _ = writeln!(
            buf,
            "- **baseline registry:** `{}`",
            s.baseline_registry_path.display()
        );
        let _ = writeln!(
            buf,
            "- **baseline storage:** `{}`",
            s.baseline_storage_path.display()
        );
        let _ = writeln!(
            buf,
            "- **review registry:** `{}`",
            s.review_registry_path.display()
        );
        let _ = writeln!(
            buf,
            "- **review storage:** `{}`",
            s.review_storage_path.display()
        );
        let _ = writeln!(buf, "- **review root:** `{}`", s.review_root.display());
        let _ = writeln!(buf, "- **run id:** `{}`", s.run_id);
        let cleanup_label = match s.cleanup_policy {
            CleanupPolicy::KeepCache => "keep-cache",
            CleanupPolicy::RemoveOnExit => "remove-on-exit",
        };
        let _ = writeln!(buf, "- **cleanup:** `{cleanup_label}`");
        let _ = writeln!(buf, "- **cache key:** `{}`", s.cache_key);

        buf.push_str("\n## Changed files\n\n");
        if self.changed_files.is_empty() {
            buf.push_str("_(no changed files)_\n");
        } else {
            for f in self.changed_files.iter().take(MAX_CHANGED_FILES_DISPLAY) {
                let _ = writeln!(buf, "- `{f}`");
            }
            if self.changed_files_truncated {
                let _ = writeln!(
                    buf,
                    "\n_(list truncated; showing {MAX_CHANGED_FILES_DISPLAY} of {} changed files)_",
                    self.changed_files.len()
                );
            }
        }

        buf.push_str("\n## Phase 2 placeholders\n\n");
        buf.push_str(
            "- **added routes:** _(empty — Phase 2 populates)_\n\
             - **added symbols:** _(empty — Phase 2 populates)_\n\
             - **added payload contracts:** _(empty — Phase 2 populates)_\n",
        );

        buf.push_str("\n## Suggested follow-up commands\n\n");
        buf.push_str(
            "> **Note:** `--registry` and `--storage` overrides on `trace`, `impact`, \
             and `pack` are not yet exposed as top-level CLI flags.  These commands \
             are shown for documentation purposes.  A follow-up CLI patch (Phase 1 \
             follow-up) will surface `StorageContext` overrides as CLI flags.  \
             All suggested commands require `--keep-cache` to have been used.\n\n",
        );
        for cmd in &self.suggested_followups {
            let _ = writeln!(
                buf,
                "### {}\n\n```bash\n{}\n```\n",
                cmd.label, cmd.command
            );
        }

        buf
    }
}

// ─── Builder helpers ──────────────────────────────────────────────────────────

/// Build the list of suggested follow-up commands parameterized with the
/// review artifact root paths.
pub fn build_suggested_followups(
    workspace: &std::path::Path,
    review_registry_path: &std::path::Path,
    review_storage_path: &std::path::Path,
) -> Vec<SuggestedCommand> {
    let ws = workspace.display();
    let reg = review_registry_path.display();
    let stor = review_storage_path.display();

    // TODO(Phase 1 follow-up): --registry and --storage overrides on trace,
    // impact, and pack are not yet surfaced as CLI flags.  These commands show
    // the intended invocation; the flags will be wired in a follow-up patch.
    vec![
        SuggestedCommand {
            label: "Trace a CRUD route in the PR branch".to_owned(),
            command: format!(
                "gather-step --workspace {ws} trace crud --method GET --path /<example> \\\n  \
                 --registry {reg} --storage {stor}"
            ),
            requires_keep_cache: true,
        },
        SuggestedCommand {
            label: "Impact analysis for a symbol in the PR branch".to_owned(),
            command: format!(
                "gather-step --workspace {ws} impact <SymbolName> \\\n  \
                 --registry {reg} --storage {stor}"
            ),
            requires_keep_cache: true,
        },
        SuggestedCommand {
            label: "Pack review changes into an AI context bundle".to_owned(),
            command: format!(
                "gather-step --workspace {ws} pack --topic review-changes \\\n  \
                 --registry {reg} --storage {stor}"
            ),
            requires_keep_cache: true,
        },
    ]
}
