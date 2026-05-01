//! Delta report — typed output struct for `gather-step pr-review`.
//!
//! Phase 2 Task 1 formalises the schema: placeholder `Vec<serde_json::Value>`
//! fields are replaced with typed structs.  `schema_version` is bumped to 2.
//!
//! Surfaces not yet populated (symbols, payload contracts, events,
//! removed-surface risks) ship as empty arrays; Tasks 3-6 will fill them in.

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

    // ── Strongly-typed delta surfaces (Phase 2) ──────────────────────────────
    pub routes: RouteDeltas,
    pub symbols: SymbolDeltas,
    pub payload_contracts: PayloadContractDeltas,
    pub events: EventDeltas,
    pub removed_surface_risks: Vec<RemovedSurfaceRisk>,

    pub suggested_followups: Vec<SuggestedCommand>,
}

// ─── Route deltas ─────────────────────────────────────────────────────────────

/// Added / removed / changed HTTP routes.
#[derive(Debug, Clone, Serialize, Default)]
pub struct RouteDeltas {
    pub added: Vec<RouteDelta>,
    pub removed: Vec<RouteDelta>,
    pub changed: Vec<RouteDeltaChange>,
}

/// A single HTTP route surface point as observed in one index snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct RouteDelta {
    pub method: String,
    /// Canonical path with `:id`-style params (e.g. `/orders/:id`).
    pub path: String,
    /// Owning repo (the repo that contains the handler).  `None` when no
    /// `Serves` edge links the route virtual node to a handler yet.
    pub repo: Option<String>,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub handler_qualified_name: Option<String>,
}

/// A route present in both baseline and review whose handler details changed.
#[derive(Debug, Clone, Serialize)]
pub struct RouteDeltaChange {
    pub method: String,
    pub path: String,
    /// Baseline view of the route (handler info as of base).
    pub before: Option<RouteDelta>,
    /// Review view of the route (handler info as of head).
    pub after: Option<RouteDelta>,
    /// `true` when the handler symbol, file, or owning repo changed.
    pub handler_changed: bool,
}

// ─── Placeholder surfaces (Tasks 3-6) ─────────────────────────────────────────

/// Shared-symbol deltas — populated by Phase 2 Task 3.
#[derive(Debug, Clone, Serialize, Default)]
pub struct SymbolDeltas {
    pub added: Vec<serde_json::Value>,
    pub removed: Vec<serde_json::Value>,
    pub changed: Vec<serde_json::Value>,
}

/// Payload-contract deltas — populated by Phase 2 Task 4.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PayloadContractDeltas {
    pub added: Vec<serde_json::Value>,
    pub removed: Vec<serde_json::Value>,
    pub changed: Vec<serde_json::Value>,
}

/// Event deltas (topics / queues / events) — populated by Phase 2 Task 5.
#[derive(Debug, Clone, Serialize, Default)]
pub struct EventDeltas {
    pub added: Vec<serde_json::Value>,
    pub removed: Vec<serde_json::Value>,
    pub changed: Vec<serde_json::Value>,
}

/// A surface (route / symbol / event) that was removed in the PR and still has
/// surviving consumers in the graph — populated by Phase 2 Task 6.
#[derive(Debug, Clone, Serialize)]
pub struct RemovedSurfaceRisk {
    pub kind: String,
    pub identity: String,
    pub surviving_consumers: u32,
}

// ─── Other shared types ───────────────────────────────────────────────────────

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

        // ── Route deltas ──────────────────────────────────────────────────────
        render_route_section(&mut buf, "New routes", &self.routes.added);
        render_route_section(&mut buf, "Removed routes", &self.routes.removed);
        render_route_changed_section(&mut buf, &self.routes.changed);

        buf.push_str("\n## Suggested follow-up commands\n\n");
        buf.push_str("> **Note:** These commands require `--keep-cache` to have been used.\n\n");
        for cmd in &self.suggested_followups {
            let _ = writeln!(buf, "### {}\n\n```bash\n{}\n```\n", cmd.label, cmd.command);
        }

        buf
    }
}

fn render_route_section(buf: &mut String, heading: &str, routes: &[RouteDelta]) {
    let _ = writeln!(buf, "\n## {heading}\n");
    if routes.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    buf.push_str("| Method | Path | Repo | File | Line | Handler |\n");
    buf.push_str("|--------|------|------|------|------|---------|\n");
    for r in routes {
        let repo = r.repo.as_deref().unwrap_or("—");
        let file = r.file.as_deref().unwrap_or("—");
        let line = r.line.map_or_else(|| "—".to_owned(), |l| l.to_string());
        let handler = r.handler_qualified_name.as_deref().unwrap_or("—");
        let _ = writeln!(
            buf,
            "| `{}` | `{}` | {} | {} | {} | {} |",
            r.method, r.path, repo, file, line, handler
        );
    }
}

fn render_route_changed_section(buf: &mut String, changes: &[RouteDeltaChange]) {
    let _ = writeln!(buf, "\n## Changed routes\n");
    if changes.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    buf.push_str("| Method | Path | Before handler | After handler |\n");
    buf.push_str("|--------|------|----------------|---------------|\n");
    for c in changes {
        let before_h = c
            .before
            .as_ref()
            .and_then(|b| b.handler_qualified_name.as_deref())
            .unwrap_or("—");
        let after_h = c
            .after
            .as_ref()
            .and_then(|a| a.handler_qualified_name.as_deref())
            .unwrap_or("—");
        let _ = writeln!(
            buf,
            "| `{}` | `{}` | {} | {} |",
            c.method, c.path, before_h, after_h
        );
    }
}

// ─── Builder helpers ──────────────────────────────────────────────────────────

/// Shell-quote a path so it is safe to embed in a suggested shell command.
///
/// Paths that consist entirely of "safe" shell characters are returned as-is.
/// Everything else is wrapped in single quotes, with any embedded single quote
/// escaped via the standard `'\''` idiom.
fn shell_quote(p: &std::path::Path) -> String {
    let s = p.to_string_lossy();
    if s.is_empty() {
        return "''".to_owned();
    }
    // Characters safe without quoting in POSIX shells.
    if s.bytes()
        .all(|b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'/' | b'=' | b':'))
    {
        return s.into_owned();
    }
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Build the list of suggested follow-up commands parameterized with the
/// review artifact root paths.
pub fn build_suggested_followups(
    workspace: &std::path::Path,
    review_registry_path: &std::path::Path,
    review_storage_path: &std::path::Path,
) -> Vec<SuggestedCommand> {
    let ws = shell_quote(workspace);
    let reg = shell_quote(review_registry_path);
    let stor = shell_quote(review_storage_path);

    vec![
        SuggestedCommand {
            label: "Trace a CRUD route in the PR branch".to_owned(),
            command: format!(
                "gather-step --workspace {ws} trace --registry {reg} --storage {stor} crud --method GET --path /example"
            ),
            requires_keep_cache: true,
        },
        SuggestedCommand {
            label: "Impact analysis for a symbol in the PR branch".to_owned(),
            command: format!(
                "gather-step --workspace {ws} impact --registry {reg} --storage {stor} ExampleSymbol"
            ),
            requires_keep_cache: true,
        },
        SuggestedCommand {
            label: "Pack review changes into an AI context bundle".to_owned(),
            command: format!(
                "gather-step --workspace {ws} pack --registry {reg} --storage {stor} --mode review ExampleSymbol"
            ),
            requires_keep_cache: true,
        },
    ]
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::commands::Cli;

    // ── schema snapshot ───────────────────────────────────────────────────────

    /// Assert the JSON top-level keys are stable across refactors.
    #[test]
    fn snapshot_top_level_keys() {
        let report = DeltaReport {
            schema_version: 2,
            metadata: ReviewMetadata {
                workspace: std::path::PathBuf::from("/tmp/ws"),
                base_input: "main".to_owned(),
                base_sha: "a".repeat(40),
                head_input: "HEAD".to_owned(),
                head_sha: "b".repeat(40),
                checkout_mode: "head".to_owned(),
                changed_repos: vec![],
                indexed_repos: vec![],
                elapsed_ms: 0,
            },
            safety: SafetyMetadata {
                baseline_registry_path: std::path::PathBuf::from("/tmp/reg.json"),
                baseline_storage_path: std::path::PathBuf::from("/tmp/storage"),
                review_registry_path: std::path::PathBuf::from("/tmp/rev/reg.json"),
                review_storage_path: std::path::PathBuf::from("/tmp/rev/storage"),
                review_root: std::path::PathBuf::from("/tmp/rev"),
                run_id: "test-run".to_owned(),
                cleanup_policy: CleanupPolicy::RemoveOnExit,
                cache_key: "hash:aaa:bbb".to_owned(),
            },
            changed_files: vec![],
            changed_files_truncated: false,
            routes: RouteDeltas::default(),
            symbols: SymbolDeltas::default(),
            payload_contracts: PayloadContractDeltas::default(),
            events: EventDeltas::default(),
            removed_surface_risks: vec![],
            suggested_followups: vec![],
        };

        let json = serde_json::to_value(&report).unwrap();
        let keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(std::string::String::as_str)
            .collect();
        // serde_json (without preserve_order feature) serialises object keys in
        // alphabetical order.  This list must stay sorted.
        assert_eq!(
            keys,
            [
                "changed_files",
                "changed_files_truncated",
                "events",
                "metadata",
                "payload_contracts",
                "removed_surface_risks",
                "routes",
                "safety",
                "schema_version",
                "suggested_followups",
                "symbols",
            ]
        );
    }

    // ── schema_version ────────────────────────────────────────────────────────

    #[test]
    fn schema_version_is_2() {
        let report = DeltaReport {
            schema_version: 2,
            metadata: ReviewMetadata {
                workspace: std::path::PathBuf::from("/tmp/ws"),
                base_input: "main".to_owned(),
                base_sha: "a".repeat(40),
                head_input: "HEAD".to_owned(),
                head_sha: "b".repeat(40),
                checkout_mode: "head".to_owned(),
                changed_repos: vec![],
                indexed_repos: vec![],
                elapsed_ms: 0,
            },
            safety: SafetyMetadata {
                baseline_registry_path: std::path::PathBuf::from("/tmp/reg.json"),
                baseline_storage_path: std::path::PathBuf::from("/tmp/storage"),
                review_registry_path: std::path::PathBuf::from("/tmp/rev/reg.json"),
                review_storage_path: std::path::PathBuf::from("/tmp/rev/storage"),
                review_root: std::path::PathBuf::from("/tmp/rev"),
                run_id: "test-run".to_owned(),
                cleanup_policy: CleanupPolicy::RemoveOnExit,
                cache_key: "hash:aaa:bbb".to_owned(),
            },
            changed_files: vec![],
            changed_files_truncated: false,
            routes: RouteDeltas::default(),
            symbols: SymbolDeltas::default(),
            payload_contracts: PayloadContractDeltas::default(),
            events: EventDeltas::default(),
            removed_surface_risks: vec![],
            suggested_followups: vec![],
        };
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["schema_version"], 2);
    }

    // ── follow-up command helpers ─────────────────────────────────────────────

    #[test]
    fn suggested_followups_parse_against_real_cli_surface() {
        let commands = build_suggested_followups(
            std::path::Path::new("/tmp/ws"),
            std::path::Path::new("/tmp/review/registry.json"),
            std::path::Path::new("/tmp/review/storage"),
        );

        assert_eq!(commands.len(), 3);
        for command in commands {
            assert!(
                command.requires_keep_cache,
                "follow-up commands must require kept artifacts"
            );
            assert!(
                !command.command.contains("--topic"),
                "pack suggestion must not use the removed --topic flag: {}",
                command.command
            );
            Cli::try_parse_from(command.command.split_whitespace()).unwrap_or_else(|err| {
                panic!("suggested command must parse: {err}\n{}", command.command)
            });
        }
    }

    // Finding 4: followup_command_shell_quotes_paths_with_spaces
    #[test]
    fn followup_command_shell_quotes_paths_with_spaces() {
        let workspace = std::path::Path::new("/Users/foo/My Projects/gather-step");
        let registry = std::path::Path::new("/Users/foo/My Projects/.cache/registry.json");
        let storage = std::path::Path::new("/Users/foo/My Projects/.cache/storage");

        let commands = build_suggested_followups(workspace, registry, storage);

        for cmd in &commands {
            // Each path component with spaces must be single-quoted in the command.
            assert!(
                cmd.command.contains("'/Users/foo/My Projects/gather-step'"),
                "workspace path with spaces must be single-quoted: {}",
                cmd.command
            );
            assert!(
                cmd.command
                    .contains("'/Users/foo/My Projects/.cache/registry.json'"),
                "registry path with spaces must be single-quoted: {}",
                cmd.command
            );
            assert!(
                cmd.command
                    .contains("'/Users/foo/My Projects/.cache/storage'"),
                "storage path with spaces must be single-quoted: {}",
                cmd.command
            );
            // Verify the original path round-trips: single-quoted value between the
            // surrounding quotes equals the original path string.
            let ws_expected = "/Users/foo/My Projects/gather-step";
            assert!(
                cmd.command.contains(&format!("'{ws_expected}'")),
                "round-trip of workspace path must match: {}",
                cmd.command
            );
        }

        // Paths without spaces must NOT get quoted.
        let commands_plain = build_suggested_followups(
            std::path::Path::new("/tmp/ws"),
            std::path::Path::new("/tmp/registry.json"),
            std::path::Path::new("/tmp/storage"),
        );
        for cmd in &commands_plain {
            assert!(
                !cmd.command.contains('\''),
                "plain paths must not be quoted: {}",
                cmd.command
            );
        }
    }
}
