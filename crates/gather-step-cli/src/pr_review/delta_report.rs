//! Delta report — typed output struct for `gather-step pr-review`.
//!
//! Phase 2 Task 1 formalises the schema: placeholder `Vec<serde_json::Value>`
//! fields are replaced with typed structs.  `schema_version` is bumped to 2.
//!
//! Tasks 3+4 populate symbols and `payload_contracts`.
//! Events and removed-surface risks remain placeholder for the next batch.

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

// ─── Symbol deltas (Phase 2 Task 3) ──────────────────────────────────────────

/// Added / removed / changed exported symbols and cross-repo shared-symbol stubs.
#[derive(Debug, Clone, Serialize, Default)]
pub struct SymbolDeltas {
    pub added: Vec<SymbolDelta>,
    pub removed: Vec<SymbolDelta>,
    pub changed: Vec<SymbolDeltaChange>,
}

/// One exported symbol as observed in a single index snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct SymbolDelta {
    /// `"function"`, `"class"`, `"type"`, or `"shared_symbol"`.
    pub kind: String,
    pub repo: String,
    pub qualified_name: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    pub signature: Option<String>,
    /// `"public"`, `"private"`, `"protected"`, `"package"`, or `"internal"`.
    pub visibility: Option<String>,
    pub is_virtual: bool,
}

/// Same `(repo, qualified_name)` key in both snapshots but signature or
/// visibility changed.
#[derive(Debug, Clone, Serialize)]
pub struct SymbolDeltaChange {
    pub kind: String,
    pub repo: String,
    pub qualified_name: String,
    pub before: SymbolDelta,
    pub after: SymbolDelta,
    pub signature_changed: bool,
    pub visibility_changed: bool,
}

// ─── Payload-contract deltas (Phase 2 Task 4) ─────────────────────────────────

/// Added / removed / changed payload contracts.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PayloadContractDeltas {
    pub added: Vec<PayloadContractDelta>,
    pub removed: Vec<PayloadContractDelta>,
    pub changed: Vec<PayloadContractDeltaChange>,
}

/// One payload contract as observed in a single index snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct PayloadContractDelta {
    pub repo: String,
    pub file: String,
    pub target_qualified_name: String,
    /// `"producer"` or `"consumer"`.
    pub side: String,
    pub fields: Vec<PayloadFieldSummary>,
}

/// Compact field descriptor used in the PR-review report.
#[derive(Debug, Clone, Serialize)]
pub struct PayloadFieldSummary {
    pub name: String,
    pub type_name: Option<String>,
    pub optional: bool,
}

/// Same `(repo, file, target_qualified_name, side)` key in both snapshots but
/// the field set differs.
#[derive(Debug, Clone, Serialize)]
pub struct PayloadContractDeltaChange {
    pub repo: String,
    pub file: String,
    pub target_qualified_name: String,
    pub side: String,
    pub fields_added: Vec<PayloadFieldSummary>,
    pub fields_removed: Vec<PayloadFieldSummary>,
    /// Field names that flipped from `optional = true` to `optional = false`.
    pub fields_optional_to_required: Vec<String>,
    /// Field names that flipped from `optional = false` to `optional = true`.
    pub fields_required_to_optional: Vec<String>,
    pub fields_type_changed: Vec<PayloadFieldTypeChange>,
}

/// A single field whose declared type changed between baseline and review.
#[derive(Debug, Clone, Serialize)]
pub struct PayloadFieldTypeChange {
    pub name: String,
    pub before_type: Option<String>,
    pub after_type: Option<String>,
}

// ─── Event deltas (Phase 2 Task 5) ────────────────────────────────────────────

/// Event deltas (topics / queues / subjects / streams / events).
#[derive(Debug, Clone, Serialize, Default)]
pub struct EventDeltas {
    pub added: Vec<EventDelta>,
    pub removed: Vec<EventDelta>,
    pub changed: Vec<EventDeltaChange>,
}

/// One event virtual node as observed in a single index snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct EventDelta {
    /// `"topic"`, `"queue"`, `"subject"`, `"stream"`, or `"event"`.
    pub event_kind: String,
    pub event_name: String,
    /// Full `external_id` of the virtual node.
    pub external_id: String,
    pub producers: Vec<EventEndpointSummary>,
    pub consumers: Vec<EventEndpointSummary>,
}

/// A producer or consumer endpoint connected to an event virtual node.
#[derive(Debug, Clone, Serialize)]
pub struct EventEndpointSummary {
    pub repo: String,
    pub qualified_name: String,
    pub file: Option<String>,
    pub line: Option<u32>,
}

/// Same `(event_kind, event_name)` key in both snapshots but producer/consumer
/// sets differ.
#[derive(Debug, Clone, Serialize)]
pub struct EventDeltaChange {
    pub event_kind: String,
    pub event_name: String,
    pub producers_added: Vec<EventEndpointSummary>,
    pub producers_removed: Vec<EventEndpointSummary>,
    pub consumers_added: Vec<EventEndpointSummary>,
    pub consumers_removed: Vec<EventEndpointSummary>,
}

// ─── Removed-surface risks (Phase 2 Task 6) ───────────────────────────────────

/// A surface (route / symbol / event) removed in the PR that still has
/// surviving consumers in the graph.
#[derive(Debug, Clone, Serialize)]
pub struct RemovedSurfaceRisk {
    /// `"route"` | `"shared_symbol"` | `"event"`
    pub kind: String,
    /// route: `"GET /orders/:id"`; symbol: `qualified_name`; event: `"<kind>:<name>"`
    pub identity: String,
    /// Owner repo (for routes / symbols).
    pub repo: Option<String>,
    pub surviving_consumers: Vec<RemovedSurfaceConsumer>,
    pub severity: RiskSeverity,
}

/// One surviving consumer of a removed surface.
#[derive(Debug, Clone, Serialize)]
pub struct RemovedSurfaceConsumer {
    pub repo: String,
    pub qualified_name: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    /// e.g. `"ConsumesApiFrom"`, `"UsesShared"`, `"Consumes"`.
    pub edge_kind: String,
}

/// Severity of a [`RemovedSurfaceRisk`].
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RiskSeverity {
    Low,
    Medium,
    High,
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

        // ── Symbol deltas ─────────────────────────────────────────────────────
        render_symbol_section(&mut buf, "New symbols", &self.symbols.added);
        render_symbol_section(&mut buf, "Removed symbols", &self.symbols.removed);
        render_symbol_changed_section(&mut buf, &self.symbols.changed);

        // ── Payload-contract deltas ───────────────────────────────────────────
        render_contract_section(&mut buf, "New payload contracts", &self.payload_contracts.added);
        render_contract_section(
            &mut buf,
            "Removed payload contracts",
            &self.payload_contracts.removed,
        );
        render_contract_changed_section(&mut buf, &self.payload_contracts.changed);

        // ── Event deltas ──────────────────────────────────────────────────────
        render_event_section(&mut buf, "Events: new producers/consumers", &self.events.added);
        render_event_section(&mut buf, "Events: removed producers/consumers", &self.events.removed);
        render_event_changed_section(&mut buf, &self.events.changed);

        // ── Removed-surface risks ─────────────────────────────────────────────
        render_risks_section(&mut buf, &self.removed_surface_risks);

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

fn render_symbol_section(buf: &mut String, heading: &str, symbols: &[SymbolDelta]) {
    let _ = writeln!(buf, "\n## {heading}\n");
    if symbols.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    buf.push_str("| Kind | Repo | Qualified name | File | Line | Signature |\n");
    buf.push_str("|------|------|----------------|------|------|-----------|\n");
    for s in symbols {
        let file = s.file.as_deref().unwrap_or("—");
        let line = s.line.map_or_else(|| "—".to_owned(), |l| l.to_string());
        let sig = s.signature.as_deref().unwrap_or("—");
        let _ = writeln!(
            buf,
            "| {} | {} | `{}` | {} | {} | `{}` |",
            s.kind, s.repo, s.qualified_name, file, line, sig
        );
    }
}

fn render_symbol_changed_section(buf: &mut String, changes: &[SymbolDeltaChange]) {
    let _ = writeln!(buf, "\n## Changed symbols\n");
    if changes.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    buf.push_str("| Kind | Repo | Qualified name | Signature changed | Visibility changed |\n");
    buf.push_str("|------|------|----------------|------------------|-------------------|\n");
    for c in changes {
        let _ = writeln!(
            buf,
            "| {} | {} | `{}` | {} | {} |",
            c.kind,
            c.repo,
            c.qualified_name,
            if c.signature_changed { "yes" } else { "no" },
            if c.visibility_changed { "yes" } else { "no" },
        );
    }
}

fn render_contract_section(buf: &mut String, heading: &str, contracts: &[PayloadContractDelta]) {
    let _ = writeln!(buf, "\n## {heading}\n");
    if contracts.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    buf.push_str("| Repo | File | Target | Side | Fields |\n");
    buf.push_str("|------|------|--------|------|--------|\n");
    for c in contracts {
        let fields: Vec<&str> = c.fields.iter().map(|f| f.name.as_str()).collect();
        let _ = writeln!(
            buf,
            "| {} | {} | `{}` | {} | {} |",
            c.repo,
            c.file,
            c.target_qualified_name,
            c.side,
            fields.join(", ")
        );
    }
}

fn render_contract_changed_section(buf: &mut String, changes: &[PayloadContractDeltaChange]) {
    let _ = writeln!(buf, "\n## Changed payload contracts\n");
    if changes.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    for c in changes {
        let _ = writeln!(
            buf,
            "### `{}` — {} / {}\n",
            c.target_qualified_name, c.repo, c.side
        );
        if !c.fields_added.is_empty() {
            let names: Vec<&str> = c.fields_added.iter().map(|f| f.name.as_str()).collect();
            let _ = writeln!(buf, "- **Fields added:** {}", names.join(", "));
        }
        if !c.fields_removed.is_empty() {
            let names: Vec<&str> = c.fields_removed.iter().map(|f| f.name.as_str()).collect();
            let _ = writeln!(buf, "- **Fields removed:** {}", names.join(", "));
        }
        if !c.fields_optional_to_required.is_empty() {
            let _ = writeln!(
                buf,
                "- **Now required:** {}",
                c.fields_optional_to_required.join(", ")
            );
        }
        if !c.fields_required_to_optional.is_empty() {
            let _ = writeln!(
                buf,
                "- **Now optional:** {}",
                c.fields_required_to_optional.join(", ")
            );
        }
        if !c.fields_type_changed.is_empty() {
            let _ = writeln!(buf, "- **Type changes:**");
            for tc in &c.fields_type_changed {
                let before = tc.before_type.as_deref().unwrap_or("unknown");
                let after = tc.after_type.as_deref().unwrap_or("unknown");
                let _ = writeln!(buf, "  - `{}`: `{}` → `{}`", tc.name, before, after);
            }
        }
        buf.push('\n');
    }
}

fn render_event_section(buf: &mut String, heading: &str, events: &[EventDelta]) {
    let _ = writeln!(buf, "\n## {heading}\n");
    if events.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    for e in events {
        let _ = writeln!(buf, "### `{}` ({})\n", e.event_name, e.event_kind);
        if !e.producers.is_empty() {
            buf.push_str("**Producers:**\n");
            for p in &e.producers {
                let loc = format_loc(p.file.as_deref(), p.line);
                let _ = writeln!(buf, "- `{}` / `{}`{}", p.repo, p.qualified_name, loc);
            }
        }
        if !e.consumers.is_empty() {
            buf.push_str("**Consumers:**\n");
            for c in &e.consumers {
                let loc = format_loc(c.file.as_deref(), c.line);
                let _ = writeln!(buf, "- `{}` / `{}`{}", c.repo, c.qualified_name, loc);
            }
        }
        buf.push('\n');
    }
}

fn render_event_changed_section(buf: &mut String, changes: &[EventDeltaChange]) {
    let _ = writeln!(buf, "\n## Events: changed producers/consumers\n");
    if changes.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    for c in changes {
        let _ = writeln!(buf, "### `{}` ({})\n", c.event_name, c.event_kind);
        if !c.producers_added.is_empty() {
            buf.push_str("**Producers added:**\n");
            for p in &c.producers_added {
                let loc = format_loc(p.file.as_deref(), p.line);
                let _ = writeln!(buf, "- `{}` / `{}`{}", p.repo, p.qualified_name, loc);
            }
        }
        if !c.producers_removed.is_empty() {
            buf.push_str("**Producers removed:**\n");
            for p in &c.producers_removed {
                let loc = format_loc(p.file.as_deref(), p.line);
                let _ = writeln!(buf, "- `{}` / `{}`{}", p.repo, p.qualified_name, loc);
            }
        }
        if !c.consumers_added.is_empty() {
            buf.push_str("**Consumers added:**\n");
            for p in &c.consumers_added {
                let loc = format_loc(p.file.as_deref(), p.line);
                let _ = writeln!(buf, "- `{}` / `{}`{}", p.repo, p.qualified_name, loc);
            }
        }
        if !c.consumers_removed.is_empty() {
            buf.push_str("**Consumers removed:**\n");
            for p in &c.consumers_removed {
                let loc = format_loc(p.file.as_deref(), p.line);
                let _ = writeln!(buf, "- `{}` / `{}`{}", p.repo, p.qualified_name, loc);
            }
        }
        buf.push('\n');
    }
}

fn render_risks_section(buf: &mut String, risks: &[RemovedSurfaceRisk]) {
    let _ = writeln!(buf, "\n## Removed-surface risks\n");
    if risks.is_empty() {
        buf.push_str("_no risks_\n");
        return;
    }
    for r in risks {
        let severity_label = match r.severity {
            RiskSeverity::High => "HIGH",
            RiskSeverity::Medium => "MEDIUM",
            RiskSeverity::Low => "LOW",
        };
        let repo_part = r
            .repo
            .as_deref()
            .map_or_else(String::new, |repo| format!(" (`{repo}`)"));
        let _ = writeln!(
            buf,
            "### [{severity_label}] `{}` — {}{}\n",
            r.identity, r.kind, repo_part
        );
        if r.surviving_consumers.is_empty() {
            buf.push_str("_no surviving consumers_\n");
        } else {
            buf.push_str("**Surviving consumers:**\n");
            for c in &r.surviving_consumers {
                let loc = format_loc(c.file.as_deref(), c.line);
                let _ = writeln!(
                    buf,
                    "- `{}` / `{}` via `{}`{}",
                    c.repo, c.qualified_name, c.edge_kind, loc
                );
            }
        }
        buf.push('\n');
    }
}

fn format_loc(file: Option<&str>, line: Option<u32>) -> String {
    match (file, line) {
        (Some(f), Some(l)) => format!(" ({f}:{l})"),
        (Some(f), None) => format!(" ({f})"),
        _ => String::new(),
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
