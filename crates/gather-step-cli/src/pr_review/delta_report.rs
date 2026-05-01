//! Delta report — typed output struct for `gather-step pr-review`.
//!
//! Phase 2 Task 1 formalises the schema: placeholder `Vec<serde_json::Value>`
//! fields are replaced with typed structs.  `schema_version` is bumped to 2.
//!
//! Phase 3 Tasks 3+4+5 add contract alignments, decorator deltas, and review
//! pack synthesis.  `schema_version` is bumped to 3.
//!
//! Phase 5 Tasks 1+2 add `unsupported_surfaces` so the renderer can print
//! "_unavailable on the {engine} engine_" instead of "_no changes_" for
//! surfaces the active engine cannot populate.  `schema_version` is bumped
//! to 4.

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

    // ── Phase 3 additions ────────────────────────────────────────────────────
    pub contract_alignments: ContractAlignments,
    pub decorators: DecoratorDeltas,

    pub suggested_followups: Vec<SuggestedCommand>,

    // ── Phase 5 additions ────────────────────────────────────────────────────
    /// Surfaces not supported by the active review engine.
    ///
    /// When non-empty, the renderer should print a note under each affected
    /// section explaining that the data is unavailable on the current engine
    /// rather than implying there are no changes.
    ///
    /// Serialised as `null` when the engine supports all surfaces (i.e. the
    /// field is skipped in output for the `temp-index` engine).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unsupported_surfaces: Vec<String>,
}

// ─── Impact summary (Phase 3 Tasks 1+2) ──────────────────────────────────────

/// Downstream impact summary attached to removed / changed surfaces.
///
/// Populated by `extract/impact_attach.rs` for any surface present in the
/// baseline graph.  Added surfaces always have `impact = None` because they
/// have no baseline node to walk.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ImpactSummary {
    /// Repos that contain at least one consumer.
    pub consumer_repos: Vec<String>,
    /// Total number of consuming symbols across all repos.
    pub consumer_count: u32,
    /// Per-repo classified counts, sorted by `total` descending.
    pub by_repo: Vec<RepoImpact>,
    /// `true` if the BFS hit its cap and the result was truncated.
    pub truncated: bool,
}

/// Per-repo breakdown of consumer classifications.
#[derive(Debug, Clone, Serialize)]
pub struct RepoImpact {
    pub repo: String,
    pub total: u32,
    pub read_only: u32,
    pub write_mutate: u32,
    pub construct_payload: u32,
    pub unknown: u32,
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
    /// Downstream impact summary.  `None` for `added` routes (no baseline
    /// node).  Populated for `removed` and `changed` routes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<ImpactSummary>,
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
    /// Downstream impact summary.  `None` for `added` symbols.
    /// Populated for `removed` and `changed` symbols.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<ImpactSummary>,
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

// ─── Contract alignments (Phase 3 Task 3) ────────────────────────────────────

/// Cross-repo payload-contract alignment findings.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ContractAlignments {
    pub findings: Vec<ContractAlignmentFinding>,
}

/// A cluster of related contracts that share the same canonical identity.
#[derive(Debug, Clone, Serialize)]
pub struct ContractAlignmentFinding {
    /// Identity of the cluster (e.g. `"UpdateLabelProject"`).
    pub identity: String,
    /// Members of the alignment cluster: frontend payload, backend DTO,
    /// gateway mapping, route, shared symbol.
    pub members: Vec<ContractAlignmentMember>,
    pub confidence: AlignmentConfidence,
    /// `true` if any member is in the changed-payload-contracts set for this PR.
    pub touched_by_pr: bool,
}

/// One participant in a contract alignment cluster.
#[derive(Debug, Clone, Serialize)]
pub struct ContractAlignmentMember {
    /// `"frontend_payload"` | `"backend_dto"` | `"gateway_mapping"` |
    /// `"route"` | `"shared_symbol"` | `"unknown"`.
    pub role: String,
    pub repo: String,
    pub qualified_name: String,
    pub file: Option<String>,
}

/// Confidence that two contract records represent the same logical contract.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum AlignmentConfidence {
    Low,
    Medium,
    High,
}

// ─── Decorator deltas (Phase 3 Task 4) ────────────────────────────────────────

/// Added / removed / changed decorator annotations (RBAC, audit, auth guards).
#[derive(Debug, Clone, Serialize, Default)]
pub struct DecoratorDeltas {
    pub added: Vec<DecoratorDelta>,
    pub removed: Vec<DecoratorDelta>,
    pub changed: Vec<DecoratorDeltaChange>,
}

/// A single decorator annotation as observed in one index snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct DecoratorDelta {
    pub repo: String,
    pub file: Option<String>,
    pub line: Option<u32>,
    /// Decorator name, e.g. `"Audit"`, `"Permission"`, `"Authenticated"`.
    pub decorator_name: String,
    /// The symbol this decorator is attached to (when available).
    pub target_qualified_name: Option<String>,
    /// Raw argument signature, e.g. `"'read:labels'"` (when available).
    pub args: Option<String>,
}

/// A decorator whose arguments or position changed between baseline and review.
#[derive(Debug, Clone, Serialize)]
pub struct DecoratorDeltaChange {
    pub repo: String,
    pub target_qualified_name: String,
    pub before: DecoratorDelta,
    pub after: DecoratorDelta,
    pub args_changed: bool,
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

        // ── Contract alignments ───────────────────────────────────────────────
        render_contract_alignments_section(&mut buf, &self.contract_alignments);

        // ── Decorator deltas ──────────────────────────────────────────────────
        render_decorator_section(&mut buf, "New decorators", &self.decorators.added);
        render_decorator_section(&mut buf, "Removed decorators", &self.decorators.removed);
        render_decorator_changed_section(&mut buf, &self.decorators.changed);

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

fn render_contract_alignments_section(buf: &mut String, alignments: &ContractAlignments) {
    let _ = writeln!(buf, "\n## Contract alignments\n");
    if alignments.findings.is_empty() {
        buf.push_str("_no alignment findings_\n");
        return;
    }
    for f in &alignments.findings {
        let confidence_badge = match f.confidence {
            AlignmentConfidence::High => "HIGH",
            AlignmentConfidence::Medium => "MEDIUM",
            AlignmentConfidence::Low => "LOW",
        };
        let touched = if f.touched_by_pr { " *(touched by PR)*" } else { "" };
        let _ = writeln!(buf, "### `{}` — confidence: {}{}\n", f.identity, confidence_badge, touched);
        buf.push_str("| Role | Repo | Qualified name | File |\n");
        buf.push_str("|------|------|----------------|------|\n");
        for m in &f.members {
            let file = m.file.as_deref().unwrap_or("—");
            let _ = writeln!(buf, "| {} | {} | `{}` | {} |", m.role, m.repo, m.qualified_name, file);
        }
        buf.push('\n');
    }
}

fn render_decorator_section(buf: &mut String, heading: &str, decorators: &[DecoratorDelta]) {
    let _ = writeln!(buf, "\n## {heading}\n");
    if decorators.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    buf.push_str("| Decorator | Repo | Target | File | Line | Args |\n");
    buf.push_str("|-----------|------|--------|------|------|------|\n");
    for d in decorators {
        let target = d.target_qualified_name.as_deref().unwrap_or("—");
        let file = d.file.as_deref().unwrap_or("—");
        let line = d.line.map_or_else(|| "—".to_owned(), |l| l.to_string());
        let args = d.args.as_deref().unwrap_or("—");
        let _ = writeln!(
            buf,
            "| `{}` | {} | {} | {} | {} | {} |",
            d.decorator_name, d.repo, target, file, line, args
        );
    }
}

fn render_decorator_changed_section(buf: &mut String, changes: &[DecoratorDeltaChange]) {
    let _ = writeln!(buf, "\n## Changed decorators\n");
    if changes.is_empty() {
        buf.push_str("_no changes_\n");
        return;
    }
    for c in changes {
        let args_note = if c.args_changed { " *(args changed)*" } else { "" };
        let _ = writeln!(
            buf,
            "### `{}` on `{}`{}\n",
            c.before.decorator_name, c.target_qualified_name, args_note
        );
        let before_args = c.before.args.as_deref().unwrap_or("—");
        let after_args = c.after.args.as_deref().unwrap_or("—");
        let _ = writeln!(buf, "- **before args:** `{before_args}`");
        let _ = writeln!(buf, "- **after args:** `{after_args}`");
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

/// Maximum number of suggested follow-up commands emitted by the pack synthesizer.
const MAX_SYNTHESIZED_FOLLOWUPS: usize = 10;

/// Synthesize targeted `pack` / `trace` commands for the highest-impact deltas
/// found in a PR review run.
///
/// Emits at most [`MAX_SYNTHESIZED_FOLLOWUPS`] commands, pruning the least
/// impactful ones first if the cap is exceeded.  All emitted commands set
/// `requires_keep_cache = true` because they reference the review artifact root.
pub fn synthesize_review_pack_commands(
    workspace: &std::path::Path,
    review_registry: &std::path::Path,
    review_storage: &std::path::Path,
    routes: &RouteDeltas,
    _symbols: &SymbolDeltas,
    payloads: &PayloadContractDeltas,
    risks: &[RemovedSurfaceRisk],
) -> Vec<SuggestedCommand> {
    let ws = shell_quote(workspace);
    let reg = shell_quote(review_registry);
    let stor = shell_quote(review_storage);

    let mut cmds: Vec<SuggestedCommand> = Vec::new();

    // ── High-severity removed-surface risks → pack the symbol ────────────────
    for risk in risks {
        if cmds.len() >= MAX_SYNTHESIZED_FOLLOWUPS {
            break;
        }
        if risk.severity == RiskSeverity::High {
            let identity = &risk.identity;
            cmds.push(SuggestedCommand {
                label: format!("trace caller graph for removed {identity}"),
                command: format!(
                    "gather-step --workspace {ws} pack {identity} \
                     --registry {reg} --storage {stor}"
                ),
                requires_keep_cache: true,
            });
        }
    }

    // ── Changed payload contracts with ≥3 field changes → pack the contract ──
    for change in &payloads.changed {
        if cmds.len() >= MAX_SYNTHESIZED_FOLLOWUPS {
            break;
        }
        let total_field_changes = change.fields_added.len()
            + change.fields_removed.len()
            + change.fields_optional_to_required.len()
            + change.fields_required_to_optional.len()
            + change.fields_type_changed.len();
        if total_field_changes >= 3 {
            let qn = &change.target_qualified_name;
            cmds.push(SuggestedCommand {
                label: format!("inspect field changes in {qn}"),
                command: format!(
                    "gather-step --workspace {ws} pack {qn} \
                     --registry {reg} --storage {stor}"
                ),
                requires_keep_cache: true,
            });
        }
    }

    // ── Changed routes with handler change → trace route ─────────────────────
    for change in &routes.changed {
        if cmds.len() >= MAX_SYNTHESIZED_FOLLOWUPS {
            break;
        }
        if change.handler_changed {
            let method = &change.method;
            let path = &change.path;
            cmds.push(SuggestedCommand {
                label: format!("trace handler change for {method} {path}"),
                command: format!(
                    "gather-step --workspace {ws} trace crud \
                     --method {method} --path {path} \
                     --registry {reg} --storage {stor}"
                ),
                requires_keep_cache: true,
            });
        }
    }

    cmds.truncate(MAX_SYNTHESIZED_FOLLOWUPS);
    cmds
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::commands::Cli;

    // ── schema snapshot ───────────────────────────────────────────────────────

    fn make_empty_report(schema_version: u32) -> DeltaReport {
        DeltaReport {
            schema_version,
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
            contract_alignments: ContractAlignments::default(),
            decorators: DecoratorDeltas::default(),
            suggested_followups: vec![],
            unsupported_surfaces: vec![],
        }
    }

    /// Assert the JSON top-level keys are stable across refactors.
    #[test]
    fn snapshot_top_level_keys() {
        let report = make_empty_report(3);

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
                "contract_alignments",
                "decorators",
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
    fn schema_version_is_4() {
        let report = make_empty_report(4);
        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["schema_version"], 4);
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

    // ── Task 5: review pack synthesis ────────────────────────────────────────

    fn make_risk(identity: &str, severity: RiskSeverity) -> RemovedSurfaceRisk {
        RemovedSurfaceRisk {
            kind: "shared_symbol".to_owned(),
            identity: identity.to_owned(),
            repo: Some("backend".to_owned()),
            surviving_consumers: vec![],
            severity,
        }
    }

    fn make_payload_change(qn: &str, field_count: usize) -> PayloadContractDeltaChange {
        PayloadContractDeltaChange {
            repo: "backend".to_owned(),
            file: "src/dto.ts".to_owned(),
            target_qualified_name: qn.to_owned(),
            side: "producer".to_owned(),
            fields_added: (0..field_count)
                .map(|i| PayloadFieldSummary { name: format!("field{i}"), type_name: Some("string".to_owned()), optional: false })
                .collect(),
            fields_removed: vec![],
            fields_optional_to_required: vec![],
            fields_required_to_optional: vec![],
            fields_type_changed: vec![],
        }
    }

    /// High-severity risk emits a `pack` command targeting the risk identity.
    #[test]
    fn high_severity_risk_emits_pack_command() {
        let risks = vec![make_risk("UpdateLabelProjectInput", RiskSeverity::High)];
        let cmds = synthesize_review_pack_commands(
            std::path::Path::new("/tmp/ws"),
            std::path::Path::new("/tmp/reg.json"),
            std::path::Path::new("/tmp/storage"),
            &RouteDeltas::default(),
            &SymbolDeltas::default(),
            &PayloadContractDeltas::default(),
            &risks,
        );
        assert_eq!(cmds.len(), 1);
        assert!(cmds[0].command.contains("pack UpdateLabelProjectInput"), "command should pack the identity: {}", cmds[0].command);
        assert!(cmds[0].requires_keep_cache);
    }

    /// Changed payload with ≥3 field changes emits a `pack` command.
    #[test]
    fn changed_payload_with_three_field_changes_emits_pack() {
        let mut payloads = PayloadContractDeltas::default();
        payloads.changed.push(make_payload_change("UpdateLabelProjectDto", 3));
        let cmds = synthesize_review_pack_commands(
            std::path::Path::new("/tmp/ws"),
            std::path::Path::new("/tmp/reg.json"),
            std::path::Path::new("/tmp/storage"),
            &RouteDeltas::default(),
            &SymbolDeltas::default(),
            &payloads,
            &[],
        );
        assert_eq!(cmds.len(), 1);
        assert!(cmds[0].command.contains("pack UpdateLabelProjectDto"), "command should pack the contract: {}", cmds[0].command);
        assert!(cmds[0].requires_keep_cache);
    }

    /// Cap at 10: with 20 high-severity risks only 10 commands are emitted.
    #[test]
    fn followups_capped_at_ten() {
        let risks: Vec<RemovedSurfaceRisk> = (0..20)
            .map(|i| make_risk(&format!("Symbol{i}"), RiskSeverity::High))
            .collect();
        let cmds = synthesize_review_pack_commands(
            std::path::Path::new("/tmp/ws"),
            std::path::Path::new("/tmp/reg.json"),
            std::path::Path::new("/tmp/storage"),
            &RouteDeltas::default(),
            &SymbolDeltas::default(),
            &PayloadContractDeltas::default(),
            &risks,
        );
        assert_eq!(cmds.len(), 10, "followups must be capped at 10");
    }
}
