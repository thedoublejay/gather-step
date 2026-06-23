//! Removed-surface risk detection — Phase 2 Task 6.
//!
//! For each surface (route / shared symbol / event) removed in the PR, this
//! extractor walks the baseline graph for surviving consumers — nodes that still
//! exist in the review graph but depended on the removed surface.
//!
//! # Severity rules
//!
//! - `High`   — cross-repo consumers exist (consuming node's `repo` ≠ surface owner's `repo`).
//! - `Medium` — same-repo consumers exist (or any consumers for events, which have no owning repo).
//! - `Low`    — no surviving consumers found.
//!
//! # "Surviving" consumer definition
//!
//! A consumer found in the baseline is surviving if its node ID still exists in
//! the review graph (`review.get_node(consumer_id)?.is_some()`).  If the
//! consumer was also removed in the same PR it does not count.

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::GraphStore;
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{
    EventDelta, RemovedSurfaceConsumer, RemovedSurfaceRisk, RiskSeverity, RouteDelta, SymbolDelta,
};

/// AI surface node kinds checked for removal, paired with their risk-`kind`
/// label. These are the real AI `NodeKind`s; faceted surfaces (a `Tool` /
/// `AgentNode` is a `Function` with an `ai_role`) are covered by the
/// symbol-delta `removed` list, not here.
const AI_SURFACE_KINDS: &[(NodeKind, &str)] = &[
    (NodeKind::Prompt, "ai_prompt"),
    (NodeKind::VectorIndex, "ai_vector_index"),
    (NodeKind::McpTool, "ai_mcp_tool"),
    (NodeKind::McpServer, "ai_mcp_server"),
    (NodeKind::LlmModel, "ai_llm_model"),
    (NodeKind::AgentGraph, "ai_agent_graph"),
];

/// Enforcement / authorization decorator names (case-insensitive). Removing one
/// of these from a surviving route/handler strips an access guard and is flagged
/// as a High-severity `security_decorator` risk. Extend this set as new guards
/// are registered.
///
/// `useguards` is the primary `NestJS` enforcement mechanism — `@UseGuards(
/// AuthGuard, RolesGuard)` is what actually wires authn/authz onto a handler —
/// so dropping it removes auth entirely and must be caught here.
const ENFORCEMENT_DECORATORS: &[&str] = &[
    "roles",
    "permission",
    "permissions",
    "authenticated",
    "audit",
    "rolesallowed",
    "requirespermission",
    "useguards",
    "auth",
    "authorized",
    "requireauth",
];

/// Auth-*weakening* decorator names (case-insensitive). These mark a surface as
/// intentionally unguarded (`@Public`, `@SkipAuth`). *Adding* one of these to a
/// surviving surface is the inverse of removing an enforcement decorator — it
/// opens up a previously-guarded surface — and is flagged High.
const AUTH_WEAKENING_DECORATORS: &[&str] = &["public", "skipauth"];

/// Real symbol kinds (non-virtual) whose direct removal we scan for surviving
/// cross-repo callers. Mirrors the kinds the shared-symbol fallback matches.
const DIRECT_SYMBOL_KINDS: &[NodeKind] = &[NodeKind::Function, NodeKind::Class, NodeKind::Type];

/// Incoming edge kinds marking a *direct* code-level consumer of a symbol: a
/// call site (`Calls`) or a reference (`References`). These bypass the virtual
/// route/event/shared-symbol stubs (which use `ConsumesApiFrom`, `Consumes`,
/// `UsesShared`, …), so they catch cross-repo consumers that those passes miss.
const DIRECT_CONSUMER_EDGE_KINDS: &[EdgeKind] = &[EdgeKind::Calls, EdgeKind::References];

/// Incoming edge kinds that mark a consumer of an AI surface node.
const AI_CONSUMER_EDGE_KINDS: &[EdgeKind] = &[
    EdgeKind::UsesPrompt,
    EdgeKind::FetchesPromptFrom,
    EdgeKind::RetrievesFrom,
    EdgeKind::IndexesVector,
    EdgeKind::CallsMcpTool,
    EdgeKind::ExposesMcpTool,
    EdgeKind::ComposesAgent,
    EdgeKind::InvokesLlm,
];

/// Extract removed-surface risks by scanning consumers of every removed surface
/// in the baseline graph and checking whether those consumers are still present
/// in the review graph.
pub fn extract_removed_surface_risks<S: GraphStore>(
    baseline: &S,
    review: &S,
    routes_removed: &[RouteDelta],
    symbols_removed: &[SymbolDelta],
    events_removed: &[EventDelta],
) -> Result<Vec<RemovedSurfaceRisk>> {
    let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();

    // ── Removed routes ────────────────────────────────────────────────────────
    for route in routes_removed {
        let identity = format!("{} {}", route.method, route.path);
        let qn = format!("__route__{}__{}", route.method, route.path);

        // Find the baseline Route virtual node by external_id.
        let nodes = baseline.nodes_by_type(NodeKind::Route)?;
        let route_node = nodes.into_iter().find(|n| {
            n.is_virtual
                && (n.external_id.as_deref() == Some(&qn)
                    || n.qualified_name.as_deref() == Some(&qn))
        });

        let Some(route_node) = route_node else {
            continue;
        };

        let consumers = surviving_consumers(
            baseline,
            review,
            route_node.id,
            &[EdgeKind::ConsumesApiFrom],
        )?;

        let severity = severity_for_consumers(
            &consumers,
            route.repo.as_deref(),
            true, // cross-repo check
        );

        risks.push(RemovedSurfaceRisk {
            kind: "route".to_owned(),
            identity,
            repo: route.repo.clone(),
            surviving_consumers: consumers,
            severity,
            detail: None,
        });
    }

    // ── Removed shared symbols ────────────────────────────────────────────────
    for symbol in symbols_removed {
        // SharedSymbol stubs are often virtual with repo="__virtual__"; search
        // by kind to avoid depending on repo registration in BY_REPO.
        // SharedSymbol stubs are virtual and stored with repo="__virtual__"
        // by `bulk_insert` regardless of the symbol's owning package — match
        // by qualified_name alone (it's globally unique by design).
        let shared_nodes = baseline.nodes_by_type(NodeKind::SharedSymbol)?;
        let sym_node = shared_nodes
            .into_iter()
            .find(|n| n.qualified_name.as_deref() == Some(&symbol.qualified_name));

        // If no SharedSymbol, also look in Function/Class/Type.
        let sym_node = if sym_node.is_some() {
            sym_node
        } else {
            let nodes = baseline.nodes_by_repo(&symbol.repo)?;
            nodes.into_iter().find(|n| {
                n.qualified_name.as_deref() == Some(&symbol.qualified_name)
                    && matches!(
                        n.kind,
                        NodeKind::Function | NodeKind::Class | NodeKind::Type
                    )
            })
        };

        let Some(sym_node) = sym_node else {
            continue;
        };

        let consumers = surviving_consumers(
            baseline,
            review,
            sym_node.id,
            &[
                EdgeKind::UsesShared,
                EdgeKind::UsesTypeFrom,
                EdgeKind::ImplementsContractFrom,
            ],
        )?;

        let severity = severity_for_consumers(&consumers, Some(&symbol.repo), true);

        risks.push(RemovedSurfaceRisk {
            kind: "shared_symbol".to_owned(),
            identity: symbol.qualified_name.clone(),
            repo: Some(symbol.repo.clone()),
            surviving_consumers: consumers,
            severity,
            detail: None,
        });
    }

    // ── Removed events ────────────────────────────────────────────────────────
    for event in events_removed {
        // Find the baseline event virtual node by external_id.
        let kind = event_kind_to_node_kind(&event.event_kind);
        let nodes = baseline.nodes_by_type(kind)?;
        let event_node = nodes
            .into_iter()
            .find(|n| n.is_virtual && n.external_id.as_deref() == Some(&event.external_id));

        let Some(event_node) = event_node else {
            continue;
        };

        let consumers = surviving_consumers(
            baseline,
            review,
            event_node.id,
            &[EdgeKind::Consumes, EdgeKind::UsesEventFrom],
        )?;

        // For events: High if any cross-repo consumers, else Medium (events
        // don't have a single owning repo).
        let severity = if consumers.is_empty() {
            RiskSeverity::Low
        } else {
            // Check if any consumer is from a different "producer" repo.
            // Since events are virtual (no owning repo), any consumer is
            // considered cross-repo for severity purposes → High.
            RiskSeverity::High
        };

        risks.push(RemovedSurfaceRisk {
            kind: "event".to_owned(),
            identity: format!("{}:{}", event.event_kind, event.event_name),
            repo: None,
            surviving_consumers: consumers,
            severity,
            detail: None,
        });
    }

    // ── Removed AI surfaces (v5 Phase 4) ──────────────────────────────────────
    // Tools / prompts / vector indexes / MCP tools / agent graphs are real (often
    // convergence-virtual) nodes. When one disappears in review but an agent that
    // bound/used it survives, that binding now dangles — the AI analogue of a
    // removed route with surviving callers.
    for &(kind, label) in AI_SURFACE_KINDS {
        for node in baseline.nodes_by_type(kind)? {
            // Removed = present in baseline, absent in review.
            if review.get_node(node.id)?.is_some() {
                continue;
            }
            let consumers = surviving_consumers(baseline, review, node.id, AI_CONSUMER_EDGE_KINDS)?;
            // Virtual AI nodes have no single owning repo, so (like events) any
            // surviving consumer is treated as cross-repo → High.
            let severity = if node.is_virtual {
                if consumers.is_empty() {
                    RiskSeverity::Low
                } else {
                    RiskSeverity::High
                }
            } else {
                severity_for_consumers(&consumers, Some(&node.repo), true)
            };
            let identity = node
                .qualified_name
                .clone()
                .or_else(|| node.external_id.clone())
                .unwrap_or_else(|| node.name.clone());
            risks.push(RemovedSurfaceRisk {
                kind: label.to_owned(),
                identity,
                repo: (node.repo != "__virtual__").then(|| node.repo.clone()),
                surviving_consumers: consumers,
                severity,
                detail: None,
            });
        }
    }

    // ── Auth-decorator changes on surviving surfaces (v5.4 H2) ───────────────
    // When a PR strips an authorization/enforcement decorator (@Roles,
    // @UseGuards, @Permission, …) off a handler that still exists — or *adds* an
    // auth-weakening decorator (@Public, @SkipAuth) — the access guard changed
    // silently; flag it High. Surfaces are matched baseline↔review with a
    // rename-tolerant key so a rename-disguised guard drop is still caught, and
    // a guard hoisted to the enclosing class suppresses the per-method risk. A
    // fully-removed surface is covered by route/symbol removal risks instead.
    risks.extend(auth_decorator_change_risks(baseline, review)?);

    // ── Removed symbols with direct cross-repo consumers (v5.4 H5) ───────────
    // When a PR removes a symbol that another repo calls/references *directly*
    // (a `Calls` / `References` edge, not a virtual route/event/shared-symbol
    // stub), the cross-repo caller is left dangling and is invisible to the
    // passes above. The FP gate skips symbols already covered by the
    // shared-symbol removal pass (`symbols_removed`).
    risks.extend(removed_symbol_cross_repo_consumer_risks(
        baseline,
        review,
        symbols_removed,
    )?);

    // ── Value-mirror risks (v5.1 Task 5) ─────────────────────────────────────
    // Cross-repo precision layer over the value-mirror graph (Task 4): the
    // add-and-forget completeness check (`value_mirror_incomplete`) and the
    // modified-value edge-walk (`value_mirror`).
    super::value_mirror::extend_with_value_mirror_risks(baseline, review, &mut risks)?;

    // Sort: severity descending (High > Medium > Low), then kind, then identity.
    risks.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.identity.cmp(&b.identity))
    });

    Ok(risks)
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Walk incoming edges on `node_id` in `baseline` filtered to `edge_kinds`.
/// Return only those source nodes that still exist in `review`.
fn surviving_consumers<S: GraphStore>(
    baseline: &S,
    review: &S,
    node_id: NodeId,
    edge_kinds: &[EdgeKind],
) -> Result<Vec<RemovedSurfaceConsumer>> {
    let mut consumers: Vec<RemovedSurfaceConsumer> = Vec::new();

    for edge in baseline.get_incoming(node_id)? {
        if !edge_kinds.contains(&edge.kind) {
            continue;
        }
        let Some(source) = baseline.get_node(edge.source)? else {
            continue;
        };
        // Only surviving consumers: the source node must still exist in review.
        if review.get_node(source.id)?.is_none() {
            continue;
        }

        consumers.push(RemovedSurfaceConsumer {
            repo: source.repo.clone(),
            qualified_name: source
                .qualified_name
                .clone()
                .unwrap_or_else(|| source.name.clone()),
            file: Some(source.file_path.clone()).filter(|s| !s.is_empty()),
            line: source.span.as_ref().map(|s| s.line_start),
            edge_kind: edge_kind_name(edge.kind),
        });
    }

    // Sort for determinism.
    consumers.sort_by(|a, b| (&a.repo, &a.qualified_name).cmp(&(&b.repo, &b.qualified_name)));

    Ok(consumers)
}

/// Compute severity from a consumer list.
///
/// - `cross_repo_check = true`: check whether any consumer's repo differs from
///   `owner_repo`.  If yes → `High`; otherwise → `Medium` (if consumers exist).
fn severity_for_consumers(
    consumers: &[RemovedSurfaceConsumer],
    owner_repo: Option<&str>,
    cross_repo_check: bool,
) -> RiskSeverity {
    if consumers.is_empty() {
        return RiskSeverity::Low;
    }
    if cross_repo_check {
        let has_cross_repo = consumers
            .iter()
            .any(|c| owner_repo.is_none_or(|owner| c.repo != owner));
        if has_cross_repo {
            return RiskSeverity::High;
        }
    }
    RiskSeverity::Medium
}

/// Returns `true` if the decorator name is an enforcement/authorization guard.
fn is_enforcement_decorator(name: &str) -> bool {
    ENFORCEMENT_DECORATORS
        .iter()
        .any(|&n| n.eq_ignore_ascii_case(name))
}

/// Returns `true` if the decorator name marks a surface as intentionally
/// unguarded (`@Public`, `@SkipAuth`).
fn is_auth_weakening_decorator(name: &str) -> bool {
    AUTH_WEAKENING_DECORATORS
        .iter()
        .any(|&n| n.eq_ignore_ascii_case(name))
}

/// A decorated surface and the decorators attached to it in one snapshot.
struct DecoratedSurface {
    surface: NodeData,
    /// Original-cased names of the decorators on this surface.
    decorators: Vec<String>,
}

/// Flag auth-decorator changes on surfaces that survive baseline→review:
///
/// * an **enforcement** decorator (`@Roles`, `@UseGuards`, …) present in
///   baseline but absent in review → guard dropped (H2-1);
/// * an **auth-weakening** decorator (`@Public`, `@SkipAuth`) absent in baseline
///   but present in review → surface opened up (H2-2 inverse).
///
/// Surfaces are paired with a rename-tolerant key (`surface_match_key`), so a
/// rename that also drops a guard is still caught — node ids change on rename
/// and would defeat an id-based survival gate. A dropped enforcement decorator
/// is suppressed when the surviving surface *or its enclosing class* still
/// carries any enforcement decorator in review (H2-3: a hoist to a class-level
/// guard is not a regression).
fn auth_decorator_change_risks<S: GraphStore>(
    baseline: &S,
    review: &S,
) -> Result<Vec<RemovedSurfaceRisk>> {
    let baseline_surfaces = decorated_surfaces(baseline)?;
    let review_surfaces = decorated_surfaces(review)?;

    // Index *all* non-virtual Function/Class nodes in each snapshot by their
    // rename-tolerant match key — a surface that lost (or gained) its only
    // decorator must still be matchable, so the index cannot be restricted to
    // decorated nodes. A key with a single node pairs unambiguously; ambiguous
    // keys (>1 node) are dropped so a rename never pairs the wrong surface.
    let review_by_key = surface_index(review)?;
    let baseline_by_key = surface_index(baseline)?;

    let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();

    // H2-1 / H2-3 — enforcement decorator dropped from a surviving surface.
    for base in &baseline_surfaces {
        let Some(review_surface) = single_match(&review_by_key, &base.surface) else {
            // No (or ambiguous) surviving counterpart → fully-removed surface,
            // covered by the route/symbol removal passes.
            continue;
        };

        let review_decorators = auth_decorator_names_on(review, review_surface.id)?;
        let review_enforcement: Vec<&String> = review_decorators
            .iter()
            .filter(|d| is_enforcement_decorator(d.as_str()))
            .collect();
        // Enclosing-class enforcement decorators in review (H2-3 hoist).
        let class_has_enforcement =
            enclosing_class_has_enforcement_decorator(review, review_surface.id)?;

        for dec in &base.decorators {
            if !is_enforcement_decorator(dec) {
                continue;
            }
            if review_enforcement
                .iter()
                .any(|d| d.eq_ignore_ascii_case(dec))
            {
                continue;
            }
            // Hoist suppression: the surface (any enforcement decorator) or its
            // enclosing class still guards it in review → not a regression.
            if !review_enforcement.is_empty() || class_has_enforcement {
                continue;
            }
            risks.push(auth_decorator_risk(
                review_surface,
                &format!("@{dec} removed from surviving surface"),
                &format!("@{dec}"),
            ));
        }
    }

    // H2-2 inverse — auth-weakening decorator added to a surviving surface.
    // Driven from review so a surface that had *no* auth decorator in baseline
    // but gained `@Public` / `@SkipAuth` is caught.
    for rev in &review_surfaces {
        let Some(base_surface) = single_match(&baseline_by_key, &rev.surface) else {
            // No (or ambiguous) baseline counterpart → newly-added surface, not
            // a weakening of an existing one.
            continue;
        };
        let base_weakening: Vec<String> = auth_decorator_names_on(baseline, base_surface.id)?
            .into_iter()
            .filter(|d| is_auth_weakening_decorator(d))
            .collect();
        for dec in &rev.decorators {
            if !is_auth_weakening_decorator(dec) {
                continue;
            }
            if base_weakening.iter().any(|d| d.eq_ignore_ascii_case(dec)) {
                continue;
            }
            risks.push(auth_decorator_risk(
                &rev.surface,
                &format!("@{dec} added to surviving surface"),
                &format!("@{dec}"),
            ));
        }
    }

    risks.sort_by(|a, b| a.identity.cmp(&b.identity));

    Ok(risks)
}

/// Index every non-virtual `Function` / `Class` node in `store` by its
/// rename-tolerant [`surface_match_key`].
fn surface_index<S: GraphStore>(store: &S) -> Result<FxHashMap<String, Vec<NodeData>>> {
    let mut by_key: FxHashMap<String, Vec<NodeData>> = FxHashMap::default();
    for kind in [NodeKind::Function, NodeKind::Class] {
        for node in store.nodes_by_type(kind)? {
            if node.is_virtual {
                continue;
            }
            by_key
                .entry(surface_match_key(&node))
                .or_default()
                .push(node);
        }
    }
    Ok(by_key)
}

/// Return the single surface in `index` matching `surface`'s key, or `None` when
/// the key is missing or ambiguous (>1 node).
fn single_match<'a>(
    index: &'a FxHashMap<String, Vec<NodeData>>,
    surface: &NodeData,
) -> Option<&'a NodeData> {
    match index.get(&surface_match_key(surface)).map(Vec::as_slice) {
        Some([only]) => Some(only),
        _ => None,
    }
}

/// Build a `security_decorator` risk for a changed auth decorator on `surface`.
/// `detail_prefix` describes the change (e.g. `"@Roles removed from surviving
/// surface"`); `identity_prefix` is the decorator label (e.g. `"@Roles"`).
fn auth_decorator_risk(
    surface: &NodeData,
    detail_prefix: &str,
    identity_prefix: &str,
) -> RemovedSurfaceRisk {
    let surface_qn = surface
        .qualified_name
        .clone()
        .unwrap_or_else(|| surface.name.clone());
    let surface_file = Some(surface.file_path.clone()).filter(|s| !s.is_empty());
    let detail = match &surface_file {
        Some(file) => format!("{detail_prefix} {surface_qn} ({file})"),
        None => format!("{detail_prefix} {surface_qn}"),
    };
    RemovedSurfaceRisk {
        kind: "security_decorator".to_owned(),
        identity: format!("{identity_prefix}::{surface_qn}"),
        repo: (surface.repo != "__virtual__").then(|| surface.repo.clone()),
        surviving_consumers: Vec::new(),
        severity: RiskSeverity::High,
        detail: Some(detail),
    }
}

/// Rename-tolerant key for pairing a surface across snapshots. Keyed on
/// `(repo, file_path, kind)` — *not* the name — so a renamed handler in the
/// same file still pairs, as long as it is the only decorated surface of its
/// kind in that file (ambiguous keys are dropped by the caller). Prefers the
/// stable `external_id` when present.
fn surface_match_key(surface: &NodeData) -> String {
    surface.external_id.clone().unwrap_or_else(|| {
        format!(
            "{}\0{}\0{:?}",
            surface.repo, surface.file_path, surface.kind
        )
    })
}

/// Enumerate every surface carrying at least one enforcement or auth-weakening
/// decorator in `store`, resolved via outgoing `UsesDecorator` edges
/// (surface → decorator).
fn decorated_surfaces<S: GraphStore>(store: &S) -> Result<Vec<DecoratedSurface>> {
    let mut surfaces: Vec<DecoratedSurface> = Vec::new();

    for kind in [NodeKind::Function, NodeKind::Class] {
        for surface in store.nodes_by_type(kind)? {
            if surface.is_virtual {
                continue;
            }
            let decorators = auth_decorator_names_on(store, surface.id)?;
            if decorators.is_empty() {
                continue;
            }
            surfaces.push(DecoratedSurface {
                surface,
                decorators,
            });
        }
    }

    Ok(surfaces)
}

/// Names of the enforcement / auth-weakening decorators attached to `surface_id`
/// via outgoing `UsesDecorator` edges (surface → decorator). Non-auth decorators
/// (`@ApiTags`, …) are filtered out.
fn auth_decorator_names_on<S: GraphStore>(store: &S, surface_id: NodeId) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for edge in store.get_outgoing(surface_id)? {
        if edge.kind != EdgeKind::UsesDecorator {
            continue;
        }
        let Some(dec) = store.get_node(edge.target)? else {
            continue;
        };
        if is_enforcement_decorator(&dec.name) || is_auth_weakening_decorator(&dec.name) {
            names.push(dec.name);
        }
    }
    Ok(names)
}

/// `true` if the enclosing class of `surface_id` (reached via the incoming
/// `Defines` edge from a `Class` node) carries any enforcement decorator in
/// `store`. Used to suppress the per-method risk when a guard was hoisted to a
/// still-effective class-level decorator (H2-3).
fn enclosing_class_has_enforcement_decorator<S: GraphStore>(
    store: &S,
    surface_id: NodeId,
) -> Result<bool> {
    for edge in store.get_incoming(surface_id)? {
        if edge.kind != EdgeKind::Defines {
            continue;
        }
        let Some(parent) = store.get_node(edge.source)? else {
            continue;
        };
        if parent.kind != NodeKind::Class {
            continue;
        }
        let class_decorators = auth_decorator_names_on(store, parent.id)?;
        if class_decorators.iter().any(|d| is_enforcement_decorator(d)) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Flag every removed real symbol that a *different repo* still consumes via a
/// direct `Calls` / `References` edge.
///
/// A symbol node is "removed" when its ID exists in `baseline` but not in
/// `review`. Virtual stubs are skipped (their removals are covered by the route
/// / event / shared-symbol passes), as are symbols already reported by the
/// shared-symbol removal pass (`symbols_removed`, matched on `qualified_name`
/// alone — the stub's `repo` is `"__virtual__"`, not the defining repo). A risk
/// is only emitted when at least one *surviving cross-repo* consumer remains —
/// same-repo consumers and consumers via virtual stubs do not qualify.
fn removed_symbol_cross_repo_consumer_risks<S: GraphStore>(
    baseline: &S,
    review: &S,
    symbols_removed: &[SymbolDelta],
) -> Result<Vec<RemovedSurfaceRisk>> {
    let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();

    for &kind in DIRECT_SYMBOL_KINDS {
        for node in baseline.nodes_by_type(kind)? {
            // Virtual nodes are handled by the route/event/shared-symbol passes.
            if node.is_virtual {
                continue;
            }
            // Removed = present in baseline, absent in review.
            if review.get_node(node.id)?.is_some() {
                continue;
            }
            // FP gate: the shared-symbol pass already covers these. De-dup on
            // `qualified_name` alone — a removed shared symbol's `SymbolDelta`
            // carries `repo == "__virtual__"` (the stub's repo), not the real
            // defining repo, so a `repo`-qualified match would miss it and the
            // same removal would be reported by both passes (duplicate High).
            let qn = node.qualified_name.as_deref().unwrap_or(&node.name);
            if symbols_removed.iter().any(|s| s.qualified_name == qn) {
                continue;
            }

            // Direct consumers (Calls / References) that still exist in review.
            let consumers =
                surviving_consumers(baseline, review, node.id, DIRECT_CONSUMER_EDGE_KINDS)?;
            // Keep only direct cross-repo consumers: a different *real* repo.
            // Same-repo callers are not the blind spot; virtual-stub sources
            // (`repo == "__virtual__"`) are covered by the route/event passes.
            let cross_repo: Vec<RemovedSurfaceConsumer> = consumers
                .into_iter()
                .filter(|c| c.repo != node.repo && c.repo != "__virtual__")
                .collect();
            if cross_repo.is_empty() {
                continue;
            }

            let severity = severity_for_consumers(&cross_repo, Some(&node.repo), true);
            risks.push(RemovedSurfaceRisk {
                kind: "cross_repo_consumer".to_owned(),
                identity: qn.to_owned(),
                repo: Some(node.repo.clone()),
                surviving_consumers: cross_repo,
                severity,
                detail: None,
            });
        }
    }

    risks.sort_by(|a, b| a.identity.cmp(&b.identity));

    Ok(risks)
}

/// Map event kind string back to `NodeKind`.
fn event_kind_to_node_kind(kind: &str) -> NodeKind {
    match kind {
        "topic" => NodeKind::Topic,
        "queue" => NodeKind::Queue,
        "subject" => NodeKind::Subject,
        "stream" => NodeKind::Stream,
        _ => NodeKind::Event,
    }
}

/// Human-readable name for an `EdgeKind`.
fn edge_kind_name(kind: EdgeKind) -> String {
    match kind {
        EdgeKind::Calls => "Calls".to_owned(),
        EdgeKind::References => "References".to_owned(),
        EdgeKind::ConsumesApiFrom => "ConsumesApiFrom".to_owned(),
        EdgeKind::UsesShared => "UsesShared".to_owned(),
        EdgeKind::UsesTypeFrom => "UsesTypeFrom".to_owned(),
        EdgeKind::ImplementsContractFrom => "ImplementsContractFrom".to_owned(),
        EdgeKind::Consumes => "Consumes".to_owned(),
        EdgeKind::UsesEventFrom => "UsesEventFrom".to_owned(),
        other => format!("{other:?}"),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use crate::pr_review::delta_report::{EventDelta, RiskSeverity, RouteDelta, SymbolDelta};

    use super::extract_removed_surface_risks;

    // ── temp helpers ──────────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-risk-extractor-{label}-{}-{id}.redb",
                std::process::id()
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn open_store(label: &str) -> (TempDb, GraphStoreDb) {
        let tmp = TempDb::new(label);
        let db = GraphStoreDb::open(tmp.path()).expect("store should open");
        (tmp, db)
    }

    // ── graph-building helpers ────────────────────────────────────────────────

    fn file_node(repo: &str, path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, path, NodeKind::File, path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: path.to_owned(),
            name: path.to_owned(),
            qualified_name: Some(format!("{repo}::{path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    fn function_node(repo: &str, file: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 5,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
            ai_role: None,
        }
    }

    fn route_virtual_node(method: &str, path: &str) -> NodeData {
        let qn = format!("__route__{method}__{path}");
        NodeData {
            id: node_id("__virtual__", &qn, NodeKind::Route, &qn),
            kind: NodeKind::Route,
            repo: "__virtual__".to_owned(),
            file_path: qn.clone(),
            name: format!("{method} {path}"),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        }
    }

    fn shared_symbol_node(repo: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, name, NodeKind::SharedSymbol, name),
            kind: NodeKind::SharedSymbol,
            repo: repo.to_owned(),
            file_path: name.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(name.to_owned()),
            external_id: Some(format!("__shared__{repo}__{name}")),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        }
    }

    fn topic_virtual_node(topic_name: &str) -> NodeData {
        let external_id = format!("__topic__kafka__{topic_name}");
        NodeData {
            id: node_id("__virtual__", &external_id, NodeKind::Topic, &external_id),
            kind: NodeKind::Topic,
            repo: "__virtual__".to_owned(),
            file_path: external_id.clone(),
            name: topic_name.to_owned(),
            qualified_name: Some(external_id.clone()),
            external_id: Some(external_id),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        }
    }

    fn edge(source: &NodeData, target: &NodeData, kind: EdgeKind, owner: &NodeData) -> EdgeData {
        EdgeData {
            source: source.id,
            target: target.id,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: owner.id,
            is_cross_file: true,
        }
    }

    fn decorator_node(repo: &str, file: &str, name: &str) -> NodeData {
        let qn = format!("{repo}::{name}");
        NodeData {
            id: node_id(repo, file, NodeKind::Decorator, &qn),
            kind: NodeKind::Decorator,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 3,
                line_len: 1,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
            ai_role: None,
        }
    }

    fn class_node(repo: &str, file: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Class, name),
            kind: NodeKind::Class,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 20,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
            ai_role: None,
        }
    }

    fn prompt_virtual_node(key: &str) -> NodeData {
        let qn = format!("__prompt__managed__{key}");
        NodeData {
            id: node_id("__virtual__", &qn, NodeKind::Prompt, &qn),
            kind: NodeKind::Prompt,
            repo: "__virtual__".to_owned(),
            file_path: qn.clone(),
            name: key.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
            ai_role: None,
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// Removed route with a `ConsumesApiFrom` edge from a different repo → High.
    #[test]
    fn removed_route_with_cross_repo_consumer_is_high_risk() {
        let (_td_b, baseline) = open_store("route-high-baseline");
        let (_td_r, review) = open_store("route-high-review");

        let route = route_virtual_node("GET", "/orders");
        let consumer_fn = function_node("frontend", "src/api.ts", "fetchOrders");
        let owner = file_node("frontend", "src/api.ts");
        let consume_edge = edge(&consumer_fn, &route, EdgeKind::ConsumesApiFrom, &owner);

        // Insert into baseline.
        baseline
            .bulk_insert(&[route, consumer_fn.clone(), owner], &[consume_edge])
            .expect("baseline insert");

        // Insert consumer into review (it still exists).
        let review_owner = file_node("frontend", "src/api.ts");
        review
            .bulk_insert(&[consumer_fn.clone(), review_owner], &[])
            .expect("review insert");

        // The route was removed in the PR — simulate by passing it in routes_removed.
        let removed_route = RouteDelta {
            method: "GET".to_owned(),
            path: "/orders".to_owned(),
            repo: Some("api".to_owned()),
            file: None,
            line: None,
            handler_qualified_name: None,
            impact: None,
        };

        let risks = extract_removed_surface_risks(&baseline, &review, &[removed_route], &[], &[])
            .expect("should succeed");

        assert_eq!(risks.len(), 1);
        assert_eq!(risks[0].severity, RiskSeverity::High);
        assert_eq!(risks[0].kind, "route");
        assert!(!risks[0].surviving_consumers.is_empty());
    }

    /// Removed shared symbol with no incoming edges → Low severity.
    #[test]
    fn removed_shared_symbol_with_no_consumers_is_low_risk() {
        let (_td_b, baseline) = open_store("sym-low-baseline");
        let (_td_r, review) = open_store("sym-low-review");

        let sym = shared_symbol_node("shared-lib", "SharedUtil");
        baseline.bulk_insert(&[sym], &[]).expect("baseline insert");

        let removed_symbol = SymbolDelta {
            kind: "shared_symbol".to_owned(),
            repo: "shared-lib".to_owned(),
            qualified_name: "SharedUtil".to_owned(),
            file: None,
            line: None,
            signature: None,
            visibility: None,
            is_virtual: true,
            impact: None,
        };

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[removed_symbol], &[])
            .expect("should succeed");

        assert_eq!(risks.len(), 1);
        assert_eq!(risks[0].severity, RiskSeverity::Low);
        assert!(risks[0].surviving_consumers.is_empty());
    }

    /// Removed event with a cross-repo `Consumes` edge → High.
    #[test]
    fn removed_event_with_surviving_consumer_is_high_risk() {
        let (_td_b, baseline) = open_store("event-high-baseline");
        let (_td_r, review) = open_store("event-high-review");

        let topic = topic_virtual_node("order-paid");
        let consumer_fn = function_node("notifications", "src/notify.ts", "onOrderPaid");
        let owner = file_node("notifications", "src/notify.ts");
        let consume_edge = edge(&consumer_fn, &topic, EdgeKind::Consumes, &owner);

        baseline
            .bulk_insert(&[topic, consumer_fn.clone(), owner], &[consume_edge])
            .expect("baseline insert");

        // Consumer still exists in review.
        let review_owner = file_node("notifications", "src/notify.ts");
        review
            .bulk_insert(&[consumer_fn, review_owner], &[])
            .expect("review insert");

        let removed_event = EventDelta {
            event_kind: "topic".to_owned(),
            event_name: "order-paid".to_owned(),
            external_id: "__topic__kafka__order-paid".to_owned(),
            producers: vec![],
            consumers: vec![],
        };

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[removed_event])
            .expect("should succeed");

        assert_eq!(risks.len(), 1);
        assert_eq!(risks[0].severity, RiskSeverity::High);
        assert_eq!(risks[0].kind, "event");
        assert!(!risks[0].surviving_consumers.is_empty());
    }

    /// A removed managed Prompt still used by a surviving (cross-repo) caller →
    /// flagged as a High-risk removed AI surface.
    #[test]
    fn removed_ai_prompt_with_surviving_consumer_is_flagged() {
        let (_td_b, baseline) = open_store("ai-prompt-baseline");
        let (_td_r, review) = open_store("ai-prompt-review");

        let prompt = prompt_virtual_node("doc-summary");
        let caller_fn = function_node("service-api", "src/usecase.ts", "sendMessage");
        let owner = file_node("service-api", "src/usecase.ts");
        let use_edge = edge(&caller_fn, &prompt, EdgeKind::UsesPrompt, &owner);

        baseline
            .bulk_insert(&[prompt, caller_fn.clone(), owner], &[use_edge])
            .expect("baseline insert");

        // The caller that uses the prompt still exists in review; the prompt is gone.
        let review_owner = file_node("service-api", "src/usecase.ts");
        review
            .bulk_insert(&[caller_fn, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        let prompt_risk = risks
            .iter()
            .find(|r| r.kind == "ai_prompt")
            .expect("an ai_prompt risk must be flagged");
        assert_eq!(prompt_risk.identity, "__prompt__managed__doc-summary");
        assert_eq!(prompt_risk.severity, RiskSeverity::High);
        assert!(
            !prompt_risk.surviving_consumers.is_empty(),
            "the surviving caller must be listed"
        );
    }

    /// Symbol A consumed by Symbol B (cross-repo), both removed → B does not count.
    #[test]
    fn consumer_also_removed_does_not_count_as_surviving() {
        let (_td_b, baseline) = open_store("both-removed-baseline");
        let (_td_r, review) = open_store("both-removed-review");

        let sym_a = shared_symbol_node("shared-lib", "SymbolA");
        let sym_b = function_node("consumer-svc", "src/use.ts", "useSymbolA");
        let owner = file_node("consumer-svc", "src/use.ts");
        let use_edge = edge(&sym_b, &sym_a, EdgeKind::UsesShared, &owner);

        // Both A and B in baseline.
        baseline
            .bulk_insert(&[sym_a, sym_b.clone(), owner], &[use_edge])
            .expect("baseline insert");

        // Review is empty — both were removed.

        let removed_symbol = SymbolDelta {
            kind: "shared_symbol".to_owned(),
            repo: "shared-lib".to_owned(),
            qualified_name: "SymbolA".to_owned(),
            file: None,
            line: None,
            signature: None,
            visibility: None,
            is_virtual: true,
            impact: None,
        };

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[removed_symbol], &[])
            .expect("should succeed");

        // B was also removed, so no surviving consumers.
        assert_eq!(risks.len(), 1);
        assert!(
            risks[0].surviving_consumers.is_empty(),
            "consumer also removed must not appear in surviving list"
        );
        assert_eq!(risks[0].severity, RiskSeverity::Low);
    }

    /// Removing `@Roles` from a route/handler that still exists → one High
    /// `security_decorator` risk.
    #[test]
    fn removed_enforcement_decorator_on_surviving_surface_is_high_risk() {
        let (_td_b, baseline) = open_store("dec-removed-baseline");
        let (_td_r, review) = open_store("dec-removed-review");

        let handler = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let decorator = decorator_node("api", "src/orders.controller.ts", "Roles");
        let owner = file_node("api", "src/orders.controller.ts");
        // target (handler) → UsesDecorator → decorator.
        let uses_edge = edge(&handler, &decorator, EdgeKind::UsesDecorator, &owner);

        baseline
            .bulk_insert(&[handler.clone(), decorator, owner], &[uses_edge])
            .expect("baseline insert");

        // Handler survives in review; the decorator is gone.
        let review_owner = file_node("api", "src/orders.controller.ts");
        review
            .bulk_insert(&[handler, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        let dec_risks: Vec<_> = risks
            .iter()
            .filter(|r| r.kind == "security_decorator")
            .collect();
        assert_eq!(dec_risks.len(), 1, "exactly one security_decorator risk");
        assert_eq!(dec_risks[0].severity, RiskSeverity::High);
        assert_eq!(dec_risks[0].identity, "@Roles::api::OrdersController::list");
        let detail = dec_risks[0].detail.as_deref().expect("detail must be set");
        assert!(detail.contains("@Roles"), "detail names the decorator");
        assert!(
            detail.contains("OrdersController::list"),
            "detail names the surface"
        );
    }

    /// Removing a non-enforcement decorator (`@ApiTags`) → no `security_decorator` risk.
    #[test]
    fn removed_non_enforcement_decorator_produces_no_risk() {
        let (_td_b, baseline) = open_store("dec-nonenf-baseline");
        let (_td_r, review) = open_store("dec-nonenf-review");

        let handler = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let decorator = decorator_node("api", "src/orders.controller.ts", "ApiTags");
        let owner = file_node("api", "src/orders.controller.ts");
        let uses_edge = edge(&handler, &decorator, EdgeKind::UsesDecorator, &owner);

        baseline
            .bulk_insert(&[handler.clone(), decorator, owner], &[uses_edge])
            .expect("baseline insert");

        let review_owner = file_node("api", "src/orders.controller.ts");
        review
            .bulk_insert(&[handler, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        assert!(
            !risks.iter().any(|r| r.kind == "security_decorator"),
            "a non-enforcement decorator must not raise a security_decorator risk"
        );
    }

    /// Removing the whole route (handler + decorator both gone) → no
    /// `security_decorator` risk (covered by route/symbol removal).
    #[test]
    fn removed_decorator_with_removed_surface_is_not_security_flagged() {
        let (_td_b, baseline) = open_store("dec-surface-gone-baseline");
        let (_td_r, review) = open_store("dec-surface-gone-review");

        let handler = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let decorator = decorator_node("api", "src/orders.controller.ts", "Roles");
        let owner = file_node("api", "src/orders.controller.ts");
        let uses_edge = edge(&handler, &decorator, EdgeKind::UsesDecorator, &owner);

        baseline
            .bulk_insert(&[handler, decorator, owner], &[uses_edge])
            .expect("baseline insert");

        // Review is empty — the whole surface (handler + decorator) was removed.

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        assert!(
            !risks.iter().any(|r| r.kind == "security_decorator"),
            "a fully-removed surface must not raise a security_decorator risk"
        );
    }

    /// H2-1: a renamed handler that *also* drops `@Roles` gets a new node id, so
    /// an id-based survival gate would treat it as fully removed and skip it.
    /// The rename-tolerant `(repo, file, kind)` match must still flag the drop.
    #[test]
    fn renamed_handler_with_dropped_guard_is_flagged() {
        let (_td_b, baseline) = open_store("rename-drop-baseline");
        let (_td_r, review) = open_store("rename-drop-review");

        // Baseline: guarded `list` handler.
        let handler = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let decorator = decorator_node("api", "src/orders.controller.ts", "Roles");
        let owner = file_node("api", "src/orders.controller.ts");
        let uses_edge = edge(&handler, &decorator, EdgeKind::UsesDecorator, &owner);
        baseline
            .bulk_insert(&[handler, decorator, owner], &[uses_edge])
            .expect("baseline insert");

        // Review: same file/kind, renamed to `listAll`, guard gone. The renamed
        // handler is the *only* function in the file, so the key is unambiguous.
        let renamed = function_node(
            "api",
            "src/orders.controller.ts",
            "OrdersController::listAll",
        );
        let review_owner = file_node("api", "src/orders.controller.ts");
        review
            .bulk_insert(&[renamed, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        let dec_risks: Vec<_> = risks
            .iter()
            .filter(|r| r.kind == "security_decorator")
            .collect();
        assert_eq!(
            dec_risks.len(),
            1,
            "a rename that drops the guard must still flag one risk"
        );
        assert_eq!(dec_risks[0].severity, RiskSeverity::High);
        // The risk is reported against the surviving (renamed) surface.
        assert_eq!(
            dec_risks[0].identity,
            "@Roles::api::OrdersController::listAll"
        );
    }

    /// H2-2: `@UseGuards` is the primary `NestJS` enforcement decorator; dropping
    /// it from a surviving handler must raise a High `security_decorator` risk.
    #[test]
    fn removed_use_guards_on_surviving_surface_is_high_risk() {
        let (_td_b, baseline) = open_store("useguards-removed-baseline");
        let (_td_r, review) = open_store("useguards-removed-review");

        let handler = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let decorator = decorator_node("api", "src/orders.controller.ts", "UseGuards");
        let owner = file_node("api", "src/orders.controller.ts");
        let uses_edge = edge(&handler, &decorator, EdgeKind::UsesDecorator, &owner);
        baseline
            .bulk_insert(&[handler.clone(), decorator, owner], &[uses_edge])
            .expect("baseline insert");

        let review_owner = file_node("api", "src/orders.controller.ts");
        review
            .bulk_insert(&[handler, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        let dec_risks: Vec<_> = risks
            .iter()
            .filter(|r| r.kind == "security_decorator")
            .collect();
        assert_eq!(dec_risks.len(), 1, "dropping @UseGuards must flag one risk");
        assert_eq!(dec_risks[0].severity, RiskSeverity::High);
        assert_eq!(
            dec_risks[0].identity,
            "@UseGuards::api::OrdersController::list"
        );
    }

    /// H2-2 inverse: *adding* `@Public` to a surviving surface that previously
    /// carried no auth-weakening decorator opens it up → High risk.
    #[test]
    fn added_public_on_surviving_surface_is_high_risk() {
        let (_td_b, baseline) = open_store("public-added-baseline");
        let (_td_r, review) = open_store("public-added-review");

        // Baseline: plain handler, no auth decorators at all.
        let handler = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let owner = file_node("api", "src/orders.controller.ts");
        baseline
            .bulk_insert(&[handler.clone(), owner], &[])
            .expect("baseline insert");

        // Review: same handler now decorated `@Public`.
        let decorator = decorator_node("api", "src/orders.controller.ts", "Public");
        let review_owner = file_node("api", "src/orders.controller.ts");
        let uses_edge = edge(&handler, &decorator, EdgeKind::UsesDecorator, &review_owner);
        review
            .bulk_insert(&[handler, decorator, review_owner], &[uses_edge])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        let dec_risks: Vec<_> = risks
            .iter()
            .filter(|r| r.kind == "security_decorator")
            .collect();
        assert_eq!(dec_risks.len(), 1, "adding @Public must flag one risk");
        assert_eq!(dec_risks[0].severity, RiskSeverity::High);
        assert_eq!(
            dec_risks[0].identity,
            "@Public::api::OrdersController::list"
        );
        let detail = dec_risks[0].detail.as_deref().expect("detail must be set");
        assert!(detail.contains("added"), "detail describes the addition");
    }

    /// H2-3: dropping a method-level `@Roles` after hoisting it to a still-
    /// effective class-level guard is not a regression → no risk.
    #[test]
    fn method_guard_hoisted_to_class_is_not_flagged() {
        let (_td_b, baseline) = open_store("hoist-baseline");
        let (_td_r, review) = open_store("hoist-review");

        // Baseline: method-level @Roles, class is undecorated.
        let class_b = class_node("api", "src/orders.controller.ts", "OrdersController");
        let handler_b = function_node("api", "src/orders.controller.ts", "OrdersController::list");
        let method_dec = decorator_node("api", "src/orders.controller.ts", "Roles");
        let owner_b = file_node("api", "src/orders.controller.ts");
        // class Defines method; method UsesDecorator @Roles.
        let defines_b = edge(&class_b, &handler_b, EdgeKind::Defines, &owner_b);
        let uses_b = edge(&handler_b, &method_dec, EdgeKind::UsesDecorator, &owner_b);
        baseline
            .bulk_insert(
                &[class_b.clone(), handler_b.clone(), method_dec, owner_b],
                &[defines_b, uses_b],
            )
            .expect("baseline insert");

        // Review: method-level @Roles gone; hoisted to a class-level @Roles.
        let class_dec = decorator_node("api", "src/orders.controller.ts", "Roles");
        let owner_r = file_node("api", "src/orders.controller.ts");
        let defines_r = edge(&class_b, &handler_b, EdgeKind::Defines, &owner_r);
        let class_uses = edge(&class_b, &class_dec, EdgeKind::UsesDecorator, &owner_r);
        review
            .bulk_insert(
                &[class_b, handler_b, class_dec, owner_r],
                &[defines_r, class_uses],
            )
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        assert!(
            !risks.iter().any(|r| r.kind == "security_decorator"),
            "a guard hoisted to a still-effective class-level decorator is not a regression"
        );
    }

    /// H5-3: a removed shared symbol whose `SymbolDelta.repo` is `"__virtual__"`
    /// (not the real defining repo) must be reported by exactly ONE pass — the
    /// shared-symbol pass — not also by the direct cross-repo-consumer pass.
    #[test]
    fn shared_symbol_removal_is_not_double_reported() {
        let (_td_b, baseline) = open_store("dedup-baseline");
        let (_td_r, review) = open_store("dedup-review");

        // A real Function in `service-a` plus its SharedSymbol stub. In
        // production both carry the *same* `qualified_name`; build the real node
        // inline so its qn is exactly `OrderContract` (not repo-prefixed).
        let real = NodeData {
            id: node_id(
                "service-a",
                "src/contract.ts",
                NodeKind::Function,
                "OrderContract",
            ),
            kind: NodeKind::Function,
            repo: "service-a".to_owned(),
            file_path: "src/contract.ts".to_owned(),
            name: "OrderContract".to_owned(),
            qualified_name: Some("OrderContract".to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 3,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
            ai_role: None,
        };
        let stub = shared_symbol_node("service-a", "OrderContract");
        // Cross-repo consumer survives, reachable both via the stub (UsesShared)
        // and via a direct call to the real symbol (Calls).
        let caller = function_node("service-b", "src/order.ts", "checkout");
        let owner = file_node("service-b", "src/order.ts");
        let shared_edge = edge(&caller, &stub, EdgeKind::UsesShared, &owner);
        let call_edge = edge(&caller, &real, EdgeKind::Calls, &owner);
        baseline
            .bulk_insert(
                &[real, stub, caller.clone(), owner],
                &[shared_edge, call_edge],
            )
            .expect("baseline insert");

        let review_owner = file_node("service-b", "src/order.ts");
        review
            .bulk_insert(&[caller, review_owner], &[])
            .expect("review insert");

        // The delta reports the removed shared symbol with repo "__virtual__" —
        // the stub's repo, NOT the defining repo "service-a".
        let removed_symbol = SymbolDelta {
            kind: "shared_symbol".to_owned(),
            repo: "__virtual__".to_owned(),
            qualified_name: "OrderContract".to_owned(),
            file: None,
            line: None,
            signature: None,
            visibility: None,
            is_virtual: true,
            impact: None,
        };

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[removed_symbol], &[])
            .expect("should succeed");

        // Exactly one risk for OrderContract: the shared_symbol pass. The
        // cross_repo_consumer pass must de-dup on qualified_name alone.
        let order_risks: Vec<_> = risks
            .iter()
            .filter(|r| r.identity == "OrderContract")
            .collect();
        assert_eq!(
            order_risks.len(),
            1,
            "a removed shared symbol must be reported once, not by both passes"
        );
        assert_eq!(order_risks[0].kind, "shared_symbol");
        assert!(
            !risks.iter().any(|r| r.kind == "cross_repo_consumer"),
            "the cross_repo_consumer pass must not re-report the shared symbol"
        );
    }

    /// A removed function with a surviving cross-repo `Calls` consumer →
    /// High `cross_repo_consumer` risk.
    #[test]
    fn removed_symbol_with_cross_repo_caller_is_flagged() {
        let (_td_b, baseline) = open_store("xrepo-call-baseline");
        let (_td_r, review) = open_store("xrepo-call-review");

        let removed = function_node("service-a", "src/util.ts", "computeTotal");
        let caller = function_node("service-b", "src/order.ts", "checkout");
        let owner = file_node("service-b", "src/order.ts");
        let call_edge = edge(&caller, &removed, EdgeKind::Calls, &owner);

        baseline
            .bulk_insert(&[removed, caller.clone(), owner], &[call_edge])
            .expect("baseline insert");

        // The cross-repo caller survives; the called symbol is gone.
        let review_owner = file_node("service-b", "src/order.ts");
        review
            .bulk_insert(&[caller, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| r.kind == "cross_repo_consumer")
            .expect("a cross_repo_consumer risk must be flagged");
        assert_eq!(risk.identity, "service-a::computeTotal");
        assert_eq!(risk.severity, RiskSeverity::High);
        assert_eq!(risk.surviving_consumers.len(), 1);
        assert_eq!(risk.surviving_consumers[0].repo, "service-b");
        assert_eq!(risk.surviving_consumers[0].edge_kind, "Calls");
    }

    /// A removed function consumed only by a same-repo caller → no
    /// `cross_repo_consumer` risk.
    #[test]
    fn removed_symbol_with_only_same_repo_caller_is_not_flagged() {
        let (_td_b, baseline) = open_store("samerepo-call-baseline");
        let (_td_r, review) = open_store("samerepo-call-review");

        let removed = function_node("service-a", "src/util.ts", "computeTotal");
        let caller = function_node("service-a", "src/order.ts", "checkout");
        let owner = file_node("service-a", "src/order.ts");
        let call_edge = edge(&caller, &removed, EdgeKind::Calls, &owner);

        baseline
            .bulk_insert(&[removed, caller.clone(), owner], &[call_edge])
            .expect("baseline insert");

        let review_owner = file_node("service-a", "src/order.ts");
        review
            .bulk_insert(&[caller, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        assert!(
            !risks.iter().any(|r| r.kind == "cross_repo_consumer"),
            "a same-repo-only caller must not raise a cross_repo_consumer risk"
        );
    }

    /// A removed symbol consumed only through a virtual route stub
    /// (`ConsumesApiFrom`, not `Calls`/`References`) → no spurious
    /// `cross_repo_consumer` risk (covered by the route pass instead).
    #[test]
    fn removed_symbol_consumed_via_virtual_stub_is_not_flagged_as_direct() {
        let (_td_b, baseline) = open_store("virtual-stub-baseline");
        let (_td_r, review) = open_store("virtual-stub-review");

        let removed = function_node("service-a", "src/handler.ts", "listOrders");
        // Consumption flows through a virtual route stub, not a direct call.
        let route = route_virtual_node("GET", "/orders");
        let caller = function_node("frontend", "src/api.ts", "fetchOrders");
        let owner = file_node("frontend", "src/api.ts");
        let api_edge = edge(&caller, &route, EdgeKind::ConsumesApiFrom, &owner);

        baseline
            .bulk_insert(&[removed, route, caller.clone(), owner], &[api_edge])
            .expect("baseline insert");

        // The frontend caller survives; the handler symbol is gone.
        let review_owner = file_node("frontend", "src/api.ts");
        review
            .bulk_insert(&[caller, review_owner], &[])
            .expect("review insert");

        let risks = extract_removed_surface_risks(&baseline, &review, &[], &[], &[])
            .expect("should succeed");

        assert!(
            !risks.iter().any(|r| r.kind == "cross_repo_consumer"),
            "virtual-stub consumption must not raise a direct cross_repo_consumer risk"
        );
    }
}
