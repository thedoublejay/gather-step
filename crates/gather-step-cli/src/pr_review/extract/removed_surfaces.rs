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
use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_storage::GraphStore;

use crate::pr_review::delta_report::{
    EventDelta, RemovedSurfaceConsumer, RemovedSurfaceRisk, RiskSeverity, RouteDelta, SymbolDelta,
};

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
                    && matches!(n.kind, NodeKind::Function | NodeKind::Class | NodeKind::Type)
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
        });
    }

    // ── Removed events ────────────────────────────────────────────────────────
    for event in events_removed {
        // Find the baseline event virtual node by external_id.
        let kind = event_kind_to_node_kind(&event.event_kind);
        let nodes = baseline.nodes_by_type(kind)?;
        let event_node = nodes.into_iter().find(|n| {
            n.is_virtual && n.external_id.as_deref() == Some(&event.external_id)
        });

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
        });
    }

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
        };

        let risks = extract_removed_surface_risks(
            &baseline,
            &review,
            &[removed_route],
            &[],
            &[],
        )
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
        baseline
            .bulk_insert(&[sym], &[])
            .expect("baseline insert");

        let removed_symbol = SymbolDelta {
            kind: "shared_symbol".to_owned(),
            repo: "shared-lib".to_owned(),
            qualified_name: "SharedUtil".to_owned(),
            file: None,
            line: None,
            signature: None,
            visibility: None,
            is_virtual: true,
        };

        let risks = extract_removed_surface_risks(
            &baseline,
            &review,
            &[],
            &[removed_symbol],
            &[],
        )
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

        let risks = extract_removed_surface_risks(
            &baseline,
            &review,
            &[],
            &[],
            &[removed_event],
        )
        .expect("should succeed");

        assert_eq!(risks.len(), 1);
        assert_eq!(risks[0].severity, RiskSeverity::High);
        assert_eq!(risks[0].kind, "event");
        assert!(!risks[0].surviving_consumers.is_empty());
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
        };

        let risks = extract_removed_surface_risks(
            &baseline,
            &review,
            &[],
            &[removed_symbol],
            &[],
        )
        .expect("should succeed");

        // B was also removed, so no surviving consumers.
        assert_eq!(risks.len(), 1);
        assert!(
            risks[0].surviving_consumers.is_empty(),
            "consumer also removed must not appear in surviving list"
        );
        assert_eq!(risks[0].severity, RiskSeverity::Low);
    }
}
