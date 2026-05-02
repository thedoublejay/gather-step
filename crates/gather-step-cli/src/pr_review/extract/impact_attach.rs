//! Impact attachment — Phase 3 Tasks 1+2.
//!
//! Given a baseline [`GraphStore`] and a node that existed in the baseline
//! (a removed or changed surface), walks the direct incoming edges (depth = 1)
//! and classifies each consumer as one of:
//!
//! - `read_only` — `ReadsField` or `ConsumesApiFrom` edges.
//! - `write_mutate` — `WritesField`, `BackfillsField`, `DerivesFieldFrom`,
//!   `FiltersOnField`, `IndexesField` edges.
//! - `construct_payload` — `ImplementsContractFrom` or `UsesShared`/`UsesTypeFrom`
//!   where the source's qualified name matches the DTO/Payload/Request/Response pattern.
//! - `unknown` — everything else.
//!
//! BFS is capped at [`IMPACT_CAP`] nodes. When the cap is hit `truncated = true`.
//!
//! # Phase scope
//!
//! Phase 3 uses depth = 1 (direct incoming edges only). Multi-hop transitive
//! impact is a Phase 4 optimisation.

use std::collections::BTreeMap;

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeId};
use gather_step_storage::GraphStore;

use crate::pr_review::delta_report::{ImpactSummary, RepoImpact};

/// Maximum number of consumer nodes visited before truncating.
const IMPACT_CAP: u32 = 200;

/// Return `true` when `qn` looks like a DTO/Payload/Request/Response type.
///
/// The check is case-insensitive and matches any of the four ASCII substrings.
fn matches_dto_pattern(qn: &str) -> bool {
    // Use a lowercased copy so we can do substring search case-insensitively.
    // The pattern is ASCII-only so `make_ascii_lowercase` is correct and avoids
    // Unicode allocation overhead.
    #[expect(
        clippy::disallowed_methods,
        reason = "one-shot owned lowercase needed for multi-substring search; ASCII-only pattern"
    )]
    let lower = qn.to_lowercase();
    lower.contains("dto")
        || lower.contains("payload")
        || lower.contains("request")
        || lower.contains("response")
}

/// Consumer classification label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    ReadOnly,
    WriteMutate,
    ConstructPayload,
    Unknown,
}

/// Classify a single incoming edge by its `EdgeKind` and the source node's
/// `qualified_name`.
///
/// Rules (in priority order):
/// 1. `ReadsField` → `ReadOnly`
/// 2. `WritesField`, `BackfillsField`, `DerivesFieldFrom`, `FiltersOnField`,
///    `IndexesField` → `WriteMutate`
/// 3. `ImplementsContractFrom` → `ConstructPayload`
/// 4. `UsesShared` | `UsesTypeFrom` AND `source_qn` matches DTO pattern
///    → `ConstructPayload`
/// 5. `ConsumesApiFrom` → `ReadOnly` (conservative: calling an API is read)
/// 6. everything else → `Unknown`
pub fn classify(edge_kind: EdgeKind, source_qn: Option<&str>) -> Classification {
    match edge_kind {
        EdgeKind::ReadsField | EdgeKind::ConsumesApiFrom => Classification::ReadOnly,
        EdgeKind::WritesField
        | EdgeKind::BackfillsField
        | EdgeKind::DerivesFieldFrom
        | EdgeKind::FiltersOnField
        | EdgeKind::IndexesField => Classification::WriteMutate,
        EdgeKind::ImplementsContractFrom => Classification::ConstructPayload,
        EdgeKind::UsesShared | EdgeKind::UsesTypeFrom => {
            if source_qn.is_some_and(matches_dto_pattern) {
                Classification::ConstructPayload
            } else {
                Classification::Unknown
            }
        }
        _ => Classification::Unknown,
    }
}

/// Per-repo accumulator during the BFS walk.
#[derive(Default)]
struct RepoAccumulator {
    read_only: u32,
    write_mutate: u32,
    construct_payload: u32,
    unknown: u32,
}

impl RepoAccumulator {
    fn total(&self) -> u32 {
        self.read_only + self.write_mutate + self.construct_payload + self.unknown
    }

    fn increment(&mut self, label: Classification) {
        match label {
            Classification::ReadOnly => self.read_only += 1,
            Classification::WriteMutate => self.write_mutate += 1,
            Classification::ConstructPayload => self.construct_payload += 1,
            Classification::Unknown => self.unknown += 1,
        }
    }
}

/// Build an [`ImpactSummary`] for `node_id` by walking its direct incoming edges
/// in `baseline`.
///
/// `owner_repo` is the repo that owns `node_id`; consumers in the same repo are
/// included (internal consumers exist and are worth reporting).
///
/// BFS is capped at `IMPACT_CAP` nodes. When the cap is exceeded `truncated`
/// is set to `true` and the partial result is returned.
pub fn impact_for_node<S: GraphStore>(
    baseline: &S,
    node_id: NodeId,
    _owner_repo: Option<&str>,
) -> Result<ImpactSummary> {
    let mut by_repo: BTreeMap<String, RepoAccumulator> = BTreeMap::new();
    let mut consumer_count: u32 = 0;
    let mut truncated = false;

    // Depth = 1: only direct incoming edges from `node_id`.
    let mut visited_sources = std::collections::BTreeSet::new();
    let incoming = baseline.get_incoming(node_id)?;

    for edge in incoming {
        // Skip structural edges that do not represent consumption.
        // `Serves` means "this handler implements the route" — the handler is
        // not a consumer of the route; it is the implementation.
        if matches!(edge.kind, EdgeKind::Serves) {
            continue;
        }

        if !visited_sources.insert(edge.source) {
            continue;
        }

        if consumer_count >= IMPACT_CAP {
            truncated = true;
            break;
        }

        // Resolve the source node's qualified_name for DTO pattern matching.
        let source_node = baseline.get_node(edge.source)?;
        let source_qn = source_node
            .as_ref()
            .and_then(|n| n.qualified_name.as_deref());

        // Determine which repo this consumer belongs to.
        let repo = source_node
            .as_ref()
            .map(|n| n.repo.clone())
            .unwrap_or_default();

        let label = classify(edge.kind, source_qn);
        by_repo.entry(repo).or_default().increment(label);
        consumer_count += 1;
    }

    // Build sorted `by_repo` vec: sort by total descending, then repo name ascending.
    let mut by_repo_vec: Vec<RepoImpact> = by_repo
        .into_iter()
        .map(|(repo, acc)| RepoImpact {
            total: acc.total(),
            read_only: acc.read_only,
            write_mutate: acc.write_mutate,
            construct_payload: acc.construct_payload,
            unknown: acc.unknown,
            repo,
        })
        .collect();
    by_repo_vec.sort_by(|a, b| b.total.cmp(&a.total).then(a.repo.cmp(&b.repo)));

    let consumer_repos: Vec<String> = by_repo_vec.iter().map(|r| r.repo.clone()).collect();

    Ok(ImpactSummary {
        consumer_repos,
        consumer_count,
        by_repo: by_repo_vec,
        truncated,
    })
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
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, Visibility, node_id,
        route_qn, virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::{Classification, classify, impact_for_node};
    use crate::pr_review::extract::routes::extract_route_deltas;

    // ── temp-db helpers ───────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-impact-attach-{label}-{}-{id}.redb",
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

    fn symbol_node(repo: &str, file: &str, name: &str, qn: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        }
    }

    fn shared_symbol(repo: &str, file: &str, name: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::SharedSymbol, name),
            kind: NodeKind::SharedSymbol,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: true,
        }
    }

    fn file_node(repo: &str, path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, path, NodeKind::File, path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: path.to_owned(),
            name: path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn edge(source: NodeId, target: NodeId, kind: EdgeKind, owner_file: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: true,
        }
    }

    fn route_node(method: &str, path: &str) -> NodeData {
        let qn = route_qn(method, path);
        virtual_node(
            NodeKind::Route,
            "__virtual__",
            "__virtual__",
            format!("{method} {path}"),
            qn,
        )
    }

    fn handler_node(repo: &str, file: &str, name: &str) -> NodeData {
        symbol_node(repo, file, name, &format!("{repo}::{name}"))
    }

    fn serves_edge(handler: &NodeData, route: &NodeData, owner: &NodeData) -> EdgeData {
        EdgeData {
            source: handler.id,
            target: route.id,
            kind: EdgeKind::Serves,
            metadata: EdgeMetadata::default(),
            owner_file: owner.id,
            is_cross_file: true,
        }
    }

    // ── test 1: consumer counts per repo ─────────────────────────────────────

    /// A symbol node with 3 incoming `UsesShared` edges from 2 repos.
    /// `impact_for_node` must return `consumer_count = 3`, 2 consumer repos,
    /// and correct per-repo totals.
    #[test]
    fn impact_summary_counts_consumers_per_repo() {
        let (_td, store) = open_store("counts");

        let target = shared_symbol("contracts", "src/order.ts", "Order");
        let f1 = file_node("frontend", "src/api.ts");
        let f2 = file_node("frontend", "src/api2.ts");
        let b1 = file_node("backend", "src/svc.ts");
        let c1 = symbol_node(
            "frontend",
            "src/api.ts",
            "fetchOrder",
            "frontend::fetchOrder",
        );
        let c2 = symbol_node(
            "frontend",
            "src/api2.ts",
            "fetchOrders",
            "frontend::fetchOrders",
        );
        let c3 = symbol_node(
            "backend",
            "src/svc.ts",
            "OrderService",
            "backend::OrderService",
        );

        store
            .bulk_insert(
                &[
                    target.clone(),
                    f1.clone(),
                    f2.clone(),
                    b1.clone(),
                    c1.clone(),
                    c2.clone(),
                    c3.clone(),
                ],
                &[
                    edge(c1.id, target.id, EdgeKind::UsesShared, f1.id),
                    edge(c2.id, target.id, EdgeKind::UsesShared, f2.id),
                    edge(c3.id, target.id, EdgeKind::UsesShared, b1.id),
                ],
            )
            .expect("bulk insert");

        let summary = impact_for_node(&store, target.id, Some("contracts")).expect("impact");

        assert_eq!(summary.consumer_count, 3, "3 consumers total");
        assert_eq!(summary.consumer_repos.len(), 2, "2 distinct repos");
        let frontend_row = summary
            .by_repo
            .iter()
            .find(|r| r.repo == "frontend")
            .expect("frontend");
        assert_eq!(frontend_row.total, 2, "frontend has 2 consumers");
        let backend_row = summary
            .by_repo
            .iter()
            .find(|r| r.repo == "backend")
            .expect("backend");
        assert_eq!(backend_row.total, 1, "backend has 1 consumer");
    }

    // ── test 2: ReadsField → ReadOnly ────────────────────────────────────────

    /// `ReadsField` edge is classified as `ReadOnly`.
    #[test]
    fn classify_reads_field_as_read_only() {
        assert_eq!(
            classify(EdgeKind::ReadsField, None),
            Classification::ReadOnly
        );

        let (_td, store) = open_store("reads-field");
        let target = shared_symbol("contracts", "src/order.ts", "OrderField");
        let owner = file_node("consumer-svc", "src/reader.ts");
        let consumer = symbol_node(
            "consumer-svc",
            "src/reader.ts",
            "readField",
            "consumer-svc::readField",
        );

        store
            .bulk_insert(
                &[target.clone(), owner.clone(), consumer.clone()],
                &[edge(consumer.id, target.id, EdgeKind::ReadsField, owner.id)],
            )
            .expect("insert");

        let summary = impact_for_node(&store, target.id, None).expect("impact");
        assert_eq!(summary.consumer_count, 1);
        let row = &summary.by_repo[0];
        assert_eq!(row.read_only, 1, "read_only must be 1");
        assert_eq!(row.write_mutate, 0, "write_mutate must be 0");
        assert_eq!(row.construct_payload, 0, "construct_payload must be 0");
    }

    // ── test 3: DTO constructor → ConstructPayload ────────────────────────────

    /// `UsesShared` edge from a Symbol whose `qualified_name` contains "Dto"
    /// must be classified as `ConstructPayload`.
    #[test]
    fn classify_dto_constructor_as_construct_payload() {
        // Unit-level classify check.
        assert_eq!(
            classify(EdgeKind::UsesShared, Some("orders::CreateOrderDto")),
            Classification::ConstructPayload
        );

        // Graph-level check.
        let (_td, store) = open_store("dto-constructor");
        let target = shared_symbol("contracts", "src/order.ts", "Order");
        let owner = file_node("api", "src/dto/create-order.dto.ts");
        let consumer = symbol_node(
            "api",
            "src/dto/create-order.dto.ts",
            "CreateOrderDto",
            "api::CreateOrderDto",
        );

        store
            .bulk_insert(
                &[target.clone(), owner.clone(), consumer.clone()],
                &[edge(consumer.id, target.id, EdgeKind::UsesShared, owner.id)],
            )
            .expect("insert");

        let summary = impact_for_node(&store, target.id, None).expect("impact");
        assert_eq!(summary.consumer_count, 1);
        let row = &summary.by_repo[0];
        assert_eq!(row.construct_payload, 1, "construct_payload must be 1");
        assert_eq!(row.read_only, 0);
        assert_eq!(row.write_mutate, 0);
    }

    // ── test 4: truncation ────────────────────────────────────────────────────

    /// When more than `IMPACT_CAP` incoming edges exist, `truncated = true`
    /// and `consumer_count` is capped.
    #[test]
    fn impact_truncates_when_cap_reached() {
        use super::IMPACT_CAP;

        let (_td, store) = open_store("truncation");
        let target = shared_symbol("contracts", "src/heavy.ts", "HeavyType");

        // Build IMPACT_CAP + 50 consumers.
        let total_consumers = IMPACT_CAP + 50;
        let mut nodes: Vec<NodeData> = vec![target.clone()];
        let mut edges_vec: Vec<EdgeData> = Vec::new();

        for i in 0..total_consumers {
            let owner = NodeData {
                id: node_id(
                    "consumer-svc",
                    &format!("src/consumer{i}.ts"),
                    NodeKind::File,
                    &format!("src/consumer{i}.ts"),
                ),
                kind: NodeKind::File,
                repo: "consumer-svc".to_owned(),
                file_path: format!("src/consumer{i}.ts"),
                name: format!("src/consumer{i}.ts"),
                qualified_name: None,
                external_id: None,
                signature: None,
                visibility: None,
                span: None,
                is_virtual: false,
            };
            let consumer = NodeData {
                id: node_id(
                    "consumer-svc",
                    &format!("src/consumer{i}.ts"),
                    NodeKind::Function,
                    &format!("fn{i}"),
                ),
                kind: NodeKind::Function,
                repo: "consumer-svc".to_owned(),
                file_path: format!("src/consumer{i}.ts"),
                name: format!("fn{i}"),
                qualified_name: Some(format!("consumer-svc::fn{i}")),
                external_id: None,
                signature: None,
                visibility: Some(Visibility::Public),
                span: None,
                is_virtual: false,
            };
            edges_vec.push(EdgeData {
                source: consumer.id,
                target: target.id,
                kind: EdgeKind::UsesShared,
                metadata: EdgeMetadata::default(),
                owner_file: owner.id,
                is_cross_file: true,
            });
            nodes.push(owner);
            nodes.push(consumer);
        }

        // Insert in batches to avoid the owner-file deduplication issue.
        // All edges share different owner files, so a single bulk_insert works.
        store.bulk_insert(&nodes, &edges_vec).expect("bulk insert");

        let summary = impact_for_node(&store, target.id, None).expect("impact");

        assert!(
            summary.truncated,
            "truncated must be true when cap exceeded"
        );
        assert_eq!(
            summary.consumer_count, IMPACT_CAP,
            "consumer_count must equal the cap"
        );
    }

    // ── test 5: route delta carries impact ────────────────────────────────────

    /// End-to-end: build a baseline with a route consumed cross-repo, extract
    /// route deltas (route is removed in review), then attach impact.  Assert
    /// the removed route gets a non-None impact with the consumer repo present.
    #[test]
    fn route_delta_carries_impact() {
        let (_td_b, baseline) = open_store("route-impact-baseline");
        let (_td_r, review) = open_store("route-impact-review");

        // Baseline: GET /orders route + handler + a cross-repo consumer.
        let route = route_node("GET", "/orders");
        let handler_owner = file_node("api", "src/orders.ts.GET.route");
        let handler = handler_node("api", "src/orders.ts", "listOrders");
        let srv_edge = serves_edge(&handler, &route, &handler_owner);

        // Cross-repo consumer via ConsumesApiFrom.
        let consumer_owner = file_node("frontend", "src/api.ts");
        let consumer = symbol_node(
            "frontend",
            "src/api.ts",
            "fetchOrders",
            "frontend::fetchOrders",
        );
        let consume_edge = edge(
            consumer.id,
            route.id,
            EdgeKind::ConsumesApiFrom,
            consumer_owner.id,
        );

        baseline
            .bulk_insert(
                &[
                    route.clone(),
                    handler_owner,
                    handler,
                    consumer_owner,
                    consumer,
                ],
                &[srv_edge, consume_edge],
            )
            .expect("baseline insert");

        // Review: route is gone (handler removed).
        // (empty review — route not present)

        let mut route_deltas = extract_route_deltas(&baseline, &review).expect("deltas");

        // The route must appear as removed.
        assert_eq!(route_deltas.removed.len(), 1);
        let removed = &mut route_deltas.removed[0];
        assert_eq!(removed.method, "GET");
        assert_eq!(removed.path, "/orders");

        // Attach impact to the removed route.
        let summary = impact_for_node(&baseline, route.id, None).expect("impact");
        removed.impact = Some(summary);

        assert!(removed.impact.is_some(), "impact must be Some");
        let imp = removed.impact.as_ref().unwrap();
        assert!(
            imp.consumer_repos.contains(&"frontend".to_string()),
            "consumer_repos must include 'frontend'; got {:?}",
            imp.consumer_repos
        );
        assert_eq!(imp.consumer_count, 1);
    }
}
