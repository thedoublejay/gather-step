//! Event delta extraction — Phase 2 Task 5.
//!
//! Diffs virtual event nodes (Topic / Queue / Subject / Stream / Event) between
//! a baseline graph and a review graph to produce [`EventDeltas`] (added /
//! removed / changed producer-consumer sets).
//!
//! # Key design decisions
//!
//! - The canonical key for an event is `(event_kind, event_name)`.  `event_name`
//!   is derived from the node's `external_id` (or `qualified_name` / `name`) by
//!   splitting on `__` and taking the last segment, lowercased.  This matches the
//!   `event_name_for_node` logic in `gather-step-analysis/src/event_topology.rs`.
//! - Producer edges: `EdgeKind::Publishes` and `EdgeKind::ProducesEventFor`.
//! - Consumer edges: `EdgeKind::Consumes` and `EdgeKind::UsesEventFrom`.
//! - The extractor is generic over `GraphStore` so it can be unit-tested with a
//!   direct-insertion `GraphStoreDb` without running the indexer.

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeData, NodeKind};
use gather_step_storage::GraphStore;
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{
    EventDelta, EventDeltaChange, EventDeltas, EventEndpointSummary,
};

/// `(event_kind, event_name)` → `EventDelta` mapping built from one snapshot.
type EventMap = FxHashMap<(String, String), EventDelta>;

/// Extract added / removed / changed event deltas by diffing virtual event nodes
/// in `baseline` against those in `review`.
pub fn extract_event_deltas<S: GraphStore>(baseline: &S, review: &S) -> Result<EventDeltas> {
    let baseline_map = build_event_map(baseline)?;
    let review_map = build_event_map(review)?;

    let mut added: Vec<EventDelta> = Vec::new();
    let mut removed: Vec<EventDelta> = Vec::new();
    let mut changed: Vec<EventDeltaChange> = Vec::new();

    // Added: in review but not in baseline.
    for (key, delta) in &review_map {
        if !baseline_map.contains_key(key) {
            added.push(delta.clone());
        }
    }

    // Removed: in baseline but not in review.
    for (key, delta) in &baseline_map {
        if !review_map.contains_key(key) {
            removed.push(delta.clone());
        }
    }

    // Changed: same key in both — diff producer/consumer sets.
    for (key, review_delta) in &review_map {
        if let Some(baseline_delta) = baseline_map.get(key) {
            let producers_added =
                endpoint_diff(&baseline_delta.producers, &review_delta.producers);
            let producers_removed =
                endpoint_diff(&review_delta.producers, &baseline_delta.producers);
            let consumers_added =
                endpoint_diff(&baseline_delta.consumers, &review_delta.consumers);
            let consumers_removed =
                endpoint_diff(&review_delta.consumers, &baseline_delta.consumers);

            let has_changes = !producers_added.is_empty()
                || !producers_removed.is_empty()
                || !consumers_added.is_empty()
                || !consumers_removed.is_empty();

            if has_changes {
                changed.push(EventDeltaChange {
                    event_kind: key.0.clone(),
                    event_name: key.1.clone(),
                    producers_added,
                    producers_removed,
                    consumers_added,
                    consumers_removed,
                });
            }
        }
    }

    // Deterministic output — sort by (event_kind, event_name).
    added.sort_by(|a, b| (&a.event_kind, &a.event_name).cmp(&(&b.event_kind, &b.event_name)));
    removed.sort_by(|a, b| (&a.event_kind, &a.event_name).cmp(&(&b.event_kind, &b.event_name)));
    changed.sort_by(|a, b| (&a.event_kind, &a.event_name).cmp(&(&b.event_kind, &b.event_name)));

    Ok(EventDeltas {
        added,
        removed,
        changed,
    })
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Build `(event_kind, event_name) → EventDelta` for all virtual event nodes in
/// `store`.
fn build_event_map<S: GraphStore>(store: &S) -> Result<EventMap> {
    const EVENT_KINDS: &[NodeKind] = &[
        NodeKind::Topic,
        NodeKind::Queue,
        NodeKind::Subject,
        NodeKind::Stream,
        NodeKind::Event,
    ];

    let mut map = EventMap::default();

    for &kind in EVENT_KINDS {
        let kind_str = node_kind_str(kind);
        for node in store.nodes_by_type(kind)? {
            if !node.is_virtual {
                continue;
            }
            let Some(event_name) = event_name_for_node(&node) else {
                continue;
            };
            let external_id = node
                .external_id
                .clone()
                .unwrap_or_else(|| node.qualified_name.clone().unwrap_or_else(|| node.name.clone()));

            let (producers, consumers) = resolve_endpoints(store, &node)?;

            let key = (kind_str.to_owned(), event_name.clone());
            map.insert(
                key,
                EventDelta {
                    event_kind: kind_str.to_owned(),
                    event_name,
                    external_id,
                    producers,
                    consumers,
                },
            );
        }
    }

    Ok(map)
}

/// Derive the human-readable event name from a node.
///
/// Mirrors `event_name_for_node` in `gather-step-analysis/src/event_topology.rs:507`:
/// split on `__`, take the last segment, lowercase.
fn event_name_for_node(node: &NodeData) -> Option<String> {
    let raw = node
        .external_id
        .as_deref()
        .or(node.qualified_name.as_deref())
        .unwrap_or(&node.name);
    if raw.is_empty() {
        return None;
    }
    let mut normalized = raw
        .rsplit_once("__")
        .map_or(raw, |(_, suffix)| suffix)
        .to_owned();
    normalized.make_ascii_lowercase();
    Some(normalized)
}

/// Map a virtual-event `NodeKind` to its display string.
fn node_kind_str(kind: NodeKind) -> &'static str {
    match kind {
        NodeKind::Topic => "topic",
        NodeKind::Queue => "queue",
        NodeKind::Subject => "subject",
        NodeKind::Stream => "stream",
        NodeKind::Event => "event",
        _ => "unknown",
    }
}

/// Walk incoming edges on the event node and collect producers and consumers.
///
/// - `Publishes` (20) / `ProducesEventFor` (30) → producer
/// - `Consumes` (21) / `UsesEventFrom` (27) → consumer
fn resolve_endpoints<S: GraphStore>(
    store: &S,
    node: &NodeData,
) -> Result<(Vec<EventEndpointSummary>, Vec<EventEndpointSummary>)> {
    let mut producers: Vec<EventEndpointSummary> = Vec::new();
    let mut consumers: Vec<EventEndpointSummary> = Vec::new();

    for edge in store.get_incoming(node.id)? {
        let is_producer = matches!(edge.kind, EdgeKind::Publishes | EdgeKind::ProducesEventFor);
        let is_consumer = matches!(edge.kind, EdgeKind::Consumes | EdgeKind::UsesEventFrom);

        if !is_producer && !is_consumer {
            continue;
        }

        let Some(source) = store.get_node(edge.source)? else {
            continue;
        };

        let summary = node_to_summary(&source);

        if is_producer {
            producers.push(summary);
        } else {
            consumers.push(summary);
        }
    }

    // Sort for determinism.
    producers.sort_by(|a, b| (&a.repo, &a.qualified_name).cmp(&(&b.repo, &b.qualified_name)));
    consumers.sort_by(|a, b| (&a.repo, &a.qualified_name).cmp(&(&b.repo, &b.qualified_name)));

    Ok((producers, consumers))
}

/// Build an `EventEndpointSummary` from a source `NodeData`.
fn node_to_summary(node: &NodeData) -> EventEndpointSummary {
    EventEndpointSummary {
        repo: node.repo.clone(),
        qualified_name: node
            .qualified_name
            .clone()
            .unwrap_or_else(|| node.name.clone()),
        file: Some(node.file_path.clone()).filter(|s| !s.is_empty()),
        line: node.span.as_ref().map(|s| s.line_start),
    }
}

/// Return endpoints in `b` that are NOT in `a` (by `(repo, qualified_name)`).
fn endpoint_diff(
    a: &[EventEndpointSummary],
    b: &[EventEndpointSummary],
) -> Vec<EventEndpointSummary> {
    let a_keys: rustc_hash::FxHashSet<(&str, &str)> = a
        .iter()
        .map(|e| (e.repo.as_str(), e.qualified_name.as_str()))
        .collect();
    b.iter()
        .filter(|e| !a_keys.contains(&(e.repo.as_str(), e.qualified_name.as_str())))
        .cloned()
        .collect()
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

    use super::extract_event_deltas;

    // ── temp helpers ──────────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-event-extractor-{label}-{}-{id}.redb",
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

    fn owner_node(repo: &str, file: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file, NodeKind::File, file),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: file.to_owned(),
            qualified_name: Some(format!("{repo}::{file}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn function_node(repo: &str, file: &str, name: &str, line: u32) -> NodeData {
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
                line_start: line,
                line_len: 5,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
        }
    }

    fn topic_virtual_node(name: &str) -> NodeData {
        let external_id = format!("__topic__kafka__{name}");
        NodeData {
            id: node_id("__virtual__", &external_id, NodeKind::Topic, &external_id),
            kind: NodeKind::Topic,
            repo: "__virtual__".to_owned(),
            file_path: external_id.clone(),
            name: name.to_owned(),
            qualified_name: Some(external_id.clone()),
            external_id: Some(external_id),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    fn publishes_edge(source: &NodeData, target: &NodeData, owner: &NodeData) -> EdgeData {
        EdgeData {
            source: source.id,
            target: target.id,
            kind: EdgeKind::Publishes,
            metadata: EdgeMetadata::default(),
            owner_file: owner.id,
            is_cross_file: true,
        }
    }

    fn consumes_edge(source: &NodeData, target: &NodeData, owner: &NodeData) -> EdgeData {
        EdgeData {
            source: source.id,
            target: target.id,
            kind: EdgeKind::Consumes,
            metadata: EdgeMetadata::default(),
            owner_file: owner.id,
            is_cross_file: true,
        }
    }

    // Helper to insert a topic + a publisher into a store.
    fn insert_topic_with_publisher(
        store: &GraphStoreDb,
        topic_name: &str,
        producer_repo: &str,
        producer_file: &str,
        producer_fn: &str,
    ) -> (NodeData, NodeData) {
        let topic = topic_virtual_node(topic_name);
        let owner = owner_node(producer_repo, producer_file);
        let producer = function_node(producer_repo, producer_file, producer_fn, 10);
        let edge = publishes_edge(&producer, &topic, &owner);
        store
            .bulk_insert(&[topic.clone(), owner, producer.clone()], &[edge])
            .expect("bulk insert should succeed");
        (topic, producer)
    }

    // Helper to add a consumer edge to an existing topic.
    fn insert_consumer_for_topic(
        store: &GraphStoreDb,
        topic: &NodeData,
        consumer_repo: &str,
        consumer_file: &str,
        consumer_fn: &str,
    ) -> NodeData {
        let owner = owner_node(consumer_repo, &format!("{consumer_file}.consumer"));
        let consumer = function_node(consumer_repo, consumer_file, consumer_fn, 20);
        let edge = consumes_edge(&consumer, topic, &owner);
        store
            .bulk_insert(&[owner, consumer.clone()], &[edge])
            .expect("bulk insert should succeed");
        consumer
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// A virtual Topic only in the review index appears in `added`.
    #[test]
    fn event_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("added-baseline");
        let (_td_r, review) = open_store("added-review");

        // Baseline: empty (no topics).
        // Review: one Topic with one publisher.
        insert_topic_with_publisher(&review, "order-created", "api", "src/orders.ts", "emitOrder");

        let deltas = extract_event_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected exactly one added event");
        assert_eq!(deltas.added[0].event_kind, "topic");
        assert_eq!(deltas.added[0].event_name, "order-created");
        assert_eq!(deltas.added[0].producers.len(), 1);
        assert!(deltas.removed.is_empty());
        assert!(deltas.changed.is_empty());
    }

    /// Same topic in both; review adds a new `Consumes` edge → appears in `changed`.
    #[test]
    fn event_consumer_added_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("consumer-added-baseline");
        let (_td_r, review) = open_store("consumer-added-review");

        // Both have the same topic with the same producer.
        insert_topic_with_publisher(
            &baseline,
            "order-created",
            "api",
            "src/orders.ts",
            "emitOrder",
        );
        let (topic_r, _) = insert_topic_with_publisher(
            &review,
            "order-created",
            "api",
            "src/orders.ts",
            "emitOrder",
        );

        // Review adds a consumer from a different repo.
        insert_consumer_for_topic(
            &review,
            &topic_r,
            "notifications",
            "src/notify.ts",
            "onOrderCreated",
        );

        let deltas = extract_event_deltas(&baseline, &review).expect("should succeed");

        assert!(deltas.added.is_empty(), "event must not appear in added");
        assert!(deltas.removed.is_empty());
        assert_eq!(deltas.changed.len(), 1, "expected exactly one changed event");
        let change = &deltas.changed[0];
        assert_eq!(change.event_name, "order-created");
        assert_eq!(change.consumers_added.len(), 1);
        assert_eq!(change.consumers_added[0].repo, "notifications");
        assert!(change.consumers_removed.is_empty());
        assert!(change.producers_added.is_empty());
        assert!(change.producers_removed.is_empty());
    }

    /// Same topic in both; baseline had a producer that review removed → `changed`.
    #[test]
    fn event_producer_removed_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("producer-removed-baseline");
        let (_td_r, review) = open_store("producer-removed-review");

        // Baseline has two producers.
        let (topic_b, _) = insert_topic_with_publisher(
            &baseline,
            "payment-processed",
            "payments",
            "src/pay.ts",
            "emitPayment",
        );
        insert_consumer_for_topic(
            &baseline,
            &topic_b,
            "legacy",
            "src/legacy.ts",
            "emitLegacy",
        );
        // (legacy consumer acts as a second "publisher" for test purposes;
        //  instead, let's add a real second producer via a raw edge)
        let second_producer = function_node("payments", "src/retry.ts", "retryEmit", 5);
        let owner2 = owner_node("payments", "src/retry.ts.extra");
        let edge2 = publishes_edge(&second_producer, &topic_b, &owner2);
        baseline
            .bulk_insert(&[second_producer.clone(), owner2], &[edge2])
            .expect("insert");

        // Review has only ONE producer (the second was removed).
        insert_topic_with_publisher(
            &review,
            "payment-processed",
            "payments",
            "src/pay.ts",
            "emitPayment",
        );

        let deltas = extract_event_deltas(&baseline, &review).expect("should succeed");

        assert!(deltas.added.is_empty());
        assert!(deltas.removed.is_empty());
        assert_eq!(deltas.changed.len(), 1);
        let change = &deltas.changed[0];
        assert_eq!(change.event_name, "payment-processed");
        assert!(!change.producers_removed.is_empty(), "a producer must be removed");
    }

    /// Same topic with identical producer/consumer sets → not in any list.
    #[test]
    fn event_with_no_diff_is_omitted() {
        let (_td_b, baseline) = open_store("no-diff-baseline");
        let (_td_r, review) = open_store("no-diff-review");

        insert_topic_with_publisher(
            &baseline,
            "user-signed-up",
            "auth",
            "src/auth.ts",
            "emitSignup",
        );
        insert_topic_with_publisher(
            &review,
            "user-signed-up",
            "auth",
            "src/auth.ts",
            "emitSignup",
        );

        let deltas = extract_event_deltas(&baseline, &review).expect("should succeed");

        assert!(deltas.added.is_empty(), "must not appear in added");
        assert!(deltas.removed.is_empty(), "must not appear in removed");
        assert!(deltas.changed.is_empty(), "must not appear in changed");
    }
}
