//! Payload-contract delta extraction — Phase 2 Task 4.
//!
//! Diffs payload contracts between a baseline metadata store and a review
//! metadata store to produce [`PayloadContractDeltas`].
//!
//! # Diff key
//!
//! `(repo, file_path, contract_target_qualified_name, side)` — per the
//! data-model map.  Contracts without a `contract_target_qualified_name` are
//! skipped (they have no stable identity for PR-review purposes).
//!
//! # Field comparison
//!
//! Fields are compared by name within each contract.  Per-field differences
//! recorded in [`PayloadContractDeltaChange`]:
//!
//! - `fields_added` / `fields_removed` — field present in one snapshot only.
//! - `fields_optional_to_required` / `fields_required_to_optional` — same name,
//!   `optional` flag flipped.
//! - `fields_type_changed` — same name, different `type_name`.

use anyhow::Result;
use gather_step_core::{NodeId, PayloadSide};
use gather_step_storage::{MetadataStore, PayloadContractQuery, PayloadContractStoreRecord};
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{
    PayloadContractDelta, PayloadContractDeltaChange, PayloadContractDeltas, PayloadFieldSummary,
    PayloadFieldTypeChange,
};

/// `(repo, file_path, target_qualified_name, side)` diff key.
type ContractKey = (String, String, String, String);

/// Map from diff key → contract record.
type ContractMap = FxHashMap<ContractKey, PayloadContractStoreRecord>;

/// Extract added / removed / changed payload contracts by diffing the metadata
/// stores `baseline` and `review`.
///
/// If `baseline` has no records every review contract is reported as `added`.
pub fn extract_payload_contract_deltas<M: MetadataStore>(
    baseline: &M,
    review: &M,
) -> Result<PayloadContractDeltas> {
    let baseline_map = build_contract_map(baseline)?;
    let review_map = build_contract_map(review)?;

    let mut added: Vec<PayloadContractDelta> = Vec::new();
    let mut removed: Vec<PayloadContractDelta> = Vec::new();
    let mut changed: Vec<PayloadContractDeltaChange> = Vec::new();

    // Added: in review but not in baseline.
    for (key, record) in &review_map {
        if !baseline_map.contains_key(key) {
            added.push(record_to_delta(record));
        }
    }

    // Removed: in baseline but not in review.
    for (key, record) in &baseline_map {
        if !review_map.contains_key(key) {
            removed.push(record_to_delta(record));
        }
    }

    // Changed: same key in both — diff field sets.
    for (key, review_record) in &review_map {
        if let Some(baseline_record) = baseline_map.get(key)
            && let Some(change) = diff_fields(key, baseline_record, review_record)
        {
            changed.push(change);
        }
    }

    // Sort for deterministic output. Uses `sort_by` with `as_str()` borrows
    // rather than `sort_by_key` with cloned `String`s — `sort_by_key` would
    // call its closure O(n log n) times, allocating four owned strings per
    // comparison.
    added.sort_by(cmp_delta);
    removed.sort_by(cmp_delta);
    changed.sort_by(cmp_change);

    Ok(PayloadContractDeltas {
        added,
        removed,
        changed,
        unavailable: false,
    })
}

fn cmp_delta(a: &PayloadContractDelta, b: &PayloadContractDelta) -> std::cmp::Ordering {
    (
        a.repo.as_str(),
        a.file.as_str(),
        a.target_qualified_name.as_str(),
        a.side.as_str(),
    )
        .cmp(&(
            b.repo.as_str(),
            b.file.as_str(),
            b.target_qualified_name.as_str(),
            b.side.as_str(),
        ))
}

fn cmp_change(
    a: &PayloadContractDeltaChange,
    b: &PayloadContractDeltaChange,
) -> std::cmp::Ordering {
    (
        a.repo.as_str(),
        a.file.as_str(),
        a.target_qualified_name.as_str(),
        a.side.as_str(),
    )
        .cmp(&(
            b.repo.as_str(),
            b.file.as_str(),
            b.target_qualified_name.as_str(),
            b.side.as_str(),
        ))
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Fetch all payload contracts from the store and index them by diff key.
///
/// Contracts without a `contract_target_qualified_name` are skipped because
/// they have no stable identity.
fn build_contract_map<M: MetadataStore>(store: &M) -> Result<ContractMap> {
    let records = store.payload_contracts_for_query(PayloadContractQuery::default())?;
    let mut map = ContractMap::default();
    for record in records {
        let Some(qn) = record.record.contract_target_qualified_name.clone() else {
            continue;
        };
        let side = side_str(record.record.side);
        let key = (
            record.record.repo.clone(),
            record.record.file_path.clone(),
            qn,
            side.to_owned(),
        );
        // Last record for this key wins (deduplication).
        map.insert(key, record);
    }
    Ok(map)
}

fn side_str(side: PayloadSide) -> &'static str {
    match side {
        PayloadSide::Producer => "producer",
        PayloadSide::Consumer => "consumer",
    }
}

fn record_to_delta(record: &PayloadContractStoreRecord) -> PayloadContractDelta {
    let fields = record
        .record
        .contract
        .fields
        .iter()
        .map(|f| PayloadFieldSummary {
            name: f.name.clone(),
            type_name: if f.type_name.is_empty() {
                None
            } else {
                Some(f.type_name.clone())
            },
            optional: f.optional,
        })
        .collect();

    PayloadContractDelta {
        repo: record.record.repo.clone(),
        file: record.record.file_path.clone(),
        target_qualified_name: record
            .record
            .contract_target_qualified_name
            .clone()
            .unwrap_or_default(),
        side: side_str(record.record.side).to_owned(),
        fields,
        impact: None,
    }
}

/// Compare field sets between baseline and review for the same contract key.
/// Returns `None` when the field sets are identical.
fn diff_fields(
    key: &ContractKey,
    baseline: &PayloadContractStoreRecord,
    review: &PayloadContractStoreRecord,
) -> Option<PayloadContractDeltaChange> {
    // Build name → field maps.
    let baseline_fields: FxHashMap<&str, &gather_step_core::PayloadField> = baseline
        .record
        .contract
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();
    let review_fields: FxHashMap<&str, &gather_step_core::PayloadField> = review
        .record
        .contract
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    let mut fields_added: Vec<PayloadFieldSummary> = Vec::new();
    let mut fields_removed: Vec<PayloadFieldSummary> = Vec::new();
    let mut fields_optional_to_required: Vec<String> = Vec::new();
    let mut fields_required_to_optional: Vec<String> = Vec::new();
    let mut fields_type_changed: Vec<PayloadFieldTypeChange> = Vec::new();

    // Fields in review only → added.
    for (name, field) in &review_fields {
        if !baseline_fields.contains_key(name) {
            fields_added.push(PayloadFieldSummary {
                name: (*name).to_owned(),
                type_name: if field.type_name.is_empty() {
                    None
                } else {
                    Some(field.type_name.clone())
                },
                optional: field.optional,
            });
        }
    }

    // Fields in baseline only → removed.
    for (name, field) in &baseline_fields {
        if !review_fields.contains_key(name) {
            fields_removed.push(PayloadFieldSummary {
                name: (*name).to_owned(),
                type_name: if field.type_name.is_empty() {
                    None
                } else {
                    Some(field.type_name.clone())
                },
                optional: field.optional,
            });
        }
    }

    // Fields in both — check optionality and type changes.
    for (name, review_field) in &review_fields {
        if let Some(baseline_field) = baseline_fields.get(name) {
            if baseline_field.optional && !review_field.optional {
                fields_optional_to_required.push((*name).to_owned());
            } else if !baseline_field.optional && review_field.optional {
                fields_required_to_optional.push((*name).to_owned());
            }
            if baseline_field.type_name != review_field.type_name {
                let before = if baseline_field.type_name.is_empty() {
                    None
                } else {
                    Some(baseline_field.type_name.clone())
                };
                let after = if review_field.type_name.is_empty() {
                    None
                } else {
                    Some(review_field.type_name.clone())
                };
                fields_type_changed.push(PayloadFieldTypeChange {
                    name: (*name).to_owned(),
                    before_type: before,
                    after_type: after,
                });
            }
        }
    }

    // No diffs → not a changed contract.
    if fields_added.is_empty()
        && fields_removed.is_empty()
        && fields_optional_to_required.is_empty()
        && fields_required_to_optional.is_empty()
        && fields_type_changed.is_empty()
    {
        return None;
    }

    // Sort sub-lists for determinism.
    fields_added.sort_by(|a, b| a.name.cmp(&b.name));
    fields_removed.sort_by(|a, b| a.name.cmp(&b.name));
    fields_optional_to_required.sort();
    fields_required_to_optional.sort();
    fields_type_changed.sort_by(|a, b| a.name.cmp(&b.name));

    Some(PayloadContractDeltaChange {
        repo: key.0.clone(),
        file: key.1.clone(),
        target_qualified_name: key.2.clone(),
        side: key.3.clone(),
        fields_added,
        fields_removed,
        fields_optional_to_required,
        fields_required_to_optional,
        fields_type_changed,
        impact: None,
    })
}

/// Look up the `NodeId` of a payload-contract virtual node in a baseline
/// metadata store by its diff key `(repo, file_path, target_qualified_name, side)`.
///
/// Uses [`MetadataStore::payload_contracts_for_query`] with `repo`,
/// `contract_target_qualified_name`, and `side` filters so no external-id
/// parsing is needed.  Returns the first matching record's
/// `payload_contract_node_id`, or `None` when no record matches.
///
/// If the metadata query fails the error is propagated; callers should log and
/// continue rather than abort (the `Option<ImpactSummary>` shape supports
/// missing lookups).
pub fn find_payload_contract_node_id<M: MetadataStore>(
    store: &M,
    repo: &str,
    file_path: &str,
    target_qualified_name: &str,
    side: &str,
) -> Result<Option<NodeId>> {
    let side_filter = match side {
        "producer" => Some(PayloadSide::Producer),
        "consumer" => Some(PayloadSide::Consumer),
        _ => None,
    };

    let records = store.payload_contracts_for_query(PayloadContractQuery {
        repo: Some(repo.to_owned()),
        contract_target_qualified_name: Some(target_qualified_name.to_owned()),
        side: side_filter,
        ..PayloadContractQuery::default()
    })?;

    // Filter by file_path (not a query field) and return the first match.
    Ok(records
        .into_iter()
        .find(|r| r.record.file_path == file_path)
        .map(|r| r.record.payload_contract_node_id))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        NodeKind, PayloadContractDoc, PayloadContractRecord, PayloadField, PayloadInferenceKind,
        PayloadSide, ref_node_id,
    };
    use gather_step_storage::{MetadataStore, MetadataStoreDb, PayloadContractStoreRecord};

    use super::extract_payload_contract_deltas;

    // ── temp helpers ──────────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-pc-extractor-{label}-{}-{id}.sqlite3",
                std::process::id()
            ));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = fs::remove_file(format!("{}{suffix}", self.path.display()));
            }
        }
    }

    fn open_store(label: &str) -> (TempDb, MetadataStoreDb) {
        let tmp = TempDb::new(label);
        let db = MetadataStoreDb::open(&tmp.path).expect("store should open");
        (tmp, db)
    }

    // ── contract builders ─────────────────────────────────────────────────────

    fn make_contract(
        repo: &str,
        file: &str,
        target_qn: &str,
        side: PayloadSide,
        fields: Vec<PayloadField>,
    ) -> PayloadContractStoreRecord {
        let target_id = ref_node_id(NodeKind::Topic, target_qn);
        let source_id = ref_node_id(NodeKind::Function, &format!("{repo}::handler"));
        let contract_id = ref_node_id(
            NodeKind::PayloadContract,
            &format!("__pc__{repo}__{target_qn}"),
        );
        PayloadContractStoreRecord {
            record: PayloadContractRecord {
                payload_contract_node_id: contract_id,
                contract_target_node_id: target_id,
                contract_target_kind: NodeKind::Topic,
                contract_target_qualified_name: Some(target_qn.to_owned()),
                repo: repo.to_owned(),
                file_path: file.to_owned(),
                source_symbol_node_id: source_id,
                line_start: Some(10),
                side,
                inference_kind: PayloadInferenceKind::LiteralObject,
                confidence: 900,
                source_type_name: None,
                contract: PayloadContractDoc {
                    content_type: "application/json".to_owned(),
                    schema_format: "normalized_object".to_owned(),
                    side,
                    inference_kind: PayloadInferenceKind::LiteralObject,
                    confidence: 900,
                    fields,
                    source_type_name: None,
                },
            },
        }
    }

    fn field(name: &str, type_name: &str, optional: bool) -> PayloadField {
        PayloadField {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            optional,
            confidence: 900,
        }
    }

    fn insert_contract(store: &MetadataStoreDb, record: PayloadContractStoreRecord) {
        let repo = record.record.repo.clone();
        let file_path = record.record.file_path.clone();
        store
            .replace_payload_contracts_for_files(&repo, &[file_path], &[record])
            .expect("insert should succeed");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// A contract present in review only must appear in `added`.
    #[test]
    fn new_contract_appears_in_added_list() {
        let (_td_b, baseline) = open_store("pc-added-baseline");
        let (_td_r, review) = open_store("pc-added-review");

        let record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.created",
            PayloadSide::Producer,
            vec![field("orderId", "string", false)],
        );
        insert_contract(&review, record);

        let deltas = extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected one added contract");
        assert_eq!(
            deltas.added[0].target_qualified_name,
            "__topic__order.created"
        );
        assert_eq!(deltas.added[0].side, "producer");
        assert!(deltas.removed.is_empty(), "nothing removed");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// A contract present in baseline only must appear in `removed`.
    #[test]
    fn removed_contract_appears_in_removed_list() {
        let (_td_b, baseline) = open_store("pc-removed-baseline");
        let (_td_r, review) = open_store("pc-removed-review");

        let record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.created",
            PayloadSide::Producer,
            vec![field("orderId", "string", false)],
        );
        insert_contract(&baseline, record);

        let deltas = extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "expected one removed contract");
        assert_eq!(
            deltas.removed[0].target_qualified_name,
            "__topic__order.created"
        );
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// Same key in both stores but review version has an extra field →
    /// appears in `changed` with `fields_added` containing the new field name.
    #[test]
    fn field_added_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("pc-field-added-baseline");
        let (_td_r, review) = open_store("pc-field-added-review");

        let base_record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.created",
            PayloadSide::Producer,
            vec![field("orderId", "string", false)],
        );
        let review_record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.created",
            PayloadSide::Producer,
            vec![
                field("orderId", "string", false),
                field("customerId", "string", false),
            ],
        );
        insert_contract(&baseline, base_record);
        insert_contract(&review, review_record);

        let deltas = extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.changed.len(), 1, "expected one changed contract");
        let c = &deltas.changed[0];
        assert_eq!(c.fields_added.len(), 1, "one field added");
        assert_eq!(c.fields_added[0].name, "customerId");
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.removed.is_empty(), "nothing removed");
    }

    /// Same field name in both stores but `optional` flips →
    /// appears in `changed` with `fields_optional_to_required` or its reverse.
    #[test]
    fn field_optional_flip_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("pc-optional-flip-baseline");
        let (_td_r, review) = open_store("pc-optional-flip-review");

        // Baseline: `description` is optional.
        let base_record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.created",
            PayloadSide::Producer,
            vec![
                field("orderId", "string", false),
                field("description", "string", true), // optional
            ],
        );
        // Review: `description` becomes required.
        let review_record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.created",
            PayloadSide::Producer,
            vec![
                field("orderId", "string", false),
                field("description", "string", false), // required
            ],
        );
        insert_contract(&baseline, base_record);
        insert_contract(&review, review_record);

        let deltas = extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.changed.len(), 1, "expected one changed contract");
        let c = &deltas.changed[0];
        assert!(
            c.fields_optional_to_required
                .contains(&"description".to_owned()),
            "description must be in fields_optional_to_required; got {:?}",
            c.fields_optional_to_required
        );
        assert!(c.fields_required_to_optional.is_empty());
        assert!(deltas.added.is_empty());
        assert!(deltas.removed.is_empty());
    }

    // ── impact-attach tests ───────────────────────────────────────────────────

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind as NK, Visibility,
        node_id as make_node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::find_payload_contract_node_id;
    use crate::pr_review::extract::impact_attach::impact_for_node;

    struct TempGraph {
        path: PathBuf,
    }

    impl TempGraph {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-pc-graph-{label}-{}-{id}.redb",
                std::process::id()
            ));
            Self { path }
        }
    }

    impl Drop for TempGraph {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn open_graph(label: &str) -> (TempGraph, GraphStoreDb) {
        let tmp = TempGraph::new(label);
        let db = GraphStoreDb::open(&tmp.path).expect("graph store should open");
        (tmp, db)
    }

    /// A contract in `changed` (baseline only differs in fields) must have
    /// `impact` populated when the node exists in the baseline graph and has
    /// at least one cross-repo `UsesShared` consumer.
    ///
    /// This test exercises `find_payload_contract_node_id` + `impact_for_node`
    /// in isolation (the handler wiring path), mirroring how `run_inner` calls
    /// them.
    #[test]
    fn payload_contract_change_carries_impact_when_lookup_succeeds() {
        let (_td_m, meta) = open_store("pc-impact-changed-meta");
        let (_td_g, graph) = open_graph("pc-impact-changed-graph");

        // Build a contract record and insert into the metadata store.
        let record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.updated",
            PayloadSide::Producer,
            vec![field("orderId", "string", false)],
        );
        let contract_node_id = record.record.payload_contract_node_id;
        insert_contract(&meta, record);

        // Insert the PayloadContract virtual node into the graph store.
        let pc_node = NodeData {
            id: contract_node_id,
            kind: NK::PayloadContract,
            repo: "__virtual__".to_owned(),
            file_path: "src/events.ts".to_owned(),
            name: "__pc__".to_owned(),
            qualified_name: Some("__topic__order.updated".to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        // Add a cross-repo consumer via UsesShared edge.
        let consumer_owner = NodeData {
            id: make_node_id("frontend", "src/consumer.ts", NK::File, "src/consumer.ts"),
            kind: NK::File,
            repo: "frontend".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "src/consumer.ts".to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        };
        let consumer = NodeData {
            id: make_node_id("frontend", "src/consumer.ts", NK::Function, "handleUpdate"),
            kind: NK::Function,
            repo: "frontend".to_owned(),
            file_path: "src/consumer.ts".to_owned(),
            name: "handleUpdate".to_owned(),
            qualified_name: Some("frontend::handleUpdate".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        };
        let consume_edge = EdgeData {
            source: consumer.id,
            target: contract_node_id,
            kind: EdgeKind::UsesShared,
            metadata: EdgeMetadata::default(),
            owner_file: consumer_owner.id,
            is_cross_file: true,
        };
        graph
            .bulk_insert(&[pc_node, consumer_owner, consumer], &[consume_edge])
            .expect("graph insert");

        // Simulate handler wiring: find node_id then attach impact.
        let node_id = find_payload_contract_node_id(
            &meta,
            "backend",
            "src/events.ts",
            "__topic__order.updated",
            "producer",
        )
        .expect("lookup should succeed")
        .expect("node_id must be Some");

        let summary = impact_for_node(&graph, node_id, Some("backend")).expect("impact");

        assert!(
            summary.consumer_count >= 1,
            "expected at least 1 consumer; got {}",
            summary.consumer_count
        );
        assert!(
            summary.consumer_repos.contains(&"frontend".to_owned()),
            "frontend must be in consumer_repos; got {:?}",
            summary.consumer_repos
        );
    }

    /// An added payload contract has `impact == None` because there is no
    /// baseline node — no lookup is performed for added entries.
    #[test]
    fn payload_contract_added_has_no_impact() {
        let (_td_b, baseline) = open_store("pc-impact-added-baseline");
        let (_td_r, review) = open_store("pc-impact-added-review");

        let record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.placed",
            PayloadSide::Producer,
            vec![field("orderId", "string", false)],
        );
        insert_contract(&review, record);

        let deltas = extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected one added contract");
        assert!(
            deltas.added[0].impact.is_none(),
            "added contract must have impact == None"
        );
    }

    /// A removed payload contract gets `impact.is_some()` when the lookup
    /// succeeds and the baseline graph has consumers.
    #[test]
    fn payload_contract_removed_carries_impact() {
        let (_td_m, meta) = open_store("pc-impact-removed-meta");
        let (_td_g, graph) = open_graph("pc-impact-removed-graph");

        let record = make_contract(
            "backend",
            "src/events.ts",
            "__topic__order.cancelled",
            PayloadSide::Producer,
            vec![field("orderId", "string", false)],
        );
        let contract_node_id = record.record.payload_contract_node_id;
        insert_contract(&meta, record);

        // Graph: PayloadContract node + one consumer.
        let pc_node = NodeData {
            id: contract_node_id,
            kind: NK::PayloadContract,
            repo: "__virtual__".to_owned(),
            file_path: "src/events.ts".to_owned(),
            name: "__pc__cancelled".to_owned(),
            qualified_name: Some("__topic__order.cancelled".to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        };
        let consumer_owner = NodeData {
            id: make_node_id("notifier", "src/handler.ts", NK::File, "src/handler.ts"),
            kind: NK::File,
            repo: "notifier".to_owned(),
            file_path: "src/handler.ts".to_owned(),
            name: "src/handler.ts".to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        };
        let consumer = NodeData {
            id: make_node_id("notifier", "src/handler.ts", NK::Function, "onCancelled"),
            kind: NK::Function,
            repo: "notifier".to_owned(),
            file_path: "src/handler.ts".to_owned(),
            name: "onCancelled".to_owned(),
            qualified_name: Some("notifier::onCancelled".to_owned()),
            external_id: None,
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: false,
        };
        let consume_edge = EdgeData {
            source: consumer.id,
            target: contract_node_id,
            kind: EdgeKind::UsesShared,
            metadata: EdgeMetadata::default(),
            owner_file: consumer_owner.id,
            is_cross_file: true,
        };
        graph
            .bulk_insert(&[pc_node, consumer_owner, consumer], &[consume_edge])
            .expect("graph insert");

        // Look up the node_id via metadata store and build impact summary
        // (mirrors what run_inner does for removed payload contracts).
        let node_id = find_payload_contract_node_id(
            &meta,
            "backend",
            "src/events.ts",
            "__topic__order.cancelled",
            "producer",
        )
        .expect("lookup should succeed")
        .expect("node_id must be Some");

        let summary = impact_for_node(&graph, node_id, Some("backend")).expect("impact");

        // Simulate the PayloadContractDelta that extract_payload_contract_deltas
        // produces, then attach the impact exactly as run_inner does.
        let mut delta = crate::pr_review::delta_report::PayloadContractDelta {
            repo: "backend".to_owned(),
            file: "src/events.ts".to_owned(),
            target_qualified_name: "__topic__order.cancelled".to_owned(),
            side: "producer".to_owned(),
            fields: vec![],
            impact: None,
        };
        delta.impact = Some(summary);

        assert!(delta.impact.is_some(), "removed contract must carry impact");
        let imp = delta.impact.as_ref().unwrap();
        assert!(
            imp.consumer_count >= 1,
            "expected at least 1 consumer; got {}",
            imp.consumer_count
        );
    }
}
