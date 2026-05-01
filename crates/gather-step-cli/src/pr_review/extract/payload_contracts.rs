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
use gather_step_core::PayloadSide;
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

    // Sort for deterministic output.
    let sort_key =
        |d: &PayloadContractDelta| (d.repo.clone(), d.file.clone(), d.target_qualified_name.clone(), d.side.clone());
    added.sort_by_key(sort_key);
    removed.sort_by_key(sort_key);
    let change_sort_key = |c: &PayloadContractDeltaChange| {
        (c.repo.clone(), c.file.clone(), c.target_qualified_name.clone(), c.side.clone())
    };
    changed.sort_by_key(change_sort_key);

    Ok(PayloadContractDeltas {
        added,
        removed,
        changed,
    })
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
    })
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
    use gather_step_storage::{
        MetadataStore, MetadataStoreDb, PayloadContractStoreRecord,
    };

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
        let contract_id = ref_node_id(NodeKind::PayloadContract, &format!("__pc__{repo}__{target_qn}"));
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

        let deltas =
            extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected one added contract");
        assert_eq!(deltas.added[0].target_qualified_name, "__topic__order.created");
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

        let deltas =
            extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "expected one removed contract");
        assert_eq!(deltas.removed[0].target_qualified_name, "__topic__order.created");
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

        let deltas =
            extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

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

        let deltas =
            extract_payload_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.changed.len(), 1, "expected one changed contract");
        let c = &deltas.changed[0];
        assert!(
            c.fields_optional_to_required.contains(&"description".to_owned()),
            "description must be in fields_optional_to_required; got {:?}",
            c.fields_optional_to_required
        );
        assert!(c.fields_required_to_optional.is_empty());
        assert!(deltas.added.is_empty());
        assert!(deltas.removed.is_empty());
    }
}
