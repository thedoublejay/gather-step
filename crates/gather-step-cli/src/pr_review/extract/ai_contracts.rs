//! AI-contract (structured-output) delta extraction — Phase 4.
//!
//! Diffs AI contracts between a baseline metadata store and a review metadata
//! store to produce [`AiContractDeltas`], the structured-output analogue of the
//! `payload_contracts` delta section.
//!
//! # Diff key
//!
//! `ai_contract_node_id` — unique per call site, minted by the producer from
//! `(repo, file, target_node_id, source_symbol_node_id)`. This means multiple
//! `withStructuredOutput` / structured-output calls inside the same function each
//! get their own entry. An earlier keying scheme used `source_symbol_node_id`
//! alone, which collapsed all contracts in the same function to a single map
//! entry (last-wins), silently dropping deltas for every call site past the
//! first.
//!
//! # Comparison
//!
//! Schema fields are compared by name (added / removed / optionality flip / type
//! change), parallel to `payload_contracts`. AI-specific facets (`provider`,
//! `model`, `temperature`, `inference_kind`, `source_type_name`, `schema_format`)
//! are compared as `(before, after)` pairs in `facets_changed`.

use anyhow::Result;
use gather_step_core::{AiConfidenceBand, NodeId, ai_confidence_band};
use gather_step_storage::{AiContractQuery, AiContractStoreRecord, MetadataStore};
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{
    AiContractDelta, AiContractDeltaChange, AiContractDeltas, AiContractFieldSummary,
    AiContractFieldTypeChange, AiFacetChange,
};

/// `ai_contract_node_id` diff key — globally unique per call site because the
/// producer mints it from `(repo, file, target_node_id, source_symbol_node_id)`.
/// Keying by source symbol alone collapsed multiple structured-output calls in
/// the same function to a single map entry (last wins), silently dropping
/// added/removed/changed deltas for all but the surviving record.
type ContractKey = NodeId;

type ContractMap = FxHashMap<ContractKey, AiContractStoreRecord>;

/// Extract added / removed / changed AI contracts by diffing the metadata stores
/// `baseline` and `review`.
///
/// If `baseline` has no records every review contract is reported as `added`.
pub fn extract_ai_contract_deltas<M: MetadataStore>(
    baseline: &M,
    review: &M,
) -> Result<AiContractDeltas> {
    let baseline_map = build_contract_map(baseline)?;
    let review_map = build_contract_map(review)?;

    let mut added: Vec<AiContractDelta> = Vec::new();
    let mut removed: Vec<AiContractDelta> = Vec::new();
    let mut changed: Vec<AiContractDeltaChange> = Vec::new();

    for (key, record) in &review_map {
        if !baseline_map.contains_key(key) {
            added.push(record_to_delta(record));
        }
    }
    for (key, record) in &baseline_map {
        if !review_map.contains_key(key) {
            removed.push(record_to_delta(record));
        }
    }
    for (key, review_record) in &review_map {
        if let Some(baseline_record) = baseline_map.get(key)
            && let Some(change) = diff_contract(baseline_record, review_record)
        {
            changed.push(change);
        }
    }

    added.sort_by(cmp_delta);
    removed.sort_by(cmp_delta);
    changed.sort_by(cmp_change);

    Ok(AiContractDeltas {
        added,
        removed,
        changed,
        unavailable: false,
    })
}

fn cmp_delta(a: &AiContractDelta, b: &AiContractDelta) -> std::cmp::Ordering {
    (
        a.repo.as_str(),
        a.file.as_str(),
        a.source_type_name.as_deref().unwrap_or(""),
        a.target_qualified_name.as_deref().unwrap_or(""),
        a.inference_kind.as_str(),
    )
        .cmp(&(
            b.repo.as_str(),
            b.file.as_str(),
            b.source_type_name.as_deref().unwrap_or(""),
            b.target_qualified_name.as_deref().unwrap_or(""),
            b.inference_kind.as_str(),
        ))
}

fn cmp_change(a: &AiContractDeltaChange, b: &AiContractDeltaChange) -> std::cmp::Ordering {
    (
        a.repo.as_str(),
        a.file.as_str(),
        a.source_type_name.as_deref().unwrap_or(""),
        a.target_qualified_name.as_deref().unwrap_or(""),
    )
        .cmp(&(
            b.repo.as_str(),
            b.file.as_str(),
            b.source_type_name.as_deref().unwrap_or(""),
            b.target_qualified_name.as_deref().unwrap_or(""),
        ))
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Fetch all AI contracts from the store and index them by diff key.
///
/// Keyed by `ai_contract_node_id` — unique per call site — so multiple
/// structured-output calls inside the same function each get their own entry.
fn build_contract_map<M: MetadataStore>(store: &M) -> Result<ContractMap> {
    let records = store.ai_contracts_for_query(AiContractQuery::default())?;
    let mut map = ContractMap::default();
    for record in records {
        map.insert(record.record.ai_contract_node_id, record);
    }
    Ok(map)
}

fn band_str(confidence: u16) -> &'static str {
    match ai_confidence_band(confidence) {
        AiConfidenceBand::Strong => "strong",
        AiConfidenceBand::Medium => "medium",
        AiConfidenceBand::Weak => "weak",
    }
}

fn field_summary(f: &gather_step_core::AiContractField) -> AiContractFieldSummary {
    AiContractFieldSummary {
        name: f.name.clone(),
        type_name: if f.type_name.is_empty() {
            None
        } else {
            Some(f.type_name.clone())
        },
        optional: f.optional,
    }
}

fn record_to_delta(record: &AiContractStoreRecord) -> AiContractDelta {
    let doc = &record.record.contract;
    AiContractDelta {
        repo: record.record.repo.clone(),
        file: record.record.file_path.clone(),
        source_type_name: record.record.source_type_name.clone(),
        target_qualified_name: record.record.contract_target_qualified_name.clone(),
        provider: doc.provider.clone(),
        model: doc.model.clone(),
        temperature: doc.temperature.clone(),
        inference_kind: record.record.inference_kind.as_sql_str().to_owned(),
        confidence_band: band_str(record.record.confidence).to_owned(),
        fields: doc.fields.iter().map(field_summary).collect(),
    }
}

/// Compare two records sharing a diff key. Returns `None` when nothing differs.
fn diff_contract(
    baseline: &AiContractStoreRecord,
    review: &AiContractStoreRecord,
) -> Option<AiContractDeltaChange> {
    let baseline_doc = &baseline.record.contract;
    let review_doc = &review.record.contract;

    let baseline_fields: FxHashMap<&str, &gather_step_core::AiContractField> = baseline_doc
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();
    let review_fields: FxHashMap<&str, &gather_step_core::AiContractField> = review_doc
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f))
        .collect();

    let mut fields_added: Vec<AiContractFieldSummary> = Vec::new();
    let mut fields_removed: Vec<AiContractFieldSummary> = Vec::new();
    let mut fields_optional_to_required: Vec<String> = Vec::new();
    let mut fields_required_to_optional: Vec<String> = Vec::new();
    let mut fields_type_changed: Vec<AiContractFieldTypeChange> = Vec::new();

    for (name, field) in &review_fields {
        if !baseline_fields.contains_key(name) {
            fields_added.push(field_summary(field));
        }
    }
    for (name, field) in &baseline_fields {
        if !review_fields.contains_key(name) {
            fields_removed.push(field_summary(field));
        }
    }
    for (name, review_field) in &review_fields {
        if let Some(baseline_field) = baseline_fields.get(name) {
            if baseline_field.optional && !review_field.optional {
                fields_optional_to_required.push((*name).to_owned());
            } else if !baseline_field.optional && review_field.optional {
                fields_required_to_optional.push((*name).to_owned());
            }
            if baseline_field.type_name != review_field.type_name {
                fields_type_changed.push(AiContractFieldTypeChange {
                    name: (*name).to_owned(),
                    before_type: non_empty(&baseline_field.type_name),
                    after_type: non_empty(&review_field.type_name),
                });
            }
        }
    }

    let mut facets_changed: Vec<AiFacetChange> = Vec::new();
    push_facet(
        &mut facets_changed,
        "provider",
        baseline_doc.provider.as_deref(),
        review_doc.provider.as_deref(),
    );
    push_facet(
        &mut facets_changed,
        "model",
        baseline_doc.model.as_deref(),
        review_doc.model.as_deref(),
    );
    push_facet(
        &mut facets_changed,
        "temperature",
        baseline_doc.temperature.as_deref(),
        review_doc.temperature.as_deref(),
    );
    push_facet(
        &mut facets_changed,
        "source_type_name",
        baseline_doc.source_type_name.as_deref(),
        review_doc.source_type_name.as_deref(),
    );
    if baseline_doc.schema_format != review_doc.schema_format {
        facets_changed.push(AiFacetChange {
            facet: "schema_format".to_owned(),
            before: non_empty(&baseline_doc.schema_format),
            after: non_empty(&review_doc.schema_format),
        });
    }
    if baseline.record.inference_kind != review.record.inference_kind {
        facets_changed.push(AiFacetChange {
            facet: "inference_kind".to_owned(),
            before: Some(baseline.record.inference_kind.as_sql_str().to_owned()),
            after: Some(review.record.inference_kind.as_sql_str().to_owned()),
        });
    }

    if fields_added.is_empty()
        && fields_removed.is_empty()
        && fields_optional_to_required.is_empty()
        && fields_required_to_optional.is_empty()
        && fields_type_changed.is_empty()
        && facets_changed.is_empty()
    {
        return None;
    }

    fields_added.sort_by(|a, b| a.name.cmp(&b.name));
    fields_removed.sort_by(|a, b| a.name.cmp(&b.name));
    fields_optional_to_required.sort();
    fields_required_to_optional.sort();
    fields_type_changed.sort_by(|a, b| a.name.cmp(&b.name));
    facets_changed.sort_by(|a, b| a.facet.cmp(&b.facet));

    Some(AiContractDeltaChange {
        repo: review.record.repo.clone(),
        file: review.record.file_path.clone(),
        source_type_name: review.record.source_type_name.clone(),
        target_qualified_name: review.record.contract_target_qualified_name.clone(),
        fields_added,
        fields_removed,
        fields_optional_to_required,
        fields_required_to_optional,
        fields_type_changed,
        facets_changed,
    })
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_owned())
    }
}

fn push_facet(
    out: &mut Vec<AiFacetChange>,
    facet: &str,
    before: Option<&str>,
    after: Option<&str>,
) {
    if before != after {
        out.push(AiFacetChange {
            facet: facet.to_owned(),
            before: before.map(str::to_owned),
            after: after.map(str::to_owned),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        AiContractDoc, AiContractField, AiContractInferenceKind, AiContractRecord, NodeKind,
        ai_contract_external_id, ai_contract_node_id, node_id,
    };
    use gather_step_storage::{AiContractStoreRecord, MetadataStore, MetadataStoreDb};

    use super::extract_ai_contract_deltas;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-ai-extractor-{label}-{}-{id}.sqlite3",
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

    fn field(name: &str, type_name: &str, optional: bool) -> AiContractField {
        AiContractField {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            optional,
            confidence: 900,
        }
    }

    /// Build a contract shaped exactly like the R1 TS producer
    /// (`structured_output_record`): `contract_target_qualified_name`, `provider`,
    /// `model`, and `temperature` are `None`; identity is the source symbol +
    /// `source_type_name`. `fields` is empty in R1 — the `fields` param is for
    /// forward-looking (R2 field-extraction) coverage of the diff machinery.
    fn r1_contract(
        repo: &str,
        file: &str,
        symbol: &str,
        source_type: Option<&str>,
        inference: AiContractInferenceKind,
        fields: Vec<AiContractField>,
    ) -> AiContractStoreRecord {
        // The producer keys the synthetic target on the schema/line; mirror that
        // so distinct schemas at one symbol get distinct contract node ids.
        let target_seed = format!("__ai_target__{}", source_type.unwrap_or("inline"));
        let target = node_id(repo, file, NodeKind::AiContract, &target_seed);
        let source = node_id(repo, file, NodeKind::Function, symbol);
        let external_id = ai_contract_external_id(repo, file, target, source);
        let confidence = match inference {
            AiContractInferenceKind::LiteralSchema => 850,
            AiContractInferenceKind::ReferencedSchema => 700,
            AiContractInferenceKind::UsageInferred => 500,
        };
        AiContractStoreRecord {
            record: AiContractRecord {
                ai_contract_node_id: ai_contract_node_id(&external_id),
                contract_target_node_id: target,
                contract_target_kind: NodeKind::AiContract,
                contract_target_qualified_name: None,
                repo: repo.to_owned(),
                file_path: file.to_owned(),
                source_symbol_node_id: source,
                line_start: None,
                inference_kind: inference,
                confidence,
                source_type_name: source_type.map(str::to_owned),
                contract: AiContractDoc {
                    provider: None,
                    model: None,
                    temperature: None,
                    structured: true,
                    schema_format: "zod".to_owned(),
                    inference_kind: inference,
                    confidence,
                    fields,
                    prompt_keys: vec![],
                    source_type_name: source_type.map(str::to_owned),
                },
            },
        }
    }

    fn insert(store: &MetadataStoreDb, record: AiContractStoreRecord) {
        let repo = record.record.repo.clone();
        let file = record.record.file_path.clone();
        store
            .replace_ai_contracts_for_files(&repo, &[file], &[record])
            .expect("insert should succeed");
    }

    /// Insert multiple contracts from the same repo+file in a single write so
    /// the replace-for-files operation does not evict earlier records.
    fn insert_batch(store: &MetadataStoreDb, records: &[AiContractStoreRecord]) {
        assert!(!records.is_empty());
        let repo = records[0].record.repo.clone();
        let file = records[0].record.file_path.clone();
        assert!(
            records.iter().all(|r| r.record.file_path == file),
            "insert_batch: all records must share the same file (DELETE keys on file path)"
        );
        store
            .replace_ai_contracts_for_files(&repo, &[file], records)
            .expect("insert_batch should succeed");
    }

    /// REGRESSION (review finding #1): a producer-shaped record has a `None`
    /// target QN; it must still appear in `added` (an earlier build dropped it).
    #[test]
    fn realistic_r1_contract_appears_in_added_list() {
        let (_b, baseline) = open_store("ai-added-baseline");
        let (_r, review) = open_store("ai-added-review");

        insert(
            &review,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("ItemComparisonOutputSchema"),
                AiContractInferenceKind::ReferencedSchema,
                vec![],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.added.len(),
            1,
            "real R1 contract must not be dropped"
        );
        assert_eq!(
            deltas.added[0].source_type_name.as_deref(),
            Some("ItemComparisonOutputSchema")
        );
        assert_eq!(
            deltas.added[0].target_qualified_name, None,
            "R1 records have no resolved model target"
        );
        assert_eq!(deltas.added[0].confidence_band, "medium");
        assert!(deltas.removed.is_empty(), "nothing removed");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// A contract present only in baseline must appear in `removed`.
    #[test]
    fn removed_contract_appears_in_removed_list() {
        let (_b, baseline) = open_store("ai-removed-baseline");
        let (_r, review) = open_store("ai-removed-review");

        insert(
            &baseline,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("ItemComparisonOutputSchema"),
                AiContractInferenceKind::ReferencedSchema,
                vec![],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "expected one removed contract");
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// When the structured-output schema is swapped at a call site (`OldSchema` →
    /// `NewSchema`) the two records have different `ai_contract_node_id` values
    /// because the producer keys the id on the schema label/target. The result is
    /// one removed (`OldSchema`) and one added (`NewSchema`), not a single `changed`.
    #[test]
    fn source_type_swap_appears_as_removed_and_added() {
        let (_b, baseline) = open_store("ai-stype-baseline");
        let (_r, review) = open_store("ai-stype-review");

        insert(
            &baseline,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("OldSchema"),
                AiContractInferenceKind::ReferencedSchema,
                vec![],
            ),
        );
        insert(
            &review,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("NewSchema"),
                AiContractInferenceKind::ReferencedSchema,
                vec![],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "OldSchema must appear as removed");
        assert_eq!(
            deltas.removed[0].source_type_name.as_deref(),
            Some("OldSchema")
        );
        assert_eq!(deltas.added.len(), 1, "NewSchema must appear as added");
        assert_eq!(
            deltas.added[0].source_type_name.as_deref(),
            Some("NewSchema")
        );
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// Inline schema becomes referenced (or vice versa) → `inference_kind` facet
    /// change. Schemas keep the same name so identity stays stable.
    #[test]
    fn inference_kind_change_appears_in_changed_list() {
        let (_b, baseline) = open_store("ai-infer-baseline");
        let (_r, review) = open_store("ai-infer-review");

        insert(
            &baseline,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("OutputSchema"),
                AiContractInferenceKind::LiteralSchema,
                vec![],
            ),
        );
        insert(
            &review,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("OutputSchema"),
                AiContractInferenceKind::ReferencedSchema,
                vec![],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.changed.len(), 1, "expected one changed contract");
        assert!(
            deltas.changed[0]
                .facets_changed
                .iter()
                .any(|f| f.facet == "inference_kind"),
            "inference_kind change must surface; got {:?}",
            deltas.changed[0].facets_changed
        );
    }

    /// REGRESSION: when one function contains two structured-output calls the
    /// delta map must report both contracts. Previously, keying by
    /// `source_symbol_node_id` caused the second record to overwrite the first
    /// so one contract was silently dropped.
    #[test]
    fn two_contracts_in_same_symbol_both_appear_in_delta() {
        let (_b, baseline) = open_store("ai-two-contracts-baseline");
        let (_r, review) = open_store("ai-two-contracts-review");

        // One function ("processData") with two distinct structured-output calls.
        // Both are present in baseline; one is added in review (review has an
        // extra third call) and one is removed (baseline has a unique contract).
        // Simpler: baseline has SchemaA only; review has SchemaA + SchemaB.
        // Expected: SchemaB surfaces as `added`, nothing in removed/changed.
        insert(
            &baseline,
            r1_contract(
                "events",
                "src/processor.ts",
                "processData",
                Some("SchemaA"),
                AiContractInferenceKind::LiteralSchema,
                vec![],
            ),
        );
        // Use insert_batch so both review records survive: replace_ai_contracts_for_files
        // issues a DELETE for the file before inserting, so two separate `insert` calls
        // would leave only the last one.
        let review_records = vec![
            r1_contract(
                "events",
                "src/processor.ts",
                "processData",
                Some("SchemaA"),
                AiContractInferenceKind::LiteralSchema,
                vec![],
            ),
            r1_contract(
                "events",
                "src/processor.ts",
                "processData",
                Some("SchemaB"),
                AiContractInferenceKind::LiteralSchema,
                vec![],
            ),
        ];
        insert_batch(&review, &review_records);

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.added.len(),
            1,
            "SchemaB contract must appear as added; got added={:?}",
            deltas
                .added
                .iter()
                .map(|d| d.source_type_name.as_deref())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            deltas.added[0].source_type_name.as_deref(),
            Some("SchemaB"),
            "the added contract must be SchemaB"
        );
        assert!(deltas.removed.is_empty(), "nothing should be removed");
        assert!(deltas.changed.is_empty(), "nothing should be changed");
    }

    /// Forward-looking (R2 field extraction): when the producer eventually
    /// populates schema fields, an added field surfaces in `fields_added`. R1
    /// records have empty fields so this exercises the diff machinery only.
    #[test]
    fn schema_field_added_appears_in_changed_list() {
        let (_b, baseline) = open_store("ai-field-added-baseline");
        let (_r, review) = open_store("ai-field-added-review");

        insert(
            &baseline,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("OutputSchema"),
                AiContractInferenceKind::LiteralSchema,
                vec![field("is_related", "boolean", false)],
            ),
        );
        insert(
            &review,
            r1_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                Some("OutputSchema"),
                AiContractInferenceKind::LiteralSchema,
                vec![
                    field("is_related", "boolean", false),
                    field("reason", "string", false),
                ],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.changed.len(), 1, "expected one changed contract");
        let c = &deltas.changed[0];
        assert_eq!(c.fields_added.len(), 1, "one field added");
        assert_eq!(c.fields_added[0].name, "reason");
    }
}
