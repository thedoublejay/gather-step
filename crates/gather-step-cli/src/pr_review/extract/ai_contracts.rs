//! AI-contract (structured-output) delta extraction — Phase 4.
//!
//! Diffs AI contracts between a baseline metadata store and a review metadata
//! store to produce [`AiContractDeltas`], the structured-output analogue of the
//! `payload_contracts` delta section.
//!
//! # Diff key
//!
//! `(repo, file_path, source_symbol_node_id)` — "the structured-output call site
//! in a given symbol". Unlike payload contracts (keyed by target + side), an AI
//! contract is keyed by its source symbol so that swapping the model or editing
//! the schema both surface as a *change* on the same call site rather than a
//! remove + add. Contracts without a `contract_target_qualified_name` are skipped
//! (they have no stable target identity to report).
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

/// `(repo, file_path, source_symbol_node_id)` diff key — see the module-level
/// note on why AI contracts key by source symbol rather than target + side.
type ContractKey = (String, String, NodeId);

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
        a.target_qualified_name.as_str(),
    )
        .cmp(&(
            b.repo.as_str(),
            b.file.as_str(),
            b.target_qualified_name.as_str(),
        ))
}

fn cmp_change(a: &AiContractDeltaChange, b: &AiContractDeltaChange) -> std::cmp::Ordering {
    (
        a.repo.as_str(),
        a.file.as_str(),
        a.target_qualified_name.as_str(),
    )
        .cmp(&(
            b.repo.as_str(),
            b.file.as_str(),
            b.target_qualified_name.as_str(),
        ))
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Fetch all AI contracts from the store and index them by diff key.
///
/// Contracts without a `contract_target_qualified_name` are skipped because they
/// have no stable target identity to report.
fn build_contract_map<M: MetadataStore>(store: &M) -> Result<ContractMap> {
    let records = store.ai_contracts_for_query(AiContractQuery::default())?;
    let mut map = ContractMap::default();
    for record in records {
        if record.record.contract_target_qualified_name.is_none() {
            continue;
        }
        let key = (
            record.record.repo.clone(),
            record.record.file_path.clone(),
            record.record.source_symbol_node_id,
        );
        // Last record for this key wins (deduplication).
        map.insert(key, record);
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
        target_qualified_name: record
            .record
            .contract_target_qualified_name
            .clone()
            .unwrap_or_default(),
        provider: doc.provider.clone(),
        model: doc.model.clone(),
        temperature: doc.temperature.clone(),
        inference_kind: record.record.inference_kind.as_sql_str().to_owned(),
        confidence_band: band_str(record.record.confidence).to_owned(),
        source_type_name: record.record.source_type_name.clone(),
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
        target_qualified_name: review
            .record
            .contract_target_qualified_name
            .clone()
            .unwrap_or_default(),
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

    /// Build an AI contract whose target model is `__llm__<provider>__<model>`
    /// and whose source symbol is `<repo>::<symbol>` (temperature fixed at `0`).
    fn make_ai_contract(
        repo: &str,
        file: &str,
        symbol: &str,
        provider: &str,
        model: &str,
        fields: Vec<AiContractField>,
    ) -> AiContractStoreRecord {
        let target_qn = format!("__llm__{provider}__{model}");
        let target = node_id(repo, file, NodeKind::LlmModel, &target_qn);
        let source = node_id(repo, file, NodeKind::Function, symbol);
        let external_id = ai_contract_external_id(repo, file, target, source);
        AiContractStoreRecord {
            record: AiContractRecord {
                ai_contract_node_id: ai_contract_node_id(&external_id),
                contract_target_node_id: target,
                contract_target_kind: NodeKind::LlmModel,
                contract_target_qualified_name: Some(target_qn),
                repo: repo.to_owned(),
                file_path: file.to_owned(),
                source_symbol_node_id: source,
                line_start: Some(10),
                inference_kind: AiContractInferenceKind::LiteralSchema,
                confidence: 850,
                source_type_name: Some("OutputSchema".to_owned()),
                contract: AiContractDoc {
                    provider: Some(provider.to_owned()),
                    model: Some(model.to_owned()),
                    temperature: Some("0".to_owned()),
                    structured: true,
                    schema_format: "zod".to_owned(),
                    inference_kind: AiContractInferenceKind::LiteralSchema,
                    confidence: 850,
                    fields,
                    prompt_keys: vec![],
                    source_type_name: Some("OutputSchema".to_owned()),
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

    /// A contract present only in review must appear in `added`.
    #[test]
    fn new_contract_appears_in_added_list() {
        let (_b, baseline) = open_store("ai-added-baseline");
        let (_r, review) = open_store("ai-added-review");

        insert(
            &review,
            make_ai_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                "openai",
                "gpt-4.1-mini",
                vec![field("is_related", "boolean", false)],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.added.len(), 1, "expected one added contract");
        assert_eq!(
            deltas.added[0].target_qualified_name,
            "__llm__openai__gpt-4.1-mini"
        );
        assert_eq!(deltas.added[0].provider.as_deref(), Some("openai"));
        assert_eq!(deltas.added[0].confidence_band, "strong");
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
            make_ai_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                "openai",
                "gpt-4.1-mini",
                vec![field("is_related", "boolean", false)],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.removed.len(), 1, "expected one removed contract");
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.changed.is_empty(), "nothing changed");
    }

    /// Same call site, review adds a schema field → `changed` with the new field
    /// in `fields_added`. This is the core pr-review payoff (a Zod schema edit).
    #[test]
    fn schema_field_added_appears_in_changed_list() {
        let (_b, baseline) = open_store("ai-field-added-baseline");
        let (_r, review) = open_store("ai-field-added-review");

        insert(
            &baseline,
            make_ai_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                "openai",
                "gpt-4.1-mini",
                vec![field("is_related", "boolean", false)],
            ),
        );
        insert(
            &review,
            make_ai_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                "openai",
                "gpt-4.1-mini",
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
        assert!(c.facets_changed.is_empty(), "no facet changes");
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.removed.is_empty(), "nothing removed");
    }

    /// Same call site, the model swaps → `changed` with a `model` facet change
    /// (and a `provider`/target change), not a remove + add.
    #[test]
    fn model_swap_appears_as_facet_change() {
        let (_b, baseline) = open_store("ai-model-swap-baseline");
        let (_r, review) = open_store("ai-model-swap-review");

        insert(
            &baseline,
            make_ai_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                "openai",
                "gpt-4.1-mini",
                vec![field("is_related", "boolean", false)],
            ),
        );
        insert(
            &review,
            make_ai_contract(
                "events",
                "src/agent.ts",
                "compareItems",
                "anthropic",
                "claude-sonnet",
                vec![field("is_related", "boolean", false)],
            ),
        );

        let deltas = extract_ai_contract_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.changed.len(),
            1,
            "model swap is a change, not add/remove"
        );
        let c = &deltas.changed[0];
        let model_change = c
            .facets_changed
            .iter()
            .find(|f| f.facet == "model")
            .expect("model facet must be in facets_changed");
        assert_eq!(model_change.before.as_deref(), Some("gpt-4.1-mini"));
        assert_eq!(model_change.after.as_deref(), Some("claude-sonnet"));
        assert!(deltas.added.is_empty(), "nothing added");
        assert!(deltas.removed.is_empty(), "nothing removed");
    }
}
