use std::collections::{BTreeMap, BTreeSet};

use gather_step_core::{
    DriftKind, NodeId, NodeKind, PayloadContractRecord, PayloadField, PayloadSide,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ContractDriftAnalysisError {
    #[error("no producer or consumer payload contracts available for target")]
    MissingContracts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadSchema {
    pub producer_contracts: Vec<PayloadContractRecord>,
    pub consumer_contracts: Vec<PayloadContractRecord>,
    pub producer_fields: Vec<PayloadField>,
    pub consumer_fields: Vec<PayloadField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriftField {
    pub field_name: String,
    pub drift_kind: DriftKind,
    pub producer_type: Option<String>,
    pub consumer_type: Option<String>,
    pub producer_optional: Option<bool>,
    pub consumer_optional: Option<bool>,
    pub confidence: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContractDrift {
    pub target_node_id: NodeId,
    pub producer_contract_ids: Vec<NodeId>,
    pub consumer_contract_ids: Vec<NodeId>,
    pub fields: Vec<DriftField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BreakingChangeCandidate {
    pub target_node_id: NodeId,
    pub consumer_contract_ids: Vec<NodeId>,
    pub drift_fields: Vec<DriftField>,
}

pub fn payload_schema(
    records: &[PayloadContractRecord],
    min_confidence: u16,
) -> Option<PayloadSchema> {
    let producer_contracts = filter_side(records, PayloadSide::Producer, min_confidence);
    let consumer_contracts = filter_side(records, PayloadSide::Consumer, min_confidence);
    if producer_contracts.is_empty() && consumer_contracts.is_empty() {
        return None;
    }
    Some(PayloadSchema {
        producer_fields: consolidate_fields(&producer_contracts),
        consumer_fields: consolidate_fields(&consumer_contracts),
        producer_contracts,
        consumer_contracts,
    })
}

pub fn compare_contracts(
    target_node_id: NodeId,
    records: &[PayloadContractRecord],
    min_confidence: u16,
) -> Result<ContractDrift, ContractDriftAnalysisError> {
    let Some(schema) = payload_schema(records, min_confidence) else {
        return Err(ContractDriftAnalysisError::MissingContracts);
    };
    if schema.producer_contracts.is_empty() || schema.consumer_contracts.is_empty() {
        return Err(ContractDriftAnalysisError::MissingContracts);
    }

    let mut fields = Vec::new();
    let producer = field_map(&schema.producer_fields);
    let consumer = field_map(&schema.consumer_fields);
    let names = producer
        .keys()
        .chain(consumer.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    for name in names {
        match (producer.get(&name), consumer.get(&name)) {
            (Some(producer), Some(consumer)) => {
                if !producer.type_name.eq_ignore_ascii_case(&consumer.type_name) {
                    fields.push(DriftField {
                        field_name: name.clone(),
                        drift_kind: DriftKind::Type,
                        producer_type: Some(producer.type_name.clone()),
                        consumer_type: Some(consumer.type_name.clone()),
                        producer_optional: Some(producer.optional),
                        consumer_optional: Some(consumer.optional),
                        confidence: producer.confidence.min(consumer.confidence),
                    });
                } else if producer.optional != consumer.optional {
                    fields.push(DriftField {
                        field_name: name.clone(),
                        drift_kind: DriftKind::Optionality,
                        producer_type: Some(producer.type_name.clone()),
                        consumer_type: Some(consumer.type_name.clone()),
                        producer_optional: Some(producer.optional),
                        consumer_optional: Some(consumer.optional),
                        confidence: producer.confidence.min(consumer.confidence),
                    });
                }
            }
            (Some(producer), None) => fields.push(DriftField {
                field_name: name.clone(),
                drift_kind: DriftKind::ExtraField,
                producer_type: Some(producer.type_name.clone()),
                consumer_type: None,
                producer_optional: Some(producer.optional),
                consumer_optional: None,
                confidence: producer.confidence,
            }),
            (None, Some(consumer)) => fields.push(DriftField {
                field_name: name.clone(),
                drift_kind: DriftKind::MissingField,
                producer_type: None,
                consumer_type: Some(consumer.type_name.clone()),
                producer_optional: None,
                consumer_optional: Some(consumer.optional),
                confidence: consumer.confidence,
            }),
            (None, None) => {}
        }
    }

    Ok(ContractDrift {
        target_node_id,
        producer_contract_ids: schema
            .producer_contracts
            .iter()
            .map(|record| record.payload_contract_node_id)
            .collect(),
        consumer_contract_ids: schema
            .consumer_contracts
            .iter()
            .map(|record| record.payload_contract_node_id)
            .collect(),
        fields,
    })
}

pub fn breaking_change_candidates(
    source_records: &[PayloadContractRecord],
    all_records: &[PayloadContractRecord],
    min_confidence: u16,
) -> Vec<BreakingChangeCandidate> {
    let mut targets = BTreeSet::new();
    for record in source_records {
        targets.insert(target_match_key(record));
    }

    let mut results = Vec::new();
    for target in targets {
        let records = all_records
            .iter()
            .filter(|record| target_match_key(record) == target)
            .cloned()
            .collect::<Vec<_>>();
        let representative = records
            .first()
            .map_or(target.1, |record| record.contract_target_node_id);
        if let Ok(drift) = compare_contracts(representative, &records, min_confidence)
            && !drift.fields.is_empty()
        {
            results.push(BreakingChangeCandidate {
                target_node_id: representative,
                consumer_contract_ids: drift.consumer_contract_ids,
                drift_fields: drift.fields,
            });
        }
    }
    results
}

fn target_match_key(record: &PayloadContractRecord) -> (Option<(NodeKind, String)>, NodeId) {
    if let Some(qualified_name) = record.contract_target_qualified_name.clone() {
        (
            Some((record.contract_target_kind, qualified_name)),
            NodeId([0; 16]),
        )
    } else {
        (None, record.contract_target_node_id)
    }
}

fn filter_side(
    records: &[PayloadContractRecord],
    side: PayloadSide,
    min_confidence: u16,
) -> Vec<PayloadContractRecord> {
    records
        .iter()
        .filter(|record| record.side == side && record.confidence >= min_confidence)
        .cloned()
        .collect()
}

fn consolidate_fields(records: &[PayloadContractRecord]) -> Vec<PayloadField> {
    let mut fields = BTreeMap::<String, PayloadField>::new();
    for record in records {
        for field in &record.contract.fields {
            fields
                .entry(field.name.clone())
                .and_modify(|existing| {
                    if existing.type_name != field.type_name {
                        "mixed".clone_into(&mut existing.type_name);
                    }
                    existing.optional = existing.optional && field.optional;
                    existing.confidence = existing.confidence.min(field.confidence);
                })
                .or_insert_with(|| field.clone());
        }
    }
    fields.into_values().collect()
}

fn field_map(fields: &[PayloadField]) -> BTreeMap<String, PayloadField> {
    fields
        .iter()
        .map(|field| (field.name.clone(), field.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        DriftKind, NodeKind, PayloadContractDoc, PayloadContractRecord, PayloadField,
        PayloadInferenceKind, PayloadSide, node_id, payload_contract_external_id,
        payload_contract_node_id, ref_node_id, topic_qn,
    };

    use super::{breaking_change_candidates, compare_contracts, payload_schema};

    fn make_record(
        repo: &str,
        target_name: &str,
        source_name: &str,
        side: PayloadSide,
        confidence: u16,
        fields: Vec<(&str, &str, bool, u16)>,
    ) -> PayloadContractRecord {
        let target_id = ref_node_id(NodeKind::Topic, &topic_qn("kafka", target_name));
        let source_symbol_id = node_id(repo, "src/handler.ts", NodeKind::Function, source_name);
        let external_id =
            payload_contract_external_id(repo, "src/handler.ts", target_id, source_symbol_id, side);
        PayloadContractRecord {
            payload_contract_node_id: payload_contract_node_id(&external_id),
            contract_target_node_id: target_id,
            contract_target_kind: NodeKind::Topic,
            contract_target_qualified_name: Some(topic_qn("kafka", target_name)),
            repo: repo.to_owned(),
            file_path: "src/handler.ts".to_owned(),
            source_symbol_node_id: source_symbol_id,
            line_start: Some(10),
            side,
            inference_kind: PayloadInferenceKind::LiteralObject,
            confidence,
            source_type_name: None,
            contract: PayloadContractDoc {
                content_type: "application/json".to_owned(),
                schema_format: "inferred_object".to_owned(),
                side,
                inference_kind: PayloadInferenceKind::LiteralObject,
                confidence,
                fields: fields
                    .into_iter()
                    .map(
                        |(name, type_name, optional, field_confidence)| PayloadField {
                            name: name.to_owned(),
                            type_name: type_name.to_owned(),
                            optional,
                            confidence: field_confidence,
                        },
                    )
                    .collect(),
                source_type_name: None,
            },
        }
    }

    fn make_repo_local_record(
        repo: &str,
        target_name: &str,
        source_name: &str,
        side: PayloadSide,
        confidence: u16,
        fields: Vec<(&str, &str, bool, u16)>,
    ) -> PayloadContractRecord {
        let target_id = node_id(repo, "src/topic.ts", NodeKind::Topic, target_name);
        let source_symbol_id = node_id(repo, "src/handler.ts", NodeKind::Function, source_name);
        let external_id =
            payload_contract_external_id(repo, "src/handler.ts", target_id, source_symbol_id, side);
        PayloadContractRecord {
            payload_contract_node_id: payload_contract_node_id(&external_id),
            contract_target_node_id: target_id,
            contract_target_kind: NodeKind::Topic,
            contract_target_qualified_name: Some(topic_qn("kafka", target_name)),
            repo: repo.to_owned(),
            file_path: "src/handler.ts".to_owned(),
            source_symbol_node_id: source_symbol_id,
            line_start: Some(10),
            side,
            inference_kind: PayloadInferenceKind::LiteralObject,
            confidence,
            source_type_name: None,
            contract: PayloadContractDoc {
                content_type: "application/json".to_owned(),
                schema_format: "inferred_object".to_owned(),
                side,
                inference_kind: PayloadInferenceKind::LiteralObject,
                confidence,
                fields: fields
                    .into_iter()
                    .map(
                        |(name, type_name, optional, field_confidence)| PayloadField {
                            name: name.to_owned(),
                            type_name: type_name.to_owned(),
                            optional,
                            confidence: field_confidence,
                        },
                    )
                    .collect(),
                source_type_name: None,
            },
        }
    }

    #[test]
    fn payload_schema_filters_by_side_and_confidence() {
        let producer = make_record(
            "backend_standard",
            "order.created",
            "emit_order",
            PayloadSide::Producer,
            920,
            vec![("id", "string", false, 920)],
        );
        let weak_consumer = make_record(
            "frontend_standard",
            "order.created",
            "handle_order",
            PayloadSide::Consumer,
            650,
            vec![("id", "string", false, 650)],
        );

        let schema =
            payload_schema(&[producer.clone(), weak_consumer], 700).expect("schema should exist");

        assert_eq!(schema.producer_contracts, vec![producer]);
        assert!(schema.consumer_contracts.is_empty());
        assert_eq!(schema.producer_fields.len(), 1);
    }

    #[test]
    fn compare_contracts_reports_type_optionality_and_shape_drift() {
        let producer = make_record(
            "backend_standard",
            "order.created",
            "emit_order",
            PayloadSide::Producer,
            950,
            vec![
                ("id", "string", false, 950),
                ("status", "string", false, 900),
                ("producer_only", "number", false, 875),
            ],
        );
        let consumer = make_record(
            "frontend_standard",
            "order.created",
            "handle_order",
            PayloadSide::Consumer,
            940,
            vec![
                ("id", "number", false, 940),
                ("status", "string", true, 910),
                ("consumer_only", "boolean", true, 880),
            ],
        );

        let drift = compare_contracts(producer.contract_target_node_id, &[producer, consumer], 700)
            .expect("drift should compare");

        assert_eq!(drift.fields.len(), 4);
        assert!(drift.fields.iter().any(|field| {
            field.field_name == "id"
                && field.drift_kind == DriftKind::Type
                && field.producer_type.as_deref() == Some("string")
                && field.consumer_type.as_deref() == Some("number")
        }));
        assert!(drift.fields.iter().any(|field| {
            field.field_name == "status"
                && field.drift_kind == DriftKind::Optionality
                && field.producer_optional == Some(false)
                && field.consumer_optional == Some(true)
        }));
        assert!(drift.fields.iter().any(|field| {
            field.field_name == "producer_only" && field.drift_kind == DriftKind::ExtraField
        }));
        assert!(drift.fields.iter().any(|field| {
            field.field_name == "consumer_only" && field.drift_kind == DriftKind::MissingField
        }));
    }

    #[test]
    fn breaking_change_candidates_only_returns_targets_with_real_drift() {
        let drifting_producer = make_record(
            "backend_standard",
            "order.created",
            "emit_order",
            PayloadSide::Producer,
            950,
            vec![("id", "string", false, 950)],
        );
        let drifting_consumer = make_record(
            "frontend_standard",
            "order.created",
            "handle_order",
            PayloadSide::Consumer,
            940,
            vec![("id", "number", false, 940)],
        );
        let stable_producer = make_record(
            "backend_standard",
            "report.ready",
            "emit_report",
            PayloadSide::Producer,
            900,
            vec![("id", "string", false, 900)],
        );
        let stable_consumer = make_record(
            "frontend_standard",
            "report.ready",
            "handle_report",
            PayloadSide::Consumer,
            900,
            vec![("id", "string", false, 900)],
        );

        let all_records = vec![
            drifting_producer.clone(),
            drifting_consumer.clone(),
            stable_producer,
            stable_consumer,
        ];
        let candidates = breaking_change_candidates(&[drifting_producer], &all_records, 700);

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].target_node_id,
            drifting_consumer.contract_target_node_id
        );
        assert_eq!(candidates[0].drift_fields.len(), 1);
        assert_eq!(candidates[0].drift_fields[0].drift_kind, DriftKind::Type);
    }

    #[test]
    fn breaking_change_candidates_should_match_semantically_same_targets_even_if_repo_local() {
        let producer = make_repo_local_record(
            "backend_standard",
            "order.created",
            "emit_order",
            PayloadSide::Producer,
            950,
            vec![("id", "string", false, 950)],
        );
        let consumer = make_repo_local_record(
            "frontend_standard",
            "order.created",
            "handle_order",
            PayloadSide::Consumer,
            940,
            vec![("id", "number", false, 940)],
        );

        let candidates = breaking_change_candidates(
            std::slice::from_ref(&producer),
            &[producer.clone(), consumer],
            700,
        );

        assert_eq!(
            candidates.len(),
            1,
            "drift analysis should not miss semantically identical targets because their target ids were written repo-locally"
        );
    }
}
