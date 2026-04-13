use gather_step_analysis::{
    BreakingChangeCandidate, ContractDrift, DriftField, PayloadSchema, breaking_change_candidates,
    compare_contracts, payload_schema, resolve_event_targets, resolve_route_target,
};
use gather_step_core::{DriftKind, NodeId, NodeKind};
use gather_step_storage::{GraphStore, MetadataStore, PayloadContractQuery};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    ids::{decode_node_id, encode_node_id},
};

const DEFAULT_MIN_CONFIDENCE: u16 = 750;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PayloadSchemaRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub include_weak: Option<bool>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContractDriftRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub include_weak: Option<bool>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BreakingChangeCandidatesRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub include_weak: Option<bool>,
    pub symbol_or_dto: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContractMeta {
    pub budget: ResponseBudget,
    pub generation: i64,
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PayloadSchemaResponse {
    pub data: PayloadSchemaData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ContractMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PayloadSchemaData {
    pub consumer_contracts: Vec<ContractSummary>,
    pub consumer_schema: Vec<SchemaFieldItem>,
    pub producer_contracts: Vec<ContractSummary>,
    pub producer_schema: Vec<SchemaFieldItem>,
    pub target_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContractDriftResponse {
    pub data: ContractDriftData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ContractMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContractDriftData {
    pub consumer_contract_ids: Vec<String>,
    pub drifts: Vec<DriftFieldItem>,
    pub producer_contract_ids: Vec<String>,
    pub target_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BreakingChangeCandidatesResponse {
    pub data: BreakingChangeCandidatesData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ContractMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BreakingChangeCandidatesData {
    pub candidates: Vec<BreakingChangeCandidateItem>,
    pub subject: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContractSummary {
    pub confidence: u16,
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub repo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_type_name: Option<String>,
    pub symbol_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SchemaFieldItem {
    pub confidence: u16,
    pub name: String,
    pub optional: bool,
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DriftFieldItem {
    pub confidence: u16,
    pub drift_kind: String,
    pub field_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_optional: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BreakingChangeCandidateItem {
    pub consumer_contract_ids: Vec<String>,
    pub drifts: Vec<DriftFieldItem>,
    pub target_id: String,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn payload_schema_tool(
    ctx: &McpContext,
    request: PayloadSchemaRequest,
) -> Result<PayloadSchemaResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let target = resolve_virtual_target(ctx, &request.target)?;
    let min_confidence = min_confidence(request.include_weak);
    let records = payload_contract_records_for_target(ctx, &target, min_confidence)?;
    let schema = payload_schema(&records, min_confidence).ok_or_else(|| {
        McpServerError::NotFound(format!(
            "no payload contracts found for `{}`",
            request.target
        ))
    })?;
    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = PayloadSchemaResponse {
        data: map_schema(target.id, schema),
        meta: None,
    };
    let budget = apply_response_budget(
        BudgetedTool::Contract,
        request.budget_bytes,
        &mut response,
        trim_payload_schema,
    )?;
    let included = response.data.consumer_schema.len()
        + response.data.producer_schema.len()
        + response.data.consumer_contracts.len()
        + response.data.producer_contracts.len();
    response.meta = Some(ContractMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn contract_drift_tool(
    ctx: &McpContext,
    request: ContractDriftRequest,
) -> Result<ContractDriftResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let target = resolve_virtual_target(ctx, &request.target)?;
    let min_confidence = min_confidence(request.include_weak);
    let records = payload_contract_records_for_target(ctx, &target, min_confidence)?;
    let drift = compare_contracts(target.id, &records, min_confidence)?;
    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = ContractDriftResponse {
        data: map_drift(drift),
        meta: None,
    };
    // Sort drifts by confidence descending so we can safely pop the tail when trimming.
    response
        .data
        .drifts
        .sort_by(|left, right| right.confidence.cmp(&left.confidence));
    let budget = apply_response_budget(
        BudgetedTool::Contract,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.drifts.pop().is_some(),
    )?;
    let included = response.data.drifts.len();
    response.meta = Some(ContractMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

pub fn breaking_change_candidates_tool(
    ctx: &McpContext,
    request: BreakingChangeCandidatesRequest,
) -> Result<BreakingChangeCandidatesResponse, McpServerError> {
    validate_input_length("symbol_or_dto", &request.symbol_or_dto)?;
    let min_confidence = min_confidence(request.include_weak);

    // Step 1: Fetch only source records matching the symbol/DTO, not the entire table.
    let source_records = if let Ok(symbol_id) = decode_node_id(&request.symbol_or_dto) {
        ctx.metadata()
            .payload_contracts_for_query(PayloadContractQuery {
                source_symbol_node_id: Some(symbol_id),
                min_confidence: Some(min_confidence),
                ..PayloadContractQuery::default()
            })?
            .into_iter()
            .map(|record| record.record)
            .collect::<Vec<_>>()
    } else {
        ctx.metadata()
            .payload_contracts_for_query(PayloadContractQuery {
                source_type_name: Some(request.symbol_or_dto.clone()),
                min_confidence: Some(min_confidence),
                ..PayloadContractQuery::default()
            })?
            .into_iter()
            .map(|record| record.record)
            .collect::<Vec<_>>()
    };

    // Step 2: Collect unique target IDs and fetch records per target for drift comparison.
    let target_keys: std::collections::BTreeSet<_> = source_records
        .iter()
        .map(contract_target_query_key)
        .collect();
    let mut all_target_records = Vec::new();
    for (target_id, target_kind, target_qn) in &target_keys {
        let records = ctx
            .metadata()
            .payload_contracts_for_query(PayloadContractQuery {
                contract_target_node_id: if target_qn.is_some() {
                    None
                } else {
                    Some(*target_id)
                },
                contract_target_kind: target_qn.as_ref().map(|_| *target_kind),
                contract_target_qualified_name: target_qn.clone(),
                min_confidence: Some(min_confidence),
                ..PayloadContractQuery::default()
            })?;
        all_target_records.extend(records.into_iter().map(|record| record.record));
    }

    let candidates =
        breaking_change_candidates(&source_records, &all_target_records, min_confidence);
    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = BreakingChangeCandidatesResponse {
        data: BreakingChangeCandidatesData {
            candidates: candidates.into_iter().map(map_breaking_candidate).collect(),
            subject: request.symbol_or_dto,
        },
        meta: None,
    };
    let budget = apply_response_budget(
        BudgetedTool::Contract,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.candidates.pop().is_some(),
    )?;
    let included = response.data.candidates.len();
    response.meta = Some(ContractMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

/// Complete a [`ResponseBudget`] emitted by [`apply_response_budget`] with the
/// post-truncation `items_included` count.
fn finalize_budget(mut budget: ResponseBudget, items_included: usize) -> ResponseBudget {
    budget.items_included = items_included;
    budget
}

/// Drop the lowest-ranked element from a [`PayloadSchemaResponse`]: prefer
/// schema fields (noisier, lower per-item value) before contract summaries.
fn trim_payload_schema(response: &mut PayloadSchemaResponse) -> bool {
    response.data.consumer_schema.pop().is_some()
        || response.data.producer_schema.pop().is_some()
        || response.data.consumer_contracts.pop().is_some()
        || response.data.producer_contracts.pop().is_some()
}

#[derive(Clone, Debug)]
struct ResolvedVirtualTarget {
    id: NodeId,
    kind: NodeKind,
    qualified_name: Option<String>,
}

fn resolve_virtual_target(
    ctx: &McpContext,
    target: &str,
) -> Result<ResolvedVirtualTarget, McpServerError> {
    if let Ok(node_id) = decode_node_id(target)
        && let Some(node) = ctx.graph().get_node(node_id)?
    {
        if node.is_virtual {
            return Ok(ResolvedVirtualTarget {
                id: node_id,
                kind: node.kind,
                qualified_name: node.qualified_name.or(node.external_id),
            });
        }

        for edge in ctx.graph().get_outgoing(node_id)? {
            if let Some(target_node) = ctx.graph().get_node(edge.target)?
                && target_node.is_virtual
            {
                return Ok(ResolvedVirtualTarget {
                    id: target_node.id,
                    kind: target_node.kind,
                    qualified_name: target_node.qualified_name.or(target_node.external_id),
                });
            }
        }
        for edge in ctx.graph().get_incoming(node_id)? {
            if let Some(source_node) = ctx.graph().get_node(edge.source)?
                && source_node.is_virtual
            {
                return Ok(ResolvedVirtualTarget {
                    id: source_node.id,
                    kind: source_node.kind,
                    qualified_name: source_node.qualified_name.or(source_node.external_id),
                });
            }
        }
    }
    if let Some((method, path)) = parse_route_target(target)
        && let Some(route) = resolve_route_target(ctx.graph(), &method, &path)?
    {
        return Ok(ResolvedVirtualTarget {
            id: route.id,
            kind: route.kind,
            qualified_name: route.qualified_name.or(route.external_id),
        });
    }
    // resolve_event_targets already scans Topic, Queue, Subject, Stream, Event.
    // Only Route is missing — check it with an exact external_id lookup.
    if let Some(node) = resolve_event_targets(ctx.graph(), target)?
        .into_iter()
        .next()
    {
        return Ok(ResolvedVirtualTarget {
            id: node.id,
            kind: node.kind,
            qualified_name: node.qualified_name.or(node.external_id),
        });
    }
    if let Some(node) = ctx
        .graph()
        .nodes_by_external_id(NodeKind::Route, target)?
        .into_iter()
        .find(|n| n.is_virtual)
    {
        return Ok(ResolvedVirtualTarget {
            id: node.id,
            kind: node.kind,
            qualified_name: node.qualified_name.or(node.external_id),
        });
    }
    Err(McpServerError::NotFound(format!(
        "virtual target `{target}` was not found"
    )))
}

fn payload_contract_records_for_target(
    ctx: &McpContext,
    target: &ResolvedVirtualTarget,
    min_confidence: u16,
) -> Result<Vec<gather_step_core::PayloadContractRecord>, McpServerError> {
    let (contract_target_node_id, contract_target_kind, contract_target_qualified_name) =
        if target.qualified_name.is_some() {
            (None, Some(target.kind), target.qualified_name.clone())
        } else {
            (Some(target.id), None, None)
        };
    Ok(ctx
        .metadata()
        .payload_contracts_for_query(PayloadContractQuery {
            contract_target_node_id,
            contract_target_kind,
            contract_target_qualified_name,
            min_confidence: Some(min_confidence),
            ..PayloadContractQuery::default()
        })?
        .into_iter()
        .map(|record| record.record)
        .collect())
}

fn contract_target_query_key(
    record: &gather_step_core::PayloadContractRecord,
) -> (NodeId, NodeKind, Option<String>) {
    (
        record.contract_target_node_id,
        record.contract_target_kind,
        record.contract_target_qualified_name.clone(),
    )
}

fn parse_route_target(target: &str) -> Option<(String, String)> {
    let (method, path) = target.trim().split_once(' ')?;
    if method.is_empty() || path.is_empty() {
        return None;
    }
    Some((method.to_owned(), path.to_owned()))
}

fn min_confidence(include_weak: Option<bool>) -> u16 {
    if include_weak.unwrap_or(false) {
        0
    } else {
        DEFAULT_MIN_CONFIDENCE
    }
}

fn map_schema(target_id: NodeId, schema: PayloadSchema) -> PayloadSchemaData {
    PayloadSchemaData {
        consumer_contracts: schema
            .consumer_contracts
            .iter()
            .map(map_contract_summary)
            .collect(),
        consumer_schema: schema
            .consumer_fields
            .into_iter()
            .map(map_schema_field)
            .collect(),
        producer_contracts: schema
            .producer_contracts
            .iter()
            .map(map_contract_summary)
            .collect(),
        producer_schema: schema
            .producer_fields
            .into_iter()
            .map(map_schema_field)
            .collect(),
        target_id: encode_node_id(target_id),
    }
}

fn map_drift(drift: ContractDrift) -> ContractDriftData {
    ContractDriftData {
        consumer_contract_ids: drift
            .consumer_contract_ids
            .into_iter()
            .map(encode_node_id)
            .collect(),
        drifts: drift.fields.into_iter().map(map_drift_field).collect(),
        producer_contract_ids: drift
            .producer_contract_ids
            .into_iter()
            .map(encode_node_id)
            .collect(),
        target_id: encode_node_id(drift.target_node_id),
    }
}

fn map_breaking_candidate(candidate: BreakingChangeCandidate) -> BreakingChangeCandidateItem {
    BreakingChangeCandidateItem {
        consumer_contract_ids: candidate
            .consumer_contract_ids
            .into_iter()
            .map(encode_node_id)
            .collect(),
        drifts: candidate
            .drift_fields
            .into_iter()
            .map(map_drift_field)
            .collect(),
        target_id: encode_node_id(candidate.target_node_id),
    }
}

fn map_contract_summary(record: &gather_step_core::PayloadContractRecord) -> ContractSummary {
    ContractSummary {
        confidence: record.confidence,
        file_path: record.file_path.clone(),
        line_start: record.line_start,
        repo: record.repo.clone(),
        source_type_name: record.source_type_name.clone(),
        symbol_id: encode_node_id(record.source_symbol_node_id),
    }
}

fn map_schema_field(field: gather_step_core::PayloadField) -> SchemaFieldItem {
    SchemaFieldItem {
        confidence: field.confidence,
        name: field.name,
        optional: field.optional,
        type_name: field.type_name,
    }
}

fn map_drift_field(field: DriftField) -> DriftFieldItem {
    DriftFieldItem {
        confidence: field.confidence,
        drift_kind: match field.drift_kind {
            DriftKind::Shape => "shape",
            DriftKind::Type => "type",
            DriftKind::Optionality => "optionality",
            DriftKind::MissingField => "missing_field",
            DriftKind::ExtraField => "extra_field",
        }
        .to_owned(),
        field_name: field.field_name,
        consumer_optional: field.consumer_optional,
        consumer_type: field.consumer_type,
        producer_optional: field.producer_optional,
        producer_type: field.producer_type,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{NodeKind, RegistryStore};
    use gather_step_storage::{GraphStore, IndexingOptions, RepoIndexer};

    use crate::{McpServerConfig, config::McpContext};

    use super::{
        BreakingChangeCandidatesRequest, ContractDriftRequest, PayloadSchemaRequest,
        breaking_change_candidates_tool, contract_drift_tool, payload_schema_tool,
        response_schema_version,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-contract-{name}-{}-{id}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn contract_tools_surface_schema_and_drift() {
        let repo_root = TempDir::new("repo");
        let storage_root = TempDir::new("storage");
        fs::create_dir_all(repo_root.path().join("src")).expect("src dir");
        fs::write(
            repo_root.path().join("package.json"),
            r#"{ "name": "backend-standard", "dependencies": { "@nestjs/core": "^11.0.0" } }"#,
        )
        .expect("package");
        fs::write(
            repo_root.path().join("src/events.ts"),
            r"
import { EventPattern } from '@nestjs/microservices';

type OrderCreatedDto = {
  orderId: number;
  email: string;
};

export class Orders {
  publish(client: any) {
    return client.emit('order.created', { orderId: '123', status: 'active' });
  }

  @EventPattern('order.created')
  handle(data: OrderCreatedDto) {
    return data.email;
  }
}
",
        )
        .expect("fixture");

        let indexer =
            RepoIndexer::open(storage_root.path(), IndexingOptions::default()).expect("indexer");
        indexer
            .index_repo("backend_standard", repo_root.path(), None)
            .expect("index");
        drop(indexer);

        let mut registry =
            RegistryStore::open(storage_root.path().join("registry.json")).expect("registry");
        registry
            .register_repo("backend_standard", repo_root.path(), None)
            .expect("register");

        let ctx = McpContext::open(McpServerConfig::new(
            storage_root.path().join("registry.json"),
            storage_root.path().join("graph.redb"),
        ))
        .expect("context");
        let graph = ctx.graph();
        let target = graph
            .nodes_by_type(NodeKind::Event)
            .expect("events")
            .into_iter()
            .find(|node| node.external_id.as_deref() == Some("__event__kafka__order.created"))
            .expect("target");

        let schema = payload_schema_tool(
            &ctx,
            PayloadSchemaRequest {
                budget_bytes: None,
                include_weak: None,
                target: target.external_id.clone().expect("external id"),
            },
        )
        .expect("schema");
        assert!(!schema.data.producer_schema.is_empty());
        assert!(!schema.data.consumer_schema.is_empty());
        let schema_meta = schema.meta.expect("schema should emit budget meta");
        assert_eq!(
            schema_meta.response_schema_version,
            response_schema_version()
        );

        let drift = contract_drift_tool(
            &ctx,
            ContractDriftRequest {
                budget_bytes: None,
                include_weak: None,
                target: target.external_id.clone().expect("external id"),
            },
        )
        .expect("drift");
        assert!(
            drift
                .data
                .drifts
                .iter()
                .any(|field| field.field_name == "orderId" && field.drift_kind == "type")
        );
        assert_eq!(
            drift.meta.as_ref().map(|meta| meta.response_schema_version),
            Some(response_schema_version()),
        );

        let candidates = breaking_change_candidates_tool(
            &ctx,
            BreakingChangeCandidatesRequest {
                budget_bytes: None,
                include_weak: None,
                symbol_or_dto: "OrderCreatedDto".to_owned(),
            },
        )
        .expect("candidates");
        assert!(!candidates.data.candidates.is_empty());
        assert_eq!(
            candidates
                .meta
                .as_ref()
                .map(|meta| meta.response_schema_version),
            Some(response_schema_version()),
        );
    }
}
