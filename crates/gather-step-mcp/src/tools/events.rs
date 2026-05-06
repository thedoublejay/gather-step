use gather_step_analysis::{
    EventRole, RouteRole, canonical_event_target, event_blast_radius, list_orphan_topics,
    resolve_event_targets, resolve_route_target,
};
use gather_step_core::{NodeId, NodeKind, WorkspaceRegistry};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    evidence::{
        Evidence, EvidenceCitation, EvidenceKind, EvidenceSource, EvidenceSubject, EvidenceSupport,
        EvidenceSupportMethod,
    },
    ids::encode_node_id,
    tools::labels::{edge_kind_label, node_kind_label},
};

const DEFAULT_TOPOLOGY_LIMIT: usize = 25;
const DEFAULT_BLAST_RADIUS_DEPTH: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceEventRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceRouteRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub method: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EventBlastRadiusRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ListOrphanTopicsRequest {
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceEventResponse {
    pub data: TraceEventData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<TopologyMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceEventData {
    pub matches: Vec<EventTraceResult>,
    pub returned: usize,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EventTraceResult {
    pub consumers: Vec<TopologySymbol>,
    pub event_kind: String,
    pub producers: Vec<TopologySymbol>,
    pub target_id: String,
    pub target_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qualified_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceRouteResponse {
    pub data: TraceRouteData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<TopologyMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EventBlastRadiusResponse {
    pub data: EventBlastRadiusData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<TopologyMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct EventBlastRadiusData {
    pub edges: Vec<BlastRadiusEdgeItem>,
    pub nodes: Vec<BlastRadiusNodeItem>,
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BlastRadiusNodeItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
    pub depth: usize,
    pub file_path: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub repo: String,
    pub symbol_id: String,
    pub symbol_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BlastRadiusEdgeItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
    pub edge_kind: String,
    pub source_id: String,
    pub target_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ListOrphanTopicsResponse {
    pub data: ListOrphanTopicsData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ListOrphanTopicsData {
    pub orphans: Vec<OrphanTopicItem>,
    pub returned: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct OrphanTopicItem {
    pub classification: String,
    pub consumers: usize,
    pub kind: String,
    pub name: String,
    pub producers: usize,
    pub severity: String,
    pub target_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceRouteData {
    pub callers: Vec<TopologySymbol>,
    pub handlers: Vec<TopologySymbol>,
    pub method: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TopologyMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub budget: ResponseBudget,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TopologySymbol {
    pub edge_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
    pub evidence: Evidence,
    pub file_path: String,
    pub framework_context: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub repo: String,
    pub role: String,
    pub symbol_id: String,
    pub symbol_kind: String,
    pub symbol_name: String,
}

pub fn trace_event_tool(
    ctx: &McpContext,
    request: TraceEventRequest,
) -> Result<TraceEventResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let graph = ctx.graph();
    let registry = ctx.registry_snapshot()?;
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_TOPOLOGY_LIMIT);
    let mut matches = Vec::new();
    let mut truncated = false;
    if let Some(target) = canonical_event_target(graph, &request.target)? {
        let trace = gather_step_analysis::trace_event(graph, target.id, limit)?;
        truncated |= trace.truncated;
        matches.push(EventTraceResult {
            consumers: trace
                .consumers
                .into_iter()
                .map(|entry| {
                    topology_symbol(
                        &registry,
                        entry.node_id,
                        entry.repo,
                        entry.file_path,
                        entry.line_number,
                        entry.confidence,
                        entry.symbol_name,
                        entry.node_kind,
                        entry.edge_kind,
                        EventRole::Consumer,
                    )
                })
                .collect(),
            event_kind: node_kind_label(trace.target.kind).to_owned(),
            producers: trace
                .producers
                .into_iter()
                .map(|entry| {
                    topology_symbol(
                        &registry,
                        entry.node_id,
                        entry.repo,
                        entry.file_path,
                        entry.line_number,
                        entry.confidence,
                        entry.symbol_name,
                        entry.node_kind,
                        entry.edge_kind,
                        EventRole::Producer,
                    )
                })
                .collect(),
            target_id: encode_node_id(trace.target.id),
            target_name: trace.target.name,
            qualified_name: trace.target.qualified_name,
        });
    }

    for result in &mut matches {
        sort_topology_symbols(&mut result.producers);
        sort_topology_symbols(&mut result.consumers);
    }
    let mut response = TraceEventResponse {
        data: TraceEventData {
            returned: matches.len(),
            matches,
            target: request.target,
        },
        meta: Some(TopologyMeta {
            response_schema_version: response_schema_version(),
            budget: ResponseBudget::not_truncated(BudgetedTool::TraceEvent, 0, 0),
            truncated,
        }),
    };
    let budget = apply_response_budget(
        BudgetedTool::TraceEvent,
        request.budget_bytes,
        &mut response,
        trim_trace_event_response,
    )?;
    response.data.returned = response.data.matches.len();
    if let Some(meta) = &mut response.meta {
        meta.budget = budget;
        meta.truncated |= meta.budget.truncated;
    }
    Ok(response)
}

pub fn trace_route_tool(
    ctx: &McpContext,
    request: TraceRouteRequest,
) -> Result<TraceRouteResponse, McpServerError> {
    validate_input_length("method", &request.method)?;
    validate_input_length("path", &request.path)?;
    let graph = ctx.graph();
    let registry = ctx.registry_snapshot()?;
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_TOPOLOGY_LIMIT);
    let route = resolve_route_target(graph, &request.method, &request.path)?;

    let response = if let Some(route) = route {
        let trace = gather_step_analysis::trace_route(graph, route.id, limit)?;
        let mut response = TraceRouteResponse {
            data: TraceRouteData {
                callers: trace
                    .callers
                    .into_iter()
                    .map(|entry| {
                        topology_symbol(
                            &registry,
                            entry.node_id,
                            entry.repo,
                            entry.file_path,
                            entry.line_number,
                            entry.confidence,
                            entry.symbol_name,
                            entry.node_kind,
                            entry.edge_kind,
                            RouteRole::Caller,
                        )
                    })
                    .collect(),
                handlers: trace
                    .handlers
                    .into_iter()
                    .map(|entry| {
                        topology_symbol(
                            &registry,
                            entry.node_id,
                            entry.repo,
                            entry.file_path,
                            entry.line_number,
                            entry.confidence,
                            entry.symbol_name,
                            entry.node_kind,
                            entry.edge_kind,
                            RouteRole::Handler,
                        )
                    })
                    .collect(),
                method: request.method,
                path: request.path,
                target_id: Some(encode_node_id(trace.target.id)),
                target_name: Some(trace.target.name),
            },
            meta: Some(TopologyMeta {
                response_schema_version: response_schema_version(),
                budget: ResponseBudget::not_truncated(BudgetedTool::TraceRoute, 0, 0),
                truncated: trace.truncated,
            }),
        };
        sort_topology_symbols(&mut response.data.callers);
        sort_topology_symbols(&mut response.data.handlers);
        let budget = apply_response_budget(
            BudgetedTool::TraceRoute,
            request.budget_bytes,
            &mut response,
            trim_trace_route_response,
        )?;
        if let Some(meta) = &mut response.meta {
            meta.budget = budget;
            meta.truncated |= meta.budget.truncated;
        }
        response
    } else {
        TraceRouteResponse {
            data: TraceRouteData {
                callers: Vec::new(),
                handlers: Vec::new(),
                method: request.method,
                path: request.path,
                target_id: None,
                target_name: None,
            },
            meta: Some(TopologyMeta {
                response_schema_version: response_schema_version(),
                budget: ResponseBudget::not_truncated(
                    BudgetedTool::TraceRoute,
                    BudgetedTool::TraceRoute.default_bytes(),
                    0,
                ),
                truncated: false,
            }),
        }
    };

    Ok(response)
}

pub fn event_blast_radius_tool(
    ctx: &McpContext,
    request: EventBlastRadiusRequest,
) -> Result<EventBlastRadiusResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let graph = ctx.graph();
    let depth = request.depth.unwrap_or(DEFAULT_BLAST_RADIUS_DEPTH).min(5);
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_TOPOLOGY_LIMIT * 4);
    let target = resolve_single_event_target(graph, &request.target)?;
    let blast = event_blast_radius(graph, target.id, depth, limit)?;

    let mut response = EventBlastRadiusResponse {
        data: EventBlastRadiusData {
            edges: blast
                .edges
                .into_iter()
                .map(|edge| BlastRadiusEdgeItem {
                    confidence: edge.confidence,
                    edge_kind: edge_kind_label(edge.edge_kind).to_owned(),
                    source_id: encode_node_id(edge.source),
                    target_id: encode_node_id(edge.target),
                })
                .collect(),
            nodes: blast
                .nodes
                .into_iter()
                .map(|node| BlastRadiusNodeItem {
                    confidence: node.cumulative_confidence,
                    depth: node.depth,
                    file_path: node.file_path,
                    kind: node_kind_label(node.node_kind).to_owned(),
                    line_start: node.line_number,
                    repo: node.repo,
                    symbol_id: encode_node_id(node.node_id),
                    symbol_name: node.name,
                })
                .collect(),
            target: request.target,
            target_id: Some(encode_node_id(blast.target.id)),
        },
        meta: Some(TopologyMeta {
            response_schema_version: response_schema_version(),
            budget: ResponseBudget::not_truncated(BudgetedTool::EventBlastRadius, 0, 0),
            truncated: blast.truncated,
        }),
    };
    sort_blast_nodes(&mut response.data.nodes);
    sort_blast_edges(&mut response.data.edges);
    let budget = apply_response_budget(
        BudgetedTool::EventBlastRadius,
        request.budget_bytes,
        &mut response,
        trim_blast_radius_response,
    )?;
    if let Some(meta) = &mut response.meta {
        meta.budget = budget;
        meta.truncated |= meta.budget.truncated;
    }
    Ok(response)
}

fn resolve_single_event_target(
    graph: &impl gather_step_storage::GraphStore,
    target: &str,
) -> Result<gather_step_core::NodeData, McpServerError> {
    let matches = resolve_event_targets(graph, target)?;
    match matches.as_slice() {
        [] => Err(McpServerError::NotFound(format!(
            "event target `{target}` was not found"
        ))),
        [node] => Ok(node.clone()),
        _ => {
            let choices = matches
                .iter()
                .map(|node| format!("{}:{} ({})", node.repo, node.file_path, node.name))
                .collect::<Vec<_>>()
                .join(", ");
            Err(McpServerError::InvalidInput(format!(
                "event target `{target}` is ambiguous; refine the target or scope it to a repo: {choices}"
            )))
        }
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn list_orphan_topics_tool(
    ctx: &McpContext,
    request: ListOrphanTopicsRequest,
) -> Result<ListOrphanTopicsResponse, McpServerError> {
    if let Some(repo) = &request.repo {
        validate_input_length("repo", repo)?;
    }
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_TOPOLOGY_LIMIT * 4);
    let orphans = list_orphan_topics(ctx.graph(), request.repo.as_deref(), limit)?
        .into_iter()
        .map(|orphan| OrphanTopicItem {
            classification: orphan.classification.to_owned(),
            consumers: orphan.consumers,
            kind: node_kind_label(orphan.target.kind).to_owned(),
            name: orphan.target.name,
            producers: orphan.producers,
            severity: orphan.severity.to_owned(),
            target_id: encode_node_id(orphan.target.id),
        })
        .collect::<Vec<_>>();
    Ok(ListOrphanTopicsResponse {
        data: ListOrphanTopicsData {
            returned: orphans.len(),
            orphans,
        },
    })
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "topology role is a small by-value strategy object used only for trait dispatch"
)]
fn topology_symbol(
    registry: &WorkspaceRegistry,
    node_id: NodeId,
    repo: String,
    file_path: String,
    line_start: Option<u32>,
    confidence: Option<u16>,
    symbol_name: String,
    symbol_kind: NodeKind,
    edge_kind: gather_step_core::EdgeKind,
    role: impl TopologyRole,
) -> TopologySymbol {
    let framework_context = registry
        .repos
        .get(&repo)
        .map(|registered| registered.frameworks.clone())
        .unwrap_or_default();

    let role_label = role.label().to_owned();
    let symbol_id = encode_node_id(node_id);
    let symbol_kind = node_kind_label(symbol_kind).to_owned();
    let edge_kind_label = edge_kind_label(edge_kind).to_owned();
    let evidence = Evidence::new(
        role.evidence_kind(),
        role.evidence_source(),
        EvidenceCitation::symbol(
            repo.clone(),
            file_path.clone(),
            line_start,
            symbol_id.clone(),
            symbol_kind.clone(),
            symbol_name.clone(),
        ),
    )
    .with_subject(
        EvidenceSubject::new(
            if matches!(role.evidence_source(), EvidenceSource::TraceEvent) {
                "event"
            } else {
                "route"
            },
        )
        .with_category(role_label.clone())
        .with_name(symbol_name.clone())
        .with_reason(format!("{} edge", edge_kind_label)),
    )
    .with_support(EvidenceSupport::new(
        EvidenceSupportMethod::GraphTraversal,
        confidence,
    ));

    TopologySymbol {
        edge_kind: edge_kind_label,
        confidence,
        evidence,
        file_path,
        framework_context,
        line_start,
        repo,
        role: role_label,
        symbol_id,
        symbol_kind,
        symbol_name,
    }
}

fn sort_topology_symbols(items: &mut [TopologySymbol]) {
    items.sort_by(|left, right| {
        right
            .confidence
            .cmp(&left.confidence)
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

fn sort_blast_nodes(items: &mut [BlastRadiusNodeItem]) {
    items.sort_by(|left, right| {
        right
            .confidence
            .cmp(&left.confidence)
            .then(left.depth.cmp(&right.depth))
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

fn sort_blast_edges(items: &mut [BlastRadiusEdgeItem]) {
    items.sort_by(|left, right| {
        right
            .confidence
            .cmp(&left.confidence)
            .then(left.edge_kind.cmp(&right.edge_kind))
            .then(left.source_id.cmp(&right.source_id))
            .then(left.target_id.cmp(&right.target_id))
    });
}

fn trim_trace_event_response(response: &mut TraceEventResponse) -> bool {
    for result in response.data.matches.iter_mut().rev() {
        if result.consumers.pop().is_some() {
            return true;
        }
        if result.producers.pop().is_some() {
            return true;
        }
    }
    response.data.matches.pop().is_some()
}

fn trim_trace_route_response(response: &mut TraceRouteResponse) -> bool {
    response.data.callers.pop().is_some() || response.data.handlers.pop().is_some()
}

fn trim_blast_radius_response(response: &mut EventBlastRadiusResponse) -> bool {
    response.data.edges.pop().is_some() || response.data.nodes.pop().is_some()
}

trait TopologyRole {
    fn label(&self) -> &'static str;
    fn evidence_kind(&self) -> EvidenceKind;
    fn evidence_source(&self) -> EvidenceSource;
}

impl TopologyRole for EventRole {
    fn label(&self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }

    fn evidence_kind(&self) -> EvidenceKind {
        match self {
            Self::Producer => EvidenceKind::EventProducer,
            Self::Consumer => EvidenceKind::EventConsumer,
        }
    }

    fn evidence_source(&self) -> EvidenceSource {
        EvidenceSource::TraceEvent
    }
}

impl TopologyRole for RouteRole {
    fn label(&self) -> &'static str {
        match self {
            Self::Handler => "handler",
            Self::Caller => "caller",
        }
    }

    fn evidence_kind(&self) -> EvidenceKind {
        match self {
            Self::Handler => EvidenceKind::RouteHandler,
            Self::Caller => EvidenceKind::RouteCaller,
        }
    }

    fn evidence_source(&self) -> EvidenceSource {
        EvidenceSource::TraceRoute
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

    use gather_step_core::{
        DepthLevel, EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, RegistryStore,
        RepoIndexMetadata, SourceSpan, Visibility, node_id, route_qn, topic_qn, virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use crate::{
        McpServerConfig, config::McpContext, evidence::EvidenceKind, evidence::EvidenceSource,
        ids::encode_node_id,
    };

    use super::{
        EventBlastRadiusRequest, ListOrphanTopicsRequest, TraceEventRequest, TraceRouteRequest,
        event_blast_radius_tool, list_orphan_topics_tool, trace_event_tool, trace_route_tool,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gather-step-events-{name}-{}-{id}", process::id()));
            fs::create_dir_all(&path).expect("temp dir should create");
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

    fn file(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: Some(format!("{repo}::{file_path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn symbol(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
            visibility: Some(Visibility::Public),
            span: Some(SourceSpan {
                line_start: 20 + u32::from(ordinal),
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
        }
    }

    #[test]
    fn trace_event_tool_returns_framework_context() {
        let temp = TempDir::new("trace-event");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");

        let producer_file = file("backend_standard", "src/producer.ts");
        let consumer_file = file("frontend_standard", "src/consumer.ts");
        let producer = symbol("backend_standard", "src/producer.ts", "emit_order", 0);
        let consumer = symbol("frontend_standard", "src/consumer.ts", "handle_order", 0);
        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );

        graph
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    producer.clone(),
                    consumer.clone(),
                    topic.clone(),
                ],
                &[
                    EdgeData {
                        source: producer.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata {
                            confidence: Some(950),
                            ..EdgeMetadata::default()
                        },
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(910),
                            ..EdgeMetadata::default()
                        },
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");
        registry
            .register_repo(
                "frontend_standard",
                temp.path().join("repos/frontend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");
        registry
            .update_repo_metadata(
                "backend_standard",
                RepoIndexMetadata {
                    last_indexed_at: None,
                    file_count: 1,
                    symbol_count: 2,
                    frameworks: vec!["nestjs".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata update should succeed");
        registry
            .update_repo_metadata(
                "frontend_standard",
                RepoIndexMetadata {
                    last_indexed_at: None,
                    file_count: 1,
                    symbol_count: 2,
                    frameworks: vec!["react".to_owned()],
                    depth_level: DepthLevel::Full,
                },
            )
            .expect("metadata update should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let response = trace_event_tool(
            &ctx,
            TraceEventRequest {
                budget_bytes: None,
                limit: None,
                target: "order.created".to_owned(),
            },
        )
        .expect("tool should succeed");

        assert_eq!(response.data.returned, 1);
        assert_eq!(response.data.matches[0].target_id, encode_node_id(topic.id));
        assert_eq!(
            response.data.matches[0].producers[0].framework_context,
            vec!["nestjs"]
        );
        assert_eq!(
            response.data.matches[0].consumers[0].framework_context,
            vec!["react"]
        );
        assert_eq!(
            response.data.matches[0].producers[0].evidence.kind,
            EvidenceKind::EventProducer
        );
        assert_eq!(
            response.data.matches[0].consumers[0].evidence.source,
            EvidenceSource::TraceEvent
        );
    }

    #[test]
    fn trace_route_tool_returns_handlers_and_callers() {
        let temp = TempDir::new("trace-route");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");

        let handler_file = file("backend_standard", "src/controller.ts");
        let caller_file = file("frontend_standard", "src/api.ts");
        let handler = symbol("backend_standard", "src/controller.ts", "create_order", 0);
        let caller = symbol("frontend_standard", "src/api.ts", "post_order", 0);
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/controller.ts",
            "POST /orders",
            route_qn("POST", "/orders"),
        );

        graph
            .bulk_insert(
                &[
                    handler_file.clone(),
                    caller_file.clone(),
                    handler.clone(),
                    caller.clone(),
                    route.clone(),
                ],
                &[
                    EdgeData {
                        source: handler.id,
                        target: route.id,
                        kind: EdgeKind::Serves,
                        metadata: EdgeMetadata {
                            confidence: Some(990),
                            ..EdgeMetadata::default()
                        },
                        owner_file: handler_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: caller.id,
                        target: route.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(920),
                            ..EdgeMetadata::default()
                        },
                        owner_file: caller_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");
        registry
            .register_repo(
                "frontend_standard",
                temp.path().join("repos/frontend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let response = trace_route_tool(
            &ctx,
            TraceRouteRequest {
                budget_bytes: None,
                limit: None,
                method: "POST".to_owned(),
                path: "/orders".to_owned(),
            },
        )
        .expect("tool should succeed");

        assert_eq!(response.data.target_id, Some(encode_node_id(route.id)));
        assert_eq!(response.data.handlers.len(), 1);
        assert_eq!(response.data.handlers[0].role, "handler");
        assert_eq!(
            response.data.handlers[0].evidence.kind,
            EvidenceKind::RouteHandler
        );
        assert_eq!(response.data.callers.len(), 1);
        assert_eq!(response.data.callers[0].role, "caller");
        assert_eq!(
            response.data.callers[0].evidence.source,
            EvidenceSource::TraceRoute
        );
        assert_eq!(response.data.callers[0].line_start, Some(20));
    }

    #[test]
    fn blast_radius_and_orphan_tools_return_results() {
        let temp = TempDir::new("blast-radius");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");

        let producer_file = file("backend_standard", "src/producer.ts");
        let consumer_file = file("backend_standard", "src/consumer.ts");
        let route_file = file("backend_standard", "src/api.ts");
        let producer = symbol("backend_standard", "src/producer.ts", "emit_order", 0);
        let consumer = symbol("backend_standard", "src/consumer.ts", "handle_order", 0);
        let route_caller = symbol("backend_standard", "src/api.ts", "notify_downstream", 0);
        let topic = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let route = virtual_node(
            NodeKind::Route,
            "backend_standard",
            "src/api.ts",
            "POST /notify",
            route_qn("POST", "/notify"),
        );
        let orphan = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/events.ts",
            "orphan.created",
            topic_qn("kafka", "orphan.created"),
        );

        graph
            .bulk_insert(
                &[
                    producer_file.clone(),
                    consumer_file.clone(),
                    route_file.clone(),
                    producer.clone(),
                    consumer.clone(),
                    route_caller.clone(),
                    topic.clone(),
                    route.clone(),
                    orphan.clone(),
                ],
                &[
                    EdgeData {
                        source: producer.id,
                        target: topic.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata {
                            confidence: Some(950),
                            ..EdgeMetadata::default()
                        },
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: topic.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(920),
                            ..EdgeMetadata::default()
                        },
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: consumer.id,
                        target: route.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(910),
                            ..EdgeMetadata::default()
                        },
                        owner_file: consumer_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: route_caller.id,
                        target: route.id,
                        kind: EdgeKind::Consumes,
                        metadata: EdgeMetadata {
                            confidence: Some(905),
                            ..EdgeMetadata::default()
                        },
                        owner_file: route_file.id,
                        is_cross_file: true,
                    },
                    EdgeData {
                        source: producer.id,
                        target: orphan.id,
                        kind: EdgeKind::Publishes,
                        metadata: EdgeMetadata {
                            confidence: Some(900),
                            ..EdgeMetadata::default()
                        },
                        owner_file: producer_file.id,
                        is_cross_file: true,
                    },
                ],
            )
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let blast = event_blast_radius_tool(
            &ctx,
            EventBlastRadiusRequest {
                budget_bytes: None,
                depth: Some(3),
                limit: None,
                target: "order.created".to_owned(),
            },
        )
        .expect("blast should succeed");
        assert!(!blast.data.nodes.is_empty());
        assert!(blast.data.nodes.iter().any(|node| node.kind == "route"));

        let orphans = list_orphan_topics_tool(
            &ctx,
            ListOrphanTopicsRequest {
                limit: None,
                repo: Some("backend_standard".to_owned()),
            },
        )
        .expect("orphans should succeed");
        assert!(
            orphans
                .data
                .orphans
                .iter()
                .any(|item| item.classification == "produce_only")
        );
    }

    #[test]
    fn blast_radius_rejects_ambiguous_event_targets() {
        let temp = TempDir::new("blast-radius-ambiguous");
        let graph_path = temp.path().join("graph.redb");
        let registry_path = temp.path().join("registry.json");
        let graph = GraphStoreDb::open(&graph_path).expect("graph store should open");

        let topic_a = virtual_node(
            NodeKind::Topic,
            "backend_standard",
            "src/orders.ts",
            "order.created",
            topic_qn("kafka", "order.created"),
        );
        let topic_b = virtual_node(
            NodeKind::Topic,
            "frontend_standard",
            "src/orders.ts",
            "order.created",
            topic_qn("sns", "order.created"),
        );

        graph
            .bulk_insert(&[topic_a, topic_b], &[])
            .expect("graph write should succeed");
        drop(graph);

        let mut registry = RegistryStore::open(&registry_path).expect("registry should open");
        registry
            .register_repo(
                "backend_standard",
                temp.path().join("repos/backend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");
        registry
            .register_repo(
                "frontend_standard",
                temp.path().join("repos/frontend_standard"),
                Some(DepthLevel::Full),
            )
            .expect("repo registration should succeed");

        let ctx = McpContext::open(McpServerConfig::new(registry_path, graph_path))
            .expect("context should open");
        let error = event_blast_radius_tool(
            &ctx,
            EventBlastRadiusRequest {
                budget_bytes: None,
                depth: Some(2),
                limit: None,
                target: "order.created".to_owned(),
            },
        )
        .expect_err("ambiguous target should fail");

        assert!(
            error
                .to_string()
                .contains("event target `order.created` is ambiguous")
        );
    }
}
