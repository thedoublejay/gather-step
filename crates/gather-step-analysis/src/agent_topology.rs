//! AI agent-flow traversal (v5 Phase 4).
//!
//! `trace_agent` is the AI-flow analogue of [`crate::event_blast_radius`]. Where
//! the event walkers hop producers/consumers through a virtual topic envelope,
//! the AI graph connects real nodes directly: an `AgentGraph` defines `AgentNode`s
//! that `GraphTransitionsTo` one another, and symbols `InvokesLlm` / `BindsTool` /
//! `UsesPrompt` / `RetrievesFrom` / `ProducesAiContract` / `CallsMcpTool`. So this
//! is a plain depth-bounded forward BFS over the AI edge-kind set, surfacing each
//! reachable node with its `ai_role` facet.

use std::collections::VecDeque;

use gather_step_core::{EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphReadSession, GraphStore, GraphStoreError};
use rustc_hash::FxHashSet;
use thiserror::Error;

/// AI edge kinds the agent walker follows forward (out-edges). Mirrors the
/// Phase 0 vocabulary (`schema.rs`, disc 110-123).
const AI_EDGE_KINDS: [EdgeKind; 14] = [
    EdgeKind::DefinesAgentNode,
    EdgeKind::GraphTransitionsTo,
    EdgeKind::ComposesAgent,
    EdgeKind::SpawnsSubagent,
    EdgeKind::BindsTool,
    EdgeKind::InvokesLlm,
    EdgeKind::ProducesAiContract,
    EdgeKind::UsesPrompt,
    EdgeKind::FetchesPromptFrom,
    EdgeKind::RetrievesFrom,
    EdgeKind::Embeds,
    EdgeKind::IndexesVector,
    EdgeKind::CallsMcpTool,
    EdgeKind::ExposesMcpTool,
];

#[derive(Debug, Error)]
pub enum AgentTopologyError {
    #[error(transparent)]
    Store(#[from] GraphStoreError),
}

/// One node reached while walking the agent flow, tagged with the BFS `depth`
/// at which it was first seen and its `ai_role` facet (when set).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTraceNode {
    pub node_id: NodeId,
    pub node_kind: NodeKind,
    pub ai_role: Option<String>,
    pub name: String,
    pub repo: String,
    pub file_path: String,
    pub line_number: Option<u32>,
    pub depth: usize,
}

/// One AI edge traversed during the walk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTraceEdge {
    pub source: NodeId,
    pub target: NodeId,
    pub edge_kind: EdgeKind,
    pub confidence: Option<u16>,
}

/// The forward AI-flow topology reachable from a target node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentTrace {
    pub target: NodeData,
    pub nodes: Vec<AgentTraceNode>,
    pub edges: Vec<AgentTraceEdge>,
    pub truncated: bool,
}

/// Walk the AI flow forward from `target`, following [`AI_EDGE_KINDS`] up to
/// `max_depth` hops and `limit` nodes.
///
/// Returns an empty trace (with a placeholder target) when `target` is absent
/// from the graph. `truncated` is set when the node/edge cap is hit, or when
/// deeper AI hops exist beyond `max_depth`.
///
/// Opens a single bounded read session at the start of the trace; all BFS hops
/// reuse that session's read transaction and do point lookups instead of
/// opening a new redb read transaction per hop or materializing the whole
/// graph via a CSR snapshot.
pub fn trace_agent<S: GraphStore>(
    store: &S,
    target: NodeId,
    max_depth: usize,
    limit: usize,
) -> Result<AgentTrace, AgentTopologyError> {
    let session = store.read_session()?;

    let Some(target_node) = session.node(target)? else {
        return Ok(AgentTrace {
            target: missing_agent_node(target),
            nodes: Vec::new(),
            edges: Vec::new(),
            truncated: false,
        });
    };

    let mut nodes: Vec<AgentTraceNode> = Vec::new();
    let mut edges: Vec<AgentTraceEdge> = Vec::new();
    let mut queue: VecDeque<(NodeId, usize)> = VecDeque::from([(target_node.id, 0_usize)]);
    let mut seen_nodes = FxHashSet::from_iter([target_node.id.as_bytes()]);
    let mut seen_edges = FxHashSet::default();
    let mut truncated = false;
    let edge_limit = limit.saturating_mul(4);

    while let Some((current, depth)) = queue.pop_front() {
        if depth >= max_depth {
            if session_has_ai_out_edge(session.as_ref(), current)? {
                truncated = true;
            }
            continue;
        }

        for ai_edge in session_ai_out_edges(session.as_ref(), current)? {
            let Some(next) = session.node(ai_edge.target)? else {
                continue;
            };

            // Decide node inclusion BEFORE recording the edge so no edge ever
            // references a node the cap dropped. A node is includable when it is
            // already known (in `nodes`, or the root target) or there is room
            // under the node cap.
            let already_known = seen_nodes.contains(&next.id.as_bytes());
            if !already_known && nodes.len() >= limit {
                truncated = true;
                continue;
            }

            let edge_key = (
                ai_edge.source.as_bytes(),
                ai_edge.target.as_bytes(),
                ai_edge.edge_kind.as_u8(),
            );
            if seen_edges.insert(edge_key) {
                edges.push(ai_edge);
                if edges.len() >= edge_limit {
                    truncated = true;
                    break;
                }
            }

            if seen_nodes.insert(next.id.as_bytes()) {
                nodes.push(AgentTraceNode {
                    node_id: next.id,
                    node_kind: next.kind,
                    ai_role: next.ai_role.clone(),
                    name: next.name.clone(),
                    repo: next.repo.clone(),
                    file_path: next.file_path.clone(),
                    line_number: next.span.as_ref().map(|span| span.line_start),
                    depth: depth + 1,
                });
                queue.push_back((next.id, depth + 1));
            }
        }
    }

    nodes.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.name.cmp(&right.name))
            .then(left.node_id.as_bytes().cmp(&right.node_id.as_bytes()))
    });
    edges.sort_by(|left, right| {
        left.source
            .as_bytes()
            .cmp(&right.source.as_bytes())
            .then(left.target.as_bytes().cmp(&right.target.as_bytes()))
            .then(left.edge_kind.as_u8().cmp(&right.edge_kind.as_u8()))
    });

    Ok(AgentTrace {
        target: target_node,
        nodes,
        edges,
        truncated,
    })
}

/// Node kinds that can be a `trace_agent` entry point. `AgentGraph` is the
/// canonical start; the AI leaf kinds let a user trace from a specific model /
/// tool / prompt / index / MCP node by name.
const AI_TARGET_KINDS: [NodeKind; 6] = [
    NodeKind::AgentGraph,
    NodeKind::LlmModel,
    NodeKind::Prompt,
    NodeKind::VectorIndex,
    NodeKind::McpServer,
    NodeKind::McpTool,
];

/// Resolve a user-supplied `target` string to candidate AI nodes for tracing.
///
/// Matches an AI node whose `qualified_name`, `external_id`, or `name` equals
/// `target`, falling back to a `qualified_name` suffix match (so a bare graph
/// name resolves the `__agent_graph__<file>` qn). Exact matches rank before
/// suffix matches; ties break deterministically by `(repo, file, qn)`.
pub fn resolve_agent_targets<S: GraphStore>(
    store: &S,
    target: &str,
) -> Result<Vec<NodeData>, AgentTopologyError> {
    let mut exact: Vec<NodeData> = Vec::new();
    let mut suffix: Vec<NodeData> = Vec::new();
    for kind in AI_TARGET_KINDS {
        for node in store.nodes_by_type(kind)? {
            let qn = node.qualified_name.as_deref();
            if qn == Some(target)
                || node.external_id.as_deref() == Some(target)
                || node.name == target
            {
                exact.push(node);
            } else if qn.is_some_and(|q| q.ends_with(target)) {
                suffix.push(node);
            }
        }
    }
    let sort_key = |n: &NodeData| {
        (
            n.repo.clone(),
            n.file_path.clone(),
            n.qualified_name.clone().unwrap_or_default(),
        )
    };
    exact.sort_by_key(sort_key);
    suffix.sort_by_key(sort_key);
    exact.extend(suffix);
    Ok(exact)
}

fn session_ai_out_edges(
    session: &dyn GraphReadSession,
    source: NodeId,
) -> Result<Vec<AgentTraceEdge>, AgentTopologyError> {
    Ok(session
        .outgoing(source)?
        .into_iter()
        .filter(|edge| AI_EDGE_KINDS.contains(&edge.kind))
        .map(|edge| AgentTraceEdge {
            source: edge.source,
            target: edge.target,
            edge_kind: edge.kind,
            confidence: edge.metadata.confidence,
        })
        .collect())
}

fn session_has_ai_out_edge(
    session: &dyn GraphReadSession,
    source: NodeId,
) -> Result<bool, AgentTopologyError> {
    Ok(session
        .outgoing(source)?
        .iter()
        .any(|edge| AI_EDGE_KINDS.contains(&edge.kind)))
}

fn missing_agent_node(id: NodeId) -> NodeData {
    NodeData {
        id,
        kind: NodeKind::AgentGraph,
        repo: "__virtual__".to_owned(),
        file_path: String::new(),
        name: String::new(),
        qualified_name: None,
        external_id: None,
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
        ai_role: None,
    }
}

#[cfg(test)]
mod tests {
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, SourceSpan, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};
    use rustc_hash::FxHashSet;

    use super::{resolve_agent_targets, trace_agent};
    use crate::test_utils::TempDb;

    fn ai_node(
        repo: &str,
        file: &str,
        kind: NodeKind,
        name: &str,
        ai_role: Option<&str>,
        ordinal: u32,
    ) -> NodeData {
        NodeData {
            id: node_id(repo, file, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 10 + ordinal,
                line_len: 0,
                column_start: 0,
                column_len: 4,
            }),
            is_virtual: false,
            ai_role: ai_role.map(str::to_owned),
        }
    }

    fn file_node(repo: &str, file_path: &str) -> NodeData {
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
            ai_role: None,
        }
    }

    fn edge(source: NodeId, target: NodeId, kind: EdgeKind, owner: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file: owner,
            is_cross_file: false,
        }
    }

    // A graph: AgentGraph -DefinesAgentNode-> intent -GraphTransitionsTo-> respond,
    // intent -InvokesLlm-> LlmModel, respond -ProducesAiContract-> AiContract.
    // Tracing from the graph reaches every AI node and edge.
    #[test]
    fn trace_agent_follows_graph_llm_and_contract_edges() {
        let temp = TempDb::new("agent-topology", "reach");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let repo = "agent_repo";
        let file = "src/agent.ts";

        let src = file_node(repo, file);
        let graph = ai_node(
            repo,
            file,
            NodeKind::AgentGraph,
            "__agent_graph__agent",
            None,
            0,
        );
        let intent = ai_node(
            repo,
            file,
            NodeKind::Function,
            "intentNode",
            Some("agent_node"),
            1,
        );
        let respond = ai_node(
            repo,
            file,
            NodeKind::Function,
            "respondNode",
            Some("agent_node"),
            2,
        );
        let llm = ai_node(
            repo,
            file,
            NodeKind::LlmModel,
            "__llm__openai__gpt-4.1",
            None,
            3,
        );
        let contract = ai_node(
            repo,
            file,
            NodeKind::AiContract,
            "__ai_contract__x",
            None,
            4,
        );

        store
            .bulk_insert(
                &[
                    src.clone(),
                    graph.clone(),
                    intent.clone(),
                    respond.clone(),
                    llm.clone(),
                    contract.clone(),
                ],
                &[
                    edge(graph.id, intent.id, EdgeKind::DefinesAgentNode, src.id),
                    edge(graph.id, respond.id, EdgeKind::DefinesAgentNode, src.id),
                    edge(intent.id, respond.id, EdgeKind::GraphTransitionsTo, src.id),
                    edge(intent.id, llm.id, EdgeKind::InvokesLlm, src.id),
                    edge(
                        respond.id,
                        contract.id,
                        EdgeKind::ProducesAiContract,
                        src.id,
                    ),
                ],
            )
            .expect("insert");

        let trace = trace_agent(&store, graph.id, 8, 100).expect("trace_agent");

        let kinds: Vec<NodeKind> = trace.nodes.iter().map(|n| n.node_kind).collect();
        assert!(
            kinds.contains(&NodeKind::LlmModel),
            "LlmModel must be reached; got {kinds:?}"
        );
        assert!(
            kinds.contains(&NodeKind::AiContract),
            "AiContract must be reached; got {kinds:?}"
        );
        assert_eq!(
            trace
                .nodes
                .iter()
                .filter(|n| n.ai_role.as_deref() == Some("agent_node"))
                .count(),
            2,
            "both agent nodes reached and labelled"
        );
        assert!(
            trace
                .edges
                .iter()
                .any(|e| e.edge_kind == EdgeKind::GraphTransitionsTo),
            "GraphTransitionsTo edge surfaced"
        );
        assert!(!trace.truncated, "no truncation at limit 100");
    }

    // `max_depth = 1` stops after the graph's direct AgentNodes and flags
    // truncation because deeper AI hops exist.
    #[test]
    fn trace_agent_respects_max_depth() {
        let temp = TempDb::new("agent-topology", "depth");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let repo = "agent_repo";
        let file = "src/agent.ts";

        let src = file_node(repo, file);
        let graph = ai_node(
            repo,
            file,
            NodeKind::AgentGraph,
            "__agent_graph__agent",
            None,
            0,
        );
        let intent = ai_node(
            repo,
            file,
            NodeKind::Function,
            "intentNode",
            Some("agent_node"),
            1,
        );
        let llm = ai_node(
            repo,
            file,
            NodeKind::LlmModel,
            "__llm__openai__gpt-4.1",
            None,
            2,
        );

        store
            .bulk_insert(
                &[src.clone(), graph.clone(), intent.clone(), llm.clone()],
                &[
                    edge(graph.id, intent.id, EdgeKind::DefinesAgentNode, src.id),
                    edge(intent.id, llm.id, EdgeKind::InvokesLlm, src.id),
                ],
            )
            .expect("insert");

        let trace = trace_agent(&store, graph.id, 1, 100).expect("trace_agent");

        let kinds: Vec<NodeKind> = trace.nodes.iter().map(|n| n.node_kind).collect();
        assert!(
            kinds.contains(&NodeKind::Function),
            "depth-1 agent node reached"
        );
        assert!(
            !kinds.contains(&NodeKind::LlmModel),
            "depth-2 LlmModel must NOT be reached at max_depth 1"
        );
        assert!(trace.truncated, "deeper hops exist → truncated");
    }

    // Under a small node cap, no surfaced edge may reference a node that was
    // dropped by the cap (review finding #4). Edges to the root target are fine
    // because the consumer has it via `trace.target`.
    #[test]
    fn trace_agent_cap_never_leaves_dangling_edges() {
        let temp = TempDb::new("agent-topology", "cap");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let repo = "agent_repo";
        let file = "src/agent.ts";

        let src = file_node(repo, file);
        let graph = ai_node(
            repo,
            file,
            NodeKind::AgentGraph,
            "__agent_graph__agent",
            None,
            0,
        );
        let intent = ai_node(
            repo,
            file,
            NodeKind::Function,
            "intentNode",
            Some("agent_node"),
            1,
        );
        let respond = ai_node(
            repo,
            file,
            NodeKind::Function,
            "respondNode",
            Some("agent_node"),
            2,
        );
        let llm = ai_node(
            repo,
            file,
            NodeKind::LlmModel,
            "__llm__openai__gpt-4.1",
            None,
            3,
        );
        let contract = ai_node(
            repo,
            file,
            NodeKind::AiContract,
            "__ai_contract__x",
            None,
            4,
        );

        store
            .bulk_insert(
                &[
                    src.clone(),
                    graph.clone(),
                    intent.clone(),
                    respond.clone(),
                    llm.clone(),
                    contract.clone(),
                ],
                &[
                    edge(graph.id, intent.id, EdgeKind::DefinesAgentNode, src.id),
                    edge(graph.id, respond.id, EdgeKind::DefinesAgentNode, src.id),
                    edge(intent.id, respond.id, EdgeKind::GraphTransitionsTo, src.id),
                    edge(intent.id, llm.id, EdgeKind::InvokesLlm, src.id),
                    edge(
                        respond.id,
                        contract.id,
                        EdgeKind::ProducesAiContract,
                        src.id,
                    ),
                ],
            )
            .expect("insert");

        let trace = trace_agent(&store, graph.id, 8, 2).expect("trace_agent");

        assert!(trace.truncated, "node cap of 2 must truncate");
        let present: FxHashSet<_> = trace.nodes.iter().map(|n| n.node_id.as_bytes()).collect();
        for e in &trace.edges {
            let resolvable = e.target.as_bytes() == graph.id.as_bytes()
                || present.contains(&e.target.as_bytes());
            assert!(
                resolvable,
                "edge target {:?} is neither the root nor a surfaced node",
                e.target
            );
        }
    }

    // The resolver matches an AgentGraph by exact qualified_name and by a bare
    // name suffix, and ranks exact matches first.
    #[test]
    fn resolve_agent_targets_matches_qn_and_suffix() {
        let temp = TempDb::new("agent-topology", "resolve");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let repo = "agent_repo";
        let file = "src/agent.ts";
        let src = file_node(repo, file);
        let graph = ai_node(
            repo,
            file,
            NodeKind::AgentGraph,
            "__agent_graph__agent",
            None,
            0,
        );
        store
            .bulk_insert(&[src, graph.clone()], &[])
            .expect("insert");

        // Exact qualified_name.
        let by_qn = resolve_agent_targets(&store, "__agent_graph__agent").expect("resolve");
        assert_eq!(by_qn.len(), 1);
        assert_eq!(by_qn[0].id.as_bytes(), graph.id.as_bytes());

        // Suffix (bare name).
        let by_suffix = resolve_agent_targets(&store, "agent").expect("resolve");
        assert!(
            by_suffix
                .iter()
                .any(|n| n.id.as_bytes() == graph.id.as_bytes()),
            "suffix match must find the graph"
        );

        // No match.
        assert!(
            resolve_agent_targets(&store, "nonexistent")
                .expect("resolve")
                .is_empty()
        );
    }

    // A target with no node in the graph yields an empty trace, not an error.
    #[test]
    fn trace_agent_missing_target_is_empty() {
        let temp = TempDb::new("agent-topology", "missing");
        let store = GraphStoreDb::open(temp.path()).expect("store should open");
        let ghost = node_id("nope", "nope.ts", NodeKind::AgentGraph, "ghost");

        let trace = trace_agent(&store, ghost, 8, 100).expect("trace_agent");

        assert!(trace.nodes.is_empty(), "no nodes for a missing target");
        assert!(trace.edges.is_empty(), "no edges for a missing target");
        assert!(!trace.truncated);
    }
}
