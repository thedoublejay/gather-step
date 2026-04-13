use std::collections::{BTreeMap, BTreeSet};

use gather_step_analysis::{cross_repo_deps, trace_across_repos};
use gather_step_core::{NodeId, NodeKind};
use gather_step_storage::GraphStore;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    ids::{decode_node_id, encode_node_id},
    tools::labels::edge_kind_label,
};

const DEFAULT_TRACE_DEPTH: usize = 2;
const MAX_TRACE_DEPTH: usize = 5;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceImpactRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrossRepoDepsRequest {
    pub repo: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SharedTypeUsageRequest {
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImpactHop {
    pub direction: String,
    pub edge_kind: String,
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
    pub repo: String,
    pub symbol_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceImpactResponse {
    pub data: TraceImpactData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<TraceImpactMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceImpactData {
    pub impacted_repos: Vec<ImpactRepo>,
    pub target: String,
    pub virtual_targets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraceImpactMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub budget: ResponseBudget,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ImpactRepo {
    pub hops: Vec<ImpactHop>,
    pub repo: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrossRepoDepsResponse {
    pub data: CrossRepoDepsData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrossRepoDepsData {
    pub dependencies: Vec<RepoDependency>,
    pub repo: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RepoDependency {
    pub edge_kinds: Vec<String>,
    pub repo: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SharedTypeUsageResponse {
    pub data: SharedTypeUsageData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SharedTypeUsageData {
    pub matches: Vec<SharedTypeMatch>,
    pub type_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SharedTypeMatch {
    pub external_id: Option<String>,
    pub qualified_name: Option<String>,
    pub symbol_id: String,
    pub usages: Vec<SharedTypeUsageRepo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SharedTypeUsageRepo {
    pub files: Vec<String>,
    pub repo: String,
}

pub fn trace_impact_tool(
    ctx: &McpContext,
    request: TraceImpactRequest,
) -> Result<TraceImpactResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let graph = ctx.graph();
    let max_depth = request
        .depth
        .unwrap_or(DEFAULT_TRACE_DEPTH)
        .min(MAX_TRACE_DEPTH);
    let virtual_targets = resolve_virtual_targets(graph, &request.target)?;
    if virtual_targets.is_empty() {
        return Err(McpServerError::NotFound(format!(
            "no indexed symbol or virtual target matched `{}`",
            request.target
        )));
    }

    let mut grouped = BTreeMap::<String, Vec<ImpactHop>>::new();
    let mut seen_virtuals = BTreeSet::new();
    for virtual_id in virtual_targets {
        seen_virtuals.insert(encode_node_id(virtual_id));
        for (repo, hops) in trace_across_repos(graph, virtual_id, max_depth)? {
            let mapped = hops.into_iter().map(|hop| ImpactHop {
                direction: {
                    // The Debug impl already allocates; lowering in place
                    // avoids a second allocation.
                    let mut s = format!("{:?}", hop.direction);
                    s.make_ascii_lowercase();
                    s
                },
                edge_kind: edge_kind_label(hop.edge_kind).to_owned(),
                file_path: hop.file_path,
                line_start: hop.line_number,
                confidence: hop.confidence,
                repo: hop.repo,
                symbol_id: encode_node_id(hop.node_id),
            });
            grouped.entry(repo).or_default().extend(mapped);
        }
    }

    let mut impacted_repos = grouped
        .into_iter()
        .map(|(repo, mut hops)| {
            hops.sort_by(|left, right| {
                right
                    .confidence
                    .cmp(&left.confidence)
                    .then(left.file_path.cmp(&right.file_path))
                    .then(left.line_start.cmp(&right.line_start))
                    .then(left.symbol_id.cmp(&right.symbol_id))
            });
            ImpactRepo { hops, repo }
        })
        .collect::<Vec<_>>();
    impacted_repos.sort_by(|left, right| {
        strongest_repo_confidence(right)
            .cmp(&strongest_repo_confidence(left))
            .then(left.repo.cmp(&right.repo))
    });

    let mut response = TraceImpactResponse {
        data: TraceImpactData {
            impacted_repos,
            target: request.target,
            virtual_targets: seen_virtuals.into_iter().collect(),
        },
        meta: Some(TraceImpactMeta {
            response_schema_version: response_schema_version(),
            budget: ResponseBudget::not_truncated(BudgetedTool::TraceImpact, 0, 0),
            truncated: false,
        }),
    };
    let budget = apply_response_budget(
        BudgetedTool::TraceImpact,
        request.budget_bytes,
        &mut response,
        trim_trace_impact_response,
    )?;
    if let Some(meta) = &mut response.meta {
        meta.budget = budget;
        meta.truncated = meta.budget.truncated;
    }
    Ok(response)
}

pub fn cross_repo_deps_tool(
    ctx: &McpContext,
    request: CrossRepoDepsRequest,
) -> Result<CrossRepoDepsResponse, McpServerError> {
    validate_input_length("repo", &request.repo)?;
    let registry = ctx.registry_snapshot()?;
    if !registry.repos.contains_key(&request.repo) {
        return Err(McpServerError::NotFound(format!(
            "repo `{}` is not registered",
            request.repo
        )));
    }
    let dependencies = cross_repo_deps(ctx.graph(), &request.repo)?
        .into_iter()
        .map(|(repo, kinds)| RepoDependency {
            edge_kinds: kinds
                .into_iter()
                .map(|kind| edge_kind_label(kind).to_owned())
                .collect(),
            repo,
        })
        .collect();

    Ok(CrossRepoDepsResponse {
        data: CrossRepoDepsData {
            dependencies,
            repo: request.repo,
        },
    })
}

pub fn get_shared_type_usage_tool(
    ctx: &McpContext,
    request: SharedTypeUsageRequest,
) -> Result<SharedTypeUsageResponse, McpServerError> {
    validate_input_length("type_name", &request.type_name)?;
    let graph = ctx.graph();
    let needle = request.type_name.trim();
    let matches = graph
        .nodes_by_type(NodeKind::SharedSymbol)?
        .into_iter()
        .filter(|node| shared_symbol_matches(node, needle))
        .map(|node| {
            let mut per_repo = BTreeMap::<String, BTreeSet<String>>::new();

            for edge in graph.get_incoming(node.id)? {
                if let Some(source) = graph.get_node(edge.source)? {
                    per_repo
                        .entry(source.repo)
                        .or_default()
                        .insert(source.file_path);
                }
            }

            for edge in graph.get_outgoing(node.id)? {
                if let Some(target) = graph.get_node(edge.target)? {
                    per_repo
                        .entry(target.repo)
                        .or_default()
                        .insert(target.file_path);
                }
            }

            Ok(SharedTypeMatch {
                external_id: node.external_id,
                qualified_name: node.qualified_name,
                symbol_id: encode_node_id(node.id),
                usages: per_repo
                    .into_iter()
                    .map(|(repo, files)| SharedTypeUsageRepo {
                        files: files.into_iter().collect(),
                        repo,
                    })
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>, McpServerError>>()?;

    Ok(SharedTypeUsageResponse {
        data: SharedTypeUsageData {
            matches,
            type_name: request.type_name,
        },
    })
}

fn resolve_virtual_targets(
    graph: &impl GraphStore,
    target: &str,
) -> Result<Vec<NodeId>, McpServerError> {
    if let Ok(node_id) = decode_node_id(target)
        && let Some(node) = graph.get_node(node_id)?
    {
        if node.is_virtual {
            return expand_equivalent_virtual_targets(graph, [node.id].into_iter());
        }

        let mut related_virtuals = BTreeSet::new();
        for edge in graph.get_outgoing(node.id)? {
            if let Some(target_node) = graph.get_node(edge.target)?
                && target_node.is_virtual
            {
                related_virtuals.insert(target_node.id);
            }
        }
        for edge in graph.get_incoming(node.id)? {
            if let Some(source_node) = graph.get_node(edge.source)?
                && source_node.is_virtual
            {
                related_virtuals.insert(source_node.id);
            }
        }
        return expand_equivalent_virtual_targets(graph, related_virtuals.into_iter());
    }

    let mut matches = Vec::new();
    for kind in [
        NodeKind::SharedSymbol,
        NodeKind::Route,
        NodeKind::Topic,
        NodeKind::Queue,
        NodeKind::Event,
    ] {
        for node in graph.nodes_by_type(kind)? {
            if !node.is_virtual {
                continue;
            }
            let matches_target = node
                .external_id
                .as_deref()
                .is_some_and(|value| value == target || value.contains(target))
                || node
                    .qualified_name
                    .as_deref()
                    .is_some_and(|value| value == target || value.contains(target))
                || node.name == target;
            if matches_target {
                matches.push(node.id);
            }
        }
    }

    expand_equivalent_virtual_targets(graph, matches.into_iter())
}

fn expand_equivalent_virtual_targets(
    graph: &impl GraphStore,
    ids: impl Iterator<Item = NodeId>,
) -> Result<Vec<NodeId>, McpServerError> {
    let mut expanded = BTreeSet::new();
    for id in ids {
        expanded.insert(id);
        let Some(node) = graph.get_node(id)? else {
            continue;
        };
        if !node.is_virtual {
            continue;
        }
        for kind in [
            NodeKind::SharedSymbol,
            NodeKind::Route,
            NodeKind::Topic,
            NodeKind::Queue,
            NodeKind::Event,
        ] {
            for candidate in graph.nodes_by_type(kind)? {
                if !candidate.is_virtual {
                    continue;
                }
                let same_external_id = node
                    .external_id
                    .as_deref()
                    .zip(candidate.external_id.as_deref())
                    .is_some_and(|(left, right)| left == right);
                let same_qualified_name = node
                    .qualified_name
                    .as_deref()
                    .zip(candidate.qualified_name.as_deref())
                    .is_some_and(|(left, right)| left == right);
                if same_external_id || same_qualified_name {
                    expanded.insert(candidate.id);
                }
            }
        }
    }
    Ok(expanded.into_iter().collect())
}

fn shared_symbol_matches(node: &gather_step_core::NodeData, needle: &str) -> bool {
    node.name == needle
        || node
            .qualified_name
            .as_deref()
            .is_some_and(|value| value.ends_with(&format!("__{needle}")) || value.contains(needle))
        || node
            .external_id
            .as_deref()
            .is_some_and(|value| value.ends_with(&format!("__{needle}")) || value.contains(needle))
}

fn trim_trace_impact_response(response: &mut TraceImpactResponse) -> bool {
    for repo in response.data.impacted_repos.iter_mut().rev() {
        if repo.hops.pop().is_some() {
            return true;
        }
    }
    response.data.impacted_repos.pop().is_some()
}

fn strongest_repo_confidence(repo: &ImpactRepo) -> Option<u16> {
    repo.hops.iter().filter_map(|hop| hop.confidence).max()
}
