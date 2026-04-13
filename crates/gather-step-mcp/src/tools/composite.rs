use gather_step_core::NodeKind;
use gather_step_storage::{GraphStore, MetadataStore};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    ids::decode_node_id,
    tools::{
        contract::{breaking_change_candidates_tool, contract_drift_tool, payload_schema_tool},
        cross_repo::{
            CrossRepoDepsRequest, ImpactRepo, RepoDependency, TraceImpactRequest,
            cross_repo_deps_tool, trace_impact_tool,
        },
        crud_trace::{CrudTraceRequest, crud_trace_tool},
        events::{
            event_blast_radius_tool, list_orphan_topics_tool, trace_event_tool, trace_route_tool,
        },
        orientation::{RepoSummary, list_repos},
        packs::{
            ContextPackRequest, ModePackRequest, change_impact_pack_tool, context_pack_tool,
            debug_pack_tool, fix_pack_tool, planning_pack_tool, review_pack_tool,
        },
        repo_intelligence::{
            DeadCodeRequest, RepoScopedRequest, WhoOwnsRequest, get_conventions_tool,
            get_dead_code_tool, get_overview_tool, who_owns_tool,
        },
        search::{
            SearchRequest, SearchResultItem, SymbolResponseData, TraversalNode, TraversalRequest,
            get_callees, get_callers, get_symbol, search_symbols,
        },
    },
};

const DEFAULT_CONTEXT_DEPTH: usize = 2;
const DEFAULT_CONTEXT_LIMIT: usize = 8;
const MAX_BATCH_OPS: usize = 10;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BriefRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BatchQueryRequest {
    pub ops: Vec<BatchQueryOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BatchQueryOperation {
    #[serde(default)]
    pub arguments: Value,
    pub tool: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BriefResponse {
    pub data: BriefData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<CompositeMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BriefData {
    pub callers: usize,
    pub callees: usize,
    pub found: bool,
    pub impacted_repo_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_summary: Option<RepoSummary>,
    pub risk: RiskSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<BriefSymbol>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct BriefSymbol {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextResponse {
    pub data: ContextData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<CompositeMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextData {
    pub callers: Vec<TraversalNode>,
    pub callees: Vec<TraversalNode>,
    pub found: bool,
    pub impacted_repos: Vec<ImpactRepo>,
    pub recent_changes_available: bool,
    pub repo_dependencies: Vec<RepoDependency>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_summary: Option<RepoSummary>,
    pub risk: RiskSummary,
    pub symbol: SymbolResponseData,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CompositeMeta {
    pub budget: ResponseBudget,
    pub generation: i64,
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub follow_up_hints: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_symbol_id: Option<String>,
    pub resolution: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RiskSummary {
    pub level: String,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BatchQueryResponse {
    pub data: BatchQueryData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BatchQueryData {
    pub results: Vec<BatchQueryResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct BatchQueryResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    pub tool: String,
}

pub fn brief_tool(
    ctx: &McpContext,
    request: BriefRequest,
) -> Result<BriefResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let resolved = resolve_target(ctx, &request.target)?;
    let Some(resolved_symbol_id) = resolved.symbol_id.clone() else {
        let generation = composite_generation(ctx, None)?;
        let mut response = BriefResponse {
            data: BriefData {
                callers: 0,
                callees: 0,
                found: false,
                impacted_repo_count: 0,
                repo_summary: None,
                risk: RiskSummary {
                    level: "unknown".to_owned(),
                    reasons: vec!["target could not be resolved to an indexed symbol".to_owned()],
                },
                summary: None,
                symbol: None,
                target: request.target,
            },
            meta: Some(build_meta(&resolved, true, generation, BudgetedTool::Brief)),
        };
        apply_composite_budget(
            BudgetedTool::Brief,
            request.budget_bytes,
            &mut response,
            trim_brief_response,
        )?;
        return Ok(response);
    };

    let context = build_context(
        ctx,
        request.budget_bytes,
        request.target.clone(),
        &resolved,
        DEFAULT_CONTEXT_DEPTH,
        DEFAULT_CONTEXT_LIMIT,
    )?;
    let symbol = &context.data.symbol;
    let summary = build_brief_summary(&context.data);

    Ok(BriefResponse {
        data: BriefData {
            callers: context.data.callers.len(),
            callees: context.data.callees.len(),
            found: context.data.found,
            impacted_repo_count: context.data.impacted_repos.len(),
            repo_summary: context.data.repo_summary.clone(),
            risk: context.data.risk.clone(),
            summary: Some(summary),
            symbol: Some(BriefSymbol {
                file_path: symbol.file_path.clone(),
                kind: symbol.kind.clone(),
                name: symbol.name.clone(),
                repo: symbol.repo.clone(),
                symbol_id: Some(resolved_symbol_id),
            }),
            target: request.target,
        },
        meta: context.meta,
    })
}

pub fn context_tool(
    ctx: &McpContext,
    request: ContextRequest,
) -> Result<ContextResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let resolved = resolve_target(ctx, &request.target)?;
    build_context(
        ctx,
        request.budget_bytes,
        request.target,
        &resolved,
        request.depth.unwrap_or(DEFAULT_CONTEXT_DEPTH),
        ctx.config
            .capped_limit(request.limit, DEFAULT_CONTEXT_LIMIT),
    )
}

pub fn batch_query_tool(
    ctx: &McpContext,
    request: BatchQueryRequest,
) -> Result<BatchQueryResponse, McpServerError> {
    if request.ops.len() > MAX_BATCH_OPS {
        return Err(McpServerError::InvalidInput(format!(
            "batch_query accepts at most {MAX_BATCH_OPS} operations"
        )));
    }

    let results = request
        .ops
        .into_iter()
        .map(|op| execute_batch_op(ctx, op))
        .collect::<Vec<_>>();

    Ok(BatchQueryResponse {
        data: BatchQueryData { results },
    })
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedTarget {
    pub(crate) resolution: String,
    pub(crate) symbol_id: Option<String>,
}

fn build_context(
    ctx: &McpContext,
    requested_budget: Option<usize>,
    target: String,
    resolved: &ResolvedTarget,
    depth: usize,
    limit: usize,
) -> Result<ContextResponse, McpServerError> {
    let generation = composite_generation(ctx, resolved.symbol_id.as_deref())?;
    let Some(symbol_id) = resolved.symbol_id.clone() else {
        let mut response = ContextResponse {
            data: ContextData {
                callers: Vec::new(),
                callees: Vec::new(),
                found: false,
                impacted_repos: Vec::new(),
                recent_changes_available: false,
                repo_dependencies: Vec::new(),
                repo_summary: None,
                risk: RiskSummary {
                    level: "unknown".to_owned(),
                    reasons: vec!["target could not be resolved to an indexed symbol".to_owned()],
                },
                symbol: empty_symbol(target.clone()),
                target,
            },
            meta: Some(build_meta(resolved, true, generation, BudgetedTool::Brief)),
        };
        apply_composite_budget(
            BudgetedTool::Brief,
            requested_budget,
            &mut response,
            trim_context_response,
        )?;
        return Ok(response);
    };

    let symbol = get_symbol(
        ctx,
        crate::tools::search::SymbolRequest {
            symbol_id: symbol_id.clone(),
        },
    )?
    .data;
    if !symbol.found {
        let mut response = ContextResponse {
            data: ContextData {
                callers: Vec::new(),
                callees: Vec::new(),
                found: false,
                impacted_repos: Vec::new(),
                recent_changes_available: false,
                repo_dependencies: Vec::new(),
                repo_summary: None,
                risk: RiskSummary {
                    level: "unknown".to_owned(),
                    reasons: vec!["resolved symbol_id no longer exists in the graph".to_owned()],
                },
                symbol,
                target,
            },
            meta: Some(build_meta(resolved, true, generation, BudgetedTool::Brief)),
        };
        apply_composite_budget(
            BudgetedTool::Brief,
            requested_budget,
            &mut response,
            trim_context_response,
        )?;
        return Ok(response);
    }

    let incoming_paths = get_callers(
        ctx,
        TraversalRequest {
            budget_bytes: None,
            depth: Some(depth),
            limit: Some(limit),
            symbol_id: symbol_id.clone(),
        },
    )?
    .data
    .traversal;
    let outgoing_paths = get_callees(
        ctx,
        TraversalRequest {
            budget_bytes: None,
            depth: Some(depth),
            limit: Some(limit),
            symbol_id: symbol_id.clone(),
        },
    )?
    .data
    .traversal;
    let impact = match trace_impact_tool(
        ctx,
        TraceImpactRequest {
            budget_bytes: None,
            depth: Some(depth),
            target: symbol_id.clone(),
        },
    ) {
        Ok(response) => response.data.impacted_repos,
        Err(McpServerError::NotFound(_)) => Vec::new(),
        Err(error) => return Err(error),
    };
    let repo_dependencies = symbol
        .repo
        .as_ref()
        .map(|repo| cross_repo_deps_tool(ctx, CrossRepoDepsRequest { repo: repo.clone() }))
        .transpose()?
        .map_or_else(Vec::new, |response| response.data.dependencies);
    let repo_summary = symbol
        .repo
        .as_deref()
        .and_then(|repo| repo_summary(ctx, repo).transpose())
        .transpose()?;
    let recent_changes_available = symbol
        .repo
        .as_ref()
        .zip(symbol.file_path.as_ref())
        .map(|(repo, file_path)| ctx.metadata().get_file_analytics(repo, file_path))
        .transpose()?
        .flatten()
        .is_some();
    let risk = assess_risk(
        symbol.kind.as_deref(),
        incoming_paths.len(),
        outgoing_paths.len(),
        impact.len(),
        repo_dependencies.len(),
    );

    let mut response = ContextResponse {
        data: ContextData {
            callers: incoming_paths,
            callees: outgoing_paths,
            found: true,
            impacted_repos: impact,
            recent_changes_available,
            repo_dependencies,
            repo_summary,
            risk,
            symbol,
            target,
        },
        meta: Some(build_meta(resolved, false, generation, BudgetedTool::Brief)),
    };
    apply_composite_budget(
        BudgetedTool::Brief,
        requested_budget,
        &mut response,
        trim_context_response,
    )?;
    Ok(response)
}

fn execute_batch_op(ctx: &McpContext, op: BatchQueryOperation) -> BatchQueryResult {
    let tool = op.tool;
    let result = match tool.as_str() {
        "batch_query" => Err(McpServerError::InvalidInput(
            "nested batch_query is not supported".to_owned(),
        )),
        "get_graph_schema" | "get_graph_schema_summary" => {
            crate::tools::orientation::get_graph_schema(ctx).and_then(to_value)
        }
        "list_repos" => list_repos(ctx).and_then(to_value),
        "search" => parse_and_run(op.arguments, |args| {
            search_symbols(ctx, args).and_then(to_value)
        }),
        "get_symbol" => parse_and_run(op.arguments, |args| {
            get_symbol(ctx, args).and_then(to_value)
        }),
        "get_callers" => parse_and_run(op.arguments, |args| {
            get_callers(ctx, args).and_then(to_value)
        }),
        "get_callees" => parse_and_run(op.arguments, |args| {
            get_callees(ctx, args).and_then(to_value)
        }),
        "trace_impact" => parse_and_run(op.arguments, |args| {
            trace_impact_tool(ctx, args).and_then(to_value)
        }),
        "cross_repo_deps" => parse_and_run(op.arguments, |args| {
            cross_repo_deps_tool(ctx, args).and_then(to_value)
        }),
        "get_shared_type_usage" => parse_and_run(op.arguments, |args| {
            crate::tools::cross_repo::get_shared_type_usage_tool(ctx, args).and_then(to_value)
        }),
        "trace_event" => parse_and_run(op.arguments, |args| {
            trace_event_tool(ctx, args).and_then(to_value)
        }),
        "trace_route" => parse_and_run(op.arguments, |args| {
            trace_route_tool(ctx, args).and_then(to_value)
        }),
        "crud_trace" => parse_and_run::<CrudTraceRequest, _>(op.arguments, |args| {
            crud_trace_tool(ctx, args).and_then(to_value)
        }),
        "event_blast_radius" => parse_and_run(op.arguments, |args| {
            event_blast_radius_tool(ctx, args).and_then(to_value)
        }),
        "list_orphan_topics" => parse_and_run(op.arguments, |args| {
            list_orphan_topics_tool(ctx, args).and_then(to_value)
        }),
        "payload_schema" => parse_and_run(op.arguments, |args| {
            payload_schema_tool(ctx, args).and_then(to_value)
        }),
        "contract_drift" => parse_and_run(op.arguments, |args| {
            contract_drift_tool(ctx, args).and_then(to_value)
        }),
        "breaking_change_candidates" => parse_and_run(op.arguments, |args| {
            breaking_change_candidates_tool(ctx, args).and_then(to_value)
        }),
        "who_owns" => parse_and_run::<WhoOwnsRequest, _>(op.arguments, |args| {
            who_owns_tool(ctx, args).and_then(to_value)
        }),
        "get_dead_code" => parse_and_run::<DeadCodeRequest, _>(op.arguments, |args| {
            get_dead_code_tool(ctx, args).and_then(to_value)
        }),
        "get_conventions" => parse_and_run::<RepoScopedRequest, _>(op.arguments, |args| {
            get_conventions_tool(ctx, args).and_then(to_value)
        }),
        "get_overview" => parse_and_run::<RepoScopedRequest, _>(op.arguments, |args| {
            get_overview_tool(ctx, args).and_then(to_value)
        }),
        "brief" => parse_and_run(op.arguments, |args| {
            brief_tool(ctx, args).and_then(to_value)
        }),
        "context" => parse_and_run(op.arguments, |args| {
            context_tool(ctx, args).and_then(to_value)
        }),
        "context_pack" | "get_context_pack" => {
            parse_and_run::<ContextPackRequest, _>(op.arguments, |args| {
                context_pack_tool(ctx, args).and_then(to_value)
            })
        }
        "planning_pack" | "plan_change" => {
            parse_and_run::<ModePackRequest, _>(op.arguments, |args| {
                planning_pack_tool(ctx, args).and_then(to_value)
            })
        }
        "debug_pack" => parse_and_run::<ModePackRequest, _>(op.arguments, |args| {
            debug_pack_tool(ctx, args).and_then(to_value)
        }),
        "fix_pack" | "fix_surface" => parse_and_run::<ModePackRequest, _>(op.arguments, |args| {
            fix_pack_tool(ctx, args).and_then(to_value)
        }),
        "review_pack" => parse_and_run::<ModePackRequest, _>(op.arguments, |args| {
            review_pack_tool(ctx, args).and_then(to_value)
        }),
        "change_impact_pack" | "get_change_impact_pack" => {
            parse_and_run::<ModePackRequest, _>(op.arguments, |args| {
                change_impact_pack_tool(ctx, args).and_then(to_value)
            })
        }
        _ => Err(McpServerError::InvalidInput(format!(
            "unsupported batch tool `{tool}`"
        ))),
    };

    match result {
        Ok(value) => BatchQueryResult {
            error: None,
            ok: true,
            result: Some(value),
            tool,
        },
        Err(error) => BatchQueryResult {
            error: Some(error.to_string()),
            ok: false,
            result: None,
            tool,
        },
    }
}

fn parse_and_run<T, F>(value: Value, run: F) -> Result<Value, McpServerError>
where
    T: for<'de> Deserialize<'de>,
    F: FnOnce(T) -> Result<Value, McpServerError>,
{
    let parsed = serde_json::from_value::<T>(value)
        .map_err(|error| McpServerError::InvalidInput(error.to_string()))?;
    run(parsed)
}

fn to_value<T: Serialize>(value: T) -> Result<Value, McpServerError> {
    serde_json::to_value(value)
        .map_err(|error| McpServerError::Internal(format!("response serialize: {error}")))
}

pub(crate) fn resolve_target(
    ctx: &McpContext,
    target: &str,
) -> Result<ResolvedTarget, McpServerError> {
    if let Ok(node_id) = decode_node_id(target)
        && ctx.graph().get_node(node_id)?.is_some()
    {
        return Ok(ResolvedTarget {
            resolution: "symbol_id".to_owned(),
            symbol_id: Some(target.to_owned()),
        });
    }

    let search = search_symbols(
        ctx,
        SearchRequest {
            budget_bytes: None,
            cursor: None,
            kind: None,
            language: None,
            limit: Some(10),
            query: target.to_owned(),
            repo: None,
        },
    )?;
    let symbol_id = choose_resolved_symbol(&search.data.results, target);

    Ok(ResolvedTarget {
        resolution: if symbol_id.is_some() {
            "search_resolved".to_owned()
        } else if search.data.results.is_empty() {
            "unresolved".to_owned()
        } else {
            "ambiguous_search_match".to_owned()
        },
        symbol_id,
    })
}

fn choose_resolved_symbol(results: &[SearchResultItem], target: &str) -> Option<String> {
    let exact = results
        .iter()
        .filter(|item| item.exact_match || item.symbol_name == target)
        .collect::<Vec<_>>();
    if exact.len() == 1 {
        return exact.first().map(|item| item.symbol_id.clone());
    }
    if exact.len() > 1 {
        return None;
    }
    if results.len() == 1 {
        return results.first().map(|item| item.symbol_id.clone());
    }
    None
}

pub(crate) fn build_meta(
    resolved: &ResolvedTarget,
    local_only: bool,
    generation: i64,
    budget_tool: BudgetedTool,
) -> CompositeMeta {
    let mut follow_up_hints = vec!["get_symbol".to_owned()];
    if local_only {
        follow_up_hints.push("search".to_owned());
    } else {
        follow_up_hints.push("get_callers".to_owned());
        follow_up_hints.push("get_callees".to_owned());
        follow_up_hints.push("trace_impact".to_owned());
        follow_up_hints.push("trace_event".to_owned());
        follow_up_hints.push("trace_route".to_owned());
        follow_up_hints.push("crud_trace".to_owned());
        follow_up_hints.push("event_blast_radius".to_owned());
        follow_up_hints.push("payload_schema".to_owned());
        follow_up_hints.push("contract_drift".to_owned());
    }
    follow_up_hints.push("cross_repo_deps".to_owned());
    follow_up_hints.push("list_orphan_topics".to_owned());
    follow_up_hints.push("breaking_change_candidates".to_owned());

    CompositeMeta {
        budget: ResponseBudget::not_truncated(budget_tool, 0, 0),
        generation,
        response_schema_version: response_schema_version(),
        follow_up_hints,
        resolved_symbol_id: resolved.symbol_id.clone(),
        resolution: resolved.resolution.clone(),
    }
}

fn composite_generation(ctx: &McpContext, symbol_id: Option<&str>) -> Result<i64, McpServerError> {
    let Some(symbol_id) = symbol_id else {
        return ctx
            .metadata()
            .latest_indexed_at(None)
            .map_err(McpServerError::Metadata);
    };
    let symbol = get_symbol(
        ctx,
        crate::tools::search::SymbolRequest {
            symbol_id: symbol_id.to_owned(),
        },
    )?
    .data;
    let mut files = Vec::<(String, String)>::new();
    if let (Some(repo), Some(file_path)) = (symbol.repo, symbol.file_path) {
        files.push((repo, file_path));
    }
    ctx.metadata()
        .latest_indexed_at_for_files(&files)
        .map_err(McpServerError::Metadata)
}

fn apply_composite_budget<T>(
    tool: BudgetedTool,
    requested_budget: Option<usize>,
    response: &mut T,
    remove_lowest_ranked: impl FnMut(&mut T) -> bool,
) -> Result<(), McpServerError>
where
    T: Serialize,
    T: CompositeBudgetMeta,
{
    let budget = apply_response_budget(tool, requested_budget, response, remove_lowest_ranked)?;
    response.update_composite_budget(budget);
    Ok(())
}

trait CompositeBudgetMeta {
    fn update_composite_budget(&mut self, budget: ResponseBudget);
}

impl CompositeBudgetMeta for BriefResponse {
    fn update_composite_budget(&mut self, budget: ResponseBudget) {
        if let Some(meta) = &mut self.meta {
            meta.budget = budget;
            meta.budget.items_included = usize::from(self.data.summary.is_some())
                + usize::from(self.data.symbol.is_some())
                + self.data.callers
                + self.data.callees
                + self.data.impacted_repo_count;
        }
    }
}

impl CompositeBudgetMeta for ContextResponse {
    fn update_composite_budget(&mut self, budget: ResponseBudget) {
        if let Some(meta) = &mut self.meta {
            meta.budget = budget;
            meta.budget.items_included = self.data.callers.len()
                + self.data.callees.len()
                + self.data.impacted_repos.len()
                + self.data.repo_dependencies.len();
        }
    }
}

fn trim_brief_response(response: &mut BriefResponse) -> bool {
    response
        .meta
        .as_mut()
        .is_some_and(|meta| meta.follow_up_hints.pop().is_some())
        || response.data.summary.take().is_some()
        || response.data.repo_summary.take().is_some()
}

fn trim_context_response(response: &mut ContextResponse) -> bool {
    response.data.repo_dependencies.pop().is_some()
        || response.data.impacted_repos.pop().is_some()
        || response.data.callees.pop().is_some()
        || response.data.callers.pop().is_some()
        || response
            .meta
            .as_mut()
            .is_some_and(|meta| meta.follow_up_hints.pop().is_some())
}

fn repo_summary(ctx: &McpContext, repo: &str) -> Result<Option<RepoSummary>, McpServerError> {
    Ok(list_repos(ctx)?
        .data
        .repos
        .into_iter()
        .find(|item| item.repo == repo))
}

fn assess_risk(
    symbol_kind: Option<&str>,
    incoming_count: usize,
    outgoing_count: usize,
    impacted_repos: usize,
    repo_dependencies: usize,
) -> RiskSummary {
    let mut reasons = Vec::new();
    let mut score = 0_u8;

    if incoming_count >= 3 {
        score += 1;
        reasons.push(format!("referenced by {incoming_count} caller paths"));
    }
    if outgoing_count >= 3 {
        score += 1;
        reasons.push(format!("fans out to {outgoing_count} callee paths"));
    }
    if impacted_repos > 0 {
        score += 2;
        reasons.push(format!(
            "cross-repo impact reaches {impacted_repos} repositories"
        ));
    }
    if repo_dependencies > 0 {
        score += 1;
        reasons.push(format!(
            "owning repo has {repo_dependencies} cross-repo dependencies"
        ));
    }
    if matches!(
        symbol_kind.and_then(parse_symbol_kind),
        Some(
            NodeKind::Route
                | NodeKind::Topic
                | NodeKind::Queue
                | NodeKind::Event
                | NodeKind::SharedSymbol
        )
    ) {
        score += 1;
        reasons.push(
            "virtual or contract-like symbol can affect multiple integration points".to_owned(),
        );
    }

    if reasons.is_empty() {
        reasons.push("no broad fan-in, fan-out, or cross-repo impact was detected".to_owned());
    }

    let level = match score {
        0..=1 => "low",
        2..=3 => "medium",
        _ => "high",
    };

    RiskSummary {
        level: level.to_owned(),
        reasons,
    }
}

fn parse_symbol_kind(value: &str) -> Option<NodeKind> {
    match value {
        "route" => Some(NodeKind::Route),
        "topic" => Some(NodeKind::Topic),
        "queue" => Some(NodeKind::Queue),
        "event" => Some(NodeKind::Event),
        "shared_symbol" => Some(NodeKind::SharedSymbol),
        _ => None,
    }
}

fn empty_symbol(target: String) -> SymbolResponseData {
    SymbolResponseData {
        decorators: Vec::new(),
        file_path: None,
        found: false,
        is_virtual: None,
        kind: None,
        line_end: None,
        line_start: None,
        name: Some(target),
        qualified_name: None,
        repo: None,
        signature: None,
        symbol_id: String::new(),
        visibility: None,
    }
}

fn build_brief_summary(data: &ContextData) -> String {
    let name = data
        .symbol
        .name
        .clone()
        .unwrap_or_else(|| data.target.clone());
    let kind = data
        .symbol
        .kind
        .clone()
        .unwrap_or_else(|| "symbol".to_owned());
    let repo = data
        .symbol
        .repo
        .clone()
        .unwrap_or_else(|| "unknown repo".to_owned());
    format!(
        "{name} ({kind}) in {repo}; callers={}, callees={}, impacted_repos={}, risk={}",
        data.callers.len(),
        data.callees.len(),
        data.impacted_repos.len(),
        data.risk.level
    )
}
