use gather_step_analysis::{
    CrudTraceRole, resolve_route_target, trace_crud_route, trace_crud_symbol,
};
use gather_step_core::NodeData;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    ids::{decode_node_id, encode_node_id},
    tools::labels::{edge_kind_label, evidence_kind_label, node_kind_label},
};

const DEFAULT_CRUD_LIMIT: usize = 25;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrudTraceRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub symbol_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrudTraceResponse {
    pub data: CrudTraceData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<CrudTraceMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrudTraceData {
    pub callers: Vec<CrudTraceSymbol>,
    pub continuation: Vec<CrudTraceSymbol>,
    pub database_hints: Vec<CrudTraceSymbol>,
    pub entities: Vec<CrudTraceSymbol>,
    pub handlers: Vec<CrudTraceSymbol>,
    pub method: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrudTraceMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub budget: ResponseBudget,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrudTraceSymbol {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_kind: Option<String>,
    pub evidence_kind: String,
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub repo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolver: Option<String>,
    pub role: String,
    pub symbol_id: String,
    pub symbol_kind: String,
    pub symbol_name: String,
    pub traversal_depth: usize,
}

pub fn crud_trace_tool(
    ctx: &McpContext,
    request: CrudTraceRequest,
) -> Result<CrudTraceResponse, McpServerError> {
    let graph = ctx.graph();
    let limit = ctx.config.capped_limit(request.limit, DEFAULT_CRUD_LIMIT);
    if let Some(symbol_id) = request.symbol_id.as_deref() {
        validate_input_length("symbol_id", symbol_id)?;
    }
    if let Some(method) = request.method.as_deref() {
        validate_input_length("method", method)?;
    }
    if let Some(path) = request.path.as_deref() {
        validate_input_length("path", path)?;
    }

    let entry = match (
        request.method.as_deref(),
        request.path.as_deref(),
        request.symbol_id.as_deref(),
    ) {
        (Some(method), Some(path), None) => CrudEntry::Route {
            method: method.to_owned(),
            path: path.to_owned(),
        },
        (None, None, Some(symbol_id)) => CrudEntry::Symbol {
            symbol_id: symbol_id.to_owned(),
        },
        (Some(_), None, _) | (None, Some(_), _) => {
            return Err(McpServerError::InvalidInput(
                "`crud_trace` requires both `method` and `path` when route entry is used"
                    .to_owned(),
            ));
        }
        (_, _, Some(_)) if request.method.is_some() || request.path.is_some() => {
            return Err(McpServerError::InvalidInput(
                "`crud_trace` accepts either (`method`, `path`) or `symbol_id`, not both"
                    .to_owned(),
            ));
        }
        _ => {
            return Err(McpServerError::InvalidInput(
                "`crud_trace` requires either (`method`, `path`) or `symbol_id`".to_owned(),
            ));
        }
    };

    let response = if let Some(trace) = match &entry {
        CrudEntry::Route { method, path } => resolve_route_target(graph, method, path)?
            .map(|route| trace_crud_route(graph, route.id, limit))
            .transpose()?,
        CrudEntry::Symbol { symbol_id } => {
            let symbol_id = decode_node_id(symbol_id).map_err(McpServerError::InvalidInput)?;
            trace_crud_symbol(graph, symbol_id, limit)?
        }
    } {
        let (method, path) = response_route_fields(&trace.target, &entry);
        let mut response = CrudTraceResponse {
            data: CrudTraceData {
                callers: trace.callers.into_iter().map(symbol).collect(),
                continuation: trace.continuation.into_iter().map(symbol).collect(),
                database_hints: trace.database_hints.into_iter().map(symbol).collect(),
                entities: trace.entities.into_iter().map(symbol).collect(),
                handlers: trace.handlers.into_iter().map(symbol).collect(),
                method,
                path,
                target_id: Some(encode_node_id(trace.target.id)),
                target_name: Some(trace.target.name),
            },
            meta: Some(CrudTraceMeta {
                response_schema_version: response_schema_version(),
                budget: ResponseBudget::not_truncated(BudgetedTool::CrudTrace, 0, 0),
                truncated: trace.truncated,
            }),
        };
        sort_crud_symbols(&mut response.data.handlers);
        sort_crud_symbols(&mut response.data.callers);
        sort_crud_symbols(&mut response.data.continuation);
        sort_crud_symbols(&mut response.data.database_hints);
        sort_crud_symbols(&mut response.data.entities);
        let budget = apply_response_budget(
            BudgetedTool::CrudTrace,
            request.budget_bytes,
            &mut response,
            trim_crud_trace_response,
        )?;
        if let Some(meta) = &mut response.meta {
            meta.budget = budget;
            meta.truncated |= meta.budget.truncated;
        }
        response
    } else {
        CrudTraceResponse {
            data: CrudTraceData {
                callers: Vec::new(),
                continuation: Vec::new(),
                database_hints: Vec::new(),
                entities: Vec::new(),
                handlers: Vec::new(),
                method: request.method.unwrap_or_default(),
                path: request.path.unwrap_or_default(),
                target_id: None,
                target_name: None,
            },
            meta: Some(CrudTraceMeta {
                response_schema_version: response_schema_version(),
                budget: ResponseBudget::not_truncated(
                    BudgetedTool::CrudTrace,
                    BudgetedTool::CrudTrace.default_bytes(),
                    0,
                ),
                truncated: false,
            }),
        }
    };

    Ok(response)
}

enum CrudEntry {
    Route { method: String, path: String },
    Symbol { symbol_id: String },
}

fn response_route_fields(target: &NodeData, entry: &CrudEntry) -> (String, String) {
    if let Some((method, path)) = parse_route_target(target) {
        return (method, path);
    }

    match entry {
        CrudEntry::Route { method, path } => (method.clone(), path.clone()),
        CrudEntry::Symbol { .. } => (String::new(), target.name.clone()),
    }
}

fn parse_route_target(target: &NodeData) -> Option<(String, String)> {
    let route = target
        .external_id
        .as_deref()
        .or(target.qualified_name.as_deref())?
        .strip_prefix("__route__")?;
    let (method, path) = route.split_once("__")?;
    Some((method.to_owned(), path.to_owned()))
}

fn symbol(entry: gather_step_analysis::CrudTraceEntry) -> CrudTraceSymbol {
    let evidence_kind = evidence_kind_label(entry.resolver.as_deref()).to_owned();
    CrudTraceSymbol {
        confidence: entry.confidence,
        edge_kind: entry.edge_kind.map(|kind| edge_kind_label(kind).to_owned()),
        evidence_kind,
        file_path: entry.file_path,
        line_start: entry.line_number,
        repo: entry.repo,
        resolver: entry.resolver,
        role: role_label(entry.role).to_owned(),
        symbol_id: encode_node_id(entry.node_id),
        symbol_kind: node_kind_label(entry.node_kind).to_owned(),
        symbol_name: entry.symbol_name,
        traversal_depth: entry.depth,
    }
}

fn role_label(role: CrudTraceRole) -> &'static str {
    match role {
        CrudTraceRole::Caller => "caller",
        CrudTraceRole::Handler => "handler",
        CrudTraceRole::Service => "service",
        CrudTraceRole::Repository => "repository",
        CrudTraceRole::Entity => "entity",
        CrudTraceRole::Collection => "collection",
        CrudTraceRole::DatabaseHint => "database_hint",
    }
}

fn sort_crud_symbols(items: &mut [CrudTraceSymbol]) {
    // Evidence kind stays the leading key so the crud_trace UX keeps ordering
    // "stronger evidence before confidence". Inside each evidence-kind band the
    // The deterministic confidence tuple kicks in here:
    // `(confidence desc, strategy_weight desc, … file/line/name/id asc)`.
    items.sort_by(|left, right| {
        evidence_rank(&right.evidence_kind)
            .cmp(&evidence_rank(&left.evidence_kind))
            .then(right.confidence.cmp(&left.confidence))
            .then(
                gather_step_core::strategy_weight(right.resolver.as_deref())
                    .cmp(&gather_step_core::strategy_weight(left.resolver.as_deref())),
            )
            .then(left.traversal_depth.cmp(&right.traversal_depth))
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

fn evidence_rank(kind: &str) -> u8 {
    match kind {
        "literal" => 5,
        "imported_constant" => 4,
        "framework_route" => 3,
        "symbol_resolution" => 2,
        "derived" => 1,
        _ => 0,
    }
}

fn trim_crud_trace_response(response: &mut CrudTraceResponse) -> bool {
    response.data.database_hints.pop().is_some()
        || response.data.entities.pop().is_some()
        || response.data.continuation.pop().is_some()
        || response.data.callers.pop().is_some()
        || response.data.handlers.pop().is_some()
}

#[cfg(test)]
mod tests {
    use super::{CrudTraceSymbol, sort_crud_symbols};

    fn symbol(name: &str, evidence_kind: &str, confidence: Option<u16>) -> CrudTraceSymbol {
        CrudTraceSymbol {
            confidence,
            edge_kind: Some("consumes".to_owned()),
            evidence_kind: evidence_kind.to_owned(),
            file_path: "src/api.ts".to_owned(),
            line_start: Some(1),
            repo: "frontend_standard".to_owned(),
            resolver: None,
            role: "caller".to_owned(),
            symbol_id: format!("id-{name}"),
            symbol_kind: "function".to_owned(),
            symbol_name: name.to_owned(),
            traversal_depth: 0,
        }
    }

    #[test]
    fn sort_crud_symbols_prefers_stronger_evidence_before_confidence() {
        let mut items = vec![
            symbol("hinted", "hint", Some(990)),
            symbol("imported", "imported_constant", Some(900)),
            symbol("literal", "literal", Some(850)),
        ];

        sort_crud_symbols(&mut items);

        let names = items
            .iter()
            .map(|item| item.symbol_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["literal", "imported", "hinted"]);
    }
}
