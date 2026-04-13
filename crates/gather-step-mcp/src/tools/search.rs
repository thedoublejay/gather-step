use std::collections::{BTreeSet, VecDeque};

use gather_step_analysis::anchor::rank_anchors;
use gather_step_core::{EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, SearchFilters, SearchStore};
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    ids::{decode_node_id, encode_node_id},
    tools::labels::node_kind_label,
};

const DEFAULT_SEARCH_LIMIT: usize = 20;
const DEFAULT_TRAVERSAL_LIMIT: usize = 50;
const MAX_TRAVERSAL_DEPTH: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SearchRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub query: String,
    #[serde(default)]
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SymbolRequest {
    pub symbol_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraversalRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    pub symbol_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SearchResultItem {
    pub exact_match: bool,
    pub file_path: String,
    pub kind: String,
    pub language: String,
    pub line_start: Option<u32>,
    pub repo: String,
    pub score: f32,
    pub symbol_id: String,
    pub symbol_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SearchResponse {
    pub data: SearchResponseData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<SearchMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SearchResponseData {
    pub results: Vec<SearchResultItem>,
    pub returned: usize,
    pub total_estimate: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SearchMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub budget: ResponseBudget,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SymbolResponse {
    pub data: SymbolResponseData,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SymbolResponseData {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decorators: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_end: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qualified_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    pub symbol_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_virtual: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraversalNode {
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
pub struct TraversalResponse {
    pub data: TraversalResponseData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<TraversalMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraversalMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    pub budget: ResponseBudget,
    pub depth_capped: bool,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TraversalResponseData {
    pub returned: usize,
    pub symbol_id: String,
    pub traversal: Vec<TraversalNode>,
}

pub fn search_symbols(
    ctx: &McpContext,
    mut request: SearchRequest,
) -> Result<SearchResponse, McpServerError> {
    validate_input_length("query", &request.query)?;
    if let Some(ref cursor) = request.cursor {
        validate_input_length("cursor", cursor)?;
    }
    if request.query.trim().is_empty() {
        return Err(McpServerError::InvalidInput(
            "search query must not be empty".to_owned(),
        ));
    }
    let search = ctx.search();
    let graph = ctx.graph();
    let offset = if let Some(cursor) = request.cursor.take() {
        apply_search_cursor(&mut request, &cursor, ctx.cursor_key())?
    } else {
        0
    };
    let requested_limit = ctx.config.capped_limit(request.limit, DEFAULT_SEARCH_LIMIT);
    let fetch_limit = offset.saturating_add(requested_limit).saturating_add(1);
    let requested_kind = request.kind.as_deref().and_then(parse_node_kind);

    let mut results = search
        .search_filtered(
            &request.query,
            fetch_limit,
            SearchFilters {
                repo: request.repo.as_deref(),
                node_kind: requested_kind,
                lang: request.language.as_deref(),
            },
        )?
        .into_iter()
        .collect::<Vec<_>>();

    if offset > 0 {
        results = results.into_iter().skip(offset).collect();
    }

    let truncated = results.len() > requested_limit;
    if truncated {
        results.truncate(requested_limit);
    }

    let mut items = results
        .into_iter()
        .filter_map(|hit| {
            // `repo` and `file_path` are not stored in Tantivy (S6); rehydrate
            // from the graph store using `node_id`.
            let node = match graph.get_node(hit.node_id) {
                Ok(n) => n,
                Err(e) => return Some(Err(e.into())),
            };
            let node = node?;
            let line_start = node.span.map(|s| s.line_start);
            Some(Ok(SearchResultItem {
                exact_match: hit.exact_match,
                file_path: node.file_path,
                kind: node_kind_label(hit.node_kind).to_owned(),
                language: hit.lang,
                line_start,
                repo: node.repo,
                score: hit.adjusted_score,
                symbol_id: encode_node_id(hit.node_id),
                symbol_name: hit.symbol_name,
            }))
        })
        .collect::<Result<Vec<_>, McpServerError>>()?;
    items.sort_by(search_item_cmp);

    // Apply anchor ranking: re-sort items by anchor score descending so that
    // broad shared-contract and boundary nodes surface first.  Ranking is
    // best-effort — errors are non-fatal (e.g. node no longer in store).
    items = anchor_rerank(ctx, items);

    let mut response = SearchResponse {
        data: SearchResponseData {
            returned: items.len(),
            total_estimate: offset + items.len() + usize::from(truncated),
            results: items,
        },
        meta: Some(SearchMeta {
            response_schema_version: response_schema_version(),
            budget: ResponseBudget::not_truncated(BudgetedTool::Search, 0, 0),
            next_cursor: if truncated {
                Some(encode_search_cursor(
                    &request,
                    offset + requested_limit,
                    ctx.cursor_key(),
                )?)
            } else {
                None
            },
            truncated,
        }),
    };
    let budget = apply_response_budget(
        BudgetedTool::Search,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.results.pop().is_some(),
    )?;
    response.data.returned = response.data.results.len();
    response.data.total_estimate = offset + response.data.results.len() + usize::from(truncated);
    if let Some(meta) = &mut response.meta {
        meta.budget = budget.clone();
        meta.truncated |= budget.truncated;
        meta.next_cursor = if meta.truncated {
            Some(encode_search_cursor(
                &request,
                offset + response.data.results.len(),
                ctx.cursor_key(),
            )?)
        } else {
            None
        };
    }
    Ok(response)
}

pub fn get_symbol(
    ctx: &McpContext,
    request: SymbolRequest,
) -> Result<SymbolResponse, McpServerError> {
    validate_input_length("symbol_id", &request.symbol_id)?;
    let graph = ctx.graph();
    let symbol_id = decode_node_id(&request.symbol_id).map_err(McpServerError::InvalidInput)?;
    let node = graph.get_node(symbol_id)?;
    let symbol_id_hex = request.symbol_id;

    let data = match node {
        None => SymbolResponseData {
            decorators: Vec::new(),
            file_path: None,
            found: false,
            is_virtual: None,
            kind: None,
            line_end: None,
            line_start: None,
            name: None,
            qualified_name: None,
            repo: None,
            signature: None,
            symbol_id: symbol_id_hex,
            visibility: None,
        },
        Some(node) => SymbolResponseData {
            decorators: list_decorators(graph, node.id).unwrap_or_default(),
            file_path: Some(node.file_path),
            found: true,
            is_virtual: Some(node.is_virtual),
            kind: Some(node_kind_label(node.kind).to_owned()),
            line_end: node
                .span
                .as_ref()
                .map(gather_step_core::SourceSpan::line_end),
            line_start: node.span.as_ref().map(|span| span.line_start),
            name: Some(node.name),
            qualified_name: node.qualified_name,
            repo: Some(node.repo),
            signature: node.signature,
            symbol_id: symbol_id_hex,
            visibility: node.visibility.map(|visibility| {
                // The Debug impl already allocates; lowering in place avoids
                // a second allocation.
                let mut s = format!("{visibility:?}");
                s.make_ascii_lowercase();
                s
            }),
        },
    };

    Ok(SymbolResponse { data })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchCursorPayload {
    kind: Option<String>,
    language: Option<String>,
    offset: usize,
    query: String,
    repo: Option<String>,
}

const CURSOR_VERSION_PREFIX: &str = "v1:";
const CURSOR_MAC_HEX_LEN: usize = 64; // 32-byte blake3 MAC = 64 hex chars

fn apply_search_cursor(
    request: &mut SearchRequest,
    cursor: &str,
    key: &[u8; 32],
) -> Result<usize, McpServerError> {
    let payload = decode_search_cursor(cursor, key)?;
    if payload.query != request.query
        || payload.repo != request.repo
        || payload.language != request.language
        || payload.kind != request.kind
    {
        return Err(McpServerError::InvalidInput(
            "search cursor does not match the current query arguments".to_owned(),
        ));
    }
    Ok(payload.offset)
}

fn encode_search_cursor(
    request: &SearchRequest,
    offset: usize,
    key: &[u8; 32],
) -> Result<String, McpServerError> {
    let payload = SearchCursorPayload {
        kind: request.kind.clone(),
        language: request.language.clone(),
        offset,
        query: request.query.clone(),
        repo: request.repo.clone(),
    };
    let json = serde_json::to_vec(&payload)
        .map_err(|error| McpServerError::Internal(format!("cursor encoding: {error}")))?;
    let payload_hex = hex_encode(&json);
    let mac = blake3::keyed_hash(key, json.as_slice());
    let mac_hex = hex_encode(mac.as_bytes());
    Ok(format!("{CURSOR_VERSION_PREFIX}{payload_hex}{mac_hex}"))
}

fn decode_search_cursor(
    cursor: &str,
    key: &[u8; 32],
) -> Result<SearchCursorPayload, McpServerError> {
    let body = cursor.strip_prefix(CURSOR_VERSION_PREFIX).ok_or_else(|| {
        McpServerError::InvalidInput("search cursor version is not supported".to_owned())
    })?;
    if body.len() < CURSOR_MAC_HEX_LEN {
        return Err(McpServerError::InvalidInput(
            "search cursor is not valid opaque state".to_owned(),
        ));
    }
    let (payload_hex, mac_hex) = body.split_at(body.len() - CURSOR_MAC_HEX_LEN);
    let payload_bytes = hex_decode(payload_hex).ok_or_else(|| {
        McpServerError::InvalidInput("search cursor payload is not valid hex".to_owned())
    })?;
    let expected_mac = blake3::keyed_hash(key, &payload_bytes);
    let actual_mac = hex_decode(mac_hex).ok_or_else(|| {
        McpServerError::InvalidInput("search cursor MAC is not valid hex".to_owned())
    })?;
    // Use constant-time comparison to avoid timing oracle attacks.
    let mac_ok: bool = actual_mac.as_slice().ct_eq(expected_mac.as_bytes()).into();
    if !mac_ok {
        return Err(McpServerError::InvalidInput(
            "search cursor integrity check failed".to_owned(),
        ));
    }
    serde_json::from_slice(&payload_bytes)
        .map_err(|error| McpServerError::InvalidInput(error.to_string()))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(input: &str) -> Option<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let raw = input.as_bytes();
    for index in (0..raw.len()).step_by(2) {
        let high = decode_hex_nibble(raw[index])?;
        let low = decode_hex_nibble(raw[index + 1])?;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

/// Verify that `actual_mac_bytes` matches the BLAKE3 keyed-hash MAC of
/// `payload` under `key` using a constant-time comparison.
///
/// Returns `true` if the MAC is correct, `false` otherwise.
///
/// Available in test builds and when the `test-support` feature is enabled.
#[doc(hidden)]
pub fn verify_cursor_mac_for_test(payload: &[u8], actual_mac_bytes: &[u8], key: &[u8; 32]) -> bool {
    let expected_mac = blake3::keyed_hash(key, payload);
    let mac_ok: bool = actual_mac_bytes.ct_eq(expected_mac.as_bytes()).into();
    mac_ok
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn list_decorators(
    graph: &impl GraphStore,
    symbol_id: NodeId,
) -> Result<Vec<String>, McpServerError> {
    let mut decorators = graph
        .get_outgoing(symbol_id)?
        .into_iter()
        .filter(|edge| edge.kind == EdgeKind::UsesDecorator)
        .filter_map(|edge| graph.get_node(edge.target).ok().flatten())
        .map(|node| node.name)
        .collect::<Vec<_>>();
    decorators.sort();
    decorators.dedup();
    Ok(decorators)
}

pub fn get_callers(
    ctx: &McpContext,
    request: TraversalRequest,
) -> Result<TraversalResponse, McpServerError> {
    validate_input_length("symbol_id", &request.symbol_id)?;
    let symbol_id = decode_node_id(&request.symbol_id).map_err(McpServerError::InvalidInput)?;
    let requested_depth = request.depth.unwrap_or(1);
    let capped_depth = requested_depth.clamp(1, MAX_TRAVERSAL_DEPTH);
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_TRAVERSAL_LIMIT);
    let result = traverse_direction(
        ctx.graph(),
        symbol_id,
        capped_depth,
        limit,
        Direction::Incoming,
    )?;

    let mut response = TraversalResponse {
        data: TraversalResponseData {
            returned: result.nodes.len(),
            symbol_id: request.symbol_id,
            traversal: result.nodes,
        },
        meta: Some(TraversalMeta {
            response_schema_version: response_schema_version(),
            budget: ResponseBudget::not_truncated(BudgetedTool::Traversal, 0, 0),
            depth_capped: requested_depth != capped_depth,
            truncated: result.limit_reached,
        }),
    };
    sort_traversal(&mut response.data.traversal);
    let budget = apply_response_budget(
        BudgetedTool::Traversal,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.traversal.pop().is_some(),
    )?;
    response.data.returned = response.data.traversal.len();
    if let Some(meta) = &mut response.meta {
        meta.budget = budget;
        meta.truncated |= meta.budget.truncated;
    }
    Ok(response)
}

pub fn get_callees(
    ctx: &McpContext,
    request: TraversalRequest,
) -> Result<TraversalResponse, McpServerError> {
    validate_input_length("symbol_id", &request.symbol_id)?;
    let symbol_id = decode_node_id(&request.symbol_id).map_err(McpServerError::InvalidInput)?;
    let requested_depth = request.depth.unwrap_or(1);
    let capped_depth = requested_depth.clamp(1, MAX_TRAVERSAL_DEPTH);
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_TRAVERSAL_LIMIT);
    let result = traverse_direction(
        ctx.graph(),
        symbol_id,
        capped_depth,
        limit,
        Direction::Outgoing,
    )?;

    let mut response = TraversalResponse {
        data: TraversalResponseData {
            returned: result.nodes.len(),
            symbol_id: request.symbol_id,
            traversal: result.nodes,
        },
        meta: Some(TraversalMeta {
            response_schema_version: response_schema_version(),
            budget: ResponseBudget::not_truncated(BudgetedTool::Traversal, 0, 0),
            depth_capped: requested_depth != capped_depth,
            truncated: result.limit_reached,
        }),
    };
    sort_traversal(&mut response.data.traversal);
    let budget = apply_response_budget(
        BudgetedTool::Traversal,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.traversal.pop().is_some(),
    )?;
    response.data.returned = response.data.traversal.len();
    if let Some(meta) = &mut response.meta {
        meta.budget = budget;
        meta.truncated |= meta.budget.truncated;
    }
    Ok(response)
}

#[derive(Clone, Copy)]
enum Direction {
    Incoming,
    Outgoing,
}

struct TraversalResult {
    nodes: Vec<TraversalNode>,
    limit_reached: bool,
}

fn traverse_direction(
    graph: &impl GraphStore,
    start: NodeId,
    max_depth: usize,
    limit: usize,
    direction: Direction,
) -> Result<TraversalResult, McpServerError> {
    let mut queue = VecDeque::from([(start, 0_usize)]);
    let mut seen = BTreeSet::from([start.as_bytes()]);
    let mut traversal = Vec::new();

    while let Some((node_id, depth)) = queue.pop_front() {
        if depth >= max_depth || traversal.len() >= limit {
            continue;
        }

        let edges = match direction {
            Direction::Incoming => graph.get_incoming(node_id)?,
            Direction::Outgoing => graph.get_outgoing(node_id)?,
        };

        for edge in edges
            .into_iter()
            .filter(|edge| edge.kind == EdgeKind::Calls)
        {
            let next_id = match direction {
                Direction::Incoming => edge.source,
                Direction::Outgoing => edge.target,
            };
            if !seen.insert(next_id.as_bytes()) {
                continue;
            }

            let Some(node) = graph.get_node(next_id)? else {
                continue;
            };

            traversal.push(node_to_traversal(node, depth + 1));
            if traversal.len() >= limit {
                break;
            }
            queue.push_back((next_id, depth + 1));
        }
    }

    let limit_reached = traversal.len() >= limit;
    Ok(TraversalResult {
        nodes: traversal,
        limit_reached,
    })
}

fn node_to_traversal(node: NodeData, depth: usize) -> TraversalNode {
    TraversalNode {
        depth,
        file_path: node.file_path,
        kind: node_kind_label(node.kind).to_owned(),
        line_start: node.span.as_ref().map(|span| span.line_start),
        repo: node.repo,
        symbol_id: encode_node_id(node.id),
        symbol_name: node.name,
    }
}

fn sort_traversal(nodes: &mut [TraversalNode]) {
    nodes.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

/// Re-rank search results by anchor score descending.
///
/// Decodes each item's `symbol_id` to a `NodeId`, calls [`rank_anchors`] on
/// the full candidate set, then re-sorts `items` so that nodes with higher
/// anchor scores appear first.  Items whose `symbol_id` cannot be decoded or
/// whose score cannot be determined fall through to their original position.
///
/// Errors from the graph store are silently discarded so that a ranking
/// failure never causes a search failure.
fn anchor_rerank(ctx: &McpContext, mut items: Vec<SearchResultItem>) -> Vec<SearchResultItem> {
    let node_ids: Vec<NodeId> = items
        .iter()
        .filter_map(|item| decode_node_id(&item.symbol_id).ok())
        .collect();

    if node_ids.is_empty() {
        return items;
    }

    let Ok(ranked) = rank_anchors(ctx.graph(), &node_ids) else {
        return items;
    };

    // Build a score lookup keyed by NodeId bytes.
    let score_map: rustc_hash::FxHashMap<[u8; 16], f32> = ranked
        .into_iter()
        .map(|anchor| (anchor.node.as_bytes(), anchor.score))
        .collect();

    items.sort_by(|left, right| {
        // (1) exact_match wins first, (2) anchor score descending, (3) tie-break.
        right
            .exact_match
            .cmp(&left.exact_match)
            .then_with(|| {
                let left_score = decode_node_id(&left.symbol_id)
                    .ok()
                    .and_then(|id| score_map.get(&id.as_bytes()).copied())
                    .unwrap_or(0.0_f32);
                let right_score = decode_node_id(&right.symbol_id)
                    .ok()
                    .and_then(|id| score_map.get(&id.as_bytes()).copied())
                    .unwrap_or(0.0_f32);
                right_score.total_cmp(&left_score)
            })
            .then_with(|| search_item_cmp(left, right))
    });

    items
}

fn search_item_cmp(left: &SearchResultItem, right: &SearchResultItem) -> std::cmp::Ordering {
    right
        .exact_match
        .cmp(&left.exact_match)
        .then_with(|| right.score.total_cmp(&left.score))
        .then(left.repo.cmp(&right.repo))
        .then(left.file_path.cmp(&right.file_path))
        .then(left.line_start.cmp(&right.line_start))
        .then(left.symbol_name.cmp(&right.symbol_name))
        .then(left.symbol_id.cmp(&right.symbol_id))
}

fn parse_node_kind(input: &str) -> Option<NodeKind> {
    let mut normalized = input.trim().to_owned();
    normalized.make_ascii_lowercase();
    match normalized.as_str() {
        "file" => Some(NodeKind::File),
        "function" => Some(NodeKind::Function),
        "class" => Some(NodeKind::Class),
        "type" => Some(NodeKind::Type),
        "module" => Some(NodeKind::Module),
        "import" => Some(NodeKind::Import),
        "decorator" => Some(NodeKind::Decorator),
        "entity" => Some(NodeKind::Entity),
        "route" => Some(NodeKind::Route),
        "topic" => Some(NodeKind::Topic),
        "queue" => Some(NodeKind::Queue),
        "subject" => Some(NodeKind::Subject),
        "stream" => Some(NodeKind::Stream),
        "event" => Some(NodeKind::Event),
        "sharedsymbol" | "shared_symbol" => Some(NodeKind::SharedSymbol),
        "payloadcontract" | "payload_contract" => Some(NodeKind::PayloadContract),
        "repo" => Some(NodeKind::Repo),
        "convention" => Some(NodeKind::Convention),
        "service" => Some(NodeKind::Service),
        "commit" => Some(NodeKind::Commit),
        "pr" => Some(NodeKind::PR),
        "review" => Some(NodeKind::Review),
        "comment" => Some(NodeKind::Comment),
        "author" => Some(NodeKind::Author),
        "ticket" => Some(NodeKind::Ticket),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use gather_step_core::NodeKind;

    use super::{
        SearchRequest, SearchResultItem, apply_search_cursor, decode_search_cursor,
        encode_search_cursor, parse_node_kind, search_item_cmp,
    };

    fn cursor_key() -> [u8; 32] {
        [7_u8; 32]
    }

    fn request() -> SearchRequest {
        SearchRequest {
            budget_bytes: None,
            cursor: None,
            kind: Some("function".to_owned()),
            language: Some("typescript".to_owned()),
            limit: Some(5),
            query: "createOrder".to_owned(),
            repo: Some("backend_standard".to_owned()),
        }
    }

    #[test]
    fn search_cursor_round_trips_and_restores_offset() {
        let key = cursor_key();
        let mut request = request();
        let cursor = encode_search_cursor(&request, 12, &key).expect("cursor should encode");

        let offset = apply_search_cursor(&mut request, &cursor, &key)
            .expect("cursor should decode and match");
        let payload = decode_search_cursor(&cursor, &key).expect("cursor payload should decode");

        assert_eq!(offset, 12);
        assert_eq!(payload.offset, 12);
        assert_eq!(payload.query, "createOrder");
        assert_eq!(payload.repo.as_deref(), Some("backend_standard"));
    }

    #[test]
    fn search_cursor_rejects_mismatched_query_arguments() {
        let key = cursor_key();
        let base = request();
        let cursor = encode_search_cursor(&base, 5, &key).expect("cursor should encode");
        let mut mismatched = request();
        mismatched.repo = Some("frontend_standard".to_owned());

        let error = apply_search_cursor(&mut mismatched, &cursor, &key)
            .expect_err("cursor should reject mismatched request arguments");

        assert!(
            error
                .to_string()
                .contains("search cursor does not match the current query arguments")
        );
    }

    #[test]
    fn search_cursor_rejects_invalid_hex_payload() {
        let key = cursor_key();
        let error = decode_search_cursor("v1:zz", &key)
            .expect_err("invalid cursor payload should be rejected");

        assert!(
            error
                .to_string()
                .contains("search cursor is not valid opaque state")
                || error
                    .to_string()
                    .contains("search cursor payload is not valid hex")
        );
    }

    #[test]
    fn parse_node_kind_accepts_shared_symbol_aliases() {
        assert_eq!(
            parse_node_kind("sharedsymbol"),
            Some(NodeKind::SharedSymbol)
        );
        assert_eq!(
            parse_node_kind("shared_symbol"),
            Some(NodeKind::SharedSymbol)
        );
    }

    #[test]
    fn search_item_cmp_prefers_exact_then_score_then_location() {
        let left = SearchResultItem {
            exact_match: false,
            file_path: "b.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: Some(12),
            repo: "backend_standard".to_owned(),
            score: 0.8,
            symbol_id: "a".to_owned(),
            symbol_name: "alpha".to_owned(),
        };
        let right = SearchResultItem {
            exact_match: true,
            file_path: "a.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: Some(1),
            repo: "backend_standard".to_owned(),
            score: 0.5,
            symbol_id: "b".to_owned(),
            symbol_name: "beta".to_owned(),
        };

        assert!(search_item_cmp(&left, &right).is_gt());
    }
}
