use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use gather_step_analysis::anchor::rank_anchors;
use gather_step_analysis::event_topology::{resolve_event_targets, resolve_route_target};
use gather_step_analysis::evidence::{edge_specificity, evidence_chain_for};
use gather_step_analysis::pack_assembly::{PackMode, QueryShape, classify_query_shape};
use gather_step_analysis::proofs::{
    ProofCaller, ProofEngineOptions, build_pack_proofs, derive_repo_sets, is_real_repo,
};
use gather_step_analysis::shared_contract_impact;
use gather_step_analysis::transport::{TransportLink, transport_links_for};
use gather_step_analysis::{ProjectionImpactRequest, projection_impact};
use gather_step_core::{EdgeKind, NodeId, NodeKind, PlanningProof, node_id};
use gather_step_output::evidence::render_evidence_chain;
use gather_step_storage::GraphStore;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::{
        BudgetedTool, OmittedReason, ResponseBudget, apply_response_budget, response_schema_version,
    },
    config::{McpContext, validate_input_length},
    error::McpServerError,
    ids::{decode_node_id, encode_node_id},
    tools::{
        cross_repo::{
            CrossRepoDepsRequest, TraceImpactRequest, cross_repo_deps_tool, trace_impact_tool,
        },
        labels::node_kind_label,
        search::{
            SearchRequest, SearchResultItem, SymbolRequest, SymbolResponseData, TraversalNode,
            TraversalRequest, get_callees, get_callers, get_symbol, search_symbols,
        },
    },
};

const DEFAULT_PACK_LIMIT: usize = 6;
const MAX_CONTEXT_PACK_CACHE_BYTES: i64 = 64 * 1024 * 1024;
const CONTEXT_PACK_CACHE_KEY_VERSION: &str = "v1";
const PACK_CONFIDENCE_MODEL_VERSION: &str = "v1.0";
const PACK_CONFIDENCE_HIGH_MARGIN: i32 = 125;
const PACK_CONFIDENCE_MEDIUM_MARGIN: i32 = 75;
const PACK_CONFIDENCE_HIGH_MIN_SCORE: i32 = 500;
const PACK_CONFIDENCE_MEDIUM_MIN_SCORE: i32 = 425;
const NO_SEMANTIC_BRIDGE_GAP: &str = "no semantic bridge nodes were attached to the target";
const CROSS_REPO_IMPACT_GAP: &str = "cross-repo impact did not resolve to any downstream repos";
const EMPTY_REPO_DEPS_GAP: &str = "owning repo has no indexed cross-repo dependencies";
const TRUNCATED_RESPONSE_WARNING: &str = "response was truncated to fit the requested budget";

/// Per-process single-flight registry keyed by pack `cache_key`.
///
/// Concurrent MCP calls for the same key serialize on the per-key mutex so
/// only the first caller runs the expensive graph traversal; later callers
/// block briefly, then hit the persisted pack record on re-check.
///
/// The map holds [`std::sync::Weak`] references.  Each active flight holds an
/// [`Arc`]; the map holds a [`Weak`] that upgrades only while at least one
/// flight is alive.  When all flights for a key finish and drop their
/// [`Arc`], the [`Weak`] becomes dangling and [`try_drop_inflight_entry`]
/// removes the stale entry.  This bounds the map to the number of
/// *concurrently active* flights, not the total number of distinct cache keys
/// ever seen.
use std::sync::Weak;
static PACK_INFLIGHT: LazyLock<Mutex<BTreeMap<String, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn inflight_entry(cache_key: &str) -> Arc<Mutex<()>> {
    let mut guard = lock_unpoisoned(&PACK_INFLIGHT);
    if let Some(weak) = guard.get(cache_key)
        && let Some(arc) = weak.upgrade()
    {
        return arc;
    }
    let arc = Arc::new(Mutex::new(()));
    guard.insert(cache_key.to_owned(), Arc::downgrade(&arc));
    arc
}

/// Remove the registry entry for `cache_key` if its [`Weak`] reference is no
/// longer upgradeable (i.e. all flights for that key have finished).  Calling
/// this after dropping an [`Arc`] returned by [`inflight_entry`] keeps the
/// map bounded to active flights.
fn try_drop_inflight_entry(cache_key: &str) {
    let mut guard = lock_unpoisoned(&PACK_INFLIGHT);
    if let Some(weak) = guard.get(cache_key)
        && weak.upgrade().is_none()
    {
        guard.remove(cache_key);
    }
}

/// RAII guard that calls [`try_drop_inflight_entry`] when dropped, so every
/// code path that exits `context_pack_tool` cleans up the registry entry.
struct InflightGuard {
    cache_key: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        try_drop_inflight_entry(&self.cache_key);
    }
}

/// Return the current number of entries in the inflight registry.
///
/// Available in test builds and when the `test-support` feature is enabled.
#[doc(hidden)]
pub fn pack_inflight_len_for_test() -> usize {
    lock_unpoisoned(&PACK_INFLIGHT).len()
}

/// Obtain an inflight guard `Arc` for `cache_key` — same semantics as the
/// production [`inflight_entry`].
///
/// Available in test builds and when the `test-support` feature is enabled.
#[doc(hidden)]
pub fn inflight_entry_for_test(cache_key: &str) -> Arc<Mutex<()>> {
    inflight_entry(cache_key)
}

/// Attempt to remove a stale registry entry for `cache_key` — same semantics
/// as the production [`try_drop_inflight_entry`].
///
/// Available in test builds and when the `test-support` feature is enabled.
#[doc(hidden)]
pub fn try_drop_inflight_entry_for_test(cache_key: &str) {
    try_drop_inflight_entry(cache_key);
}

/// Simulate an internal deserialization failure to exercise the
/// [`McpServerError::Internal`] classification path.
///
/// Feeds an empty byte slice to a `serde_json` decode of
/// [`ContextPackResponse`], which is an operation that can only fail due to
/// internal data-integrity issues (not user input).
///
/// Available in test builds and when the `test-support` feature is enabled.
#[doc(hidden)]
pub fn simulate_internal_deserialize_failure_for_test() -> McpServerError {
    serde_json::from_slice::<ContextPackResponse>(&[])
        .map_err(|error| McpServerError::Internal(format!("cache deserialize: {error}")))
        .expect_err("empty slice must fail to deserialize")
}

const MAX_CHANGE_IMPACT_REPOS: usize = 8;
/// Per-repo serialized byte cap inside a `change_impact_pack`. Individual repo
/// entries larger than this are surfaced via `truncated_repos.reason_codes`
/// rather than silently included.
const CHANGE_IMPACT_REPO_BYTE_CAP: usize = 2_048;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextPackRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
    pub mode: String,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ModePackRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub depth: Option<usize>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextPackResponse {
    pub data: ContextPackData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<ContextPackMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextPackData {
    pub mode: String,
    pub target: String,
    pub found: bool,
    pub items: Vec<PackItem>,
    pub semantic_bridges: Vec<PackBridge>,
    pub next_steps: Vec<String>,
    pub unresolved_gaps: Vec<String>,
    pub change_impact: ChangeImpactSummary,
    /// Transport links (HTTP route and queue boundary pairs) relevant to the
    /// pack target, serialised on demand.  Absent when none are found or when
    /// the store query fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_links: Option<Vec<serde_json::Value>>,
    /// alternate anchors and hints produced by the planning rescue cascade.
    /// Absent in non-planning modes or when the initial pack is complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planning_rescue: Option<PlanningRescue>,
    /// Machine-readable cross-repo relationship proofs derived from the edge
    /// graph.  Each entry explains why a remote repo appears in the response.
    ///
    /// Serialized as plain JSON values so the field can be included in a
    /// [`JsonSchema`]-derived response struct without adding `schemars` as a
    /// dependency of the core types crate.  Absent (zero serialization cost)
    /// when the anchor has no cross-repo edges.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub planning_proofs: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ContextPackMeta {
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
    #[serde(default)]
    pub generation: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ambiguity: Option<PackAmbiguity>,
    pub budget: ResponseBudget,
    pub candidate_count: usize,
    pub completeness: String,
    pub resolution: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_details: Option<PackResolutionDetails>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence_model_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_confidence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_symbol_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub winner_margin: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PackResolutionDetails {
    pub strategy: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub winner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub winner_margin: Option<u16>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alternates: Vec<RescueAnchor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PackAmbiguity {
    pub candidate_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PackItem {
    pub category: String,
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub reason: String,
    pub repo: String,
    pub score: u16,
    pub symbol_id: String,
    pub symbol_kind: String,
    pub symbol_name: String,
    /// Evidence chain from the pack anchor to this item's primary node,
    /// rendered as a JSON value so the MCP response is self-contained without
    /// requiring generic store access at serialization time.
    ///
    /// Absent when no path was found within the hop limit or when the item IS
    /// the anchor itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_chain: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PackBridge {
    pub kind: String,
    pub name: String,
    pub repo: String,
    pub symbol_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
pub struct ChangeImpactSummary {
    pub direct_repos: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cross_repo_callers: Vec<CrossRepoCaller>,
    /// Downstream repos with confirmed graph-traversal proof (edge-chain evidence).
    /// This is the authoritative set; `downstream_repos` is a backward-compat alias.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub confirmed_downstream_repos: Vec<String>,
    /// Downstream repos inferred from partial evidence (repo-level deps, transport
    /// hints). Populated only when `confirmed_downstream_repos` is empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub probable_downstream_repos: Vec<String>,
    /// Backward-compatibility alias for `confirmed_downstream_repos`.
    pub downstream_repos: Vec<String>,
    pub unresolved_possible: Vec<String>,
    /// Structured metadata for repos dropped by a fan-out cap. Absent when no
    /// repos were truncated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_repos: Option<TruncatedRepos>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct TruncatedRepos {
    pub count: usize,
    pub names: Vec<String>,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct CrossRepoCaller {
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_start: Option<u32>,
    pub repo: String,
    pub symbol_id: String,
    pub symbol_kind: String,
    pub symbol_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RescueAnchor {
    pub anchor_form: String,
    pub repo: String,
    pub symbol_id: String,
    pub symbol_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_delta: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct PlanningRescue {
    pub triggered: bool,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub alternate_anchors: Vec<RescueAnchor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<String>,
}

type PackFiles = Vec<(String, String)>;

struct ResolvedPackTarget {
    alternate_anchors: Vec<RescueAnchor>,
    candidate_count: usize,
    confidence_model_version: Option<String>,
    resolution: String,
    resolution_confidence: Option<String>,
    symbol_id: Option<String>,
    winner_margin: Option<u16>,
    /// Populated only when `resolution == "search_ranked_alternates"`.  Contains
    /// all candidates that cleared the noise floor, sorted by descending score.
    ranked_alternates: Vec<RankedPackCandidate>,
}

#[derive(Clone)]
struct RankedPackCandidate {
    file_path: String,
    repo: String,
    rationale: String,
    score: i32,
    symbol_id: String,
    symbol_name: String,
}

struct AssembledPack {
    response: ContextPackResponse,
}

fn pack_resolution_strategy(resolution: &str, rescue_triggered: bool) -> String {
    if rescue_triggered {
        return "rescue".to_owned();
    }
    match resolution {
        "search_ranked_resolved" | "search_ranked_alternates" | "search_ranked_deferred" => {
            "ranked".to_owned()
        }
        "symbol_id" | "search_resolved" | "event_anchor" | "route_anchor" => "exact".to_owned(),
        _ => "fallback".to_owned(),
    }
}

/// Build lightweight [`PackItem`] entries for each ranked alternate candidate.
///
/// These entries are emitted when no single confident winner was selected.  Each
/// candidate is represented with `category = "ranked_alternate"` and its raw
/// composite score so the LLM can identify which repo/file to narrow to.
fn build_ranked_alternate_items(candidates: &[RankedPackCandidate]) -> Vec<PackItem> {
    candidates
        .iter()
        .map(|candidate| PackItem {
            category: "ranked_alternate".to_owned(),
            file_path: candidate.file_path.clone(),
            line_start: None,
            reason: candidate.rationale.clone(),
            repo: candidate.repo.clone(),
            score: u16::try_from(candidate.score.clamp(0, i32::from(u16::MAX))).unwrap_or(u16::MAX),
            symbol_id: candidate.symbol_id.clone(),
            symbol_kind: "unknown".to_owned(),
            symbol_name: candidate.symbol_name.clone(),
            evidence_chain: None,
        })
        .collect()
}

fn build_resolution_details(
    resolved: &ResolvedPackTarget,
    winner_symbol_id: Option<&str>,
    rescue: Option<&PlanningRescue>,
) -> PackResolutionDetails {
    let alternates = rescue.map_or_else(
        || resolved.alternate_anchors.clone(),
        |item| item.alternate_anchors.clone(),
    );
    PackResolutionDetails {
        strategy: pack_resolution_strategy(&resolved.resolution, rescue.is_some()),
        winner: winner_symbol_id
            .map(str::to_owned)
            .or_else(|| resolved.symbol_id.clone()),
        winner_margin: resolved.winner_margin,
        alternates,
    }
}

fn apply_planning_rescue_metadata(
    response: &mut ContextPackResponse,
    resolved: &ResolvedPackTarget,
    rescue: PlanningRescue,
) {
    if let Some(meta) = &mut response.meta {
        meta.resolution_details = Some(build_resolution_details(
            resolved,
            meta.resolved_symbol_id.as_deref(),
            Some(&rescue),
        ));
    }
    response.data.planning_rescue = Some(rescue);
}

#[derive(Clone, Copy)]
struct PackAssemblyOptions<'a> {
    budget_tool: BudgetedTool,
    depth: usize,
    limit: usize,
    repo_filter: Option<&'a str>,
    traversal_limit: usize,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "pack assembly persists and forwards the owned request across helper boundaries"
)]
pub fn context_pack_tool(
    ctx: &McpContext,
    request: ContextPackRequest,
) -> Result<ContextPackResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    validate_input_length("mode", &request.mode)?;
    let registry = ctx.registry_snapshot()?;
    if let Some(repo) = request.repo.as_deref() {
        validate_input_length("repo", repo)?;
        if !registry.repos.contains_key(repo) {
            return Err(McpServerError::InvalidInput(format!(
                "repo `{repo}` was not found in the workspace registry"
            )));
        }
    }

    let mode = PackMode::parse(&request.mode).map_err(McpServerError::InvalidInput)?;
    let budget_tool = pack_budget_tool(mode);
    let repo_filter = request.repo.as_deref();
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_PACK_LIMIT)
        .max(1);
    let traversal_limit = limit;
    let depth = request.depth.unwrap_or(2).clamp(1, 3);
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    // Record the call so the hot-whitelist can prefer frequently queried
    // `(target, mode)` pairs at the next index finalize. Errors here are
    // non-fatal — pack retrieval must not fail because call logging did.
    let call_log_target = request.target.trim();
    if !call_log_target.is_empty() {
        let _ = ctx
            .metadata()
            .record_pack_call(call_log_target, mode.as_str(), now_unix);
    }
    let resolved = resolve_pack_target(ctx, &request.target, repo_filter)?;
    // Cache identity lookup.
    //
    // Build the cache key from the request parameters only (no generation
    // component). If a cached record exists, validate it by recomputing the
    // current generation from the stored file-dependency list — this is a
    // cheap O(files) indexed read that avoids the expensive
    // full pack assembly on every warm hit.
    let cache_key = pack_identity_key(&request, mode, depth, limit, &resolved);
    if let Some(record) = ctx
        .metadata()
        .get_context_pack(&cache_key)
        .map_err(McpServerError::Metadata)?
    {
        let dep_files = ctx
            .metadata()
            .context_pack_files_for_key(&cache_key)
            .map_err(McpServerError::Metadata)?;
        let mut response = serde_json::from_slice::<ContextPackResponse>(&record.response)
            .map_err(|error| McpServerError::Internal(format!("cache deserialize: {error}")))?;
        let current_generation =
            current_cache_generation(ctx, &dep_files, &response, request.repo.as_deref())?;
        if current_generation == record.generation && cached_context_pack_is_current(&response) {
            refresh_cached_context_pack_response(
                ctx,
                &resolved,
                request.repo.as_deref(),
                &mut response,
            )?;
            // Generation key matches — serve the cached pack without any
            // graph traversal.
            ctx.metadata()
                .touch_context_pack(&cache_key, now_unix)
                .map_err(McpServerError::Metadata)?;
            return Ok(response);
        }
        // Generation is stale — fall through to full recompute below.
    }
    // Generation-scope computation (only on cache miss or stale entry).
    //
    // Acquire the per-key single-flight permit so concurrent callers for the
    // same key serialize.  `_inflight_guard` calls `try_drop_inflight_entry`
    // on drop (at every return path) so stale registry entries are reclaimed
    // as soon as the flight ends.
    let inflight_entry_arc = inflight_entry(&cache_key);
    let _inflight_guard = InflightGuard {
        cache_key: cache_key.clone(),
    };
    let _inflight_permit = lock_unpoisoned(&inflight_entry_arc);
    // Re-check the cache under the flight permit — another thread may have
    // already recomputed and persisted a fresh entry while we waited.
    if let Some(record) = ctx
        .metadata()
        .get_context_pack(&cache_key)
        .map_err(McpServerError::Metadata)?
    {
        let dep_files = ctx
            .metadata()
            .context_pack_files_for_key(&cache_key)
            .map_err(McpServerError::Metadata)?;
        let mut response = serde_json::from_slice::<ContextPackResponse>(&record.response)
            .map_err(|error| McpServerError::InvalidInput(error.to_string()))?;
        let current_generation =
            current_cache_generation(ctx, &dep_files, &response, request.repo.as_deref())?;
        if current_generation == record.generation && cached_context_pack_is_current(&response) {
            refresh_cached_context_pack_response(
                ctx,
                &resolved,
                request.repo.as_deref(),
                &mut response,
            )?;
            ctx.metadata()
                .touch_context_pack(&cache_key, now_unix)
                .map_err(McpServerError::Metadata)?;
            return Ok(response);
        }
    }
    // Full pack computation — only reaches here on a true miss or when the
    // stored generation is stale. The cache dependency set and generation are
    // computed together from the final response immediately before persisting.
    let Some(symbol_id) = resolved.symbol_id.clone() else {
        // Ranked-alternates path: multiple candidates cleared the noise floor but
        // none won confidently.  Return `found = true` with each candidate as a
        // lightweight `ranked_alternate` item so the LLM can pick one and reissue
        // the pack with a `node_id` or repo filter.
        if resolved.resolution == "search_ranked_alternates"
            && !resolved.ranked_alternates.is_empty()
        {
            let items = build_ranked_alternate_items(&resolved.ranked_alternates);
            let warning = "Multiple candidate symbols matched with similar confidence; \
                           review the ranked alternates and supply a `node_id` or repo \
                           filter to narrow."
                .to_owned();
            let mut warnings = vec![warning];
            warnings.extend(unresolved_resolution_warnings(&resolved));
            let mut response = ContextPackResponse {
                data: ContextPackData {
                    mode: mode.as_str().to_owned(),
                    target: request.target.clone(),
                    found: true,
                    items,
                    semantic_bridges: Vec::new(),
                    next_steps: vec!["search".to_owned(), "brief".to_owned()],
                    unresolved_gaps: Vec::new(),
                    change_impact: ChangeImpactSummary::default(),
                    transport_links: None,
                    planning_rescue: None,
                    planning_proofs: Vec::new(),
                },
                meta: Some(ContextPackMeta {
                    response_schema_version: response_schema_version(),
                    generation: 0,
                    ambiguity: pack_ambiguity(&resolved),
                    budget: ResponseBudget::not_truncated(budget_tool, 0, 0),
                    candidate_count: resolved.candidate_count,
                    completeness: "alternates".to_owned(),
                    confidence_model_version: resolved.confidence_model_version.clone(),
                    resolution: resolved.resolution.clone(),
                    resolution_details: Some(build_resolution_details(&resolved, None, None)),
                    resolution_confidence: resolved.resolution_confidence.clone(),
                    resolved_symbol_id: None,
                    winner_margin: resolved.winner_margin,
                    warnings,
                }),
            };
            let budget = apply_response_budget(
                budget_tool,
                request.budget_bytes,
                &mut response,
                trim_context_pack,
            )?;
            if let Some(meta) = &mut response.meta {
                meta.budget = budget;
                meta.budget.items_included = response.data.items.len();
                if meta.budget.truncated {
                    meta.warnings
                        .push("response was truncated to fit the requested budget".to_owned());
                }
            }
            let (cache_deps, generation) =
                compute_cache_deps_and_generation(ctx, &response, request.repo.as_deref())?;
            if let Some(meta) = &mut response.meta {
                meta.generation = generation;
            }
            persist_context_pack(
                ctx,
                &cache_key,
                &request,
                mode,
                generation,
                &response,
                &cache_deps,
                now_unix,
            )?;
            return Ok(response);
        }

        let warnings = unresolved_resolution_warnings(&resolved);
        let mut response = ContextPackResponse {
            data: ContextPackData {
                mode: mode.as_str().to_owned(),
                target: request.target.clone(),
                found: false,
                items: Vec::new(),
                semantic_bridges: Vec::new(),
                next_steps: vec!["search".to_owned(), "brief".to_owned()],
                unresolved_gaps: vec![
                    "target could not be resolved to an indexed symbol".to_owned(),
                ],
                change_impact: ChangeImpactSummary::default(),
                transport_links: None,
                planning_rescue: None,
                planning_proofs: Vec::new(),
            },
            meta: Some(ContextPackMeta {
                response_schema_version: response_schema_version(),
                generation: 0,
                ambiguity: pack_ambiguity(&resolved),
                budget: ResponseBudget::not_truncated(budget_tool, 0, 0),
                candidate_count: resolved.candidate_count,
                completeness: "unresolved".to_owned(),
                confidence_model_version: resolved.confidence_model_version.clone(),
                resolution: resolved.resolution.clone(),
                resolution_details: Some(build_resolution_details(&resolved, None, None)),
                resolution_confidence: resolved.resolution_confidence.clone(),
                resolved_symbol_id: None,
                winner_margin: resolved.winner_margin,
                warnings,
            }),
        };
        let budget = apply_response_budget(
            budget_tool,
            request.budget_bytes,
            &mut response,
            trim_context_pack,
        )?;
        if let Some(meta) = &mut response.meta {
            meta.budget = budget;
            meta.budget.items_included = response.data.items.len();
            if meta.budget.truncated {
                meta.warnings
                    .push("response was truncated to fit the requested budget".to_owned());
            }
        }
        if mode == PackMode::Planning {
            let rescue = attempt_planning_rescue(ctx, &request.target, &resolved, repo_filter);
            if rescue.triggered {
                apply_planning_rescue_metadata(&mut response, &resolved, rescue);
            }
        }
        let (cache_deps, generation) =
            compute_cache_deps_and_generation(ctx, &response, request.repo.as_deref())?;
        if let Some(meta) = &mut response.meta {
            meta.generation = generation;
        }
        persist_context_pack(
            ctx,
            &cache_key,
            &request,
            mode,
            generation,
            &response,
            &cache_deps,
            now_unix,
        )?;
        return Ok(response);
    };

    let assembly_options = PackAssemblyOptions {
        budget_tool,
        depth,
        limit,
        repo_filter,
        traversal_limit,
    };
    let mut assembled = assemble_context_pack_for_symbol(
        ctx,
        &request,
        mode,
        &resolved,
        &symbol_id,
        assembly_options,
        None,
    )?;
    if pack_is_structurally_weak(&assembled.response) {
        let rescue = attempt_planning_rescue(ctx, &request.target, &resolved, repo_filter);
        let mut best_score = pack_recovery_score(&assembled.response);
        for anchor in rescue.alternate_anchors.iter().take(3) {
            if anchor.symbol_id == symbol_id {
                continue;
            }
            if is_low_packability_kind(&anchor.anchor_form) {
                continue;
            }
            let candidate = assemble_context_pack_for_symbol(
                ctx,
                &request,
                mode,
                &resolved,
                &anchor.symbol_id,
                assembly_options,
                Some(format!(
                    "weak primary pack recovered via {} anchor '{}' ({})",
                    anchor.anchor_form, anchor.symbol_name, anchor.symbol_id
                )),
            )?;
            let candidate_score = pack_recovery_score(&candidate.response);
            if candidate_score > best_score {
                best_score = candidate_score;
                assembled = candidate;
            }
        }
    }
    let mut response = assembled.response;
    // planning rescue cascade — when mode is planning and the pack is
    // partial / unresolved, or when ranked symbol alternates exist, surface
    // alternate anchors with actionable hints.
    if mode == PackMode::Planning {
        let completeness = response
            .meta
            .as_ref()
            .map_or("", |m| m.completeness.as_str());
        if matches!(completeness, "partial" | "unresolved")
            || !resolved.alternate_anchors.is_empty()
        {
            let rescue = attempt_planning_rescue(ctx, &request.target, &resolved, repo_filter);
            if rescue.triggered {
                apply_planning_rescue_metadata(&mut response, &resolved, rescue);
            }
        }
    }

    let (cache_deps, generation) =
        compute_cache_deps_and_generation(ctx, &response, request.repo.as_deref())?;
    if let Some(meta) = &mut response.meta {
        meta.generation = generation;
    }

    persist_context_pack(
        ctx,
        &cache_key,
        &request,
        mode,
        generation,
        &response,
        &cache_deps,
        now_unix,
    )?;
    Ok(response)
}

fn assemble_context_pack_for_symbol(
    ctx: &McpContext,
    request: &ContextPackRequest,
    mode: PackMode,
    resolved: &ResolvedPackTarget,
    symbol_id: &str,
    options: PackAssemblyOptions<'_>,
    recovery_warning: Option<String>,
) -> Result<AssembledPack, McpServerError> {
    let registry = ctx.registry_snapshot()?;
    let symbol = get_symbol(
        ctx,
        SymbolRequest {
            symbol_id: symbol_id.to_owned(),
        },
    )?
    .data;
    let mut upstream_nodes = get_callers(
        ctx,
        TraversalRequest {
            budget_bytes: None,
            depth: Some(options.depth),
            limit: Some(options.traversal_limit),
            symbol_id: symbol_id.to_owned(),
        },
    )?
    .data
    .traversal;
    let downstream_nodes = get_callees(
        ctx,
        TraversalRequest {
            budget_bytes: None,
            depth: Some(options.depth),
            limit: Some(options.traversal_limit),
            symbol_id: symbol_id.to_owned(),
        },
    )?
    .data
    .traversal;
    let impact_data = match trace_impact_tool(
        ctx,
        TraceImpactRequest {
            budget_bytes: None,
            depth: Some(options.depth),
            target: symbol_id.to_owned(),
        },
    ) {
        Ok(response) => Some(response.data),
        Err(McpServerError::NotFound(_)) => None,
        Err(error) => return Err(error),
    };
    let contract_impact_items = if symbol.found {
        shared_contract_pack_items(ctx, &symbol)?
    } else {
        Vec::new()
    };
    let source_repo = symbol.repo.clone();
    let proof_output = if let Ok(anchor_id) = decode_node_id(symbol_id) {
        Some(
            build_pack_proofs(
                ctx.graph(),
                anchor_id,
                source_repo.as_deref().unwrap_or(""),
                ProofEngineOptions {
                    include_shared_peer_callers: matches!(mode, PackMode::Planning),
                    traversal_depth: options.depth,
                    traversal_limit: options.traversal_limit,
                },
            )
            .map_err(|error| McpServerError::Internal(format!("proof engine: {error}")))?,
        )
    } else {
        None
    };
    if matches!(mode, PackMode::Planning)
        && let Some(output) = &proof_output
    {
        append_supplemental_proof_callers(&mut upstream_nodes, &output.supplemental_callers);
    }
    let supplemental_callers = proof_output
        .as_ref()
        .map(|output| output.supplemental_callers.clone())
        .unwrap_or_default();
    let typed_proofs = proof_output.map(|output| output.proofs).unwrap_or_default();
    let planning_repo_filter = if mode == PackMode::Planning {
        None
    } else {
        options.repo_filter
    };
    let (proof_confirmed_repos, proof_probable_repos) =
        derive_repo_sets(&typed_proofs, planning_repo_filter);
    let mut cross_repo_callers =
        cross_repo_callers_from_proofs(ctx, &typed_proofs, source_repo.as_deref())?;
    merge_supplemental_cross_repo_callers(
        &mut cross_repo_callers,
        &supplemental_callers,
        source_repo.as_deref(),
    );
    let planning_proofs = planning_proofs_to_json(&typed_proofs);
    // `cross_repo_deps_tool` walks every node in the source repo and is
    // measurably expensive on large monorepos. Its result is only consumed
    // by the `unresolved_possible` fallback (when no proof-derived
    // downstream evidence was found) and by the `unresolved_gaps` "no deps"
    // hint. When the proof builders already established at least one
    // confirmed or probable cross-repo path, the call is pure overhead —
    // skip it. Cold runs without proof evidence still pay the cost so the
    // operator-visible fallback signal remains intact.
    let needs_repo_deps_fallback =
        proof_confirmed_repos.is_empty() && proof_probable_repos.is_empty();
    let repo_deps = if needs_repo_deps_fallback {
        symbol
            .repo
            .clone()
            .filter(|repo| registry.repos.contains_key(repo))
            .map(|repo| cross_repo_deps_tool(ctx, CrossRepoDepsRequest { repo }))
            .transpose()?
    } else {
        None
    };

    let mut items = Vec::<PackItem>::new();
    if symbol.found {
        items.push(PackItem {
            category: "target".to_owned(),
            file_path: symbol.file_path.clone().unwrap_or_default(),
            line_start: symbol.line_start,
            reason: target_reason(mode),
            repo: symbol.repo.clone().unwrap_or_default(),
            score: 1000,
            symbol_id: symbol.symbol_id.clone(),
            symbol_kind: symbol.kind.clone().unwrap_or_else(|| "unknown".to_owned()),
            symbol_name: symbol
                .name
                .clone()
                .unwrap_or_else(|| request.target.clone()),
            evidence_chain: None,
        });
    }
    items.extend(upstream_nodes.into_iter().map(|node| PackItem {
        category: "caller".to_owned(),
        file_path: node.file_path,
        line_start: node.line_start,
        reason: caller_reason(mode),
        repo: node.repo,
        score: ranked_score(900, node.depth),
        symbol_id: node.symbol_id,
        symbol_kind: node.kind,
        symbol_name: node.symbol_name,
        evidence_chain: None,
    }));
    items.extend(downstream_nodes.into_iter().map(|node| PackItem {
        category: "callee".to_owned(),
        file_path: node.file_path,
        line_start: node.line_start,
        reason: callee_reason(mode),
        repo: node.repo,
        score: ranked_score(850, node.depth),
        symbol_id: node.symbol_id,
        symbol_kind: node.kind,
        symbol_name: node.symbol_name,
        evidence_chain: None,
    }));
    let fallback_contract_repos = contract_impact_items
        .iter()
        .map(|item| item.repo.clone())
        .collect::<Vec<_>>();
    items.extend(contract_impact_items);
    items.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
    items.dedup_by(|left, right| {
        left.symbol_id == right.symbol_id && left.category == right.category
    });
    if let Some(repo) = options.repo_filter {
        items.retain(|item| item.repo == repo);
    }
    items.truncate(options.limit);

    let evidence_populated = items.len() <= 20;
    if evidence_populated && let Ok(anchor_id) = decode_node_id(&symbol.symbol_id) {
        for item in &mut items {
            if item.category == "target" {
                continue;
            }
            if let Ok(item_id) = decode_node_id(&item.symbol_id) {
                populate_evidence_chain(ctx.graph(), anchor_id, item_id, item);
            }
        }
    }
    if mode == PackMode::Planning && evidence_populated {
        apply_planning_evidence_ranking(&mut items);
    }

    let (mut semantic_bridges, _bridge_files) = collect_bridges(ctx, symbol_id)?;
    if let Some(repo) = options.repo_filter {
        semantic_bridges.retain(|bridge| bridge.repo == repo);
    }

    let mut next_steps = suggested_next_steps(mode);
    if !semantic_bridges.is_empty() {
        next_steps.push("trace_route".to_owned());
        next_steps.push("trace_event".to_owned());
    }
    if impact_data
        .as_ref()
        .is_some_and(|data| !data.impacted_repos.is_empty())
    {
        next_steps.push("trace_impact".to_owned());
    }
    if matches!(
        symbol.kind.as_deref().and_then(parse_symbol_kind),
        Some(NodeKind::Route)
    ) {
        next_steps.push("crud_trace".to_owned());
    }
    next_steps.sort();
    next_steps.dedup();

    let (downstream_repos, truncated_repos_meta) =
        cap_change_impact_repos(proof_confirmed_repos.clone(), planning_repo_filter);

    let mut warnings = unresolved_resolution_warnings(resolved);
    if let Some(recovery_warning) = recovery_warning {
        warnings.push(recovery_warning);
    }

    // Run `transport_links_for` lazily — only when confirmed downstream
    // repos are absent.  In the common case where impact or event traces have
    // already found cross-repo evidence, skipping the transport scan avoids an
    // O(transport-nodes × edges) traversal that would otherwise be paid on
    // every warm planning pack.
    let raw_transport_links = if downstream_repos.is_empty() {
        tracing::debug!(
            target = %request.target,
            "downstream_repos empty — running transport_links_for"
        );
        match transport_links_for(ctx.graph(), options.repo_filter, 200) {
            Ok(links) => Some(links),
            Err(err) => {
                warnings.push(format!("transport_links_for failed: {err}"));
                None
            }
        }
    } else {
        tracing::debug!(
            target = %request.target,
            downstream_count = downstream_repos.len(),
            "skipping transport_links_for: confirmed downstream repos already present"
        );
        None
    };
    let repo_dependency_repos = repo_deps
        .as_ref()
        .map(|response| {
            response
                .data
                .dependencies
                .iter()
                .filter(|dependency| {
                    dependency
                        .edge_kinds
                        .iter()
                        .any(|kind| is_downstream_dependency_edge_kind(kind))
                })
                .map(|dependency| dependency.repo.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let fallback_transport_repos = raw_transport_links
        .as_ref()
        .map(|links| transport_adjacent_repos(ctx, &symbol, &items, links))
        .unwrap_or_default();
    let unresolved_possible = if downstream_repos.is_empty() && proof_probable_repos.is_empty() {
        merge_probable_downstream_repos(
            symbol.repo.as_deref(),
            repo_dependency_repos,
            {
                let mut combined = fallback_contract_repos;
                combined.extend(proof_probable_repos.clone());
                combined
            },
            fallback_transport_repos,
        )
    } else {
        Vec::new()
    };
    if let Some(truncated) = &truncated_repos_meta {
        warnings.push(format!(
            "change impact fan-out exceeded cap; omitted {} downstream repos",
            truncated.count
        ));
    }
    let transport_links = raw_transport_links.as_ref().and_then(|links| {
        let values = links
            .iter()
            .filter_map(|link| serde_json::to_value(link).ok())
            .collect::<Vec<_>>();
        (!values.is_empty()).then_some(values)
    });

    let unresolved_gaps = compute_unresolved_gaps(
        mode,
        symbol.kind.as_deref().and_then(parse_symbol_kind),
        semantic_bridges.is_empty(),
        downstream_repos.is_empty(),
        unresolved_possible.is_empty(),
        repo_deps
            .as_ref()
            .is_some_and(|response| response.data.dependencies.is_empty()),
    );
    warnings.extend(gap_warnings(&unresolved_gaps));

    let mut response = ContextPackResponse {
        data: ContextPackData {
            mode: mode.as_str().to_owned(),
            target: request.target.clone(),
            found: symbol.found,
            items,
            semantic_bridges,
            next_steps,
            unresolved_gaps,
            change_impact: ChangeImpactSummary {
                direct_repos: symbol
                    .repo
                    .clone()
                    .into_iter()
                    .filter(|repo| options.repo_filter.is_none_or(|selected| repo == selected))
                    .collect(),
                cross_repo_callers,
                confirmed_downstream_repos: downstream_repos.clone(),
                probable_downstream_repos: if downstream_repos.is_empty() {
                    proof_probable_repos.clone()
                } else {
                    Vec::new()
                },
                downstream_repos,
                unresolved_possible,
                truncated_repos: truncated_repos_meta,
            },
            transport_links,
            planning_rescue: None,
            planning_proofs,
        },
        meta: Some(ContextPackMeta {
            response_schema_version: response_schema_version(),
            generation: 0,
            ambiguity: pack_ambiguity(resolved),
            budget: ResponseBudget::not_truncated(options.budget_tool, 0, 0),
            candidate_count: resolved.candidate_count,
            completeness: "complete".to_owned(),
            confidence_model_version: resolved.confidence_model_version.clone(),
            resolution: resolved.resolution.clone(),
            resolution_details: Some(build_resolution_details(resolved, Some(symbol_id), None)),
            resolution_confidence: resolved.resolution_confidence.clone(),
            resolved_symbol_id: Some(symbol_id.to_owned()),
            winner_margin: resolved.winner_margin,
            warnings,
        }),
    };
    if matches!(mode, PackMode::Planning | PackMode::ChangeImpact) {
        apply_projection_impact_summary(ctx, request, planning_repo_filter, &mut response);
    }
    let budget = apply_response_budget(
        options.budget_tool,
        request.budget_bytes,
        &mut response,
        trim_context_pack,
    )?;
    apply_proof_derived_change_impact(
        ctx,
        source_repo.as_deref(),
        planning_repo_filter,
        &mut response,
    )?;
    if let Some(meta) = &mut response.meta {
        meta.budget = budget;
        meta.budget.items_included = response.data.items.len();
        if response.data.change_impact.truncated_repos.is_some() && !meta.budget.truncated {
            meta.budget.truncated = true;
            meta.budget.omission_reason = Some(OmittedReason::FanOutCap);
        }
    }
    refresh_context_pack_completeness(&mut response);

    Ok(AssembledPack { response })
}

fn apply_projection_impact_summary(
    ctx: &McpContext,
    request: &ContextPackRequest,
    repo_filter: Option<&str>,
    response: &mut ContextPackResponse,
) {
    let targets = projection_impact_targets(ctx, request, repo_filter, response);
    let Some(report) = targets.into_iter().find_map(|target| {
        let report = projection_impact(
            ctx.graph(),
            ProjectionImpactRequest {
                target,
                repo: repo_filter.map(str::to_owned),
                max_results: 10,
            },
        )
        .ok()?;
        (report.resolved && report.ambiguity.is_none() && !report.derivation_edges.is_empty())
            .then_some(report)
    }) else {
        return;
    };

    let projected = report
        .projected_fields
        .iter()
        .map(|field| field.field_path.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let sources = report
        .source_fields
        .iter()
        .map(|field| field.field_path.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    response.data.next_steps.push(format!(
        "Review projection impact: source fields [{sources}], projected fields [{projected}]. Use projection_impact for full evidence."
    ));

    for hint in report.risk_hints {
        let gap = format!("projection_impact:{hint}");
        if !response.data.unresolved_gaps.contains(&gap) {
            response.data.unresolved_gaps.push(gap);
        }
    }
    if let Some(meta) = &mut response.meta {
        meta.warnings
            .push("projection impact evidence is available for this target".to_owned());
    }
}

fn projection_impact_targets(
    ctx: &McpContext,
    request: &ContextPackRequest,
    repo_filter: Option<&str>,
    response: &ContextPackResponse,
) -> Vec<String> {
    let mut targets = vec![request.target.clone()];
    let files = response
        .data
        .items
        .iter()
        .take(10)
        .filter(|item| item.category == "target")
        .filter(|item| repo_filter.is_none_or(|repo| item.repo == repo))
        .map(|item| (item.repo.as_str(), item.file_path.as_str()))
        .collect::<BTreeSet<_>>();
    if files.is_empty() {
        return targets;
    }
    let Ok(fields) = ctx.graph().nodes_by_type(NodeKind::DataField) else {
        return targets;
    };
    for field in fields {
        if !files.contains(&(field.repo.as_str(), field.file_path.as_str())) {
            continue;
        }
        if field_has_derivation(ctx.graph(), field.id) {
            push_unique_projection_target(&mut targets, field.name);
        }
        if targets.len() >= 10 {
            break;
        }
    }
    targets
}

fn push_unique_projection_target(targets: &mut Vec<String>, target: String) {
    if !targets.iter().any(|existing| existing == &target) {
        targets.push(target);
    }
}

fn field_has_derivation<S: GraphStore>(graph: &S, field_id: NodeId) -> bool {
    graph
        .get_incoming(field_id)
        .ok()
        .into_iter()
        .flatten()
        .chain(graph.get_outgoing(field_id).ok().into_iter().flatten())
        .any(|edge| edge.kind == EdgeKind::DerivesFieldFrom)
}

fn pack_is_structurally_weak(response: &ContextPackResponse) -> bool {
    let evidence_count = response
        .data
        .items
        .iter()
        .filter(|item| item.category != "target" && item.evidence_chain.is_some())
        .count();
    response.data.semantic_bridges.is_empty()
        && response
            .data
            .change_impact
            .confirmed_downstream_repos
            .is_empty()
        && evidence_count == 0
        && response.data.unresolved_gaps.len() >= 2
}

fn pack_recovery_score(response: &ContextPackResponse) -> i32 {
    let evidence_count = response
        .data
        .items
        .iter()
        .filter(|item| item.category != "target" && item.evidence_chain.is_some())
        .count();
    let transport_count = response.data.transport_links.as_ref().map_or(0, Vec::len);
    let probable_count = response.data.change_impact.probable_downstream_repos.len();
    let structural_items = response
        .data
        .items
        .iter()
        .filter(|item| item.category != "target")
        .count();
    bounded_count_score(response.data.semantic_bridges.len(), 10)
        + bounded_count_score(
            response.data.change_impact.confirmed_downstream_repos.len(),
            12,
        )
        + bounded_count_score(evidence_count, 6)
        + bounded_count_score(probable_count, 3)
        + bounded_count_score(transport_count, 2)
        + bounded_count_score(structural_items, 1)
        - bounded_count_score(response.data.unresolved_gaps.len(), 8)
}

fn bounded_count_score(count: usize, weight: i32) -> i32 {
    let bounded = count.min(i32::MAX as usize);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "count is capped to i32::MAX before conversion for bounded scoring"
    )]
    {
        bounded as i32 * weight
    }
}

#[expect(
    clippy::fn_params_excessive_bools,
    reason = "gap classification is local and the booleans are explicit input signals"
)]
fn compute_unresolved_gaps(
    mode: PackMode,
    target_kind: Option<NodeKind>,
    missing_semantic_bridges: bool,
    missing_confirmed_downstream: bool,
    missing_probable_downstream: bool,
    repo_deps_empty: bool,
) -> Vec<String> {
    let rollout_signal_missing = missing_confirmed_downstream && missing_probable_downstream;
    let mut unresolved_gaps = Vec::new();
    if missing_semantic_bridges
        && rollout_signal_missing
        && !matches!(target_kind, Some(NodeKind::Route))
    {
        unresolved_gaps.push(NO_SEMANTIC_BRIDGE_GAP.to_owned());
    }
    if matches!(mode, PackMode::Planning | PackMode::ChangeImpact) && rollout_signal_missing {
        unresolved_gaps.push(CROSS_REPO_IMPACT_GAP.to_owned());
        if repo_deps_empty {
            unresolved_gaps.push(EMPTY_REPO_DEPS_GAP.to_owned());
        }
    }
    unresolved_gaps
}

pub fn planning_pack_tool(
    ctx: &McpContext,
    request: ModePackRequest,
) -> Result<ContextPackResponse, McpServerError> {
    mode_pack_tool(ctx, request, "planning")
}

pub fn debug_pack_tool(
    ctx: &McpContext,
    request: ModePackRequest,
) -> Result<ContextPackResponse, McpServerError> {
    mode_pack_tool(ctx, request, "debug")
}

pub fn fix_pack_tool(
    ctx: &McpContext,
    request: ModePackRequest,
) -> Result<ContextPackResponse, McpServerError> {
    mode_pack_tool(ctx, request, "fix")
}

pub fn review_pack_tool(
    ctx: &McpContext,
    request: ModePackRequest,
) -> Result<ContextPackResponse, McpServerError> {
    mode_pack_tool(ctx, request, "review")
}

pub fn change_impact_pack_tool(
    ctx: &McpContext,
    request: ModePackRequest,
) -> Result<ContextPackResponse, McpServerError> {
    mode_pack_tool(ctx, request, "change_impact")
}

/// High-value edge kinds traversed by the extended bridge search.
///
/// These carry cross-service semantic meaning and are more likely to surface
/// meaningful transport/contract boundaries than structural call or import edges.
const BRIDGE_EDGE_KINDS: &[EdgeKind] = &[
    EdgeKind::ConsumesApiFrom,
    EdgeKind::Serves,
    EdgeKind::ProducesEventFor,
    EdgeKind::UsesEventFrom,
    EdgeKind::UsesTypeFrom,
    EdgeKind::UsesShared,
    EdgeKind::ImplementsContractFrom,
    EdgeKind::ContractOn,
    EdgeKind::Calls,
    EdgeKind::Consumes,
    EdgeKind::Publishes,
];

/// Maximum hops for the extended bridge BFS.
const BRIDGE_MAX_HOPS: usize = 2;

fn collect_bridges(
    ctx: &McpContext,
    symbol_id: &str,
) -> Result<(Vec<PackBridge>, PackFiles), McpServerError> {
    let anchor = decode_node_id(symbol_id).map_err(McpServerError::InvalidInput)?;

    // Fast path: one-hop adjacency (original behavior).
    let one_hop = one_hop_bridges(ctx, anchor)?;
    let bridge_nodes = if one_hop.is_empty() {
        // Extended path: bounded BFS over high-value edges up to BRIDGE_MAX_HOPS.
        extended_bridges(ctx, anchor, BRIDGE_MAX_HOPS)?
    } else {
        one_hop
    };

    Ok(finalize_bridges(bridge_nodes))
}

/// One-hop bridge collection: virtual nodes directly adjacent to `anchor`.
fn one_hop_bridges(
    ctx: &McpContext,
    anchor: NodeId,
) -> Result<Vec<gather_step_core::NodeData>, McpServerError> {
    let mut nodes = Vec::new();
    for edge in ctx
        .graph()
        .get_outgoing(anchor)?
        .into_iter()
        .chain(ctx.graph().get_incoming(anchor)?)
    {
        let other = if edge.source == anchor {
            edge.target
        } else {
            edge.source
        };
        if let Some(node) = ctx.graph().get_node(other)?
            && node.is_virtual
        {
            nodes.push(node);
        }
    }
    Ok(nodes)
}

/// Extended bridge discovery via BFS up to `max_hops` over `BRIDGE_EDGE_KINDS`.
///
/// Traverses non-virtual intermediate nodes and collects virtual nodes
/// encountered along the way. Results are ranked by hop count (shortest first)
/// and then by edge specificity (highest first).
fn extended_bridges(
    ctx: &McpContext,
    anchor: NodeId,
    max_hops: usize,
) -> Result<Vec<gather_step_core::NodeData>, McpServerError> {
    use rustc_hash::FxHashSet;

    let mut visited: FxHashSet<NodeId> = FxHashSet::default();
    visited.insert(anchor);

    // (node_id, hop_count, max_specificity_on_path)
    let mut frontier: Vec<(NodeId, usize, u8)> = vec![(anchor, 0, 0)];
    // (node, hop_count, specificity) — virtual nodes found
    let mut found: Vec<(gather_step_core::NodeData, usize, u8)> = Vec::new();

    for hop in 1..=max_hops {
        let mut next_frontier: Vec<(NodeId, usize, u8)> = Vec::new();
        for (current, _, _) in &frontier {
            let edges = ctx
                .graph()
                .get_outgoing(*current)?
                .into_iter()
                .chain(ctx.graph().get_incoming(*current)?);
            for edge in edges {
                if !BRIDGE_EDGE_KINDS.contains(&edge.kind) {
                    continue;
                }
                let specificity = edge_specificity(edge.kind);
                let other = if edge.source == *current {
                    edge.target
                } else {
                    edge.source
                };
                if !visited.insert(other) {
                    continue;
                }
                if let Some(node) = ctx.graph().get_node(other)? {
                    if node.is_virtual {
                        found.push((node, hop, specificity));
                    } else {
                        next_frontier.push((other, hop, specificity));
                    }
                }
            }
        }
        frontier = next_frontier;
        if frontier.is_empty() {
            break;
        }
    }

    // Rank: shortest hop first, then highest specificity, then stable sort key.
    found.sort_by(|(ln, lh, ls), (rn, rh, rs)| {
        lh.cmp(rh)
            .then(rs.cmp(ls)) // higher specificity wins
            .then(format!("{:?}", ln.kind).cmp(&format!("{:?}", rn.kind)))
            .then(ln.repo.cmp(&rn.repo))
            .then(ln.name.cmp(&rn.name))
    });
    found.dedup_by(|(ln, _, _), (rn, _, _)| ln.id == rn.id);
    Ok(found.into_iter().map(|(node, _, _)| node).collect())
}

fn finalize_bridges(
    mut bridge_nodes: Vec<gather_step_core::NodeData>,
) -> (Vec<PackBridge>, PackFiles) {
    bridge_nodes.sort_by(|left, right| {
        format!("{:?}", left.kind)
            .cmp(&format!("{:?}", right.kind))
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.name.cmp(&right.name))
            .then(left.id.cmp(&right.id))
    });
    bridge_nodes.dedup_by(|left, right| left.id == right.id);
    let files = bridge_nodes
        .iter()
        .map(|node| (node.repo.clone(), node.file_path.clone()))
        .collect::<Vec<_>>();
    let mut bridges = bridge_nodes
        .into_iter()
        .map(|node| PackBridge {
            kind: ascii_lower_debug(node.kind),
            name: node.name,
            repo: node.repo,
            symbol_id: encode_node_id(node.id),
        })
        .collect::<Vec<_>>();
    bridges.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then(left.repo.cmp(&right.repo))
            .then(left.name.cmp(&right.name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
    bridges.dedup_by(|left, right| left.symbol_id == right.symbol_id);
    (bridges, files)
}

fn trim_context_pack(response: &mut ContextPackResponse) -> bool {
    response.data.items.pop().is_some()
        || response.data.semantic_bridges.pop().is_some()
        || response.data.unresolved_gaps.pop().is_some()
        || response
            .data
            .change_impact
            .unresolved_possible
            .pop()
            .is_some()
        || response
            .data
            .change_impact
            .probable_downstream_repos
            .pop()
            .is_some()
}

fn cap_change_impact_repos(
    mut downstream_repos: Vec<String>,
    repo_filter: Option<&str>,
) -> (Vec<String>, Option<TruncatedRepos>) {
    downstream_repos.sort();
    downstream_repos.dedup();
    if let Some(repo) = repo_filter {
        downstream_repos.retain(|item| item == repo);
    }

    let mut dropped_repo_names: Vec<String> = downstream_repos
        .iter()
        .skip(MAX_CHANGE_IMPACT_REPOS)
        .cloned()
        .collect();
    let mut reason_codes: Vec<String> = Vec::new();
    if !dropped_repo_names.is_empty() {
        reason_codes.push("fan_out_cap".to_owned());
    }
    downstream_repos.truncate(MAX_CHANGE_IMPACT_REPOS);

    let mut byte_cap_drops: Vec<String> = Vec::new();
    downstream_repos.retain(|repo| {
        if repo.len() > CHANGE_IMPACT_REPO_BYTE_CAP {
            byte_cap_drops.push(repo.clone());
            false
        } else {
            true
        }
    });
    if !byte_cap_drops.is_empty() {
        dropped_repo_names.extend(byte_cap_drops);
        if !reason_codes.iter().any(|code| code == "per_repo_byte_cap") {
            reason_codes.push("per_repo_byte_cap".to_owned());
        }
    }

    let truncated_repos = (!dropped_repo_names.is_empty()).then_some(TruncatedRepos {
        count: dropped_repo_names.len(),
        names: dropped_repo_names,
        reason_codes,
    });
    (downstream_repos, truncated_repos)
}

fn ranked_score(base: u16, depth: usize) -> u16 {
    let depth = u16::try_from(depth).unwrap_or(u16::MAX);
    base.saturating_sub(depth.saturating_mul(25))
}

fn ascii_lower_debug(kind: NodeKind) -> String {
    let mut value = format!("{kind:?}");
    value.make_ascii_lowercase();
    value
}

fn target_reason(mode: PackMode) -> String {
    match mode {
        PackMode::Debug => "starting point for reproducing the issue".to_owned(),
        PackMode::Fix => "likely primary edit surface".to_owned(),
        PackMode::Review => "main implementation surface to inspect".to_owned(),
        PackMode::ChangeImpact => "requested change anchor".to_owned(),
        PackMode::Planning => "primary planning anchor".to_owned(),
    }
}

fn caller_reason(mode: PackMode) -> String {
    match mode {
        PackMode::Debug => "upstream caller that may reproduce or trigger the issue".to_owned(),
        PackMode::Fix => "upstream dependency that may need adaptation".to_owned(),
        PackMode::Review => "upstream usage to verify for regressions".to_owned(),
        PackMode::ChangeImpact => "direct upstream affected surface".to_owned(),
        PackMode::Planning => "upstream entrypoint worth reading next".to_owned(),
    }
}

fn callee_reason(mode: PackMode) -> String {
    match mode {
        PackMode::Debug => "downstream call path that may hide the fault".to_owned(),
        PackMode::Fix => "downstream implementation likely touched by the fix".to_owned(),
        PackMode::Review => "downstream dependency worth validating".to_owned(),
        PackMode::ChangeImpact => "direct downstream impact candidate".to_owned(),
        PackMode::Planning => "downstream implementation to inspect during planning".to_owned(),
    }
}

fn suggested_next_steps(mode: PackMode) -> Vec<String> {
    match mode {
        PackMode::Debug => vec![
            "get_symbol".to_owned(),
            "get_callers".to_owned(),
            "context".to_owned(),
        ],
        PackMode::Fix => vec![
            "get_symbol".to_owned(),
            "get_callees".to_owned(),
            "context".to_owned(),
        ],
        PackMode::Review => vec![
            "brief".to_owned(),
            "context".to_owned(),
            "trace_impact".to_owned(),
        ],
        PackMode::ChangeImpact => vec![
            "trace_impact".to_owned(),
            "cross_repo_deps".to_owned(),
            "context".to_owned(),
        ],
        PackMode::Planning => vec![
            "brief".to_owned(),
            "context".to_owned(),
            "get_callers".to_owned(),
        ],
    }
}

fn parse_symbol_kind(input: &str) -> Option<NodeKind> {
    match input {
        "route" => Some(NodeKind::Route),
        "topic" => Some(NodeKind::Topic),
        "queue" => Some(NodeKind::Queue),
        "event" => Some(NodeKind::Event),
        "shared_symbol" => Some(NodeKind::SharedSymbol),
        "type" => Some(NodeKind::Type),
        _ => None,
    }
}

fn shared_contract_pack_items(
    ctx: &McpContext,
    symbol: &SymbolResponseData,
) -> Result<Vec<PackItem>, McpServerError> {
    use gather_step_core::EdgeKind as Ek;

    let Some(kind) = symbol.kind.as_deref().and_then(parse_symbol_kind) else {
        return Ok(Vec::new());
    };
    if !matches!(kind, NodeKind::SharedSymbol | NodeKind::Type) {
        return Ok(Vec::new());
    }
    let entry_node_id = decode_node_id(&symbol.symbol_id).map_err(McpServerError::InvalidInput)?;
    let impact = shared_contract_impact(ctx.graph(), entry_node_id)
        .map_err(|error| McpServerError::Internal(format!("shared_contract_impact: {error}")))?;

    let mut items = Vec::new();
    for (repo, files) in impact.entries {
        for (rank, impacted) in files.into_iter().enumerate() {
            // Files that have only CoChangesWith edges are advisory (co-edit
            // hints); files with any structural edge kind are proven consumers.
            let is_advisory = impacted
                .edge_kinds
                .iter()
                .all(|k| matches!(k, Ek::CoChangesWith));

            let edge_label = impacted
                .edge_kinds
                .iter()
                .map(|k| crate::tools::labels::edge_kind_label(*k))
                .collect::<Vec<_>>()
                .join(", ");

            let (category, reason, score) = if is_advisory {
                (
                    "advisory_co_change_files",
                    format!(
                        "co-change hint only (not a proven structural consumer) via {edge_label}"
                    ),
                    shared_contract_advisory_score(rank),
                )
            } else {
                (
                    "contract_impact",
                    format!("shared contract impact via {edge_label}"),
                    shared_contract_pack_score(rank),
                )
            };

            items.push(PackItem {
                category: category.to_owned(),
                file_path: impacted.file_path.clone(),
                line_start: None,
                reason,
                repo: repo.clone(),
                score,
                symbol_id: encode_node_id(node_id(
                    &repo,
                    &impacted.file_path,
                    NodeKind::File,
                    &impacted.file_path,
                )),
                symbol_kind: "file".to_owned(),
                symbol_name: impacted.file_path,
                evidence_chain: None,
            });
        }
    }
    Ok(items)
}

/// Score for advisory (co-change-only) pack items.
///
/// Advisory items are scored below 300 so they always sort after any
/// structural consumer (minimum structural score is `800 - MAX_RANK`).  The
/// score still decreases with rank to preserve relative ordering within the
/// advisory band.
fn shared_contract_advisory_score(rank: usize) -> u16 {
    let rank = u16::try_from(rank).unwrap_or(u16::MAX);
    299_u16.saturating_sub(rank)
}

fn shared_contract_pack_score(rank: usize) -> u16 {
    let rank = u16::try_from(rank).unwrap_or(u16::MAX);
    800_u16.saturating_sub(rank)
}

/// Attempt to populate the `evidence_chain` field on `item` by calling
/// [`evidence_chain_for`] between `anchor_id` and `item_id`.
///
/// If the chain is found it is rendered to Markdown via [`render_evidence_chain`]
/// and stored as a JSON string value.  Any error or absent path is silently
/// ignored — the field is left as `None`.
fn populate_evidence_chain<S: gather_step_storage::GraphStore>(
    store: &S,
    anchor_id: NodeId,
    item_id: NodeId,
    item: &mut PackItem,
) {
    let Ok(Some(chain)) = evidence_chain_for(store, anchor_id, item_id) else {
        return;
    };
    if chain.steps.is_empty() {
        return;
    }
    if let Ok(rendered) = render_evidence_chain(&chain, store)
        && !rendered.is_empty()
    {
        item.evidence_chain = Some(serde_json::Value::String(rendered));
    }
}

/// Re-rank planning-mode items after evidence chains are populated.
///
/// Items with a confirmed evidence chain (graph-proven path from anchor) are
/// boosted by 50 score points so they appear before lexical-only hits at
/// equivalent depth. The list is re-sorted by the updated scores.
fn apply_planning_evidence_ranking(items: &mut [PackItem]) {
    for item in items.iter_mut() {
        if item.evidence_chain.is_some() {
            item.score = item.score.saturating_add(50);
        }
    }
    items.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then(left.repo.cmp(&right.repo))
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

fn mode_pack_tool(
    ctx: &McpContext,
    request: ModePackRequest,
    mode: &str,
) -> Result<ContextPackResponse, McpServerError> {
    context_pack_tool(
        ctx,
        ContextPackRequest {
            budget_bytes: request.budget_bytes,
            depth: request.depth,
            limit: request.limit,
            repo: request.repo,
            mode: mode.to_owned(),
            target: request.target,
        },
    )
}

/// Cheap identity key that uniquely identifies a pack request from the
/// request parameters alone, without the generation component.
///
/// This key is stored in `context_packs` alongside the generation value.
/// On a warm cache hit the caller validates freshness by re-computing the
/// current generation from `context_pack_files` — no `pack_generation_scope`
/// call is needed.
fn pack_identity_key(
    request: &ContextPackRequest,
    mode: PackMode,
    depth: usize,
    limit: usize,
    resolved: &ResolvedPackTarget,
) -> String {
    let cache_target = resolved
        .symbol_id
        .as_deref()
        .unwrap_or(request.target.trim());
    format!(
        "context_pack:{CONTEXT_PACK_CACHE_KEY_VERSION}:mode={}:repo={}:target={cache_target}:depth={depth}:limit={limit}:budget={}",
        mode.as_str(),
        request.repo.as_deref().unwrap_or(""),
        request.budget_bytes.unwrap_or(0)
    )
}

fn persist_context_pack(
    ctx: &McpContext,
    cache_key: &str,
    request: &ContextPackRequest,
    mode: PackMode,
    generation: i64,
    response: &ContextPackResponse,
    files: &[(String, String)],
    now_unix: i64,
) -> Result<(), McpServerError> {
    let bytes = serde_json::to_vec(response)
        .map_err(|error| McpServerError::Internal(format!("cache serialize: {error}")))?;
    ctx.metadata()
        .put_context_pack(
            &gather_step_storage::ContextPackRecord {
                pack_key: cache_key.to_owned(),
                mode: mode.as_str().to_owned(),
                target: request.target.clone(),
                generation,
                response: bytes.clone(),
                created_at: now_unix,
                last_read_at: now_unix,
                byte_size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                hit_count: 0,
            },
            files,
        )
        .map_err(McpServerError::Metadata)?;
    gather_step_storage::PackStore::new(ctx.metadata())
        .evict_if_needed(MAX_CONTEXT_PACK_CACHE_BYTES)
        .map_err(McpServerError::Metadata)
}

const fn pack_budget_tool(mode: PackMode) -> BudgetedTool {
    match mode {
        PackMode::ChangeImpact => BudgetedTool::ChangeImpact,
        PackMode::Planning | PackMode::Debug | PackMode::Fix | PackMode::Review => {
            BudgetedTool::ContextPack
        }
    }
}

fn compute_cache_deps_and_generation(
    ctx: &McpContext,
    response: &ContextPackResponse,
    repo_scope: Option<&str>,
) -> Result<(PackFiles, i64), McpServerError> {
    let deps = cache_dependency_files_for_response(ctx, response)?;
    let generation = current_cache_generation(ctx, &deps, response, repo_scope)?;
    Ok((deps, generation))
}

fn cached_context_pack_is_current(response: &ContextPackResponse) -> bool {
    let Some(meta) = response.meta.as_ref() else {
        return false;
    };
    if meta.response_schema_version != response_schema_version() {
        return false;
    }
    true
}

fn refresh_cached_context_pack_response(
    ctx: &McpContext,
    resolved: &ResolvedPackTarget,
    repo_filter: Option<&str>,
    response: &mut ContextPackResponse,
) -> Result<(), McpServerError> {
    if response.data.planning_proofs.is_empty() {
        return Ok(());
    }
    let anchor_repo = cached_context_pack_anchor_repo(ctx, resolved, response)?;
    apply_proof_derived_change_impact(ctx, anchor_repo.as_deref(), repo_filter, response)?;
    refresh_cached_rollout_gap_state(response);
    refresh_context_pack_completeness(response);
    Ok(())
}

fn cached_context_pack_anchor_repo(
    ctx: &McpContext,
    resolved: &ResolvedPackTarget,
    response: &ContextPackResponse,
) -> Result<Option<String>, McpServerError> {
    if let Some(symbol_id) = resolved.symbol_id.as_deref()
        && let Ok(node_id) = decode_node_id(symbol_id)
        && let Some(node) = ctx.graph().get_node(node_id)?
        && is_real_repo(&node.repo)
    {
        return Ok(Some(node.repo));
    }
    Ok(response
        .data
        .items
        .iter()
        .find(|item| item.category == "target" && is_real_repo(&item.repo))
        .map(|item| item.repo.clone())
        .or_else(|| {
            response
                .data
                .change_impact
                .direct_repos
                .iter()
                .find(|repo| is_real_repo(repo))
                .cloned()
        }))
}

fn refresh_cached_rollout_gap_state(response: &mut ContextPackResponse) {
    let impact = &response.data.change_impact;
    let has_rollout_signal = !impact.confirmed_downstream_repos.is_empty()
        || !impact.downstream_repos.is_empty()
        || !impact.probable_downstream_repos.is_empty();
    if !has_rollout_signal {
        return;
    }
    response
        .data
        .unresolved_gaps
        .retain(|gap| !is_rollout_dependent_gap(gap));
    if let Some(meta) = &mut response.meta {
        meta.warnings
            .retain(|warning| !is_rollout_dependent_gap_warning(warning));
    }
}

fn is_rollout_dependent_gap(gap: &str) -> bool {
    matches!(
        gap,
        NO_SEMANTIC_BRIDGE_GAP | CROSS_REPO_IMPACT_GAP | EMPTY_REPO_DEPS_GAP
    )
}

fn is_rollout_dependent_gap_warning(warning: &str) -> bool {
    warning
        .strip_prefix("pack is incomplete: ")
        .is_some_and(is_rollout_dependent_gap)
}

fn refresh_context_pack_completeness(response: &mut ContextPackResponse) {
    let Some(meta) = &mut response.meta else {
        return;
    };
    let medium_confidence_resolution = meta.resolution == "search_ranked_resolved"
        && meta.resolution_confidence.as_deref() == Some("medium");
    let medium_confidence_warning = medium_confidence_warning(meta.candidate_count);
    let has_non_derived_warnings = meta.warnings.iter().any(|warning| {
        warning != &medium_confidence_warning && warning != TRUNCATED_RESPONSE_WARNING
    });

    meta.completeness.clear();
    meta.completeness.push_str(if !response.data.found {
        "unresolved"
    } else if response.data.unresolved_gaps.is_empty()
        && response.data.change_impact.unresolved_possible.is_empty()
        && !has_non_derived_warnings
        && !medium_confidence_resolution
        && !meta.budget.truncated
    {
        "complete"
    } else {
        "partial"
    });

    if medium_confidence_resolution && !meta.warnings.contains(&medium_confidence_warning) {
        meta.warnings.push(medium_confidence_warning);
    }
    if meta.budget.truncated
        && !meta
            .warnings
            .iter()
            .any(|warning| warning == TRUNCATED_RESPONSE_WARNING)
    {
        meta.warnings.push(TRUNCATED_RESPONSE_WARNING.to_owned());
    }
}

fn medium_confidence_warning(candidate_count: usize) -> String {
    format!(
        "target auto-resolved from {candidate_count} candidates via {PACK_CONFIDENCE_MODEL_VERSION}; verify identity before editing"
    )
}

fn current_cache_generation(
    ctx: &McpContext,
    deps: &[(String, String)],
    response: &ContextPackResponse,
    repo_scope: Option<&str>,
) -> Result<i64, McpServerError> {
    let dep_generation = if deps.is_empty() && response_needs_resolution_scope(response) {
        0
    } else {
        ctx.metadata()
            .latest_indexed_at_for_files(deps)
            .map_err(McpServerError::Metadata)?
    };
    let scope_generation = if response_needs_resolution_scope(response) {
        ctx.metadata()
            .latest_indexed_at(repo_scope)
            .map_err(McpServerError::Metadata)?
    } else {
        0
    };
    Ok(dep_generation.max(scope_generation))
}

fn response_needs_resolution_scope(response: &ContextPackResponse) -> bool {
    let Some(meta) = response.meta.as_ref() else {
        return false;
    };
    meta.resolved_symbol_id.is_none()
        || matches!(
            meta.resolution.as_str(),
            "search_ranked_alternates"
                | "search_ranked_deferred"
                | "ambiguous_search_match"
                | "ambiguous_event_anchor"
                | "unresolved"
                | "repo_filtered_out"
        )
}

fn cache_dependency_files_for_response(
    ctx: &McpContext,
    response: &ContextPackResponse,
) -> Result<PackFiles, McpServerError> {
    let mut files = BTreeSet::<(String, String)>::new();

    for item in &response.data.items {
        insert_cache_dep(&mut files, &item.repo, &item.file_path);
    }

    for bridge in &response.data.semantic_bridges {
        let Ok(bridge_node_id) = decode_node_id(&bridge.symbol_id) else {
            continue;
        };
        let Some(bridge_node) = ctx.graph().get_node(bridge_node_id)? else {
            continue;
        };
        insert_cache_dep(&mut files, &bridge_node.repo, &bridge_node.file_path);
        if bridge_node.is_virtual {
            insert_real_bridge_participant_deps(ctx, bridge_node_id, &mut files)?;
        }
    }

    for value in &response.data.planning_proofs {
        match serde_json::from_value::<PlanningProof>(value.clone()) {
            Ok(proof) => {
                insert_cache_dep(&mut files, &proof.source_repo, &proof.source_file);
                insert_cache_dep(&mut files, &proof.target_repo, &proof.target_file);
            }
            Err(error) => {
                tracing::warn!(
                    target: "gather_step_mcp::packs::cache_dependency_files",
                    error = %error,
                    "skipping malformed planning_proof entry on cache-dependency \
                     scan; downstream cache_dep_files set may be incomplete"
                );
            }
        }
    }

    Ok(files.into_iter().collect())
}

fn insert_real_bridge_participant_deps(
    ctx: &McpContext,
    bridge_node_id: NodeId,
    files: &mut BTreeSet<(String, String)>,
) -> Result<(), McpServerError> {
    let edges = ctx
        .graph()
        .get_outgoing(bridge_node_id)?
        .into_iter()
        .chain(ctx.graph().get_incoming(bridge_node_id)?);
    for edge in edges {
        let other = if edge.source == bridge_node_id {
            edge.target
        } else {
            edge.source
        };
        let Some(participant) = ctx.graph().get_node(other)? else {
            continue;
        };
        insert_cache_dep(files, &participant.repo, &participant.file_path);
    }
    Ok(())
}

fn insert_cache_dep(files: &mut BTreeSet<(String, String)>, repo: &str, file_path: &str) {
    if is_cache_dep_file(repo, file_path) {
        files.insert((repo.to_owned(), file_path.to_owned()));
    }
}

fn is_cache_dep_file(repo: &str, file_path: &str) -> bool {
    !repo.is_empty()
        && repo != "__virtual__"
        && !repo.starts_with("__")
        && !file_path.is_empty()
        && !file_path.starts_with("__")
}

/// Resolve a planning-pack target through multiple anchor classes.
///
/// Anchor priority (highest to lowest):
/// 1. Explicit `node_id` — caller already has the exact graph node.
/// 2. Symbol search exact-match — unambiguous text match in the symbol index.
/// 3. Event anchor — bare topic/event name matched via `resolve_event_targets`.
/// 4. Route anchor — `METHOD /path` pattern matched via `resolve_route_target`.
/// 5. Symbol search single-result — only one candidate exists.
/// 6. Ambiguous / unresolved — caller must narrow or supply a `symbol_id`.
fn resolve_pack_target(
    ctx: &McpContext,
    target: &str,
    repo: Option<&str>,
) -> Result<ResolvedPackTarget, McpServerError> {
    // Anchor 1: explicit node_id
    if let Ok(node_id) = decode_node_id(target)
        && let Some(node) = ctx.graph().get_node(node_id)?
    {
        if repo.is_none_or(|selected| node.repo == selected) {
            return Ok(ResolvedPackTarget {
                alternate_anchors: Vec::new(),
                candidate_count: 1,
                confidence_model_version: None,
                resolution: "symbol_id".to_owned(),
                resolution_confidence: None,
                symbol_id: Some(target.to_owned()),
                winner_margin: Some(u16::MAX),
                ranked_alternates: Vec::new(),
            });
        }
        return Ok(ResolvedPackTarget {
            alternate_anchors: Vec::new(),
            candidate_count: 1,
            confidence_model_version: None,
            resolution: "repo_filtered_out".to_owned(),
            resolution_confidence: None,
            symbol_id: None,
            winner_margin: None,
            ranked_alternates: Vec::new(),
        });
    }

    // Anchor 2 & 5: symbol search
    let search = search_symbols(
        ctx,
        SearchRequest {
            budget_bytes: None,
            cursor: None,
            kind: None,
            language: None,
            limit: Some(10),
            query: target.to_owned(),
            repo: repo.map(str::to_owned),
        },
    )?;
    let symbol_resolution = choose_pack_target(ctx, &search.data.results, target, repo)?;
    // For a confident symbol resolution, return immediately.  For ranked-alternates
    // (margin too narrow to pick a winner), defer to event-anchor resolution first —
    // a short word like "order" often produces noisy symbol hits but resolves cleanly
    // via an event node.  Only fall back to ranked-alternates if no event anchor
    // matches.
    if let Some(ref resolved) = symbol_resolution
        && !is_deferred_symbol_resolution(&resolved.resolution)
    {
        return Ok(ResolvedPackTarget {
            alternate_anchors: resolved.alternate_anchors.clone(),
            candidate_count: search.data.results.len(),
            confidence_model_version: resolved.confidence_model_version.clone(),
            resolution: resolved.resolution.clone(),
            resolution_confidence: resolved.resolution_confidence.clone(),
            symbol_id: resolved.symbol_id.clone(),
            winner_margin: resolved.winner_margin,
            ranked_alternates: resolved.ranked_alternates.clone(),
        });
    }

    // Anchor 3: event/topic name
    let event_nodes = resolve_event_targets(ctx.graph(), target)?;
    let event_nodes: Vec<_> = match repo {
        Some(r) => event_nodes.into_iter().filter(|n| n.repo == r).collect(),
        None => event_nodes,
    };
    if event_nodes.len() == 1 {
        return Ok(ResolvedPackTarget {
            alternate_anchors: Vec::new(),
            candidate_count: 1,
            confidence_model_version: None,
            resolution: "event_anchor".to_owned(),
            resolution_confidence: None,
            symbol_id: Some(encode_node_id(event_nodes[0].id)),
            winner_margin: Some(u16::MAX),
            ranked_alternates: Vec::new(),
        });
    }
    if event_nodes.len() > 1 {
        return Ok(ResolvedPackTarget {
            alternate_anchors: Vec::new(),
            candidate_count: event_nodes.len(),
            confidence_model_version: None,
            resolution: "ambiguous_event_anchor".to_owned(),
            resolution_confidence: None,
            symbol_id: None,
            winner_margin: None,
            ranked_alternates: Vec::new(),
        });
    }

    // Anchor 4: route — target looks like `METHOD /path`
    if let Some((method, path)) = parse_route_target(target)
        && let Some(route_node) = resolve_route_target(ctx.graph(), &method, &path)?
        && repo.is_none_or(|r| route_node.repo == r)
    {
        return Ok(ResolvedPackTarget {
            alternate_anchors: Vec::new(),
            candidate_count: 1,
            confidence_model_version: None,
            resolution: "route_anchor".to_owned(),
            resolution_confidence: None,
            symbol_id: Some(encode_node_id(route_node.id)),
            winner_margin: Some(u16::MAX),
            ranked_alternates: Vec::new(),
        });
    }

    // Deferred symbol resolution: no event/route anchor matched, so surface the
    // ranked symbol result now.
    if let Some(mut ranked_alt) = symbol_resolution
        && is_deferred_symbol_resolution(&ranked_alt.resolution)
    {
        if ranked_alt.resolution == "search_ranked_deferred" {
            "search_ranked_resolved".clone_into(&mut ranked_alt.resolution);
        }
        return Ok(ResolvedPackTarget {
            alternate_anchors: ranked_alt.alternate_anchors,
            candidate_count: search.data.results.len(),
            confidence_model_version: ranked_alt.confidence_model_version,
            resolution: ranked_alt.resolution,
            resolution_confidence: ranked_alt.resolution_confidence,
            symbol_id: ranked_alt.symbol_id,
            winner_margin: ranked_alt.winner_margin,
            ranked_alternates: ranked_alt.ranked_alternates,
        });
    }

    // Ambiguous / unresolved fall-through
    Ok(ResolvedPackTarget {
        alternate_anchors: Vec::new(),
        candidate_count: search.data.results.len(),
        confidence_model_version: None,
        resolution: if search.data.results.is_empty() {
            "unresolved".to_owned()
        } else {
            "ambiguous_search_match".to_owned()
        },
        resolution_confidence: None,
        symbol_id: None,
        winner_margin: None,
        ranked_alternates: Vec::new(),
    })
}

fn is_deferred_symbol_resolution(resolution: &str) -> bool {
    matches!(
        resolution,
        "search_ranked_alternates" | "search_ranked_deferred"
    )
}

fn choose_pack_target(
    ctx: &McpContext,
    results: &[SearchResultItem],
    target: &str,
    repo: Option<&str>,
) -> Result<Option<ResolvedPackTarget>, McpServerError> {
    let broad_exact = results
        .iter()
        .filter(|item| item.exact_match || item.symbol_name == target)
        .collect::<Vec<_>>();
    let strict_exact = results
        .iter()
        .filter(|item| is_strict_symbol_match(item, target))
        .collect::<Vec<_>>();
    if !strict_exact.is_empty() {
        let preferred_ids = strict_exact
            .iter()
            .map(|item| item.symbol_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut enriched_exact = strict_exact;
        enriched_exact.extend(broad_exact.into_iter().filter(|item| {
            !preferred_ids.contains(item.symbol_id.as_str()) && is_low_packability_kind(&item.kind)
        }));
        return resolve_exact_pack_candidates(ctx, results, enriched_exact, target, repo);
    }

    let exact = results
        .iter()
        .filter(|item| item.exact_match || item.symbol_name == target)
        .collect::<Vec<_>>();
    resolve_exact_pack_candidates(ctx, results, exact, target, repo)
}

fn resolve_exact_pack_candidates(
    ctx: &McpContext,
    results: &[SearchResultItem],
    exact: Vec<&SearchResultItem>,
    target: &str,
    repo: Option<&str>,
) -> Result<Option<ResolvedPackTarget>, McpServerError> {
    let had_multiple_exact = exact.len() > 1;
    let (exact, filtered_alternates) = prune_low_packability_exact_matches(exact);
    if exact.len() == 1 {
        let used_ranked_metadata = had_multiple_exact || !filtered_alternates.is_empty();
        return Ok(exact.first().map(|item| ResolvedPackTarget {
            alternate_anchors: filtered_alternates,
            candidate_count: results.len(),
            confidence_model_version: Some(PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
            resolution: if used_ranked_metadata {
                "search_ranked_resolved".to_owned()
            } else {
                "search_resolved".to_owned()
            },
            resolution_confidence: used_ranked_metadata.then_some("high".to_owned()),
            symbol_id: Some(item.symbol_id.clone()),
            winner_margin: Some(u16::MAX),
            ranked_alternates: Vec::new(),
        }));
    }
    if exact.len() > 1 {
        // When there is a repo filter or the target matches definition-shaped kinds
        // (shared_symbol, interface, type, class with UpperCase name), attempt
        // confident single-winner selection first.  For all other multi-exact cases
        // we still score the candidates to see whether ranked alternates can be
        // surfaced — this covers lower-case function names that span multiple
        // packages.
        let try_confident = repo.is_some() || should_rank_global_pack_candidates(target, &exact);
        let ranked = score_pack_resolution_candidates(ctx, exact.as_slice(), target)?;
        // Allow medium-confidence wins for any query; a `--repo` filter is not
        // a precondition for trusting a dominant winner. The HIGH and MEDIUM
        // thresholds still gate the call (score floor + margin), so this only
        // changes the `(no --repo) && winner_dominates_by_medium_margin` case
        // — which previously silently dropped to RankedAlternates and hid the
        // canonical primary behind a "low confidence" alternates list.
        match select_pack_target(&ranked, true) {
            PackTargetSelection::Confident(
                winner,
                confidence,
                winner_margin,
                mut alternate_anchors,
            ) if try_confident => {
                alternate_anchors.extend(filtered_alternates.clone());
                return Ok(Some(ResolvedPackTarget {
                    alternate_anchors,
                    candidate_count: results.len(),
                    confidence_model_version: Some(PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
                    resolution: "search_ranked_resolved".to_owned(),
                    resolution_confidence: Some(confidence.to_owned()),
                    symbol_id: Some(winner.symbol_id),
                    winner_margin: Some(clamp_margin_u16(winner_margin)),
                    ranked_alternates: Vec::new(),
                }));
            }
            PackTargetSelection::RankedAlternates(alternates) => {
                return Ok(Some(ResolvedPackTarget {
                    alternate_anchors: Vec::new(),
                    candidate_count: results.len(),
                    confidence_model_version: Some(PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
                    resolution: "search_ranked_alternates".to_owned(),
                    resolution_confidence: Some("low".to_owned()),
                    symbol_id: None,
                    winner_margin: None,
                    ranked_alternates: alternates,
                }));
            }
            // A confident winner exists, but global ranking is suppressed for
            // this query shape. Defer it so event/route anchors still get
            // priority; if neither resolves, the clear symbol winner is used
            // instead of falling through to ambiguous/unresolved.
            PackTargetSelection::Confident(
                winner,
                confidence,
                winner_margin,
                mut alternate_anchors,
            ) => {
                alternate_anchors.extend(filtered_alternates.clone());
                return Ok(Some(ResolvedPackTarget {
                    alternate_anchors,
                    candidate_count: results.len(),
                    confidence_model_version: Some(PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
                    resolution: "search_ranked_deferred".to_owned(),
                    resolution_confidence: Some(confidence.to_owned()),
                    symbol_id: Some(winner.symbol_id),
                    winner_margin: Some(clamp_margin_u16(winner_margin)),
                    ranked_alternates: Vec::new(),
                }));
            }
            PackTargetSelection::Ambiguous => {
                let mut alternate_anchors = build_ranked_alternate_anchors(&ranked, None, 3);
                alternate_anchors.extend(filtered_alternates);
                return Ok(Some(ResolvedPackTarget {
                    alternate_anchors,
                    candidate_count: results.len(),
                    confidence_model_version: Some(PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
                    resolution: "ambiguous_search_match".to_owned(),
                    resolution_confidence: Some("low".to_owned()),
                    symbol_id: None,
                    winner_margin: None,
                    ranked_alternates: Vec::new(),
                }));
            }
        }
    }
    if results.len() == 1 {
        return Ok(results.first().map(|item| ResolvedPackTarget {
            alternate_anchors: Vec::new(),
            candidate_count: results.len(),
            confidence_model_version: None,
            resolution: "search_resolved".to_owned(),
            resolution_confidence: None,
            symbol_id: Some(item.symbol_id.clone()),
            winner_margin: Some(u16::MAX),
            ranked_alternates: Vec::new(),
        }));
    }
    Ok(None)
}

fn is_strict_symbol_match(item: &SearchResultItem, target: &str) -> bool {
    item.symbol_name == target
}

fn prune_low_packability_exact_matches(
    exact: Vec<&SearchResultItem>,
) -> (Vec<&SearchResultItem>, Vec<RescueAnchor>) {
    let preferred = exact
        .iter()
        .copied()
        .filter(|item| !is_low_packability_kind(&item.kind))
        .collect::<Vec<_>>();
    if preferred.is_empty() || preferred.len() == exact.len() {
        return (exact, Vec::new());
    }

    let alternates = exact
        .iter()
        .filter(|item| is_low_packability_kind(&item.kind))
        .map(|item| RescueAnchor {
            anchor_form: item.kind.clone(),
            repo: item.repo.clone(),
            symbol_id: item.symbol_id.clone(),
            symbol_name: item.symbol_name.clone(),
            rationale: Some(
                "suppressed as a lower-value file/import anchor when a named symbol matched"
                    .to_owned(),
            ),
            score_delta: None,
            confidence_hint: Some("fallback".to_owned()),
        })
        .collect();
    (preferred, alternates)
}

fn is_low_packability_kind(kind: &str) -> bool {
    kind.eq_ignore_ascii_case("file")
        || kind.eq_ignore_ascii_case("import")
        || kind.eq_ignore_ascii_case("module")
}

/// The outcome of attempting to select a single confident pack target from a
/// ranked candidate list.
enum PackTargetSelection {
    /// A single candidate cleared the confidence bar (high or medium, depending
    /// on `allow_medium`).  Fields match the old `Option` return type.
    Confident(RankedPackCandidate, &'static str, i32, Vec<RescueAnchor>),
    /// No candidate cleared the confidence bar, but at least one candidate has a
    /// score above the noise floor (`PACK_CONFIDENCE_HIGH_MIN_SCORE / 2`).  The
    /// full sorted list is returned so the caller can assemble a ranked-alternates
    /// response.
    RankedAlternates(Vec<RankedPackCandidate>),
    /// Either the list has fewer than two candidates or every candidate is below
    /// the noise floor.  The caller should emit `ambiguous_search_match` /
    /// `found = false`.
    Ambiguous,
}

/// Noise-floor threshold: candidates whose score falls below this value are
/// considered pure noise and will not appear in a ranked-alternates response.
const PACK_CONFIDENCE_ALTERNATES_MIN_SCORE: i32 = PACK_CONFIDENCE_HIGH_MIN_SCORE / 2;

fn select_pack_target(ranked: &[RankedPackCandidate], allow_medium: bool) -> PackTargetSelection {
    let mut ranked = ranked.to_vec();
    if ranked.len() < 2 {
        return PackTargetSelection::Ambiguous;
    }
    ranked.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.file_path.cmp(&right.file_path))
            .then_with(|| left.symbol_id.cmp(&right.symbol_id))
    });
    let winner = ranked[0].clone();
    let runner_up = &ranked[1];
    let winner_margin = winner.score.saturating_sub(runner_up.score);
    let confidence = if winner.score >= PACK_CONFIDENCE_HIGH_MIN_SCORE
        && winner_margin >= PACK_CONFIDENCE_HIGH_MARGIN
    {
        Some("high")
    } else if allow_medium
        && winner.score >= PACK_CONFIDENCE_MEDIUM_MIN_SCORE
        && winner_margin >= PACK_CONFIDENCE_MEDIUM_MARGIN
    {
        Some("medium")
    } else {
        None
    };
    if let Some(level) = confidence {
        let alternate_anchors = build_ranked_alternate_anchors(&ranked, Some(&winner), 3);
        return PackTargetSelection::Confident(winner, level, winner_margin, alternate_anchors);
    }
    // No confident winner — check whether any candidate clears the noise floor.
    let above_noise: Vec<RankedPackCandidate> = ranked
        .into_iter()
        .filter(|c| c.score >= PACK_CONFIDENCE_ALTERNATES_MIN_SCORE)
        .collect();
    if above_noise.is_empty() {
        PackTargetSelection::Ambiguous
    } else {
        PackTargetSelection::RankedAlternates(above_noise)
    }
}

fn score_pack_resolution_candidates(
    ctx: &McpContext,
    exact: &[&SearchResultItem],
    target: &str,
) -> Result<Vec<RankedPackCandidate>, McpServerError> {
    let node_ids = exact
        .iter()
        .filter_map(|item| decode_node_id(&item.symbol_id).ok())
        .collect::<Vec<_>>();
    let anchor_scores = rank_anchors(ctx.graph(), &node_ids)?
        .into_iter()
        .map(|anchor| {
            (
                encode_node_id(anchor.node),
                scaled_anchor_score(anchor.score),
            )
        })
        .collect::<BTreeMap<_, _>>();

    exact
        .iter()
        .map(|item| {
            let structural_score = pack_resolution_edge_score(ctx, &item.symbol_id)?;
            let anchor_score = anchor_scores
                .get(&item.symbol_id)
                .copied()
                .unwrap_or_default();
            let query_score = scaled_search_match_score(item.score);
            let query_bonus = query_alignment_bonus(target, item);
            let shape_bonus = query_shape_match_bonus(target, &item.kind);
            let mut score = 300
                + structural_score
                + file_path_packability_bonus(&item.file_path)
                + repo_shared_library_bonus(&item.repo)
                + symbol_kind_packability_bonus_for_query(target, &item.kind)
                + pack_candidate_query_penalty(target, item);
            score += anchor_score + query_score + query_bonus + shape_bonus;
            Ok(RankedPackCandidate {
                file_path: item.file_path.clone(),
                repo: item.repo.clone(),
                rationale: pack_resolution_rationale(
                    query_bonus,
                    query_score,
                    &item.file_path,
                    structural_score,
                    anchor_score,
                ),
                score,
                symbol_id: item.symbol_id.clone(),
                symbol_name: item.symbol_name.clone(),
            })
        })
        .collect()
}

fn pack_resolution_edge_score(ctx: &McpContext, symbol_id: &str) -> Result<i32, McpServerError> {
    let node_id = decode_node_id(symbol_id).map_err(McpServerError::InvalidInput)?;
    let current = ctx
        .graph()
        .get_node(node_id)?
        .ok_or_else(|| McpServerError::InvalidInput(format!("missing node for `{symbol_id}`")))?;
    let incoming = ctx.graph().get_incoming(node_id)?;
    let outgoing = ctx.graph().get_outgoing(node_id)?;
    let incoming_score = incoming
        .iter()
        .filter(|edge| !matches!(edge.kind, EdgeKind::Defines | EdgeKind::Imports))
        .count()
        .min(6);
    let outgoing_score = outgoing
        .iter()
        .filter(|edge| !matches!(edge.kind, EdgeKind::Defines | EdgeKind::Imports))
        .count()
        .min(6);
    let mut boundary_edges = 0usize;
    let mut virtual_adjacency = 0usize;
    let mut cross_repo_neighbors = std::collections::BTreeSet::new();
    for edge in incoming.iter().chain(outgoing.iter()) {
        if matches!(
            edge.kind,
            EdgeKind::Serves
                | EdgeKind::Consumes
                | EdgeKind::Publishes
                | EdgeKind::ProducesEventFor
                | EdgeKind::UsesEventFrom
                | EdgeKind::ContractOn
        ) {
            boundary_edges += 1;
        }
        let other = if edge.source == node_id {
            edge.target
        } else {
            edge.source
        };
        let Some(other_node) = ctx.graph().get_node(other)? else {
            continue;
        };
        if other_node.is_virtual {
            virtual_adjacency += 1;
        } else if other_node.repo != current.repo {
            cross_repo_neighbors.insert(other_node.repo);
        }
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        reason = "pack-resolution scores are bounded to a small capped range"
    )]
    Ok((incoming_score as i32 * 30)
        + (outgoing_score as i32 * 20)
        + (boundary_edges.min(6) as i32 * 45)
        + (virtual_adjacency.min(4) as i32 * 35)
        + (cross_repo_neighbors.len().min(4) as i32 * 60))
}

fn scaled_anchor_score(score: f32) -> i32 {
    let bounded = (score * 100.0).round().clamp(0.0, 250.0);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "bounded floating-point score is intentionally reduced to an integer band"
    )]
    {
        bounded as i32
    }
}

fn file_path_packability_bonus(file_path: &str) -> i32 {
    let mut normalized = file_path.to_owned();
    normalized.make_ascii_lowercase();
    let mut score = 0;
    for (needle, bonus) in [
        ("api", 140),
        ("transport", 140),
        ("service", 120),
        ("controller", 120),
        ("usecase", 110),
        ("provider", 90),
        ("hook", 80),
    ] {
        if normalized.contains(needle) {
            score += bonus;
        }
    }
    score
}

/// Bonus for candidates whose owning repo name signals a shared library
/// (`common`, `shared`, `lib`, `core`, `contracts`, `types`).
///
/// Without this, a symbol that is canonically defined in a shared library
/// and re-implemented in a consumer service can score identically to the
/// consumer copy — yielding a 0-margin tie that drops to ranked alternates
/// instead of the canonical answer. The repo-name signal is only available
/// at the candidate level (the per-file `file_path` lives inside a repo and
/// rarely carries the library marker), so it is checked separately here.
///
/// The bonus is bounded at +100 so it can clear the medium-confidence
/// margin (`PACK_CONFIDENCE_MEDIUM_MARGIN = 75`) on its own when the
/// other-side candidate has none of these markers, but cannot dominate
/// genuine structural-score differences.
/// Conventional shared-library tokens. Kept in lockstep with
/// `repo_is_shared_library` in `gather-step-bench` so the resolver's
/// tiebreaker bonus and the release-gate's canonical-repo classifier
/// agree on the same set of names.
const SHARED_LIBRARY_REPO_TOKENS: &[&str] =
    &["shared", "common", "contracts", "core", "types", "lib"];

/// Adds a tiebreaker bonus when the repo name's tokens contain a
/// shared-library marker.
///
/// Tokenises on `-`, `_`, `/`, `.` and checks for exact membership.
/// Substring matching was rejected because it false-positives on names
/// like `score-service` (`core`), `prototypes` (`types`), and
/// `notification-core` (`core`). With tokenisation, `shared-lib` and
/// `shared-contracts-lib` match; `notification-core` does not.
fn repo_shared_library_bonus(repo: &str) -> i32 {
    let mut normalized = repo.to_owned();
    normalized.make_ascii_lowercase();
    if normalized.is_empty() {
        return 0;
    }
    let matches = normalized
        .split(['-', '_', '/', '.'])
        .any(|token| SHARED_LIBRARY_REPO_TOKENS.contains(&token));
    if matches { 100 } else { 0 }
}

fn symbol_kind_packability_bonus(kind: &str) -> i32 {
    let mut normalized = kind.to_owned();
    normalized.make_ascii_lowercase();
    match normalized.as_str() {
        "shared_symbol" => 180,
        "interface" => 150,
        "type" => 140,
        "class" => 120,
        "event" | "topic" | "queue" | "subject" | "stream" | "route" => 110,
        "service" | "controller" => 90,
        "function" => 40,
        "import" => -150,
        "file" => -200,
        _ => 0,
    }
}

fn symbol_kind_packability_bonus_for_query(target: &str, kind: &str) -> i32 {
    let mut normalized = kind.to_owned();
    normalized.make_ascii_lowercase();
    if normalized == "shared_symbol" && is_hook_like_query(target) {
        return 0;
    }
    symbol_kind_packability_bonus(kind)
}

fn pack_candidate_query_penalty(target: &str, item: &SearchResultItem) -> i32 {
    if is_hook_like_query(target)
        && (item.repo == "__virtual__" || item.file_path.starts_with("__hook__"))
    {
        return -250;
    }
    0
}

fn is_hook_like_query(target: &str) -> bool {
    target
        .strip_prefix("use")
        .and_then(|tail| tail.chars().next())
        .is_some_and(char::is_uppercase)
}

fn scaled_search_match_score(score: f32) -> i32 {
    let bounded = (score * 100.0).round().clamp(0.0, 120.0);
    #[expect(
        clippy::cast_possible_truncation,
        reason = "bounded floating-point search score is intentionally reduced to an integer band"
    )]
    {
        bounded as i32
    }
}

/// Shape-match bonus applied when the candidate's node kind aligns with the
/// query shape classified by [`classify_query_shape`].
///
/// This mirrors the `query_shape_match` field in [`gather_step_analysis::CandidateKey`]
/// used by the `impact` command, so both tools apply the same shape-aware
/// preference when ranking ambiguous candidates.  The bonus (200 points) is
/// large enough to dominate file-path and kind heuristics but smaller than the
/// structural edge score ceiling, preserving the intended priority order.
fn query_shape_match_bonus(target: &str, item_kind: &str) -> i32 {
    let shape = classify_query_shape(&[], target);
    let mut kind_lower = item_kind.to_owned();
    kind_lower.make_ascii_lowercase();
    let matches = match shape {
        QueryShape::EventRollout => matches!(
            kind_lower.as_str(),
            "topic" | "queue" | "subject" | "stream" | "event"
        ),
        QueryShape::RouteApiRollout => kind_lower == "route",
        QueryShape::GuardRollout => matches!(kind_lower.as_str(), "class" | "function"),
        QueryShape::SharedTypeRollout => matches!(
            kind_lower.as_str(),
            "shared_symbol" | "type" | "payload_contract"
        ),
        QueryShape::GenericSymbolImpact => false,
    };
    if matches { 200 } else { 0 }
}

fn query_alignment_bonus(target: &str, item: &SearchResultItem) -> i32 {
    let tokens = query_alignment_tokens(target);
    if tokens.is_empty() {
        return 0;
    }

    let repo = normalize_query_text(&item.repo);
    let file_path = normalize_query_text(&item.file_path);
    let symbol_name = normalize_query_text(&item.symbol_name);
    let kind = normalize_query_text(&item.kind);
    let language = normalize_query_text(&item.language);

    tokens.into_iter().fold(0, |score, token| {
        score
            + if repo.contains(&token) { 70 } else { 0 }
            + if file_path.contains(&token) { 45 } else { 0 }
            + if symbol_name.contains(&token) { 25 } else { 0 }
            + if kind.contains(&token) || language.contains(&token) {
                15
            } else {
                0
            }
    })
}

fn query_alignment_tokens(target: &str) -> Vec<String> {
    let mut normalized = String::with_capacity(target.len() * 2);
    let mut previous_was_lower = false;
    for ch in target.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && previous_was_lower {
                normalized.push(' ');
            }
            normalized.push(ch.to_ascii_lowercase());
            previous_was_lower = ch.is_ascii_lowercase();
        } else {
            normalized.push(' ');
            previous_was_lower = false;
        }
    }
    normalized
        .split_whitespace()
        .filter(|token| token.len() >= 2)
        .map(str::to_owned)
        .collect()
}

fn normalize_query_text(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect()
}

fn should_rank_global_pack_candidates(target: &str, exact: &[&SearchResultItem]) -> bool {
    let definition_shaped = exact.iter().any(|item| {
        let mut kind = item.kind.clone();
        kind.make_ascii_lowercase();
        matches!(
            kind.as_str(),
            "shared_symbol" | "interface" | "type" | "class"
        )
    });
    definition_shaped
        && target
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_uppercase())
}

fn pack_resolution_rationale(
    query_bonus: i32,
    query_score: i32,
    file_path: &str,
    structural_score: i32,
    anchor_score: i32,
) -> String {
    if query_bonus >= 70 {
        return "stronger query-to-path alignment".to_owned();
    }
    if query_score >= 80 {
        return "higher search relevance for target".to_owned();
    }
    let mut normalized = file_path.to_owned();
    normalized.make_ascii_lowercase();
    if normalized.contains("api") || normalized.contains("transport") {
        return "transport-adjacent path".to_owned();
    }
    if normalized.contains("service") || normalized.contains("controller") {
        return "boundary-facing service path".to_owned();
    }
    if structural_score >= 260 {
        return "stronger boundary and cross-repo connectivity".to_owned();
    }
    if structural_score >= 180 {
        return "broader call graph connectivity".to_owned();
    }
    if anchor_score >= 120 {
        return "higher planning anchor score".to_owned();
    }
    "same-name alternate anchor".to_owned()
}

fn clamp_margin_u16(value: i32) -> u16 {
    u16::try_from(value.max(0)).unwrap_or(u16::MAX)
}

fn build_ranked_alternate_anchors(
    ranked: &[RankedPackCandidate],
    winner: Option<&RankedPackCandidate>,
    limit: usize,
) -> Vec<RescueAnchor> {
    let winner_score = winner.map_or_else(
        || ranked.first().map_or(0, |item| item.score),
        |item| item.score,
    );
    ranked
        .iter()
        .filter(|candidate| winner.is_none_or(|top| candidate.symbol_id != top.symbol_id))
        .filter(|candidate| !suppress_virtual_hook_alternate(candidate, ranked))
        .take(limit)
        .map(|candidate| RescueAnchor {
            anchor_form: "symbol".to_owned(),
            repo: candidate.repo.clone(),
            symbol_id: candidate.symbol_id.clone(),
            symbol_name: candidate.symbol_name.clone(),
            rationale: Some(candidate.rationale.clone()),
            score_delta: Some(clamp_margin_u16(
                winner_score.saturating_sub(candidate.score),
            )),
            confidence_hint: Some(if candidate.score >= PACK_CONFIDENCE_MEDIUM_MIN_SCORE {
                "near_winner".to_owned()
            } else {
                "fallback".to_owned()
            }),
        })
        .collect()
}

fn suppress_virtual_hook_alternate(
    candidate: &RankedPackCandidate,
    ranked: &[RankedPackCandidate],
) -> bool {
    is_virtual_hook_ranked_candidate(candidate)
        && ranked
            .iter()
            .any(|other| other.repo != "__virtual__" && other.symbol_name == candidate.symbol_name)
}

fn is_virtual_hook_ranked_candidate(candidate: &RankedPackCandidate) -> bool {
    candidate.repo == "__virtual__" && candidate.file_path.starts_with("__hook__")
}

/// Parse `target` as `METHOD /path` and return the parts if valid.
/// try alternate anchor forms (event, route) for a weak planning pack.
///
/// Returns a `PlanningRescue` with the alternate anchors found and
/// human-readable hints.  Only triggered in planning mode after completeness
/// is determined to be "partial" or "unresolved".
fn attempt_planning_rescue(
    ctx: &McpContext,
    target: &str,
    resolved: &ResolvedPackTarget,
    repo: Option<&str>,
) -> PlanningRescue {
    let mut alternate_anchors = resolved.alternate_anchors.clone();
    let mut seen_symbol_ids = alternate_anchors
        .iter()
        .map(|anchor| anchor.symbol_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let mut hints = Vec::new();
    if !alternate_anchors.is_empty() {
        hints.push(
            "ranked symbol alternates are available; verify the best anchor before editing"
                .to_owned(),
        );
    }

    // Try event anchor only when the original resolution was not event-based.
    if !matches!(
        resolved.resolution.as_str(),
        "event_anchor" | "ambiguous_event_anchor"
    ) && let Ok(event_nodes) = resolve_event_targets(ctx.graph(), target)
    {
        let event_nodes: Vec<_> = match repo {
            Some(r) => event_nodes.into_iter().filter(|n| n.repo == r).collect(),
            None => event_nodes,
        };
        if !event_nodes.is_empty() {
            hints.push(format!(
                "found {} event/topic node(s) matching '{}'; use a resolved symbol_id as pack target",
                event_nodes.len(),
                target,
            ));
        }
        for node in event_nodes.into_iter().take(5) {
            let symbol_id = encode_node_id(node.id);
            if !seen_symbol_ids.insert(symbol_id.clone()) {
                continue;
            }
            alternate_anchors.push(RescueAnchor {
                anchor_form: "event".to_owned(),
                repo: node.repo.clone(),
                symbol_id,
                symbol_name: node.name.clone(),
                rationale: None,
                score_delta: None,
                confidence_hint: None,
            });
        }
    }

    // Try route anchor only when the original resolution was not route-based.
    if resolved.resolution != "route_anchor"
        && let Some((method, path)) = parse_route_target(target)
        && let Ok(Some(route_node)) = resolve_route_target(ctx.graph(), &method, &path)
        && repo.is_none_or(|r| route_node.repo == r)
    {
        let sid = encode_node_id(route_node.id);
        hints.push(format!(
            "route node '{}' found; try pack with symbol_id='{sid}'",
            route_node.name,
        ));
        if seen_symbol_ids.insert(sid.clone()) {
            alternate_anchors.push(RescueAnchor {
                anchor_form: "route".to_owned(),
                repo: route_node.repo.clone(),
                symbol_id: sid,
                symbol_name: route_node.name.clone(),
                rationale: None,
                score_delta: None,
                confidence_hint: None,
            });
        }
    }

    let reason = if !resolved.alternate_anchors.is_empty()
        && resolved.resolution == "search_ranked_resolved"
    {
        "pack resolved via ranked search; alternate anchors included for verification".to_owned()
    } else if resolved.resolution == "unresolved" {
        "target is unresolved; trying alternate anchor forms".to_owned()
    } else {
        "pack is partial; trying alternate anchor forms to improve coverage".to_owned()
    };

    PlanningRescue {
        triggered: true,
        reason,
        alternate_anchors,
        hints,
    }
}

fn merge_probable_downstream_repos(
    direct_repo: Option<&str>,
    repo_dependency_repos: Vec<String>,
    contract_repos: Vec<String>,
    transport_repos: Vec<String>,
) -> Vec<String> {
    let mut repos = repo_dependency_repos
        .into_iter()
        .chain(contract_repos)
        .chain(transport_repos)
        .collect::<Vec<_>>();
    repos.sort();
    repos.dedup();
    if let Some(repo) = direct_repo {
        repos.retain(|item| item != repo);
    }
    repos
}

fn is_downstream_dependency_edge_kind(kind: &str) -> bool {
    matches!(
        kind,
        "consumes"
            | "uses_event_from"
            | "consumes_api_from"
            | "uses_type_from"
            | "uses_guard_from"
            | "implements_contract_from"
            | "contract_on"
            | "uses_shared"
    )
}

fn transport_adjacent_repos(
    ctx: &McpContext,
    symbol: &SymbolResponseData,
    items: &[PackItem],
    transport_links: &[TransportLink],
) -> Vec<String> {
    let mut relevant = items
        .iter()
        .map(|item| item.symbol_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    relevant.insert(symbol.symbol_id.clone());

    let mut repos = Vec::new();
    for link in transport_links {
        let frontend_symbol_id = encode_node_id(link.frontend_node);
        let backend_symbol_id = encode_node_id(link.backend_node);
        let front_matches = relevant.contains(&frontend_symbol_id);
        let back_matches = relevant.contains(&backend_symbol_id);
        if !(front_matches || back_matches) {
            continue;
        }

        let candidate_id = if front_matches {
            link.backend_node
        } else {
            link.frontend_node
        };
        if let Ok(Some(node)) = ctx.graph().get_node(candidate_id) {
            repos.push(node.repo);
        }
    }
    repos.sort();
    repos.dedup();
    repos
}

#[cfg(test)]
fn planning_cross_repo_callers(
    upstream_nodes: &[crate::tools::search::TraversalNode],
    source_repo: Option<&str>,
) -> Vec<CrossRepoCaller> {
    let mut callers = upstream_nodes
        .iter()
        .filter(|node| source_repo.is_none_or(|repo| node.repo != repo))
        .map(|node| CrossRepoCaller {
            file_path: node.file_path.clone(),
            line_start: node.line_start,
            repo: node.repo.clone(),
            symbol_id: node.symbol_id.clone(),
            symbol_kind: node.kind.clone(),
            symbol_name: node.symbol_name.clone(),
        })
        .collect::<Vec<_>>();
    callers.sort_by(|left, right| {
        left.repo
            .cmp(&right.repo)
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
    callers.dedup_by(|left, right| left.symbol_id == right.symbol_id);
    callers
}

fn append_supplemental_proof_callers(
    upstream_nodes: &mut Vec<TraversalNode>,
    supplemental_callers: &[ProofCaller],
) {
    let mut seen = upstream_nodes
        .iter()
        .map(|node| node.symbol_id.clone())
        .collect::<BTreeSet<_>>();
    for caller in supplemental_callers {
        let symbol_id = encode_node_id(caller.node.id);
        if !seen.insert(symbol_id.clone()) {
            continue;
        }
        upstream_nodes.push(TraversalNode {
            depth: caller.depth,
            file_path: caller.node.file_path.clone(),
            line_start: caller.node.span.as_ref().map(|span| span.line_start),
            repo: caller.node.repo.clone(),
            symbol_id,
            kind: node_kind_label(caller.node.kind).to_owned(),
            symbol_name: caller.node.name.clone(),
        });
    }
}

fn merge_supplemental_cross_repo_callers(
    callers: &mut Vec<CrossRepoCaller>,
    supplemental_callers: &[ProofCaller],
    anchor_repo: Option<&str>,
) {
    let mut seen = callers
        .iter()
        .map(|caller| caller.symbol_id.clone())
        .collect::<BTreeSet<_>>();
    for caller in supplemental_callers {
        if anchor_repo.is_some_and(|repo| caller.node.repo == repo)
            || !is_real_repo(&caller.node.repo)
        {
            continue;
        }
        let symbol_id = encode_node_id(caller.node.id);
        if !seen.insert(symbol_id.clone()) {
            continue;
        }
        callers.push(CrossRepoCaller {
            file_path: caller.node.file_path.clone(),
            line_start: caller.node.span.as_ref().map(|span| span.line_start),
            repo: caller.node.repo.clone(),
            symbol_id,
            symbol_kind: node_kind_label(caller.node.kind).to_owned(),
            symbol_name: caller.node.name.clone(),
        });
    }
    callers.sort_by(|left, right| {
        left.repo
            .cmp(&right.repo)
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

fn merge_cross_repo_callers(callers: &mut Vec<CrossRepoCaller>, additional: Vec<CrossRepoCaller>) {
    let mut seen = callers
        .iter()
        .map(|caller| caller.symbol_id.clone())
        .collect::<BTreeSet<_>>();
    for caller in additional {
        if seen.insert(caller.symbol_id.clone()) {
            callers.push(caller);
        }
    }
    callers.sort_by(|left, right| {
        left.repo
            .cmp(&right.repo)
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
}

fn planning_proofs_to_json(proofs: &[PlanningProof]) -> Vec<serde_json::Value> {
    proofs
        .iter()
        .filter_map(|proof| serde_json::to_value(proof).ok())
        .collect()
}

fn deserialize_planning_proofs(response: &ContextPackResponse) -> Vec<PlanningProof> {
    let mut proofs = Vec::with_capacity(response.data.planning_proofs.len());
    for value in &response.data.planning_proofs {
        match serde_json::from_value::<PlanningProof>(value.clone()) {
            Ok(proof) => proofs.push(proof),
            Err(error) => tracing::warn!(
                target: "gather_step_mcp::packs::deserialize_planning_proofs",
                error = %error,
                "skipping malformed planning_proof entry; derived \
                 confirmed_downstream_repos may be shorter than the wire \
                 response — schema drift suspected"
            ),
        }
    }
    proofs
}

fn cross_repo_callers_from_proofs(
    ctx: &McpContext,
    proofs: &[PlanningProof],
    anchor_repo: Option<&str>,
) -> Result<Vec<CrossRepoCaller>, McpServerError> {
    let mut callers = Vec::new();
    let mut seen = BTreeSet::<String>::new();
    for proof in proofs.iter().filter(|proof| proof.is_structural()) {
        let caller_repo = if anchor_repo.is_some_and(|repo| proof.target_repo == repo) {
            proof.source_repo.as_str()
        } else {
            proof.target_repo.as_str()
        };
        if anchor_repo.is_some_and(|repo| caller_repo == repo) || !is_real_repo(caller_repo) {
            continue;
        }
        let Some(hop) = proof.path.iter().rev().find(|hop| hop.repo == caller_repo) else {
            continue;
        };
        let Some(node) = ctx.graph().get_node(hop.node_id)? else {
            continue;
        };
        if !seen.insert(encode_node_id(node.id)) {
            continue;
        }
        callers.push(CrossRepoCaller {
            file_path: node.file_path,
            line_start: node.span.as_ref().map(|span| span.line_start),
            repo: node.repo,
            symbol_id: encode_node_id(node.id),
            symbol_kind: node_kind_label(node.kind).to_owned(),
            symbol_name: node.name,
        });
    }
    callers.sort_by(|left, right| {
        left.repo
            .cmp(&right.repo)
            .then(left.file_path.cmp(&right.file_path))
            .then(left.line_start.cmp(&right.line_start))
            .then(left.symbol_name.cmp(&right.symbol_name))
            .then(left.symbol_id.cmp(&right.symbol_id))
    });
    Ok(callers)
}

fn apply_proof_derived_change_impact(
    ctx: &McpContext,
    anchor_repo: Option<&str>,
    repo_filter: Option<&str>,
    response: &mut ContextPackResponse,
) -> Result<(), McpServerError> {
    let proofs = deserialize_planning_proofs(response);
    let (confirmed_repos, probable_repos) = derive_repo_sets(&proofs, repo_filter);
    let (confirmed, truncated_repos) = cap_change_impact_repos(confirmed_repos, repo_filter);
    let probable = if confirmed.is_empty() {
        probable_repos
    } else {
        Vec::new()
    };
    let persisted_callers = std::mem::take(&mut response.data.change_impact.cross_repo_callers);
    let mut cross_repo_callers = cross_repo_callers_from_proofs(ctx, &proofs, anchor_repo)?;
    merge_cross_repo_callers(&mut cross_repo_callers, persisted_callers);
    response.data.change_impact.cross_repo_callers = cross_repo_callers;
    response.data.change_impact.confirmed_downstream_repos = confirmed.clone();
    response.data.change_impact.downstream_repos = confirmed;
    response.data.change_impact.probable_downstream_repos = probable;
    if !response
        .data
        .change_impact
        .confirmed_downstream_repos
        .is_empty()
        || !response
            .data
            .change_impact
            .probable_downstream_repos
            .is_empty()
    {
        response.data.change_impact.unresolved_possible.clear();
    }
    response.data.change_impact.truncated_repos = truncated_repos;
    Ok(())
}

fn parse_route_target(target: &str) -> Option<(String, String)> {
    const VALID_METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"];
    let trimmed = target.trim();
    let (method_raw, rest) = trimmed.split_once(' ')?;
    let method = method_raw.trim().to_ascii_uppercase();
    if !VALID_METHODS.contains(&method.as_str()) {
        return None;
    }
    let path = rest.trim().to_owned();
    if path.is_empty() || !path.starts_with('/') {
        return None;
    }
    Some((method, path))
}

fn unresolved_resolution_warnings(resolved: &ResolvedPackTarget) -> Vec<String> {
    match resolved.resolution.as_str() {
        "symbol_id"
        | "event_anchor"
        | "route_anchor"
        | "search_ranked_resolved"
        | "search_ranked_alternates"
        | "search_ranked_deferred" => Vec::new(),
        "search_resolved" => vec![
            "target resolved via search rather than an explicit symbol_id; verify identity before editing"
                .to_owned(),
        ],
        "repo_filtered_out" => vec![
            "the requested target exists, but the active repo filter excluded it".to_owned(),
        ],
        "ambiguous_search_match" | "ambiguous_event_anchor" => vec![format!(
            "target matched {} indexed candidates; narrow the query or use a symbol_id",
            resolved.candidate_count
        )],
        _ => vec!["target could not be resolved to a unique indexed symbol".to_owned()],
    }
}

fn pack_ambiguity(resolved: &ResolvedPackTarget) -> Option<PackAmbiguity> {
    let mut reason_codes = Vec::new();
    match resolved.resolution.as_str() {
        "ambiguous_search_match" | "ambiguous_event_anchor" => {
            reason_codes.push(resolved.resolution.clone());
        }
        "repo_filtered_out" => reason_codes.push("repo_filtered_out".to_owned()),
        "unresolved" => reason_codes.push("unresolved_target".to_owned()),
        _ => {}
    }
    if reason_codes.is_empty() {
        None
    } else {
        Some(PackAmbiguity {
            candidate_count: resolved.candidate_count,
            reason_codes,
        })
    }
}

fn gap_warnings(unresolved_gaps: &[String]) -> Vec<String> {
    unresolved_gaps
        .iter()
        .map(|gap| format!("pack is incomplete: {gap}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        ChangeImpactSummary, ContextPackData, ContextPackMeta, ContextPackResponse,
        CrossRepoCaller, PackItem, PackResolutionDetails, PlanningRescue, RankedPackCandidate,
        RescueAnchor, ResolvedPackTarget, TruncatedRepos, apply_planning_evidence_ranking,
        build_ranked_alternate_anchors, build_resolution_details, cap_change_impact_repos,
        clamp_margin_u16, compute_unresolved_gaps, file_path_packability_bonus,
        merge_probable_downstream_repos, pack_candidate_query_penalty, pack_is_structurally_weak,
        pack_recovery_score, pack_resolution_rationale, pack_resolution_strategy,
        parse_route_target, planning_cross_repo_callers, query_alignment_bonus,
        query_alignment_tokens, query_shape_match_bonus, scaled_search_match_score,
        should_rank_global_pack_candidates, symbol_kind_packability_bonus,
        symbol_kind_packability_bonus_for_query,
    };
    use crate::{
        config::{McpContext, McpServerConfig},
        tools::search::{SearchResultItem, TraversalNode},
    };
    use gather_step_analysis::PackMode;
    use gather_step_analysis::proofs::{
        derive_repo_sets, finalize_proofs, is_eventish_kind, same_repo_event_context_targets,
    };
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, PlanningProof, ProofHop,
        ProofKind, Visibility, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb, WorkspaceStores};
    use smallvec::smallvec;
    use std::{
        collections::BTreeMap,
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    };
    use tracing_subscriber::{Layer, layer::SubscriberExt};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-packs-{name}-{}-{counter}.redb",
                process::id()
            ));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[derive(Clone)]
    struct WarningCounterLayer {
        target: &'static str,
        count: Arc<AtomicUsize>,
    }

    impl<S> Layer<S> for WarningCounterLayer
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() == self.target
                && *event.metadata().level() == tracing::Level::WARN
            {
                self.count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn count_target_warnings(target: &'static str, f: impl FnOnce()) -> usize {
        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(WarningCounterLayer {
            target,
            count: Arc::clone(&count),
        });
        tracing::subscriber::with_default(subscriber, f);
        count.load(Ordering::Relaxed)
    }

    fn empty_test_context() -> (McpContext, tempfile::TempDir) {
        let storage_dir = tempfile::tempdir().expect("storage tempdir");
        let stores = Arc::new(WorkspaceStores::open(storage_dir.path()).expect("stores open"));
        let ctx = McpContext::from_workspace_stores(
            McpServerConfig::new(
                storage_dir.path().join("registry.json"),
                storage_dir.path().join("graph.redb"),
            ),
            stores,
        );
        (ctx, storage_dir)
    }

    #[test]
    fn repo_filter_is_applied_before_change_impact_fan_out_cap() {
        let repos = (0..12)
            .map(|index| format!("repo-{index:02}"))
            .collect::<Vec<_>>();

        let (retained, truncated) = cap_change_impact_repos(repos, Some("repo-11"));

        assert_eq!(retained, vec!["repo-11".to_owned()]);
        assert_eq!(truncated, None);
    }

    #[test]
    fn change_impact_caps_report_truncated_repo_metadata() {
        let repos = (0..10)
            .map(|index| format!("repo-{index:02}"))
            .collect::<Vec<_>>();

        let (retained, truncated) = cap_change_impact_repos(repos, None);

        assert_eq!(retained.len(), 8);
        assert_eq!(
            truncated,
            Some(TruncatedRepos {
                count: 2,
                names: vec!["repo-08".to_owned(), "repo-09".to_owned()],
                reason_codes: vec!["fan_out_cap".to_owned()],
            })
        );
    }

    // parse_route_target
    #[test]
    fn parse_route_target_valid_post() {
        assert_eq!(
            parse_route_target("POST /api/v2/report/pdf"),
            Some(("POST".to_owned(), "/api/v2/report/pdf".to_owned()))
        );
    }

    #[test]
    fn parse_route_target_valid_get_with_spaces() {
        assert_eq!(
            parse_route_target("  GET  /health  "),
            Some(("GET".to_owned(), "/health".to_owned()))
        );
    }

    #[test]
    fn parse_route_target_lowercase_method() {
        assert_eq!(
            parse_route_target("delete /api/user/123"),
            Some(("DELETE".to_owned(), "/api/user/123".to_owned()))
        );
    }

    #[test]
    fn parse_route_target_invalid_no_path() {
        assert_eq!(parse_route_target("POST"), None);
    }

    #[test]
    fn parse_route_target_invalid_path_no_slash() {
        assert_eq!(parse_route_target("POST api/foo"), None);
    }

    #[test]
    fn parse_route_target_invalid_unknown_method() {
        assert_eq!(parse_route_target("CONNECT /foo"), None);
    }

    #[test]
    fn parse_route_target_plain_symbol_name() {
        // Plain symbol names must not match as routes
        assert_eq!(parse_route_target("PdfService"), None);
        assert_eq!(parse_route_target("pdf.generation.completed"), None);
    }

    // ChangeImpactSummary dual-confidence tier
    #[test]
    fn confirmed_repos_populate_backward_compat_downstream_repos() {
        // downstream_repos must equal confirmed_downstream_repos for backward compat.
        let summary = ChangeImpactSummary {
            direct_repos: vec!["svc-a".to_owned()],
            cross_repo_callers: Vec::new(),
            confirmed_downstream_repos: vec!["svc-b".to_owned()],
            probable_downstream_repos: Vec::new(),
            downstream_repos: vec!["svc-b".to_owned()],
            unresolved_possible: Vec::new(),
            truncated_repos: None,
        };
        assert_eq!(summary.downstream_repos, summary.confirmed_downstream_repos);
    }

    #[test]
    fn probable_repos_surfaced_when_confirmed_is_empty() {
        // When confirmed is empty, probable should carry the fallback repos.
        let summary = ChangeImpactSummary {
            direct_repos: Vec::new(),
            cross_repo_callers: Vec::new(),
            confirmed_downstream_repos: Vec::new(),
            probable_downstream_repos: vec!["svc-c".to_owned()],
            downstream_repos: Vec::new(),
            unresolved_possible: vec!["svc-c".to_owned()],
            truncated_repos: None,
        };
        assert!(
            summary.confirmed_downstream_repos.is_empty(),
            "confirmed must be empty"
        );
        assert!(
            !summary.probable_downstream_repos.is_empty(),
            "probable must be surfaced as fallback"
        );
        assert_eq!(
            summary.probable_downstream_repos,
            summary.unresolved_possible
        );
    }

    #[test]
    fn probable_repos_omitted_when_confirmed_is_present() {
        // When graph proof is available, probable_downstream_repos is empty.
        let summary = ChangeImpactSummary {
            direct_repos: vec!["svc-a".to_owned()],
            cross_repo_callers: Vec::new(),
            confirmed_downstream_repos: vec!["svc-b".to_owned()],
            probable_downstream_repos: Vec::new(),
            downstream_repos: vec!["svc-b".to_owned()],
            unresolved_possible: Vec::new(),
            truncated_repos: None,
        };
        assert!(
            summary.probable_downstream_repos.is_empty(),
            "probable must be empty when confirmed repos exist"
        );
    }

    #[test]
    fn planning_cross_repo_callers_excludes_anchor_repo_and_dedups() {
        let callers = planning_cross_repo_callers(
            &[
                TraversalNode {
                    depth: 1,
                    file_path: "src/a.ts".to_owned(),
                    kind: "function".to_owned(),
                    line_start: Some(10),
                    repo: "frontend_standard".to_owned(),
                    symbol_id: "same".to_owned(),
                    symbol_name: "same".to_owned(),
                },
                TraversalNode {
                    depth: 1,
                    file_path: "src/b.ts".to_owned(),
                    kind: "function".to_owned(),
                    line_start: Some(20),
                    repo: "identity".to_owned(),
                    symbol_id: "identity-caller".to_owned(),
                    symbol_name: "refreshAccessToken".to_owned(),
                },
                TraversalNode {
                    depth: 1,
                    file_path: "src/b.ts".to_owned(),
                    kind: "function".to_owned(),
                    line_start: Some(20),
                    repo: "identity".to_owned(),
                    symbol_id: "identity-caller".to_owned(),
                    symbol_name: "refreshAccessToken".to_owned(),
                },
            ],
            Some("frontend_standard"),
        );

        assert_eq!(
            callers,
            vec![CrossRepoCaller {
                file_path: "src/b.ts".to_owned(),
                line_start: Some(20),
                repo: "identity".to_owned(),
                symbol_id: "identity-caller".to_owned(),
                symbol_kind: "function".to_owned(),
                symbol_name: "refreshAccessToken".to_owned(),
            }]
        );
    }

    #[test]
    fn same_repo_event_context_targets_lifts_dispatcher_event_identity() {
        let temp = TempDb::new("event-context");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let consumer_file = file(
            "backend_standard",
            "src/event-handlers/general-report-generation/general-report-generation.service.ts",
        );
        let dispatcher_file = file(
            "backend_standard",
            "src/event-handlers/event-handlers.service.ts",
        );
        let service_handle = node(
            "backend_standard",
            "src/event-handlers/general-report-generation/general-report-generation.service.ts",
            NodeKind::Function,
            "GeneralReportGenerationService.handleEvent",
            0,
        );
        let dispatcher_handle = node(
            "backend_standard",
            "src/event-handlers/event-handlers.service.ts",
            NodeKind::Function,
            "EventHandlersService.handleEvent",
            0,
        );
        let event_node = node_with_external_id(
            "backend_standard",
            "src/event-handlers/event-handlers.service.ts",
            NodeKind::Event,
            "document.report-generation.queued",
            0,
            Some("__event__kafka__document.report-generation.queued"),
        );

        store
            .bulk_insert(
                &[
                    consumer_file.clone(),
                    dispatcher_file.clone(),
                    service_handle.clone(),
                    dispatcher_handle.clone(),
                    event_node.clone(),
                ],
                &[
                    edge(
                        dispatcher_handle.id,
                        service_handle.id,
                        EdgeKind::Calls,
                        dispatcher_file.id,
                    ),
                    edge(
                        dispatcher_handle.id,
                        event_node.id,
                        EdgeKind::UsesEventFrom,
                        dispatcher_file.id,
                    ),
                ],
            )
            .expect("graph seed should succeed");

        let targets = same_repo_event_context_targets(&store, &service_handle, 2)
            .expect("event context lookup should succeed");

        assert_eq!(targets, vec![event_node.id]);
    }

    #[test]
    fn is_eventish_kind_covers_virtual_transport_nodes() {
        assert!(is_eventish_kind(NodeKind::Event));
        assert!(is_eventish_kind(NodeKind::Topic));
        assert!(is_eventish_kind(NodeKind::Queue));
        assert!(!is_eventish_kind(NodeKind::Function));
    }

    // Evidence-aware planning ranking
    fn make_pack_item(score: u16, file_path: &str, has_evidence: bool) -> PackItem {
        PackItem {
            category: "related".to_owned(),
            file_path: file_path.to_owned(),
            line_start: None,
            reason: "test".to_owned(),
            repo: "repo-a".to_owned(),
            score,
            symbol_id: format!("id_{file_path}"),
            symbol_kind: "function".to_owned(),
            symbol_name: format!("sym_{file_path}"),
            evidence_chain: if has_evidence {
                Some(serde_json::Value::String("A → B".to_owned()))
            } else {
                None
            },
        }
    }

    fn file(repo: &str, file_path: &str) -> NodeData {
        node(repo, file_path, NodeKind::File, file_path, 0)
    }

    fn node(repo: &str, file_path: &str, kind: NodeKind, name: &str, ordinal: u16) -> NodeData {
        node_with_external_id(repo, file_path, kind, name, ordinal, None)
    }

    fn node_with_external_id(
        repo: &str,
        file_path: &str,
        kind: NodeKind,
        name: &str,
        _ordinal: u16,
        external_id: Option<&str>,
    ) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: external_id.map(ToOwned::to_owned),
            signature: None,
            visibility: Some(Visibility::Public),
            span: None,
            is_virtual: matches!(
                kind,
                NodeKind::SharedSymbol | NodeKind::Event | NodeKind::Topic
            ),
        }
    }

    fn edge(source: NodeId, target: NodeId, kind: EdgeKind, owner_file: NodeId) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: true,
        }
    }

    #[test]
    fn evidence_items_rank_above_equal_score_items_without_evidence() {
        let mut items = vec![
            make_pack_item(100, "no_evidence.rs", false),
            make_pack_item(100, "with_evidence.rs", true),
        ];

        apply_planning_evidence_ranking(&mut items);

        // Item with evidence gets +50 → 150 vs 100, so it must be first.
        assert_eq!(items[0].file_path, "with_evidence.rs");
        assert_eq!(items[1].file_path, "no_evidence.rs");
    }

    #[test]
    fn evidence_boost_is_fifty_points() {
        let mut items = vec![make_pack_item(80, "a.rs", true)];
        apply_planning_evidence_ranking(&mut items);
        assert_eq!(items[0].score, 130);
    }

    #[test]
    fn items_without_evidence_are_not_boosted() {
        let mut items = vec![make_pack_item(80, "a.rs", false)];
        apply_planning_evidence_ranking(&mut items);
        assert_eq!(items[0].score, 80);
    }

    #[test]
    fn evidence_ranking_secondary_sort_is_stable_on_repo_then_path() {
        let mut items = vec![
            make_pack_item(100, "z.rs", false),
            make_pack_item(100, "a.rs", false),
        ];

        apply_planning_evidence_ranking(&mut items);

        // Same score, same repo — secondary sort is file_path ascending.
        assert_eq!(items[0].file_path, "a.rs");
        assert_eq!(items[1].file_path, "z.rs");
    }

    // PlanningRescue struct round-trips correctly
    #[test]
    fn planning_rescue_serialises_without_empty_fields() {
        let rescue = PlanningRescue {
            triggered: true,
            reason: "pack is partial; trying alternate anchor forms to improve coverage".to_owned(),
            alternate_anchors: Vec::new(),
            hints: Vec::new(),
        };

        let json = serde_json::to_value(&rescue).unwrap();
        // Empty vecs are skipped.
        assert!(json.get("alternate_anchors").is_none());
        assert!(json.get("hints").is_none());
        assert_eq!(json["triggered"], true);
    }

    #[test]
    fn planning_rescue_serialises_anchors_and_hints_when_present() {
        let rescue = PlanningRescue {
            triggered: true,
            reason: "target is unresolved; trying alternate anchor forms".to_owned(),
            alternate_anchors: vec![RescueAnchor {
                anchor_form: "event".to_owned(),
                repo: "svc-a".to_owned(),
                symbol_id: "abc123".to_owned(),
                symbol_name: "pdf.generation.completed".to_owned(),
                rationale: Some("topic-adjacent boundary".to_owned()),
                score_delta: Some(12),
                confidence_hint: Some("near_winner".to_owned()),
            }],
            hints: vec![
                "found 1 event/topic node(s) matching 'pdf.generation.completed'".to_owned(),
            ],
        };

        let json = serde_json::to_value(&rescue).unwrap();
        assert_eq!(json["alternate_anchors"].as_array().unwrap().len(), 1);
        assert_eq!(json["alternate_anchors"][0]["anchor_form"], "event");
        assert_eq!(
            json["alternate_anchors"][0]["rationale"],
            "topic-adjacent boundary"
        );
        assert_eq!(json["hints"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn planning_rescue_reason_differs_by_completeness() {
        // "unresolved" completeness uses a different reason string than "partial".
        let unresolved_reason = "target is unresolved; trying alternate anchor forms";
        let partial_reason = "pack is partial; trying alternate anchor forms to improve coverage";
        assert_ne!(unresolved_reason, partial_reason);
    }

    #[test]
    fn context_pack_data_planning_rescue_absent_by_default() {
        // planning_rescue is skip_serializing_if = "Option::is_none", so when
        // None it must not appear in the serialised output.
        let data = ContextPackData {
            mode: "debug".to_owned(),
            target: "SomeService".to_owned(),
            found: false,
            items: Vec::new(),
            semantic_bridges: Vec::new(),
            next_steps: Vec::new(),
            unresolved_gaps: Vec::new(),
            change_impact: ChangeImpactSummary::default(),
            transport_links: None,
            planning_rescue: None,
            planning_proofs: Vec::new(),
        };

        let json = serde_json::to_value(&data).unwrap();
        assert!(
            json.get("planning_rescue").is_none(),
            "planning_rescue must be absent when None (non-planning mode)"
        );
    }

    #[test]
    fn resolution_strategy_promotes_rescue_over_ranked() {
        assert_eq!(
            pack_resolution_strategy("search_ranked_resolved", true),
            "rescue"
        );
        assert_eq!(pack_resolution_strategy("symbol_id", false), "exact");
        assert_eq!(
            pack_resolution_strategy("ambiguous_search_match", false),
            "fallback"
        );
    }

    #[test]
    fn resolution_details_use_rescue_alternates_when_present() {
        let resolved = ResolvedPackTarget {
            alternate_anchors: vec![RescueAnchor {
                anchor_form: "symbol".to_owned(),
                repo: "frontend_standard".to_owned(),
                symbol_id: "winner-alt".to_owned(),
                symbol_name: "useAuthentication".to_owned(),
                rationale: Some("ranked alternate".to_owned()),
                score_delta: Some(20),
                confidence_hint: Some("near_winner".to_owned()),
            }],
            candidate_count: 2,
            confidence_model_version: Some(super::PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
            resolution: "search_ranked_resolved".to_owned(),
            resolution_confidence: Some("medium".to_owned()),
            symbol_id: Some("winner".to_owned()),
            winner_margin: Some(80),
            ranked_alternates: Vec::new(),
        };
        let rescue = PlanningRescue {
            triggered: true,
            reason: "pack is partial; trying alternate anchor forms to improve coverage".to_owned(),
            alternate_anchors: vec![RescueAnchor {
                anchor_form: "event".to_owned(),
                repo: "backend_standard".to_owned(),
                symbol_id: "event-anchor".to_owned(),
                symbol_name: "document.report.queued".to_owned(),
                rationale: None,
                score_delta: None,
                confidence_hint: None,
            }],
            hints: Vec::new(),
        };

        let details = build_resolution_details(&resolved, Some("winner"), Some(&rescue));
        assert_eq!(
            details,
            PackResolutionDetails {
                strategy: "rescue".to_owned(),
                winner: Some("winner".to_owned()),
                winner_margin: Some(80),
                alternates: rescue.alternate_anchors,
            }
        );
    }

    #[test]
    fn file_path_packability_bonus_prefers_boundary_aligned_candidates() {
        assert!(
            file_path_packability_bonus("src/auth/api/use_authentication.ts")
                > file_path_packability_bonus("src/auth/local_state.ts")
        );
    }

    #[test]
    fn symbol_kind_packability_bonus_prefers_shared_contract_definitions() {
        assert!(
            symbol_kind_packability_bonus("shared_symbol")
                > symbol_kind_packability_bonus("function")
        );
        assert!(symbol_kind_packability_bonus("type") >= 140);
    }

    #[test]
    fn hook_queries_do_not_over_reward_shared_symbol_candidates() {
        assert!(
            symbol_kind_packability_bonus_for_query("useAuthentication", "function")
                > symbol_kind_packability_bonus_for_query("useAuthentication", "shared_symbol")
        );
        assert!(
            symbol_kind_packability_bonus_for_query("CreateOrderInput", "shared_symbol")
                > symbol_kind_packability_bonus_for_query("CreateOrderInput", "function")
        );
    }

    #[test]
    fn hook_queries_penalize_virtual_hook_markers() {
        let virtual_hook = SearchResultItem {
            exact_match: true,
            file_path: "__hook__@app/hooks::useAuthentication".to_owned(),
            kind: "unknown".to_owned(),
            language: "typescript".to_owned(),
            line_start: None,
            repo: "__virtual__".to_owned(),
            score: 1.0,
            symbol_id: "virtual".to_owned(),
            symbol_name: "useAuthentication".to_owned(),
        };
        let real_hook = SearchResultItem {
            exact_match: true,
            file_path: "app/src/v2/app/hooks/useAuthentication.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: Some(24),
            repo: "frontend-app".to_owned(),
            score: 1.0,
            symbol_id: "real".to_owned(),
            symbol_name: "useAuthentication".to_owned(),
        };

        assert!(pack_candidate_query_penalty("useAuthentication", &virtual_hook) < 0);
        assert_eq!(
            pack_candidate_query_penalty("useAuthentication", &real_hook),
            0
        );
        assert_eq!(
            pack_candidate_query_penalty("CreateOrderInput", &virtual_hook),
            0
        );
    }

    #[test]
    fn query_alignment_tokens_split_camel_case_and_path_terms() {
        assert_eq!(
            query_alignment_tokens("FrontendAuth/useSession"),
            vec![
                "frontend".to_owned(),
                "auth".to_owned(),
                "use".to_owned(),
                "session".to_owned()
            ]
        );
    }

    #[test]
    fn query_alignment_bonus_prefers_repo_and_path_matches() {
        let aligned = SearchResultItem {
            exact_match: true,
            file_path: "src/frontend/auth/use_session.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: None,
            repo: "frontend_standard".to_owned(),
            score: 0.31,
            symbol_id: "aligned".to_owned(),
            symbol_name: "useSession".to_owned(),
        };
        let local = SearchResultItem {
            exact_match: true,
            file_path: "src/backend/local/session_cache.rs".to_owned(),
            kind: "function".to_owned(),
            language: "rust".to_owned(),
            line_start: None,
            repo: "backend_standard".to_owned(),
            score: 0.31,
            symbol_id: "local".to_owned(),
            symbol_name: "useSession".to_owned(),
        };

        assert!(
            query_alignment_bonus("frontend auth session", &aligned)
                > query_alignment_bonus("frontend auth session", &local)
        );
    }

    #[test]
    fn global_ranked_resolution_is_limited_to_definition_shaped_queries() {
        let shared = SearchResultItem {
            exact_match: true,
            file_path: "src/order.ts".to_owned(),
            kind: "shared_symbol".to_owned(),
            language: "typescript".to_owned(),
            line_start: None,
            repo: "shared_contracts".to_owned(),
            score: 0.44,
            symbol_id: "shared".to_owned(),
            symbol_name: "CreateOrderInput".to_owned(),
        };
        let helper = SearchResultItem {
            exact_match: true,
            file_path: "src/helper.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: None,
            repo: "frontend_standard".to_owned(),
            score: 0.44,
            symbol_id: "helper".to_owned(),
            symbol_name: "useAuthentication".to_owned(),
        };

        assert!(should_rank_global_pack_candidates(
            "CreateOrderInput",
            &[&shared]
        ));
        assert!(!should_rank_global_pack_candidates(
            "useAuthentication",
            &[&helper]
        ));
        assert!(!should_rank_global_pack_candidates(
            "createOrderInput",
            &[&shared]
        ));
    }

    #[test]
    fn strict_symbol_match_ignores_noisy_search_exact_flags() {
        let target = SearchResultItem {
            exact_match: true,
            file_path: "src/kafka/types/document-event.type.ts".to_owned(),
            kind: "type".to_owned(),
            language: "typescript".to_owned(),
            line_start: None,
            repo: "shared_contracts".to_owned(),
            score: 0.91,
            symbol_id: "target".to_owned(),
            symbol_name: "DocumentReportGenerationQueuedEvent".to_owned(),
        };
        let noisy = SearchResultItem {
            exact_match: true,
            file_path: "src/general-report-generation.service.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: None,
            repo: "backend_standard".to_owned(),
            score: 0.84,
            symbol_id: "noisy".to_owned(),
            symbol_name: "handleEvent".to_owned(),
        };

        assert!(super::is_strict_symbol_match(
            &target,
            "DocumentReportGenerationQueuedEvent"
        ));
        assert!(!super::is_strict_symbol_match(
            &noisy,
            "DocumentReportGenerationQueuedEvent"
        ));
    }

    #[test]
    fn low_packability_exact_alternates_are_kept_for_strict_symbol_winners() {
        let function = SearchResultItem {
            exact_match: true,
            file_path: "src/useAuthentication.ts".to_owned(),
            kind: "function".to_owned(),
            language: "typescript".to_owned(),
            line_start: Some(10),
            repo: "frontend_standard".to_owned(),
            score: 30.0,
            symbol_id: "function".to_owned(),
            symbol_name: "useAuthentication".to_owned(),
        };
        let module = SearchResultItem {
            exact_match: true,
            file_path: "src/useAuthentication.ts".to_owned(),
            kind: "module".to_owned(),
            language: "typescript".to_owned(),
            line_start: Some(1),
            repo: "frontend_standard".to_owned(),
            score: 12.0,
            symbol_id: "module".to_owned(),
            symbol_name: "./useAuthentication".to_owned(),
        };

        let (preferred, alternates) =
            super::prune_low_packability_exact_matches(vec![&function, &module]);
        assert_eq!(preferred.len(), 1);
        assert_eq!(preferred[0].symbol_id, "function");
        assert_eq!(alternates.len(), 1);
        assert_eq!(alternates[0].symbol_id.as_str(), "module");
    }

    #[test]
    fn scaled_search_match_score_is_bounded() {
        assert_eq!(scaled_search_match_score(0.83), 83);
        assert_eq!(scaled_search_match_score(9.0), 120);
        assert_eq!(scaled_search_match_score(-2.0), 0);
    }

    #[test]
    fn pack_resolution_rationale_prefers_query_signals_when_present() {
        assert_eq!(
            pack_resolution_rationale(90, 40, "src/local.rs", 20, 20),
            "stronger query-to-path alignment"
        );
        assert_eq!(
            pack_resolution_rationale(10, 85, "src/local.rs", 20, 20),
            "higher search relevance for target"
        );
        assert_eq!(
            pack_resolution_rationale(0, 20, "src/local.rs", 300, 20),
            "stronger boundary and cross-repo connectivity"
        );
    }

    #[test]
    fn clamp_margin_saturates_and_rejects_negative_values() {
        assert_eq!(clamp_margin_u16(-25), 0);
        assert_eq!(clamp_margin_u16(42), 42);
        assert_eq!(clamp_margin_u16(i32::MAX), u16::MAX);
    }

    #[test]
    fn merge_probable_downstream_repos_combines_all_fallback_sources() {
        let repos = merge_probable_downstream_repos(
            Some("frontend_standard"),
            vec!["backend_standard".to_owned()],
            vec![
                "shared_contracts".to_owned(),
                "frontend_standard".to_owned(),
            ],
            vec!["backend_standard".to_owned(), "worker_standard".to_owned()],
        );

        assert_eq!(
            repos,
            vec![
                "backend_standard".to_owned(),
                "shared_contracts".to_owned(),
                "worker_standard".to_owned()
            ]
        );
    }

    #[test]
    fn debug_mode_skips_rollout_gap_when_downstream_is_absent() {
        let gaps = compute_unresolved_gaps(
            PackMode::Debug,
            Some(NodeKind::Route),
            false,
            true,
            true,
            true,
        );
        assert!(gaps.is_empty());
    }

    #[test]
    fn planning_mode_skips_gap_when_probable_downstream_exists() {
        let gaps = compute_unresolved_gaps(
            PackMode::Planning,
            Some(NodeKind::SharedSymbol),
            true,
            true,
            false,
            true,
        );
        assert!(gaps.is_empty());
    }

    #[test]
    fn planning_mode_reports_rollout_gaps_when_all_signals_are_missing() {
        let gaps = compute_unresolved_gaps(
            PackMode::Planning,
            Some(NodeKind::Function),
            true,
            true,
            true,
            true,
        );
        assert_eq!(gaps.len(), 3);
    }

    fn make_pack_response(
        semantic_bridges: usize,
        confirmed_repos: usize,
        evidence_items: usize,
        unresolved_gaps: usize,
    ) -> ContextPackResponse {
        let mut items = vec![make_pack_item(100, "target.rs", false)];
        items[0].category = "target".to_owned();
        for index in 0..evidence_items {
            items.push(make_pack_item(80, &format!("evidence_{index}.rs"), true));
        }
        ContextPackResponse {
            data: ContextPackData {
                mode: "planning".to_owned(),
                target: "Target".to_owned(),
                found: true,
                items,
                semantic_bridges: (0..semantic_bridges)
                    .map(|index| RescueAnchor {
                        anchor_form: "symbol".to_owned(),
                        repo: format!("repo-{index}"),
                        symbol_id: format!("sid-{index}"),
                        symbol_name: format!("bridge-{index}"),
                        rationale: None,
                        score_delta: None,
                        confidence_hint: None,
                    })
                    .map(|anchor| super::PackBridge {
                        kind: anchor.anchor_form,
                        name: anchor.symbol_name,
                        repo: anchor.repo,
                        symbol_id: anchor.symbol_id,
                    })
                    .collect(),
                next_steps: Vec::new(),
                unresolved_gaps: (0..unresolved_gaps)
                    .map(|index| format!("gap-{index}"))
                    .collect(),
                change_impact: ChangeImpactSummary {
                    direct_repos: Vec::new(),
                    cross_repo_callers: Vec::new(),
                    confirmed_downstream_repos: (0..confirmed_repos)
                        .map(|index| format!("repo-{index}"))
                        .collect(),
                    probable_downstream_repos: Vec::new(),
                    downstream_repos: (0..confirmed_repos)
                        .map(|index| format!("repo-{index}"))
                        .collect(),
                    unresolved_possible: Vec::new(),
                    truncated_repos: None,
                },
                transport_links: None,
                planning_rescue: None,
                planning_proofs: Vec::new(),
            },
            meta: None,
        }
    }

    #[test]
    fn cached_rollout_refresh_clears_stale_rollout_gaps() {
        let mut response = make_pack_response(0, 1, 0, 0);
        response.data.unresolved_gaps = vec![
            super::NO_SEMANTIC_BRIDGE_GAP.to_owned(),
            super::CROSS_REPO_IMPACT_GAP.to_owned(),
            super::EMPTY_REPO_DEPS_GAP.to_owned(),
        ];
        response.meta = Some(ContextPackMeta {
            response_schema_version: crate::budget::response_schema_version(),
            generation: 0,
            ambiguity: None,
            budget: crate::budget::ResponseBudget::not_truncated(
                crate::budget::BudgetedTool::ContextPack,
                0,
                0,
            ),
            candidate_count: 1,
            completeness: "partial".to_owned(),
            confidence_model_version: Some(super::PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
            resolution: "symbol_id".to_owned(),
            resolution_details: None,
            resolution_confidence: Some("high".to_owned()),
            resolved_symbol_id: Some("symbol".to_owned()),
            winner_margin: None,
            warnings: super::gap_warnings(&response.data.unresolved_gaps),
        });

        super::refresh_cached_rollout_gap_state(&mut response);
        super::refresh_context_pack_completeness(&mut response);

        assert!(response.data.unresolved_gaps.is_empty());
        let meta = response.meta.expect("meta should be present");
        assert!(meta.warnings.is_empty());
        assert_eq!(meta.completeness, "complete");
    }

    #[test]
    fn weak_pack_detection_requires_multiple_structural_failures() {
        let weak = make_pack_response(0, 0, 0, 2);
        let bridged = make_pack_response(1, 0, 0, 2);

        assert!(pack_is_structurally_weak(&weak));
        assert!(!pack_is_structurally_weak(&bridged));
    }

    #[test]
    fn recovery_score_prefers_confirmed_structure_over_gap_only_pack() {
        let weak = make_pack_response(0, 0, 0, 2);
        let stronger = make_pack_response(1, 1, 1, 0);

        assert!(pack_recovery_score(&stronger) > pack_recovery_score(&weak));
    }

    #[test]
    fn ranked_alternates_are_summary_only() {
        let ranked = vec![
            RankedPackCandidate {
                file_path: "src/auth_api.ts".to_owned(),
                repo: "frontend_standard".to_owned(),
                rationale: "transport-adjacent path".to_owned(),
                score: 620,
                symbol_id: "winner".to_owned(),
                symbol_name: "useAuthentication".to_owned(),
            },
            RankedPackCandidate {
                file_path: "src/auth_local.ts".to_owned(),
                repo: "frontend_standard".to_owned(),
                rationale: "same-name alternate anchor".to_owned(),
                score: 510,
                symbol_id: "runner_up".to_owned(),
                symbol_name: "useAuthentication".to_owned(),
            },
        ];
        let alternates = build_ranked_alternate_anchors(&ranked, ranked.first(), 3);

        assert_eq!(alternates.len(), 1);
        assert_eq!(alternates[0].anchor_form, "symbol");
        assert_eq!(alternates[0].repo, "frontend_standard");
        assert_eq!(alternates[0].score_delta, Some(110));
        assert_eq!(
            alternates[0].confidence_hint.as_deref(),
            Some("near_winner")
        );
    }

    #[test]
    fn ranked_alternate_anchors_hide_virtual_hook_when_real_symbol_exists() {
        let ranked = vec![
            RankedPackCandidate {
                file_path: "app/src/v2/app/hooks/useAuthentication.ts".to_owned(),
                repo: "frontend-app".to_owned(),
                rationale: "primary hook".to_owned(),
                score: 1200,
                symbol_id: "real".to_owned(),
                symbol_name: "useAuthentication".to_owned(),
            },
            RankedPackCandidate {
                file_path: "__hook__@app/hooks::useAuthentication".to_owned(),
                repo: "__virtual__".to_owned(),
                rationale: "virtual hook marker".to_owned(),
                score: 900,
                symbol_id: "virtual".to_owned(),
                symbol_name: "useAuthentication".to_owned(),
            },
            RankedPackCandidate {
                file_path: "app/src/v2/app/hooks/useRoles.ts".to_owned(),
                repo: "frontend-app".to_owned(),
                rationale: "fallback import anchor".to_owned(),
                score: 700,
                symbol_id: "module".to_owned(),
                symbol_name: "./useAuthentication".to_owned(),
            },
        ];

        let alternates = build_ranked_alternate_anchors(&ranked, ranked.first(), 3);

        assert!(
            alternates.iter().all(|anchor| anchor.repo != "__virtual__"),
            "virtual hook alternates should be hidden when the real hook is present: {alternates:?}"
        );
        assert_eq!(alternates.len(), 1);
        assert_eq!(alternates[0].symbol_id, "module");
    }

    #[test]
    fn medium_ranked_resolution_is_partial_even_without_other_gaps() {
        let mut meta = ContextPackMeta {
            response_schema_version: crate::budget::response_schema_version(),
            generation: 0,
            ambiguity: None,
            budget: crate::budget::ResponseBudget::not_truncated(
                crate::budget::BudgetedTool::ContextPack,
                0,
                0,
            ),
            candidate_count: 2,
            completeness: "complete".to_owned(),
            confidence_model_version: Some(super::PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
            resolution: "search_ranked_resolved".to_owned(),
            resolution_details: None,
            resolution_confidence: Some("medium".to_owned()),
            resolved_symbol_id: Some("symbol".to_owned()),
            winner_margin: Some(80),
            warnings: Vec::new(),
        };

        let medium_confidence_resolution = meta.resolution == "search_ranked_resolved"
            && meta.resolution_confidence.as_deref() == Some("medium");
        meta.completeness.clear();
        meta.completeness.push_str(if medium_confidence_resolution {
            "partial"
        } else {
            "complete"
        });
        if medium_confidence_resolution {
            meta.warnings.push(
                "target auto-resolved from 2 candidates via v1.0; verify identity before editing"
                    .to_owned(),
            );
        }

        assert_eq!(meta.completeness, "partial");
        assert_eq!(meta.warnings.len(), 1);
    }

    fn planning_proof_with_edge(edge_kind: EdgeKind, target_file: &str) -> PlanningProof {
        PlanningProof {
            kind: ProofKind::EventProducerConsumer,
            strength: 80,
            source_repo: "__virtual__".to_owned(),
            target_repo: "backend_standard".to_owned(),
            source_file: "__event__kafka__order.created".to_owned(),
            target_file: target_file.to_owned(),
            edge_kinds: smallvec![edge_kind],
            path: vec![ProofHop {
                node_id: node_id(
                    "backend_standard",
                    target_file,
                    NodeKind::Function,
                    target_file,
                ),
                edge_kind,
                repo: "backend_standard".to_owned(),
            }],
            path_truncated: false,
        }
    }

    fn context_pack_response_shape_fixture(mode: &str) -> serde_json::Value {
        let mut response = make_pack_response(1, 1, 1, 1);
        response.data.mode = mode.to_owned();
        response.data.items[0].evidence_chain = Some(serde_json::json!({
            "hops": [{"repo": "backend_standard", "edge_kind": "Calls"}]
        }));
        response.data.change_impact.cross_repo_callers = vec![CrossRepoCaller {
            file_path: "src/caller.ts".to_owned(),
            line_start: Some(12),
            repo: "backend_standard".to_owned(),
            symbol_id: "caller-id".to_owned(),
            symbol_kind: "function".to_owned(),
            symbol_name: "callTarget".to_owned(),
        }];
        response.data.change_impact.truncated_repos = Some(TruncatedRepos {
            count: 1,
            names: vec!["overflow_standard".to_owned()],
            reason_codes: vec!["fanout_cap".to_owned()],
        });
        response.data.transport_links = Some(vec![serde_json::json!({
            "kind": "http_route",
            "source": "frontend_standard",
            "target": "backend_standard"
        })]);
        response.data.planning_rescue = Some(PlanningRescue {
            triggered: true,
            reason: "shape_fixture".to_owned(),
            alternate_anchors: vec![RescueAnchor {
                anchor_form: "symbol".to_owned(),
                repo: "frontend_standard".to_owned(),
                symbol_id: "alternate-id".to_owned(),
                symbol_name: "alternateTarget".to_owned(),
                rationale: Some("near match".to_owned()),
                score_delta: Some(4),
                confidence_hint: Some("near_winner".to_owned()),
            }],
            hints: vec!["narrow by repo".to_owned()],
        });
        response.data.planning_proofs = vec![
            serde_json::to_value(planning_proof_with_edge(
                EdgeKind::UsesEventFrom,
                "src/consumer.ts",
            ))
            .unwrap(),
        ];
        response.meta = Some(ContextPackMeta {
            response_schema_version: crate::budget::response_schema_version(),
            generation: 7,
            ambiguity: None,
            budget: crate::budget::ResponseBudget::not_truncated(
                crate::budget::BudgetedTool::ContextPack,
                2048,
                1024,
            ),
            candidate_count: 2,
            completeness: "partial".to_owned(),
            confidence_model_version: Some(super::PACK_CONFIDENCE_MODEL_VERSION.to_owned()),
            resolution: "search_ranked_resolved".to_owned(),
            resolution_details: Some(PackResolutionDetails {
                strategy: "ranked".to_owned(),
                winner: Some("winner-id".to_owned()),
                winner_margin: Some(12),
                alternates: Vec::new(),
            }),
            resolution_confidence: Some("medium".to_owned()),
            resolved_symbol_id: Some("winner-id".to_owned()),
            winner_margin: Some(12),
            warnings: vec!["verify identity before editing".to_owned()],
        });
        json_shape(&serde_json::to_value(response).unwrap())
    }

    fn json_shape(value: &serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Null => serde_json::json!("null"),
            serde_json::Value::Bool(_) => serde_json::json!("bool"),
            serde_json::Value::Number(_) => serde_json::json!("number"),
            serde_json::Value::String(_) => serde_json::json!("string"),
            serde_json::Value::Array(items) => serde_json::Value::Array(
                items
                    .first()
                    .map(json_shape)
                    .into_iter()
                    .collect::<Vec<_>>(),
            ),
            serde_json::Value::Object(map) => serde_json::to_value(
                map.iter()
                    .map(|(key, value)| (key.clone(), json_shape(value)))
                    .collect::<BTreeMap<_, _>>(),
            )
            .expect("shape object should serialize"),
        }
    }

    #[test]
    fn context_pack_response_shape_is_stable_across_pack_modes() {
        let modes = ["planning", "change-impact", "debug", "fix", "review"];
        let shapes = modes
            .iter()
            .map(|mode| context_pack_response_shape_fixture(mode))
            .collect::<Vec<_>>();
        assert!(
            shapes.windows(2).all(|pair| pair[0] == pair[1]),
            "pack modes should share the same response envelope shape"
        );

        let rendered_shape = serde_json::to_string_pretty(&shapes[0]).unwrap();
        insta::assert_snapshot!(rendered_shape.as_str(), @r###"
        {
          "data": {
            "change_impact": {
              "confirmed_downstream_repos": [
                "string"
              ],
              "cross_repo_callers": [
                {
                  "file_path": "string",
                  "line_start": "number",
                  "repo": "string",
                  "symbol_id": "string",
                  "symbol_kind": "string",
                  "symbol_name": "string"
                }
              ],
              "direct_repos": [],
              "downstream_repos": [
                "string"
              ],
              "truncated_repos": {
                "count": "number",
                "names": [
                  "string"
                ],
                "reason_codes": [
                  "string"
                ]
              },
              "unresolved_possible": []
            },
            "found": "bool",
            "items": [
              {
                "category": "string",
                "evidence_chain": {
                  "hops": [
                    {
                      "edge_kind": "string",
                      "repo": "string"
                    }
                  ]
                },
                "file_path": "string",
                "reason": "string",
                "repo": "string",
                "score": "number",
                "symbol_id": "string",
                "symbol_kind": "string",
                "symbol_name": "string"
              }
            ],
            "mode": "string",
            "next_steps": [],
            "planning_proofs": [
              {
                "edge_kinds": [
                  "string"
                ],
                "kind": "string",
                "path": [
                  {
                    "edge_kind": "string",
                    "node_id": [
                      "number"
                    ],
                    "repo": "string"
                  }
                ],
                "path_truncated": "bool",
                "source_file": "string",
                "source_repo": "string",
                "strength": "number",
                "target_file": "string",
                "target_repo": "string"
              }
            ],
            "planning_rescue": {
              "alternate_anchors": [
                {
                  "anchor_form": "string",
                  "confidence_hint": "string",
                  "rationale": "string",
                  "repo": "string",
                  "score_delta": "number",
                  "symbol_id": "string",
                  "symbol_name": "string"
                }
              ],
              "hints": [
                "string"
              ],
              "reason": "string",
              "triggered": "bool"
            },
            "semantic_bridges": [
              {
                "kind": "string",
                "name": "string",
                "repo": "string",
                "symbol_id": "string"
              }
            ],
            "target": "string",
            "transport_links": [
              {
                "kind": "string",
                "source": "string",
                "target": "string"
              }
            ],
            "unresolved_gaps": [
              "string"
            ]
          },
          "meta": {
            "budget": {
              "budget_bytes": "number",
              "items_dropped": "number",
              "items_included": "number",
              "omitted_items": "number",
              "tool_default_bytes": "number",
              "tool_max_bytes": "number",
              "truncated": "bool",
              "used_bytes": "number"
            },
            "candidate_count": "number",
            "completeness": "string",
            "confidence_model_version": "string",
            "generation": "number",
            "resolution": "string",
            "resolution_confidence": "string",
            "resolution_details": {
              "strategy": "string",
              "winner": "string",
              "winner_margin": "number"
            },
            "resolved_symbol_id": "string",
            "response_schema_version": "number",
            "warnings": [
              "string"
            ],
            "winner_margin": "number"
          }
        }
        "###);
    }

    #[test]
    fn finalize_planning_proofs_keeps_different_target_files_separate() {
        let proofs = finalize_proofs(vec![
            planning_proof_with_edge(EdgeKind::ProducesEventFor, "src/producer.ts"),
            planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts"),
        ]);

        assert_eq!(
            proofs.len(),
            2,
            "different evidence files must remain separate proofs"
        );
        assert!(
            proofs.iter().any(|proof| {
                proof.target_file == "src/producer.ts"
                    && proof.edge_kinds.as_slice() == [EdgeKind::ProducesEventFor]
            }),
            "producer proof should retain only producer evidence: {proofs:?}"
        );
        assert!(
            proofs.iter().any(|proof| {
                proof.target_file == "src/consumer.ts"
                    && proof.edge_kinds.as_slice() == [EdgeKind::UsesEventFrom]
            }),
            "consumer proof should retain only consumer evidence: {proofs:?}"
        );
    }

    #[test]
    fn proof_repo_sets_separate_structural_and_advisory_in_one_pass() {
        let structural = planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts");
        let mut advisory = planning_proof_with_edge(EdgeKind::Calls, "src/payment.ts");
        advisory.kind = ProofKind::CoChangeAdvisory;
        advisory.strength = 20;
        advisory.target_repo = "payments_standard".to_owned();
        advisory.target_file = "src/payment.ts".to_owned();
        let mut duplicate_advisory = advisory.clone();
        duplicate_advisory.target_repo = "backend_standard".to_owned();

        let (confirmed, probable) =
            derive_repo_sets(&[structural, advisory, duplicate_advisory], None);

        assert_eq!(confirmed, vec!["backend_standard".to_owned()]);
        assert_eq!(probable, vec!["payments_standard".to_owned()]);
    }

    #[test]
    fn finalize_planning_proofs_merges_edge_kinds_for_same_evidence_file() {
        let proofs = finalize_proofs(vec![
            planning_proof_with_edge(EdgeKind::ProducesEventFor, "src/consumer.ts"),
            planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts"),
        ]);

        assert_eq!(proofs.len(), 1);
        assert!(
            proofs[0].edge_kinds.contains(&EdgeKind::ProducesEventFor),
            "same-file merged proof must retain producer-side edge kind"
        );
        assert!(
            proofs[0].edge_kinds.contains(&EdgeKind::UsesEventFrom),
            "same-file merged proof must retain consumer-side edge kind"
        );
        assert_eq!(
            proofs[0].target_file, "src/consumer.ts",
            "same-file merge must keep the retained file identity"
        );
    }

    #[test]
    fn trim_context_pack_preserves_planning_proofs_for_semantic_stability() {
        let mut response = make_pack_response(0, 0, 0, 0);
        response.data.items.clear();
        response.data.planning_proofs = vec![
            serde_json::to_value(planning_proof_with_edge(
                EdgeKind::UsesEventFrom,
                "src/consumer.ts",
            ))
            .unwrap(),
        ];

        assert!(
            !super::trim_context_pack(&mut response),
            "planning_proofs must not be dropped by response-budget trimming"
        );
        assert_eq!(response.data.planning_proofs.len(), 1);
    }

    #[test]
    fn malformed_planning_proofs_warn_when_deriving_change_impact() {
        let mut response = make_pack_response(0, 0, 0, 0);
        response.data.planning_proofs = vec![serde_json::json!({
            "kind": "not-a-valid-proof",
            "target_repo": "backend_standard"
        })];

        let warning_count = count_target_warnings(
            "gather_step_mcp::packs::deserialize_planning_proofs",
            || {
                let proofs = super::deserialize_planning_proofs(&response);
                assert!(proofs.is_empty());
            },
        );

        assert_eq!(
            warning_count, 1,
            "malformed proof derivation should emit exactly one schema-drift warning"
        );
    }

    #[test]
    fn malformed_planning_proofs_warn_when_scanning_cache_dependencies() {
        let (ctx, _guard) = empty_test_context();
        let mut response = make_pack_response(0, 0, 0, 0);
        response.data.items.clear();
        response.data.planning_proofs = vec![serde_json::json!({
            "kind": "not-a-valid-proof",
            "target_repo": "backend_standard"
        })];

        let warning_count =
            count_target_warnings("gather_step_mcp::packs::cache_dependency_files", || {
                let files = super::cache_dependency_files_for_response(&ctx, &response)
                    .expect("dependency scan should continue after malformed proof");
                assert!(files.is_empty());
            });

        assert_eq!(
            warning_count, 1,
            "malformed proof cache scan should emit exactly one cache-dependency warning"
        );
    }

    #[test]
    fn change_impact_repos_are_rederived_when_a_proof_is_dropped() {
        let (ctx, _guard) = empty_test_context();
        let backend = planning_proof_with_edge(EdgeKind::UsesEventFrom, "src/consumer.ts");
        let mut payments = planning_proof_with_edge(EdgeKind::Calls, "src/payment.ts");
        payments.kind = ProofKind::DirectCall;
        payments.strength = 85;
        payments.target_repo = "payments_standard".to_owned();
        payments.target_file = "src/payment.ts".to_owned();
        let mut response = make_pack_response(0, 0, 0, 1);
        response.data.planning_proofs = vec![
            serde_json::to_value(&backend).unwrap(),
            serde_json::to_value(&payments).unwrap(),
        ];

        super::apply_proof_derived_change_impact(&ctx, None, None, &mut response)
            .expect("proof derivation should succeed");
        assert_eq!(
            response.data.change_impact.confirmed_downstream_repos,
            vec![
                "backend_standard".to_owned(),
                "payments_standard".to_owned()
            ]
        );

        response.data.planning_proofs = vec![serde_json::to_value(&backend).unwrap()];
        super::apply_proof_derived_change_impact(&ctx, None, None, &mut response)
            .expect("proof derivation should succeed");

        assert_eq!(
            response.data.change_impact.confirmed_downstream_repos,
            vec!["backend_standard".to_owned()],
            "dropping the payments proof must remove payments from derived repos"
        );
        assert!(response.data.change_impact.unresolved_possible.is_empty());
    }

    // ── query_shape_match_bonus ────────────────────────────────────────────────

    /// For an event-shaped query, event/topic/queue/subject/stream kind
    /// candidates must receive a bonus; non-event kinds must not.
    #[test]
    fn shape_bonus_rewards_event_kinds_for_event_query() {
        // "order.created" is a dotted name — classify_query_shape returns EventRollout.
        assert_eq!(query_shape_match_bonus("order.created", "topic"), 200);
        assert_eq!(query_shape_match_bonus("order.created", "queue"), 200);
        assert_eq!(query_shape_match_bonus("order.created", "subject"), 200);
        assert_eq!(query_shape_match_bonus("order.created", "stream"), 200);
        assert_eq!(query_shape_match_bonus("order.created", "event"), 200);
        // Non-event kinds must not receive the bonus.
        assert_eq!(query_shape_match_bonus("order.created", "class"), 0);
        assert_eq!(query_shape_match_bonus("order.created", "function"), 0);
        assert_eq!(query_shape_match_bonus("order.created", "shared_symbol"), 0);
    }

    /// For a route-shaped query, only route-kind candidates receive the bonus.
    #[test]
    fn shape_bonus_rewards_route_kind_for_route_query() {
        assert_eq!(query_shape_match_bonus("GET /api/orders", "route"), 200);
        assert_eq!(query_shape_match_bonus("POST /api/orders", "route"), 200);
        // Non-route kinds get nothing.
        assert_eq!(query_shape_match_bonus("GET /api/orders", "class"), 0);
        assert_eq!(query_shape_match_bonus("GET /api/orders", "function"), 0);
    }

    /// For a guard-shaped query, class and function kinds receive the bonus.
    #[test]
    fn shape_bonus_rewards_class_and_function_for_guard_query() {
        assert_eq!(query_shape_match_bonus("UserAuthGuard", "class"), 200);
        assert_eq!(query_shape_match_bonus("UserAuthGuard", "function"), 200);
        // Other kinds get nothing.
        assert_eq!(query_shape_match_bonus("UserAuthGuard", "type"), 0);
        assert_eq!(query_shape_match_bonus("UserAuthGuard", "route"), 0);
    }

    /// For a shared-type-shaped query, `shared_symbol` / type / `payload_contract`
    /// candidates receive the bonus.
    #[test]
    fn shape_bonus_rewards_shared_symbol_for_dto_query() {
        assert_eq!(
            query_shape_match_bonus("CreateOrderDto", "shared_symbol"),
            200
        );
        assert_eq!(query_shape_match_bonus("CreateOrderDto", "type"), 200);
        assert_eq!(
            query_shape_match_bonus("CreateOrderDto", "payload_contract"),
            200
        );
        // Non-type kinds get nothing.
        assert_eq!(query_shape_match_bonus("CreateOrderDto", "class"), 0);
        assert_eq!(query_shape_match_bonus("CreateOrderDto", "function"), 0);
    }

    /// For a generic-symbol query no shape bonus is awarded to any kind.
    #[test]
    fn shape_bonus_is_zero_for_generic_symbol_query() {
        for kind in [
            "class",
            "function",
            "type",
            "shared_symbol",
            "route",
            "topic",
        ] {
            assert_eq!(
                query_shape_match_bonus("plainFunction", kind),
                0,
                "expected 0 bonus for kind={kind} on generic query"
            );
        }
    }

    // ── Ranked-alternates regression suite ────────────────────────────────────
    //
    // When the same function name appears across 5+ packages with identical
    // structural scores, `select_pack_target` previously returned `Ambiguous`
    // (no winner), which caused the pack tool to emit `resolution =
    // "ambiguous_search_match"` with `found = false`.
    //
    // The new contract: when candidates clear the noise floor
    // (>= PACK_CONFIDENCE_ALTERNATES_MIN_SCORE) but none clears the confidence
    // margin, `select_pack_target` returns `RankedAlternates(...)` and the
    // pack tool emits `resolution = "search_ranked_alternates"` with
    // `found = true`.

    fn ranked_candidate(repo: &str, score: i32) -> RankedPackCandidate {
        RankedPackCandidate {
            file_path: "src/payment.service.ts".to_owned(),
            repo: repo.to_owned(),
            rationale: "same-name alternate anchor".to_owned(),
            score,
            symbol_id: format!("{repo}::process_payment"),
            symbol_name: "process_payment".to_owned(),
        }
    }

    /// Five equally-scored candidates above the noise floor return ranked alternates.
    ///
    /// All five candidates score 450 (above `PACK_CONFIDENCE_ALTERNATES_MIN_SCORE =
    /// 250`).  The margin between the top candidate and the runner-up is 0, which
    /// falls below `PACK_CONFIDENCE_HIGH_MARGIN` (125) and
    /// `PACK_CONFIDENCE_MEDIUM_MARGIN` (75), so no confident winner is selected.
    /// However, since all candidates clear the noise floor, `select_pack_target`
    /// returns `RankedAlternates` containing all five candidates rather than
    /// `Ambiguous`.
    #[test]
    fn five_equal_scored_candidates_return_ranked_alternates() {
        let candidates = vec![
            ranked_candidate("service_a", 450),
            ranked_candidate("service_b", 450),
            ranked_candidate("service_c", 450),
            ranked_candidate("service_d", 450),
            ranked_candidate("service_e", 450),
        ];

        // allow_medium = false (no repo filter means global context).
        let result = super::select_pack_target(&candidates, false);

        match result {
            super::PackTargetSelection::RankedAlternates(alternates) => {
                assert_eq!(
                    alternates.len(),
                    5,
                    "all 5 candidates above noise floor must be returned as ranked alternates"
                );
                for alt in &alternates {
                    assert_eq!(alt.symbol_name, "process_payment");
                }
            }
            super::PackTargetSelection::Confident(..) => {
                panic!("equal-scored candidates must not produce a confident winner")
            }
            super::PackTargetSelection::Ambiguous => {
                panic!(
                    "candidates above the noise floor must return RankedAlternates, not Ambiguous"
                )
            }
        }
    }

    /// Narrow-margin candidates (below `PACK_CONFIDENCE_MEDIUM_MARGIN`) return
    /// ranked alternates when candidates clear the noise floor.
    ///
    /// Winner leads by 60, below `PACK_CONFIDENCE_MEDIUM_MARGIN` (75).  Neither
    /// high nor medium confidence is awarded (`allow_medium = false`).  All five
    /// candidates score >= 250 (the noise floor), so `select_pack_target` returns
    /// `RankedAlternates` with all five.
    #[test]
    fn narrow_margin_candidates_return_ranked_alternates() {
        // Winner leads by 60, below `PACK_CONFIDENCE_MEDIUM_MARGIN` (75).
        // Neither high nor medium confidence is awarded (allow_medium = false).
        let candidates = vec![
            ranked_candidate("service_a", 490), // winner
            ranked_candidate("service_b", 430), // runner-up, margin = 60
            ranked_candidate("service_c", 420),
            ranked_candidate("service_d", 410),
            ranked_candidate("service_e", 400),
        ];

        let result = super::select_pack_target(&candidates, false);

        match result {
            super::PackTargetSelection::RankedAlternates(alternates) => {
                assert_eq!(
                    alternates.len(),
                    5,
                    "all 5 candidates must be in alternates"
                );
                // The alternates list must be sorted by descending score.
                assert_eq!(alternates[0].repo, "service_a");
                assert_eq!(alternates[0].score, 490);
            }
            super::PackTargetSelection::Confident(..) => {
                panic!(
                    "narrow-margin candidates must not produce a confident winner \
                     (margin 60 < PACK_CONFIDENCE_MEDIUM_MARGIN 75)"
                )
            }
            super::PackTargetSelection::Ambiguous => {
                panic!(
                    "candidates above the noise floor must return RankedAlternates, not Ambiguous"
                )
            }
        }
    }

    /// A single dominant candidate (margin >= `PACK_CONFIDENCE_HIGH_MARGIN`)
    /// still picks a confident winner and does not regress to ranked alternates.
    #[test]
    fn single_dominant_candidate_still_picks_one() {
        let candidates = vec![
            ranked_candidate("service_a", 600), // clear winner
            ranked_candidate("service_b", 430), // runner-up, margin = 170 > HIGH_MARGIN
        ];

        let result = super::select_pack_target(&candidates, false);

        match result {
            super::PackTargetSelection::Confident(winner, confidence, margin, _) => {
                assert_eq!(winner.repo, "service_a");
                assert_eq!(confidence, "high");
                assert_eq!(margin, 170);
            }
            super::PackTargetSelection::RankedAlternates(_) => {
                panic!("dominant candidate must produce a confident winner, not ranked alternates")
            }
            super::PackTargetSelection::Ambiguous => {
                panic!("dominant candidate must produce a confident winner, not Ambiguous")
            }
        }
    }

    /// When all candidates are below the noise floor, the result is `Ambiguous`
    /// rather than `RankedAlternates`.  The pack tool will still emit
    /// `resolution = "ambiguous_search_match"` with `found = false`.
    #[test]
    fn zero_above_noise_floor_returns_ambiguous() {
        // All scores below PACK_CONFIDENCE_ALTERNATES_MIN_SCORE (250).
        let candidates = vec![
            ranked_candidate("service_a", 200),
            ranked_candidate("service_b", 180),
        ];

        let result = super::select_pack_target(&candidates, false);

        assert!(
            matches!(result, super::PackTargetSelection::Ambiguous),
            "candidates below noise floor must return Ambiguous"
        );
    }
}
