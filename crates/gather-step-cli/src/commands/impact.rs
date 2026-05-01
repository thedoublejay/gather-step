use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result, bail};
use clap::Args;
use gather_step_analysis::{
    CandidateKey, EvidenceBand, ProjectionEvidence, ProjectionEvidenceVerbosity, ProjectionField,
    ProjectionImpactReport, ProjectionImpactRequest, QueryShape, anchor::rank_anchors,
    classify_query_shape, projection_impact_with_payload_contracts, resolve_event_targets,
    resolve_route_target, shared_contract::shared_contract_candidate_ids, shared_contract_impact,
    trace_across_repos,
};
use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_storage::{
    GraphStore, MetadataStore, PayloadContractQuery, SearchStore, StorageCoordinator,
};
use serde::Serialize;
use serde_json::json;

use crate::command_render::RenderedCommand;
use crate::daemon_protocol::DaemonRequest;
use crate::storage_context::StorageContext;
use crate::{app::AppContext, daemon_proxy};

#[derive(Debug, Args)]
pub struct ImpactArgs {
    #[arg(help = "Symbol name to inspect")]
    pub symbol: String,
    #[arg(
        long,
        default_value_t = 20,
        help = "Maximum number of search candidates to inspect"
    )]
    pub limit: usize,
}

#[derive(Debug, Serialize)]
struct ImpactOutput {
    event: &'static str,
    symbol: String,
    matches: Vec<ImpactMatchOutput>,
}

#[derive(Debug, Serialize)]
struct ImpactMatchOutput {
    source_repo: String,
    source_file: String,
    source_symbol: String,
    strategy: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    primary: bool,
    /// Repos / files reached via structural edges (declared code relationships).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    impacted_files: Vec<ImpactedRepoOutput>,
    /// Repos / files reached only via weak co-change signals (`CoChangesWith`).
    /// Rendered separately so consumers can distinguish proven structural
    /// consumers from probabilistic co-edit hints.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    advisory_co_change_files: Vec<ImpactedRepoOutput>,
    virtual_targets: Vec<VirtualImpactOutput>,
}

#[derive(Debug, Serialize)]
struct VirtualImpactOutput {
    target_name: String,
    target_kind: String,
    repos: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ImpactedRepoOutput {
    files: Vec<ImpactedFileOutput>,
    repo: String,
}

#[derive(Debug, Serialize)]
struct ImpactedFileOutput {
    edge_kinds: Vec<String>,
    file_path: String,
    producer_or_consumer: Option<String>,
    serialization_point: bool,
    validation_point: bool,
    weight: f32,
}

pub fn run(app: &AppContext, args: ImpactArgs) -> Result<()> {
    daemon_proxy::run_read_only_command(
        app,
        &DaemonRequest::Impact {
            symbol: args.symbol.clone(),
            limit: args.limit,
            repo_filter: app.repo_filter.clone(),
        },
        move |app| run_rendered(app, &StorageContext::workspace_read_only(app), args),
    )
}

pub(crate) fn run_rendered(
    app: &AppContext,
    ctx: &StorageContext,
    args: ImpactArgs,
) -> Result<RenderedCommand> {
    let storage = ctx.open_storage_coordinator()?;
    execute(&storage, app.repo_filter.as_deref(), args)
}

pub fn execute(
    storage: &StorageCoordinator,
    repo_filter: Option<&str>,
    args: ImpactArgs,
) -> Result<RenderedCommand> {
    if args.symbol.contains('.') {
        let payload_contracts = storage
            .metadata()
            .payload_contracts_for_query(PayloadContractQuery {
                repo: repo_filter.map(ToOwned::to_owned),
                min_confidence: Some(750),
                ..PayloadContractQuery::default()
            })?
            .into_iter()
            .map(|record| record.record)
            .collect::<Vec<_>>();
        let field_report = projection_impact_with_payload_contracts(
            storage.graph(),
            ProjectionImpactRequest {
                target: args.symbol.clone(),
                repo: repo_filter.map(ToOwned::to_owned),
                max_results: args.limit,
                evidence_verbosity: ProjectionEvidenceVerbosity::Full,
            },
            &payload_contracts,
        )?;
        if field_report.resolved
            && field_report.candidates.iter().any(|candidate| {
                candidate
                    .field_path
                    .eq_ignore_ascii_case(args.symbol.trim())
            })
        {
            let lines = render_field_impact_lines(&field_report);
            return RenderedCommand::success_serialized(&field_report, lines);
        }
    }

    let hits = storage
        .search()
        .search(&args.symbol, args.limit.max(1))
        .with_context(|| format!("searching for `{}`", args.symbol))?;

    let strict_hits = hits
        .iter()
        .filter(|hit| is_strict_impact_match(hit, &args.symbol))
        .cloned()
        .collect::<Vec<_>>();
    let candidate_hits = if strict_hits.is_empty() {
        hits
    } else {
        strict_hits
    };

    // Classify the query shape before building candidates so the comparator
    // can apply the query-shape bonus consistently.
    let shape = classify_query_shape(&[], &args.symbol);

    let mut matches = candidate_hits
        .into_iter()
        .map(|hit| {
            let Some(node) = storage.graph().get_node(hit.node_id)? else {
                return Ok(None);
            };
            // `repo` is not stored in Tantivy (S6); rehydrate from node and
            // apply the caller-supplied repo filter after the graph lookup.
            if repo_filter.is_some_and(|r| node.repo.as_str() != r) {
                return Ok(None);
            }
            let shared_contract_result = shared_contract_match(storage.graph(), &node, shape)?;
            let virtual_targets = if shared_contract_result.is_some() {
                Vec::new()
            } else {
                virtual_target_match(storage.graph(), node.id)?
            };
            let Some((strategy, impacted_files, advisory_co_change_files)) = shared_contract_result
                .map(|(structural, advisory)| ("shared_contract".to_owned(), structural, advisory))
                .or_else(|| {
                    (!virtual_targets.is_empty()).then_some((
                        "virtual_targets".to_owned(),
                        Vec::new(),
                        Vec::new(),
                    ))
                })
            else {
                return Ok(None);
            };
            let is_canonical_boundary = is_canonical_boundary(&node);

            Ok(Some(ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: node.repo,
                    source_file: node.file_path,
                    source_symbol: node.name,
                    strategy,
                    primary: false,
                    impacted_files,
                    advisory_co_change_files,
                    virtual_targets,
                },
                is_canonical_boundary,
                node_id: node.id,
                node_kind: node.kind,
                search_score: hit.adjusted_score,
                exact_match: hit.exact_match,
            }))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    // ── Event-anchor fallback ────────────────────────────────────────────────
    // When the symbol search found nothing, attempt to resolve via event/topic
    // name — the same path `pack` uses.  This ensures `impact` and `pack` agree
    // on the primary target for event-shaped queries.
    if matches.is_empty()
        && let Ok(event_nodes) = resolve_event_targets(storage.graph(), &args.symbol)
    {
        let event_nodes: Vec<_> = match repo_filter {
            Some(r) => event_nodes
                .into_iter()
                .filter(|n| n.repo.as_str() == r)
                .collect(),
            None => event_nodes,
        };
        for node in event_nodes {
            let shared_contract_result = shared_contract_match(storage.graph(), &node, shape)?;
            let virtual_targets = if shared_contract_result.is_some() {
                Vec::new()
            } else {
                virtual_target_match(storage.graph(), node.id)?
            };
            let Some((strategy, impacted_files, advisory_co_change_files)) = shared_contract_result
                .map(|(structural, advisory)| ("shared_contract".to_owned(), structural, advisory))
                .or_else(|| {
                    (!virtual_targets.is_empty()).then_some((
                        "virtual_targets".to_owned(),
                        Vec::new(),
                        Vec::new(),
                    ))
                })
            else {
                continue;
            };
            let is_canonical_boundary = is_canonical_boundary(&node);
            matches.push(ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: node.repo,
                    source_file: node.file_path,
                    source_symbol: node.name,
                    strategy,
                    primary: false,
                    impacted_files,
                    advisory_co_change_files,
                    virtual_targets,
                },
                is_canonical_boundary,
                node_id: node.id,
                node_kind: node.kind,
                search_score: 0.0,
                exact_match: true,
            });
        }
    }

    // ── Route-anchor fallback ────────────────────────────────────────────────
    // Mirrors the `pack` route-anchor path for `METHOD /path` queries.
    if matches.is_empty()
        && let Some((method, path)) = parse_route_target(&args.symbol)
        && let Ok(Some(route_node)) = resolve_route_target(storage.graph(), &method, &path)
        && repo_filter.is_none_or(|r| route_node.repo.as_str() == r)
    {
        let shared_contract_result = shared_contract_match(storage.graph(), &route_node, shape)?;
        let virtual_targets = if shared_contract_result.is_some() {
            Vec::new()
        } else {
            virtual_target_match(storage.graph(), route_node.id)?
        };
        let resolved = shared_contract_result
            .map(|(structural, advisory)| ("shared_contract".to_owned(), structural, advisory))
            .or_else(|| {
                (!virtual_targets.is_empty()).then_some((
                    "virtual_targets".to_owned(),
                    Vec::new(),
                    Vec::new(),
                ))
            });
        if let Some((strategy, impacted_files, advisory_co_change_files)) = resolved {
            let is_canonical_boundary = is_canonical_boundary(&route_node);
            matches.push(ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: route_node.repo,
                    source_file: route_node.file_path,
                    source_symbol: route_node.name,
                    strategy,
                    primary: false,
                    impacted_files,
                    advisory_co_change_files,
                    virtual_targets,
                },
                is_canonical_boundary,
                node_id: route_node.id,
                node_kind: route_node.kind,
                search_score: 0.0,
                exact_match: true,
            });
        }
    }

    rerank_impact_candidates(storage.graph(), &mut matches, shape);

    if matches.is_empty() {
        bail!(
            "no cross-repo impact targets found for symbol `{}`",
            args.symbol
        );
    }

    let payload = ImpactOutput {
        event: "impact_completed",
        symbol: args.symbol,
        matches: matches
            .into_iter()
            .map(|candidate| candidate.output)
            .collect(),
    };

    let mut lines = vec![format!("Impact for symbol {}:", payload.symbol)];
    for item in &payload.matches {
        lines.push(format!(
            "  {} {}:{} [{}{}]",
            item.source_symbol,
            item.source_repo,
            item.source_file,
            item.strategy,
            if item.primary { ", primary" } else { "" }
        ));
        if item.strategy == "shared_contract" {
            if !item.impacted_files.is_empty() {
                lines.push("    Structural consumers:".to_owned());
                for repo in &item.impacted_files {
                    let files = repo
                        .files
                        .iter()
                        .map(|file| file.file_path.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    lines.push(format!("      -> {} touched by {}", repo.repo, files));
                }
            }
            if !item.advisory_co_change_files.is_empty() {
                lines.push("    Co-change advisory (secondary, not proven structural):".to_owned());
                for repo in &item.advisory_co_change_files {
                    let files = repo
                        .files
                        .iter()
                        .map(|file| file.file_path.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    lines.push(format!("      ~> {} co-changed with {}", repo.repo, files));
                }
            }
        } else {
            for target in &item.virtual_targets {
                lines.push(format!(
                    "    -> {} [{}] touched by {}",
                    target.target_name,
                    target.target_kind,
                    if target.repos.is_empty() {
                        "-".to_owned()
                    } else {
                        target.repos.join(", ")
                    }
                ));
            }
        }
    }

    Ok(RenderedCommand::success(json!(payload), lines))
}

fn render_field_impact_lines(report: &ProjectionImpactReport) -> Vec<String> {
    let mut lines = vec![format!(
        "field impact for `{}`: {} {}, confidence {}",
        report.target,
        report.candidates.len(),
        pluralize(report.candidates.len(), "candidate", "candidates"),
        report.confidence
    )];
    if let Some(ambiguity) = &report.ambiguity {
        lines.push(format!("ambiguity: {ambiguity}"));
    }
    if report.ambiguity.is_some() && !report.candidates.is_empty() {
        lines.push(format!(
            "candidate fields: {}",
            format_projection_fields(&report.candidates)
        ));
    }
    if !report.readers.is_empty() {
        lines.push(format!(
            "readers: {}",
            format_projection_evidence(&report.readers)
        ));
    }
    if !report.writers.is_empty() {
        lines.push(format!(
            "writers: {}",
            format_projection_evidence(&report.writers)
        ));
    }
    if !report.filters.is_empty() {
        lines.push(format!(
            "filters: {}",
            format_projection_evidence(&report.filters)
        ));
    }
    if !report.indexes.is_empty() {
        lines.push(format!(
            "indexes: {}",
            format_projection_evidence(&report.indexes)
        ));
    }
    if !report.backfills.is_empty() {
        lines.push(format!(
            "backfills: {}",
            format_projection_evidence(&report.backfills)
        ));
    }
    if !report.missing_evidence.is_empty() {
        lines.push(format!(
            "missing evidence: {}",
            report.missing_evidence.join(", ")
        ));
    }
    if !report.risk_hints.is_empty() {
        lines.push(format!("next checks: {}", report.risk_hints.join(", ")));
    }
    lines
}

fn format_projection_fields(fields: &[ProjectionField]) -> String {
    fields
        .iter()
        .map(|field| format!("{}:{}", field.repo, field.field_path))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_projection_evidence(evidence: &[ProjectionEvidence]) -> String {
    evidence
        .iter()
        .map(|item| {
            let source = item
                .evidence_source
                .as_deref()
                .map_or(String::new(), |source| format!(", {source}"));
            format!(
                "{}:{} ({}{source})",
                item.repo, item.file_path, item.field_path
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn pluralize<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

/// Parse a `METHOD /path` string into `(method, path)`.
fn parse_route_target(target: &str) -> Option<(String, String)> {
    let mut parts = target.splitn(2, ' ');
    let method = parts.next()?.trim().to_ascii_uppercase();
    let path = parts.next()?.trim().to_owned();
    if path.is_empty() {
        return None;
    }
    match method.as_str() {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS" => Some((method, path)),
        _ => None,
    }
}

#[derive(Debug)]
struct ImpactCandidate {
    output: ImpactMatchOutput,
    is_canonical_boundary: bool,
    node_id: NodeId,
    node_kind: NodeKind,
    search_score: f32,
    exact_match: bool,
}

fn rerank_impact_candidates(
    graph: &impl GraphStore,
    matches: &mut [ImpactCandidate],
    shape: QueryShape,
) {
    if matches.is_empty() {
        return;
    }

    let anchor_scores = rank_anchors(
        graph,
        &matches
            .iter()
            .map(|candidate| candidate.node_id)
            .collect::<Vec<_>>(),
    )
    .map(|ranked| {
        ranked
            .into_iter()
            .map(|item| (item.node, item.score))
            .collect::<BTreeMap<_, _>>()
    })
    .unwrap_or_default();

    // Pre-compute cross-repo structural consumer evidence for every candidate.
    // A node has evidence when any outgoing or incoming structural edge (anything
    // other than `CoChangesWith`) connects it to a node that lives in a different
    // repo.  This is computed once here so the hot sort comparator stays cheap.
    let consumer_evidence: BTreeMap<NodeId, bool> = matches
        .iter()
        .map(|candidate| {
            let has_evidence = has_cross_repo_structural_edge(graph, candidate).unwrap_or(false);
            (candidate.node_id, has_evidence)
        })
        .collect();

    // Build a sort key per candidate. The comparator is:
    //   1. CandidateKey (primary rank — descending via for_descending_sort encoding)
    //   2. repo + file + symbol (alphabetical tiebreaker — matches old behaviour)
    //   3. node ID bytes (final deterministic tiebreaker across rayon scheduling)
    let mut keyed: Vec<(CandidateKey, [u8; 16], usize)> = matches
        .iter()
        .enumerate()
        .map(|(idx, candidate)| {
            let key = impact_candidate_key(candidate, &anchor_scores, &consumer_evidence, shape);
            let id_bytes = candidate.node_id.as_bytes();
            (key, id_bytes, idx)
        })
        .collect();

    keyed.sort_by(|(ka, ia, ia_idx), (kb, ib, ib_idx)| {
        let ma = &matches[*ia_idx];
        let mb = &matches[*ib_idx];
        ka.cmp(kb)
            .then(ma.output.source_repo.cmp(&mb.output.source_repo))
            .then(ma.output.source_file.cmp(&mb.output.source_file))
            .then(ma.output.source_symbol.cmp(&mb.output.source_symbol))
            .then(ia.cmp(ib))
    });

    // Re-order `matches` in-place to match the sorted order.
    // `result` holds the original indices of `matches` in sorted order.
    let result: Vec<usize> = keyed.into_iter().map(|(_, _, idx)| idx).collect();

    // Apply ordering: build a parallel "already moved" tracker.
    let len = matches.len();
    let mut position = vec![0usize; len];
    for (new_pos, &old_idx) in result.iter().enumerate() {
        position[old_idx] = new_pos;
    }
    // In-place permutation using the position map.
    for i in 0..len {
        while position[i] != i {
            let j = position[i];
            matches.swap(i, j);
            position.swap(i, j);
        }
    }
    // Suppress unused warning on result (it's only needed for the indices above).
    drop(result);

    for (index, candidate) in matches.iter_mut().enumerate() {
        candidate.output.primary = index == 0;
    }
}

/// Returns `true` when the candidate node has at least one structural edge
/// (any `EdgeKind` except `CoChangesWith`) that connects it to a node in a
/// *different* repo.  Errors are treated as absence of evidence so a graph
/// lookup failure does not crash the sort.
fn has_cross_repo_structural_edge(
    graph: &impl GraphStore,
    candidate: &ImpactCandidate,
) -> Result<bool> {
    let source_repo = &candidate.output.source_repo;
    let node_id = candidate.node_id;

    let outgoing = graph.get_outgoing(node_id)?;
    let incoming = graph.get_incoming(node_id)?;

    for edge in outgoing.iter().chain(incoming.iter()) {
        // Skip weak co-change signals — only structural edges count.
        if matches!(edge.kind, EdgeKind::CoChangesWith) {
            continue;
        }
        // Determine which end of the edge is the neighbour.
        let neighbour_id = if edge.source == node_id {
            edge.target
        } else {
            edge.source
        };
        if let Some(neighbour) = graph.get_node(neighbour_id)?
            && neighbour.repo != *source_repo
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Build a [`CandidateKey`] for a candidate using the 8-field comparator.
fn impact_candidate_key(
    candidate: &ImpactCandidate,
    anchor_scores: &BTreeMap<NodeId, f32>,
    consumer_evidence: &BTreeMap<NodeId, bool>,
    shape: QueryShape,
) -> CandidateKey {
    let has_consumer_repo_evidence = consumer_evidence
        .get(&candidate.node_id)
        .copied()
        .unwrap_or(false);

    // Query-shape match: does the candidate's node kind align with the classified shape?
    let query_shape_match = node_kind_matches_shape(candidate.node_kind, shape);

    let raw_structural_repo_span = if candidate.output.strategy == "shared_contract" {
        candidate.output.impacted_files.len()
    } else {
        candidate
            .output
            .virtual_targets
            .iter()
            .flat_map(|t| t.repos.iter().cloned())
            .collect::<BTreeSet<_>>()
            .len()
    };

    // Decay `structural_repo_span` for candidates without cross-repo structural evidence.
    let structural_repo_span = if has_consumer_repo_evidence {
        raw_structural_repo_span
    } else {
        raw_structural_repo_span / 2
    };

    let advisory_span: usize = candidate
        .output
        .advisory_co_change_files
        .iter()
        .map(|r| r.files.len())
        .sum();

    // Lexical score: anchor_score (scaled) + canonical_source_bonus + search_score.
    let anchor_f = anchor_scores
        .get(&candidate.node_id)
        .copied()
        .unwrap_or(0.0)
        + canonical_source_bonus(&candidate.output.source_repo, &candidate.output.source_file);
    // Scale to integer for CandidateKey.
    //
    // `anchor_f` is graph-topology derived (fan_out, boundary bonus, etc.) and
    // therefore more reliable than the BM25 `search_score` which can be skewed
    // by file length.  We weight anchor_f at 10 000× so even a single incoming
    // call edge (fan_out contribution ≈ 0.4 → 4 000 points) dominates any
    // plausible search score difference (max ~10 × 10 = 100 points).  Both
    // are capped at u32::MAX / 2 to keep the arithmetic safe.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "score is positive and bounded well below u32::MAX"
    )]
    let lexical_score =
        ((anchor_f * 10_000.0 + candidate.search_score * 10.0).round() as u32).min(u32::MAX / 2);

    CandidateKey::for_descending_sort(
        candidate.is_canonical_boundary,
        has_consumer_repo_evidence,
        query_shape_match,
        candidate.exact_match,
        u32::try_from(structural_repo_span).unwrap_or(u32::MAX),
        u32::try_from(advisory_span).unwrap_or(u32::MAX),
        lexical_score,
    )
}

/// Whether a node kind matches the classified query shape.
fn node_kind_matches_shape(kind: NodeKind, shape: QueryShape) -> bool {
    match shape {
        QueryShape::EventRollout => matches!(
            kind,
            NodeKind::Topic
                | NodeKind::Queue
                | NodeKind::Subject
                | NodeKind::Stream
                | NodeKind::Event
        ),
        QueryShape::RouteApiRollout => matches!(kind, NodeKind::Route),
        QueryShape::GuardRollout => {
            // Guard nodes are typically Class or Function; we accept both.
            matches!(kind, NodeKind::Class | NodeKind::Function)
        }
        QueryShape::SharedTypeRollout => matches!(
            kind,
            NodeKind::SharedSymbol | NodeKind::Type | NodeKind::PayloadContract
        ),
        QueryShape::GenericSymbolImpact => true,
    }
}

fn is_strict_impact_match(hit: &gather_step_storage::SearchHit, query: &str) -> bool {
    let tail = query.rsplit(['.', ':']).next().unwrap_or(query);
    hit.symbol_name == query || hit.symbol_name == tail
}

/// Compute a bonus score for repos that are canonical shared-contract sources.
///
/// The bonus is based on exact token match on the repo name only — not on
/// substring matching and not on file paths.  This prevents repos like
/// `application-services` from winning the bonus via file paths that happen to
/// contain "contract" while still awarding it to repos genuinely named
/// `shared-contracts` or `shared-lib`.
///
/// Repo name tokens are derived by splitting on `-` and `_`.
#[expect(
    clippy::disallowed_methods,
    reason = "one-shot owned lowercase needed for case-insensitive token/substring match on repo name and file path"
)]
fn canonical_source_bonus(repo: &str, file_path: &str) -> f32 {
    let mut score = 0.0_f32;

    // Exact whole-repo-name checks first (highest confidence).
    let mut repo_lower = repo.to_ascii_lowercase();
    // Normalise separators so `shared-contracts` and `shared_contracts` both match.
    repo_lower = repo_lower.replace('-', "_");

    // Tier 1: the repo name IS the well-known canonical contract library.
    if repo_lower == "shared_contracts" || repo_lower == "shared_lib" {
        score += 5.0;
    } else {
        // Tier 2: every dash/underscore-separated token of the repo name is
        // inspected.  Award the bonus only when the repo name itself contains
        // the contract/shared tokens as discrete words — e.g. `shared-api` or
        // `contracts-core`.  A repo named `application-services` that happens to
        // store files under `contracts/` does NOT get this bonus.
        let tokens: Vec<&str> = repo_lower.split('_').collect();
        let has_shared = tokens.contains(&"shared");
        let has_contract = tokens.contains(&"contract") || tokens.contains(&"contracts");
        if has_shared || has_contract {
            score += 2.5;
        }
    }

    // File-path segment bonus is intentionally kept to reward files inside
    // well-named directories, but only after the repo-level score is already
    // non-zero to avoid awarding it to unrelated repos.
    if score > 0.0 {
        let file_lower = file_path.to_ascii_lowercase();
        if path_has_segment(&file_lower, "contracts")
            || path_has_segment(&file_lower, "schemas")
            || path_has_segment(&file_lower, "types")
        {
            score += 0.5;
        }
    }

    score
}

/// Tuple dims: `(canonical, has_consumer_repo_evidence, repo_span, file_span,
/// structural_files, advisory_files)`.
///
/// `has_consumer_repo_evidence` is true when the candidate's structural impact
/// reaches a repo other than the anchor's own — the user-intent signal for "who
/// consumes this?". Ranking it above repo/file span stops a self-referential
/// candidate from tying a cross-repo candidate.
///
/// `structural_files`: repos reached via declared structural edges.
/// `advisory_files`: repos reached only via weak co-change signals.  Split out
/// so callers can render them under a distinct heading.
type SharedContractRank = (
    bool,
    bool,
    usize,
    usize,
    Vec<ImpactedRepoOutput>,
    Vec<ImpactedRepoOutput>,
);

/// Returns `(structural_files, advisory_files)` for the best shared-contract
/// candidate, or `None` when no candidate has any downstream impact.
fn shared_contract_match(
    graph: &impl GraphStore,
    node: &gather_step_core::NodeData,
    shape: QueryShape,
) -> Result<Option<(Vec<ImpactedRepoOutput>, Vec<ImpactedRepoOutput>)>> {
    let candidate_ids = shared_contract_candidate_ids(graph, node, shape)?;
    if candidate_ids.is_empty() {
        return Ok(None);
    }
    let mut best_match: Option<SharedContractRank> = None;
    for candidate_id in candidate_ids {
        let Some(candidate_node) = graph.get_node(candidate_id)? else {
            continue;
        };
        let impact = shared_contract_impact(graph, candidate_id).with_context(|| {
            format!(
                "computing shared-contract impact for `{}` via `{}`",
                node.name, candidate_node.name
            )
        })?;
        if impact.entries.is_empty() {
            continue;
        }

        // Split impact entries into structural vs advisory bands.
        let (structural_entries, advisory_entries): (Vec<_>, Vec<_>) = impact
            .entries
            .into_iter()
            .map(|(repo, files)| {
                let (structural_files, advisory_files): (Vec<_>, Vec<_>) = files
                    .into_iter()
                    .partition(|f| f.evidence_band == EvidenceBand::Structural);
                (repo, structural_files, advisory_files)
            })
            .partition(|(_, structural, _)| !structural.is_empty());

        let to_repo_output = |(repo, structural_files, advisory_files): (
            String,
            Vec<gather_step_analysis::ImpactedFile>,
            Vec<gather_step_analysis::ImpactedFile>,
        ),
                              use_advisory: bool| {
            let files = if use_advisory {
                advisory_files
            } else {
                structural_files
            };
            ImpactedRepoOutput {
                files: files
                    .into_iter()
                    .map(|file| ImpactedFileOutput {
                        edge_kinds: file
                            .edge_kinds
                            .into_iter()
                            .map(|kind| kind.to_string())
                            .collect(),
                        file_path: file.file_path,
                        producer_or_consumer: file.producer_or_consumer.map(|role| match role {
                            gather_step_analysis::BoundaryRole::Producer => "producer".to_owned(),
                            gather_step_analysis::BoundaryRole::Consumer => "consumer".to_owned(),
                        }),
                        serialization_point: file.serialization_point,
                        validation_point: file.validation_point,
                        weight: file.weight,
                    })
                    .collect(),
                repo,
            }
        };

        let structural_repos = structural_entries
            .clone()
            .into_iter()
            .map(|entry| to_repo_output(entry, false))
            .collect::<Vec<_>>();
        // Advisory repos: repos that are entirely weak-only (no structural files).
        let advisory_repos = advisory_entries
            .clone()
            .into_iter()
            .map(|entry| to_repo_output(entry, true))
            .collect::<Vec<_>>();

        let repo_span = structural_repos.len();
        let file_span = structural_repos.iter().map(|r| r.files.len()).sum();
        let has_consumer_repo_evidence = structural_repos
            .iter()
            .any(|entry| entry.repo != candidate_node.repo);
        let rank: SharedContractRank = (
            is_canonical_boundary(&candidate_node),
            has_consumer_repo_evidence,
            repo_span,
            file_span,
            structural_repos,
            advisory_repos,
        );
        if best_match.as_ref().is_none_or(|current| {
            (rank.0, rank.1, rank.2, rank.3) > (current.0, current.1, current.2, current.3)
        }) {
            best_match = Some(rank);
        }
    }

    Ok(best_match
        .map(|(_, _, _, _, structural_files, advisory_files)| (structural_files, advisory_files)))
}

fn is_canonical_boundary(node: &gather_step_core::NodeData) -> bool {
    if node.is_virtual {
        return true;
    }
    let mut repo = node.repo.clone();
    repo.make_ascii_lowercase();
    let mut file_path = node.file_path.clone();
    file_path.make_ascii_lowercase();
    repo.contains("contract")
        || repo.contains("shared_contracts")
        || repo.contains("shared-contracts")
        || file_path.contains("contract")
        || file_path.contains("shared_contracts")
        || file_path.contains("shared-contracts")
        || path_has_segment(&file_path, "contracts")
        || path_has_segment(&file_path, "schemas")
}

fn path_has_segment(path: &str, segment: &str) -> bool {
    path.split('/').any(|part| part == segment)
}

fn virtual_target_match(
    graph: &impl GraphStore,
    node_id: gather_step_core::NodeId,
) -> Result<Vec<VirtualImpactOutput>> {
    graph
        .get_outgoing(node_id)?
        .into_iter()
        .map(|edge| graph.get_node(edge.target))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .filter(|target| target.is_virtual)
        .map(|target| {
            let traced = trace_across_repos(graph, target.id, 2)?;
            Ok(VirtualImpactOutput {
                target_name: target.name,
                target_kind: target.kind.to_string(),
                repos: traced.into_keys().collect(),
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_analysis::QueryShape;
    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, Visibility, node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb, SearchHit};

    use gather_step_analysis::shared_contract::looks_like_guard_entrypoint;

    use super::{
        CandidateKey, ImpactCandidate, ImpactMatchOutput, VirtualImpactOutput,
        canonical_source_bonus, impact_candidate_key, is_strict_impact_match,
        rerank_impact_candidates, shared_contract_match,
    };

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(name: &str) -> Self {
            let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-cli-impact-{name}-{}-{counter}.redb",
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

    #[test]
    fn shared_contract_match_returns_rollout_repos() {
        let temp = TempDb::new("shared-contract");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let contract = node(
            "shared_contracts",
            "src/shared_audit.ts",
            NodeKind::SharedSymbol,
            "SharedAuditRecord",
            0,
        );
        let backend_file = file("backend_standard", "src/controller.ts");
        let backend_symbol = node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Class,
            "AuditController",
            0,
        );
        let frontend_file = file("frontend_standard", "src/api.ts");
        let frontend_symbol = node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Function,
            "loadAuditRecords",
            0,
        );

        store
            .bulk_insert(
                &[
                    contract.clone(),
                    backend_file.clone(),
                    backend_symbol.clone(),
                    frontend_file.clone(),
                    frontend_symbol.clone(),
                ],
                &[
                    edge(
                        backend_symbol.id,
                        contract.id,
                        EdgeKind::ImplementsContractFrom,
                        backend_file.id,
                    ),
                    edge(
                        frontend_symbol.id,
                        contract.id,
                        EdgeKind::UsesTypeFrom,
                        frontend_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let (structural, advisory) =
            shared_contract_match(&store, &contract, QueryShape::SharedTypeRollout)
                .expect("shared contract impact should succeed")
                .expect("shared contract impact should produce repos");

        // Both backend and frontend are reached via structural edges only
        // (ImplementsContractFrom and UsesTypeFrom), so they both land in the
        // structural band and the advisory band is empty.
        assert_eq!(structural.len(), 2);
        assert!(
            structural
                .iter()
                .any(|repo| repo.repo == "backend_standard")
        );
        assert!(
            structural
                .iter()
                .any(|repo| repo.repo == "frontend_standard")
        );
        assert!(
            advisory.is_empty(),
            "no advisory files expected for structural-only edges; got: {:?}",
            advisory.iter().map(|r| &r.repo).collect::<Vec<_>>()
        );
    }

    #[test]
    fn shared_contract_match_scores_peer_consumers_against_candidate_repo() {
        let temp = TempDb::new("peer-consumer-candidate-repo");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let shadow = node(
            "service_a",
            "src/audit_user.ts",
            NodeKind::Type,
            "AuditUser",
            0,
        );
        let peer = node(
            "types_core",
            "src/audit_user.ts",
            NodeKind::Type,
            "AuditUser",
            0,
        );
        let shadow_file = file("service_a", "src/shadow-consumer.ts");
        let shadow_consumer = node(
            "service_a",
            "src/shadow-consumer.ts",
            NodeKind::Function,
            "useLocalAuditUser",
            0,
        );
        let peer_file = file("service_a", "src/peer-consumer.ts");
        let peer_consumer = node(
            "service_a",
            "src/peer-consumer.ts",
            NodeKind::Function,
            "useSharedAuditUser",
            0,
        );

        store
            .bulk_insert(
                &[
                    shadow.clone(),
                    peer.clone(),
                    shadow_file.clone(),
                    shadow_consumer.clone(),
                    peer_file.clone(),
                    peer_consumer.clone(),
                ],
                &[
                    edge(
                        shadow_consumer.id,
                        shadow.id,
                        EdgeKind::UsesTypeFrom,
                        shadow_file.id,
                    ),
                    edge(
                        peer_consumer.id,
                        peer.id,
                        EdgeKind::UsesTypeFrom,
                        peer_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let (structural, advisory) =
            shared_contract_match(&store, &shadow, QueryShape::SharedTypeRollout)
                .expect("shared contract impact should succeed")
                .expect("shared contract impact should produce repos");

        assert!(advisory.is_empty());
        assert_eq!(structural.len(), 1);
        assert_eq!(structural[0].repo, "service_a");
        assert_eq!(
            structural[0].files[0].file_path, "src/peer-consumer.ts",
            "peer candidate should win because service_a is a consumer of types_core, not because it differs from the original search-hit repo"
        );
    }

    #[test]
    fn rerank_prefers_broader_virtual_rollout_candidate() {
        let temp = TempDb::new("virtual-rerank");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");
        let rollout_service = node(
            "backend_standard",
            "src/rollout/service.ts",
            NodeKind::Service,
            "RolloutService",
            0,
        );
        let rollout_helper = node(
            "backend_standard",
            "src/rollout/helper.ts",
            NodeKind::Function,
            "runRollout",
            0,
        );

        store
            .bulk_insert(&[rollout_service.clone(), rollout_helper.clone()], &[])
            .expect("graph should write");

        let mut matches = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: rollout_helper.repo.clone(),
                    source_file: rollout_helper.file_path.clone(),
                    source_symbol: rollout_helper.name.clone(),
                    strategy: "virtual_targets".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: vec![VirtualImpactOutput {
                        target_name: "rollout-plan".to_owned(),
                        target_kind: "route".to_owned(),
                        repos: vec!["frontend_standard".to_owned()],
                    }],
                },
                is_canonical_boundary: false,
                node_id: rollout_helper.id,
                node_kind: rollout_helper.kind,
                search_score: 9.0,
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: rollout_service.repo.clone(),
                    source_file: rollout_service.file_path.clone(),
                    source_symbol: rollout_service.name.clone(),
                    strategy: "virtual_targets".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: vec![
                        VirtualImpactOutput {
                            target_name: "rollout-admin".to_owned(),
                            target_kind: "route".to_owned(),
                            repos: vec![
                                "frontend_standard".to_owned(),
                                "shared_contracts".to_owned(),
                            ],
                        },
                        VirtualImpactOutput {
                            target_name: "rollout-worker".to_owned(),
                            target_kind: "queue".to_owned(),
                            repos: vec!["backend_standard".to_owned()],
                        },
                    ],
                },
                is_canonical_boundary: false,
                node_id: rollout_service.id,
                node_kind: rollout_service.kind,
                search_score: 6.0,
                exact_match: true,
            },
        ];

        rerank_impact_candidates(&store, &mut matches, QueryShape::GenericSymbolImpact);

        assert_eq!(matches[0].output.source_symbol, "RolloutService");
        assert!(matches[0].output.primary);
        assert!(!matches[1].output.primary);
    }

    #[test]
    fn strict_impact_match_uses_symbol_tail_only() {
        let hit = SearchHit {
            node_id: node_id(
                "shared_contracts",
                "src/audit-user.type.ts",
                NodeKind::Type,
                "AuditUser",
            ),
            repo: "shared_contracts".to_owned(),
            file_path: "src/audit-user.type.ts".to_owned(),
            symbol_name: "AuditUser".to_owned(),
            node_kind: NodeKind::Type,
            adjusted_score: 10.0,
            exact_match: true,
            is_exported: true,
            lang: "ts".to_owned(),
        };

        assert!(is_strict_impact_match(&hit, "AuditUser"));
        assert!(is_strict_impact_match(&hit, "UserAuthGuard.AuditUser"));
        assert!(!is_strict_impact_match(&hit, "Input"));
    }

    #[test]
    fn shared_contracts_gets_higher_canonical_source_bonus_than_local_shared_file() {
        let common = canonical_source_bonus(
            "shared_contracts",
            "src/microservices/identity/types/audit-user.type.ts",
        );
        let local = canonical_source_bonus("identity", "src/workflows/user/shared/types.ts");
        assert!(common > local);
    }

    #[test]
    fn guard_entrypoint_detects_can_activate_methods() {
        let node = node(
            "shared_contracts",
            "src/guards/user-auth-guard.guard.ts",
            NodeKind::Function,
            "canActivate",
            0,
        );
        assert!(looks_like_guard_entrypoint(&node));
    }

    #[test]
    fn canonical_boundary_outranks_non_canonical_exact_match() {
        // Regression test: `CandidateKey::for_descending_sort` places
        // `canonical_boundary` as the highest-priority field, so a canonical
        // source wins even when the peer has an exact match and higher spans.
        let canonical = CandidateKey::for_descending_sort(
            true,  // canonical_boundary
            false, // consumer_repo_evidence
            false, // query_shape_match
            false, // exact_symbol_match
            1,     // structural_repo_span
            0,     // advisory_span
            100,   // lexical_score
        );
        let local_exact = CandidateKey::for_descending_sort(
            false, // canonical_boundary
            false, false, true, // exact_symbol_match — higher
            1, 0, 100,
        );
        assert!(
            canonical < local_exact,
            "canonical must sort before non-canonical exact match"
        );
    }

    /// Canonical declaration wins over a same-name shadow copy in another package
    /// when the canonical has cross-repo structural consumers and the shadow has
    /// only high-fanout `CoChangesWith` edges.
    #[test]
    fn canonical_wins_over_shadow_copy_with_co_change_fanout() {
        let temp = TempDb::new("canonical-vs-shadow");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        // Canonical `AuditUser` declaration in shared_contracts.
        let canonical = node(
            "shared_contracts",
            "src/types/audit-user.type.ts",
            NodeKind::Type,
            "AuditUser",
            0,
        );
        // Two cross-repo structural consumers of the canonical.
        let backend_controller_file = file("backend_standard", "src/controller.ts");
        let consumer_a = node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Class,
            "AuditController",
            0,
        );
        let frontend_api_file = file("frontend_standard", "src/api.ts");
        let consumer_b = node(
            "frontend_standard",
            "src/api.ts",
            NodeKind::Function,
            "loadAudit",
            0,
        );

        // Shadow copy of `AuditUser` in service_a — no structural consumers,
        // but 5 `CoChangesWith` edges inflating its apparent span.
        let shadow = node(
            "service_a",
            "src/utils/audit-user.ts",
            NodeKind::Type,
            "AuditUser",
            0,
        );
        let co_change_nodes: Vec<NodeData> = (0_u16..5)
            .map(|i| {
                node(
                    "service_a",
                    &format!("src/other/file{i}.ts"),
                    NodeKind::Function,
                    &format!("fn{i}"),
                    i,
                )
            })
            .collect();
        let co_change_files: Vec<NodeData> = (0_u16..5)
            .map(|i| file("service_a", &format!("src/other/file{i}.ts")))
            .collect();

        let mut nodes = vec![
            canonical.clone(),
            backend_controller_file.clone(),
            consumer_a.clone(),
            frontend_api_file.clone(),
            consumer_b.clone(),
            shadow.clone(),
        ];
        nodes.extend(co_change_nodes.iter().cloned());
        nodes.extend(co_change_files.iter().cloned());

        let mut edges = vec![
            // Canonical's structural cross-repo consumers.
            edge(
                consumer_a.id,
                canonical.id,
                EdgeKind::UsesTypeFrom,
                backend_controller_file.id,
            ),
            edge(
                consumer_b.id,
                canonical.id,
                EdgeKind::UsesTypeFrom,
                frontend_api_file.id,
            ),
        ];
        // Shadow's CoChangesWith fan-out (5 same-repo edges).
        for (co_node, co_file) in co_change_nodes.iter().zip(co_change_files.iter()) {
            edges.push(edge(
                co_node.id,
                shadow.id,
                EdgeKind::CoChangesWith,
                co_file.id,
            ));
        }

        store
            .bulk_insert(&nodes, &edges)
            .expect("graph should write");

        // The canonical has 2 structural cross-repo consumers; the shadow has 5
        // CoChangesWith edges but zero structural consumers.  The canonical must rank first.
        let mut matches = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: canonical.repo.clone(),
                    source_file: canonical.file_path.clone(),
                    source_symbol: canonical.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: vec![
                        super::ImpactedRepoOutput {
                            repo: "backend_standard".to_owned(),
                            files: vec![super::ImpactedFileOutput {
                                edge_kinds: vec!["UsesTypeFrom".to_owned()],
                                file_path: "src/controller.ts".to_owned(),
                                producer_or_consumer: None,
                                serialization_point: false,
                                validation_point: false,
                                weight: 1.0,
                            }],
                        },
                        super::ImpactedRepoOutput {
                            repo: "frontend_standard".to_owned(),
                            files: vec![super::ImpactedFileOutput {
                                edge_kinds: vec!["UsesTypeFrom".to_owned()],
                                file_path: "src/api.ts".to_owned(),
                                producer_or_consumer: None,
                                serialization_point: false,
                                validation_point: false,
                                weight: 1.0,
                            }],
                        },
                    ],
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: true,
                node_id: canonical.id,
                node_kind: canonical.kind,
                search_score: 8.0,
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: shadow.repo.clone(),
                    source_file: shadow.file_path.clone(),
                    source_symbol: shadow.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    // 5 co-change repos — raw repo_span=5, decayed to 2 because no structural evidence.
                    impacted_files: Vec::new(),
                    advisory_co_change_files: (0_u16..5)
                        .map(|i| super::ImpactedRepoOutput {
                            repo: format!("co_repo_{i}"),
                            files: vec![super::ImpactedFileOutput {
                                edge_kinds: vec!["CoChangesWith".to_owned()],
                                file_path: format!("src/file{i}.ts"),
                                producer_or_consumer: None,
                                serialization_point: false,
                                validation_point: false,
                                weight: 0.1,
                            }],
                        })
                        .collect(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: shadow.id,
                node_kind: shadow.kind,
                search_score: 9.0,
                exact_match: true,
            },
        ];

        rerank_impact_candidates(&store, &mut matches, QueryShape::SharedTypeRollout);

        assert_eq!(
            matches[0].output.source_repo, "shared_contracts",
            "canonical must rank first; got: {}::{}",
            matches[0].output.source_repo, matches[0].output.source_symbol
        );
        assert!(matches[0].output.primary);
    }

    /// A co-change-only candidate decays but still loses when the canonical has
    /// more cross-repo structural consumers.
    #[test]
    fn co_change_only_candidate_decays_below_structural_candidate() {
        let temp = TempDb::new("decay-vs-structural");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let canonical = node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::Type,
            "SharedToken",
            0,
        );
        let backend_auth_file = file("backend_standard", "src/auth.ts");
        let consumer_a = node(
            "backend_standard",
            "src/auth.ts",
            NodeKind::Class,
            "AuthService",
            0,
        );
        let frontend_login_file = file("frontend_standard", "src/login.ts");
        let consumer_b = node(
            "frontend_standard",
            "src/login.ts",
            NodeKind::Function,
            "loginUser",
            0,
        );

        // Local copy with 1 structural consumer — fewer than canonical's 2.
        let local_copy = node(
            "service_a",
            "src/internal/token.ts",
            NodeKind::Type,
            "SharedToken",
            0,
        );
        let local_consumer_file = file("service_b", "src/client.ts");
        let local_consumer = node(
            "service_b",
            "src/client.ts",
            NodeKind::Function,
            "fetchToken",
            0,
        );

        store
            .bulk_insert(
                &[
                    canonical.clone(),
                    backend_auth_file.clone(),
                    consumer_a.clone(),
                    frontend_login_file.clone(),
                    consumer_b.clone(),
                    local_copy.clone(),
                    local_consumer_file.clone(),
                    local_consumer.clone(),
                ],
                &[
                    edge(
                        consumer_a.id,
                        canonical.id,
                        EdgeKind::UsesTypeFrom,
                        backend_auth_file.id,
                    ),
                    edge(
                        consumer_b.id,
                        canonical.id,
                        EdgeKind::UsesTypeFrom,
                        frontend_login_file.id,
                    ),
                    edge(
                        local_consumer.id,
                        local_copy.id,
                        EdgeKind::UsesTypeFrom,
                        local_consumer_file.id,
                    ),
                ],
            )
            .expect("graph should write");

        let mut matches = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: canonical.repo.clone(),
                    source_file: canonical.file_path.clone(),
                    source_symbol: canonical.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: vec![
                        super::ImpactedRepoOutput {
                            repo: "backend_standard".to_owned(),
                            files: vec![super::ImpactedFileOutput {
                                edge_kinds: vec!["UsesTypeFrom".to_owned()],
                                file_path: "src/auth.ts".to_owned(),
                                producer_or_consumer: None,
                                serialization_point: false,
                                validation_point: false,
                                weight: 1.0,
                            }],
                        },
                        super::ImpactedRepoOutput {
                            repo: "frontend_standard".to_owned(),
                            files: vec![super::ImpactedFileOutput {
                                edge_kinds: vec!["UsesTypeFrom".to_owned()],
                                file_path: "src/login.ts".to_owned(),
                                producer_or_consumer: None,
                                serialization_point: false,
                                validation_point: false,
                                weight: 1.0,
                            }],
                        },
                    ],
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: true,
                node_id: canonical.id,
                node_kind: canonical.kind,
                search_score: 7.0,
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: local_copy.repo.clone(),
                    source_file: local_copy.file_path.clone(),
                    source_symbol: local_copy.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: vec![super::ImpactedRepoOutput {
                        repo: "service_b".to_owned(),
                        files: vec![super::ImpactedFileOutput {
                            edge_kinds: vec!["UsesTypeFrom".to_owned()],
                            file_path: "src/client.ts".to_owned(),
                            producer_or_consumer: None,
                            serialization_point: false,
                            validation_point: false,
                            weight: 1.0,
                        }],
                    }],
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: local_copy.id,
                node_kind: local_copy.kind,
                search_score: 9.0,
                exact_match: true,
            },
        ];

        rerank_impact_candidates(&store, &mut matches, QueryShape::SharedTypeRollout);

        assert_eq!(
            matches[0].output.source_repo, "shared_contracts",
            "canonical (2 structural consumers) must beat local copy (1 structural consumer); got: {}",
            matches[0].output.source_repo
        );
        assert!(matches[0].output.primary);
    }

    /// A canonical declaration outranks a same-name local peer even when the
    /// local peer sits deeper in the directory tree (which previously raised
    /// its `depth`-driven weight).  `canonical_boundary` must dominate.
    #[test]
    fn canonical_key_dominates_over_deep_path_local() {
        // Two candidates with the same symbol name.
        // - Canonical: shared_contracts (canonical_boundary=true).
        // - Local: service_a/internal/utils (canonical_boundary=false, all other fields higher).
        let canonical_key = CandidateKey::for_descending_sort(
            true, // canonical_boundary
            true, // consumer_repo_evidence
            true, // query_shape_match
            true, // exact_symbol_match
            2,    // structural_repo_span
            0,    // advisory_span
            800,  // lexical_score
        );
        let local_deep_key = CandidateKey::for_descending_sort(
            false, // canonical_boundary — only difference
            true, true, true, 100, // structural_repo_span — far higher
            50, 999,
        );
        assert!(
            canonical_key < local_deep_key,
            "canonical must sort before deep-path local peer"
        );
    }

    /// `canonical_source_bonus` must award the bonus to repos that are named
    /// after known shared-contract patterns (exact token match on repo name),
    /// and must NOT award it to repos whose file paths happen to contain the
    /// word "contract" but whose repo name is unrelated.
    #[test]
    fn canonical_source_bonus_matches_repo_name_not_file_path() {
        // Repos that ARE canonical shared-contract sources.
        assert!(
            canonical_source_bonus("shared-contracts", "src/guards/auth.ts") > 0.0,
            "shared-contracts must get a bonus"
        );
        assert!(
            canonical_source_bonus("shared-lib", "src/index.ts") > 0.0,
            "shared-lib must get a bonus"
        );

        // A repo named `application-services` must NOT get a bonus even if a
        // file inside it lives under a `contracts/` directory.
        let platform_score =
            canonical_source_bonus("application-services", "contracts/auth-guard.ts");
        assert!(
            platform_score.abs() < f32::EPSILON,
            "application-services with a contracts/ path must NOT get the canonical bonus (got {platform_score})"
        );

        // `shared-contracts` with a contracts/ path should score higher than
        // `shared-contracts` without one (the file-path segment bonus).
        let with_contracts_dir =
            canonical_source_bonus("shared-contracts", "contracts/types/session.ts");
        let without_contracts_dir = canonical_source_bonus("shared-contracts", "src/index.ts");
        assert!(
            with_contracts_dir > without_contracts_dir,
            "file in contracts/ dir should score higher than file outside it"
        );
    }

    // ── Pack / impact parity tests ────────────────────────────────────────────
    //
    // For each query shape, `pack` and `impact` must agree on the primary target.
    // These tests build a minimal graph with two candidates and verify that
    // `rerank_impact_candidates` picks the same winner that `choose_pack_target`
    // would pick (canonical/cross-repo wins).

    /// Shared-type query: the candidate in the canonical repo (`shared_contracts`)
    /// must be the primary target regardless of which node has the higher search score.
    #[test]
    fn pack_impact_parity_shared_type() {
        let temp = TempDb::new("parity-shared-type");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let canonical = node(
            "shared_contracts",
            "src/types/order.ts",
            NodeKind::Type,
            "Order",
            0,
        );
        let local = node(
            "backend_standard",
            "src/internal/order.ts",
            NodeKind::Type,
            "Order",
            0,
        );
        let consumer_file = file("frontend_standard", "src/ui.ts");
        let consumer = node(
            "frontend_standard",
            "src/ui.ts",
            NodeKind::Function,
            "renderOrder",
            0,
        );

        store
            .bulk_insert(
                &[
                    canonical.clone(),
                    local.clone(),
                    consumer_file.clone(),
                    consumer.clone(),
                ],
                &[edge(
                    consumer.id,
                    canonical.id,
                    EdgeKind::UsesTypeFrom,
                    consumer_file.id,
                )],
            )
            .expect("graph should write");

        let mut matches = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: local.repo.clone(),
                    source_file: local.file_path.clone(),
                    source_symbol: local.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: local.id,
                node_kind: local.kind,
                search_score: 9.9, // higher search score
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: canonical.repo.clone(),
                    source_file: canonical.file_path.clone(),
                    source_symbol: canonical.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: vec![super::ImpactedRepoOutput {
                        repo: "frontend_standard".to_owned(),
                        files: vec![super::ImpactedFileOutput {
                            edge_kinds: vec!["UsesTypeFrom".to_owned()],
                            file_path: "src/ui.ts".to_owned(),
                            producer_or_consumer: None,
                            serialization_point: false,
                            validation_point: false,
                            weight: 1.0,
                        }],
                    }],
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: true,
                node_id: canonical.id,
                node_kind: canonical.kind,
                search_score: 5.0, // lower search score — should still win
                exact_match: true,
            },
        ];

        rerank_impact_candidates(&store, &mut matches, QueryShape::SharedTypeRollout);

        assert_eq!(
            matches[0].output.source_repo, "shared_contracts",
            "shared-type query: canonical must be primary target"
        );
        assert!(matches[0].output.primary);
    }

    /// Guard query: the canonical guard declaration wins over a local implementation
    /// with higher search score.
    #[test]
    fn pack_impact_parity_guard() {
        let temp = TempDb::new("parity-guard");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let canonical_guard = node(
            "shared_contracts",
            "src/guards/auth.guard.ts",
            NodeKind::Class,
            "AuthGuard",
            0,
        );
        let local_guard = node(
            "backend_standard",
            "src/guards/local-auth.guard.ts",
            NodeKind::Class,
            "AuthGuard",
            0,
        );
        let consumer_file = file("backend_standard", "src/controller.ts");
        let consumer = node(
            "backend_standard",
            "src/controller.ts",
            NodeKind::Class,
            "Controller",
            0,
        );

        store
            .bulk_insert(
                &[
                    canonical_guard.clone(),
                    local_guard.clone(),
                    consumer_file.clone(),
                    consumer.clone(),
                ],
                &[edge(
                    consumer.id,
                    canonical_guard.id,
                    EdgeKind::UsesGuardFrom,
                    consumer_file.id,
                )],
            )
            .expect("graph should write");

        let mut matches = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: local_guard.repo.clone(),
                    source_file: local_guard.file_path.clone(),
                    source_symbol: local_guard.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: local_guard.id,
                node_kind: local_guard.kind,
                search_score: 9.9,
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: canonical_guard.repo.clone(),
                    source_file: canonical_guard.file_path.clone(),
                    source_symbol: canonical_guard.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: vec![super::ImpactedRepoOutput {
                        repo: "backend_standard".to_owned(),
                        files: vec![super::ImpactedFileOutput {
                            edge_kinds: vec!["UsesGuardFrom".to_owned()],
                            file_path: "src/controller.ts".to_owned(),
                            producer_or_consumer: None,
                            serialization_point: false,
                            validation_point: false,
                            weight: 1.0,
                        }],
                    }],
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: true,
                node_id: canonical_guard.id,
                node_kind: canonical_guard.kind,
                search_score: 5.0,
                exact_match: true,
            },
        ];

        rerank_impact_candidates(&store, &mut matches, QueryShape::GuardRollout);

        assert_eq!(
            matches[0].output.source_repo, "shared_contracts",
            "guard query: canonical guard must be primary target"
        );
        assert!(matches[0].output.primary);
    }

    // ── Stable symbol-ID tie-break ─────────────────────────────────────────────

    /// Two candidates that tie on ALL comparator fields must produce a
    /// deterministic order across runs.  We verify that ordering is purely by
    /// symbol-ID bytes when all `CandidateKey` fields are equal, and that the
    /// order produced by ascending `(key, id_bytes)` sort is stable.
    #[test]
    fn stable_symbol_id_tiebreak_is_deterministic() {
        let temp = TempDb::new("stable-tiebreak");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let node_a = node("service_a", "src/foo.ts", NodeKind::Type, "Token", 0);
        let node_b = node("service_b", "src/foo.ts", NodeKind::Type, "Token", 0);

        store
            .bulk_insert(&[node_a.clone(), node_b.clone()], &[])
            .expect("graph should write");

        // Both candidates are identical in every comparator dimension.
        let mut matches_fwd = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: node_a.repo.clone(),
                    source_file: node_a.file_path.clone(),
                    source_symbol: node_a.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: node_a.id,
                node_kind: node_a.kind,
                search_score: 1.0,
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: node_b.repo.clone(),
                    source_file: node_b.file_path.clone(),
                    source_symbol: node_b.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: node_b.id,
                node_kind: node_b.kind,
                search_score: 1.0,
                exact_match: true,
            },
        ];

        // Reversed input order.
        let mut matches_rev = vec![
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: node_b.repo.clone(),
                    source_file: node_b.file_path.clone(),
                    source_symbol: node_b.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: node_b.id,
                node_kind: node_b.kind,
                search_score: 1.0,
                exact_match: true,
            },
            ImpactCandidate {
                output: ImpactMatchOutput {
                    source_repo: node_a.repo.clone(),
                    source_file: node_a.file_path.clone(),
                    source_symbol: node_a.name.clone(),
                    strategy: "shared_contract".to_owned(),
                    primary: false,
                    impacted_files: Vec::new(),
                    advisory_co_change_files: Vec::new(),
                    virtual_targets: Vec::new(),
                },
                is_canonical_boundary: false,
                node_id: node_a.id,
                node_kind: node_a.kind,
                search_score: 1.0,
                exact_match: true,
            },
        ];

        rerank_impact_candidates(&store, &mut matches_fwd, QueryShape::GenericSymbolImpact);
        rerank_impact_candidates(&store, &mut matches_rev, QueryShape::GenericSymbolImpact);

        // Both runs must produce the same first element.
        assert_eq!(
            matches_fwd[0].output.source_repo, matches_rev[0].output.source_repo,
            "tie-break must be deterministic: fwd primary={}, rev primary={}",
            matches_fwd[0].output.source_repo, matches_rev[0].output.source_repo
        );
        assert_eq!(
            matches_fwd[0].output.source_file, matches_rev[0].output.source_file,
            "tie-break must be deterministic across input orderings"
        );
    }

    // ── impact_candidate_key unit tests ───────────────────────────────────────

    /// A candidate with `canonical_boundary=true` must produce a lower key
    /// (sorts first) than any candidate with `canonical_boundary=false`,
    /// regardless of other fields.
    #[test]
    fn impact_candidate_key_canonical_boundary_is_first_priority() {
        use std::collections::BTreeMap;

        let temp = TempDb::new("key-canonical-first");
        let store = GraphStoreDb::open(temp.path()).expect("graph should open");

        let canonical = node(
            "shared_contracts",
            "src/types.ts",
            NodeKind::SharedSymbol,
            "Token",
            0,
        );
        let local = node("service_a", "src/token.ts", NodeKind::Type, "Token", 0);
        store
            .bulk_insert(&[canonical.clone(), local.clone()], &[])
            .expect("graph should write");

        let anchor_scores: BTreeMap<gather_step_core::NodeId, f32> = BTreeMap::new();
        let consumer_evidence: BTreeMap<gather_step_core::NodeId, bool> = BTreeMap::new();

        let canonical_candidate = ImpactCandidate {
            output: ImpactMatchOutput {
                source_repo: canonical.repo.clone(),
                source_file: canonical.file_path.clone(),
                source_symbol: canonical.name.clone(),
                strategy: "shared_contract".to_owned(),
                primary: false,
                impacted_files: Vec::new(),
                advisory_co_change_files: Vec::new(),
                virtual_targets: Vec::new(),
            },
            is_canonical_boundary: true,
            node_id: canonical.id,
            node_kind: canonical.kind,
            search_score: 1.0,
            exact_match: false,
        };
        let local_candidate = ImpactCandidate {
            output: ImpactMatchOutput {
                source_repo: local.repo.clone(),
                source_file: local.file_path.clone(),
                source_symbol: local.name.clone(),
                strategy: "shared_contract".to_owned(),
                primary: false,
                impacted_files: vec![super::ImpactedRepoOutput {
                    repo: "service_b".to_owned(),
                    files: (0..10)
                        .map(|i| super::ImpactedFileOutput {
                            edge_kinds: vec!["UsesTypeFrom".to_owned()],
                            file_path: format!("src/file{i}.ts"),
                            producer_or_consumer: None,
                            serialization_point: false,
                            validation_point: false,
                            weight: 1.0,
                        })
                        .collect(),
                }],
                advisory_co_change_files: Vec::new(),
                virtual_targets: Vec::new(),
            },
            is_canonical_boundary: false,
            node_id: local.id,
            node_kind: local.kind,
            search_score: 9.9,
            exact_match: true,
        };

        let canonical_key = impact_candidate_key(
            &canonical_candidate,
            &anchor_scores,
            &consumer_evidence,
            QueryShape::SharedTypeRollout,
        );
        let local_key = impact_candidate_key(
            &local_candidate,
            &anchor_scores,
            &consumer_evidence,
            QueryShape::SharedTypeRollout,
        );

        assert!(
            canonical_key < local_key,
            "canonical_boundary candidate must have lower (better) key than non-canonical"
        );
    }

    fn file(repo: &str, file_path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            kind: NodeKind::File,
            name: file_path.to_owned(),
            qualified_name: Some(format!("{repo}::{file_path}")),
            external_id: None,
            signature: None,
            span: None,
            is_virtual: false,
            visibility: Some(Visibility::Private),
        }
    }

    fn node(repo: &str, file_path: &str, kind: NodeKind, name: &str, _ordinal: u16) -> NodeData {
        NodeData {
            id: node_id(repo, file_path, kind, name),
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            kind,
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            span: None,
            is_virtual: false,
            visibility: Some(Visibility::Public),
        }
    }

    fn edge(
        source: gather_step_core::NodeId,
        target: gather_step_core::NodeId,
        kind: EdgeKind,
        owner_file: gather_step_core::NodeId,
    ) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata::default(),
            owner_file,
            is_cross_file: true,
        }
    }
}
