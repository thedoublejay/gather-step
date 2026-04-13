use gather_step_core::NodeData;

use crate::canonical::{Canonical, canonical_for_node};

/// Operating mode that controls how [`SimplePackAssembler`] prioritises and
/// selects nodes when assembling a context pack.
///
/// The canonical definition lives here so that MCP and CLI layers can share
/// the same type without a private duplicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackMode {
    /// Broad context: prefer callers, shared contracts, and blast-radius nodes
    /// to give the LLM maximum understanding of a feature before making a plan.
    Planning,
    /// Tight call chain: start from the reported fault and trace inward to
    /// expose the narrowest set of nodes that could hide the bug.
    Debug,
    /// Direct dependencies: surface only the nodes a targeted fix will touch,
    /// keeping context small and precise.
    Fix,
    /// Balanced coverage: callers, callees, and the implementation surface in
    /// equal measure for a thorough review.
    Review,
    /// Blast-radius maximisation: cross-repo impact and every downstream
    /// consumer, to make the full scope of a change visible.
    ChangeImpact,
}

impl PackMode {
    /// Parse a case-insensitive mode string.
    ///
    /// Returns `Err(String)` with a human-readable message on unknown input.
    ///
    /// # Errors
    ///
    /// Returns an error string when `input` does not match a known mode name.
    pub fn parse(input: &str) -> Result<Self, String> {
        let mut normalized = input.trim().to_owned();
        normalized.make_ascii_lowercase();
        match normalized.as_str() {
            "planning" => Ok(Self::Planning),
            "debug" => Ok(Self::Debug),
            "fix" => Ok(Self::Fix),
            "review" => Ok(Self::Review),
            "change_impact" => Ok(Self::ChangeImpact),
            _ => Err(format!(
                "unsupported pack mode `{input}`; expected one of: planning, debug, fix, review, change_impact"
            )),
        }
    }

    /// Return the canonical lowercase string representation of the mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Debug => "debug",
            Self::Fix => "fix",
            Self::Review => "review",
            Self::ChangeImpact => "change_impact",
        }
    }
}

/// Query-shape strategy that routes resolution and traversal for a given
/// natural-language or symbol query.
///
/// The classifier inspects the anchor set (node kinds, edge kinds present in
/// the graph) and the query text to select the most appropriate traversal
/// strategy.  Both `pack` and `impact` run through the same classifier so
/// they always agree on which strategy applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryShape {
    /// The query targets a shared type / DTO / payload contract.  Traversal
    /// follows `UsesTypeFrom` edges favouring canonical-boundary sources.
    SharedTypeRollout,
    /// The query targets a guard or authentication boundary.  Traversal follows
    /// `UsesGuardFrom` edges.
    GuardRollout,
    /// The query targets an event topic, queue, or subject.  Traversal follows
    /// `ProducesEventFor` / `UsesEventFrom` edges first.
    EventRollout,
    /// The query targets an HTTP route or API endpoint.  Traversal follows
    /// `ConsumesApiFrom` edges.
    RouteApiRollout,
    /// Fallback strategy: standard impact behaviour with no shape-specific bias.
    GenericSymbolImpact,
}

impl QueryShape {
    /// Return a stable lowercase identifier for the strategy.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SharedTypeRollout => "shared_type_rollout",
            Self::GuardRollout => "guard_rollout",
            Self::EventRollout => "event_rollout",
            Self::RouteApiRollout => "route_api_rollout",
            Self::GenericSymbolImpact => "generic_symbol_impact",
        }
    }
}

/// Classify a query into a [`QueryShape`] based on node kind, edge hints, and
/// optional query text.
///
/// The priority order mirrors the traversal preference: event/route shapes are
/// detected from node kind first; guard and shared-type shapes are derived from
/// the name / kind; everything else falls through to `GenericSymbolImpact`.
///
/// `node_kinds` is the slice of [`gather_step_core::NodeKind`] values for
/// candidate nodes the caller has already resolved.  Pass an empty slice when
/// no prior resolution has been done — the classifier will fall back to
/// text-pattern matching on `query`.
#[must_use]
#[expect(
    clippy::disallowed_methods,
    reason = "one-shot owned lowercase needed for multi-pattern substring matching on query text"
)]
pub fn classify_query_shape(node_kinds: &[gather_step_core::NodeKind], query: &str) -> QueryShape {
    use gather_step_core::NodeKind;

    // ── Node-kind driven classification (highest priority) ──────────────────
    for &kind in node_kinds {
        match kind {
            NodeKind::Topic
            | NodeKind::Queue
            | NodeKind::Subject
            | NodeKind::Stream
            | NodeKind::Event => return QueryShape::EventRollout,
            NodeKind::Route => return QueryShape::RouteApiRollout,
            NodeKind::SharedSymbol | NodeKind::PayloadContract => {
                return QueryShape::SharedTypeRollout;
            }
            _ => {}
        }
    }

    // ── Text-pattern classification ─────────────────────────────────────────
    let q = query.to_ascii_lowercase();

    // Route: looks like `METHOD /path`.
    if is_route_shaped(&q) {
        return QueryShape::RouteApiRollout;
    }

    // Guard: name ends with "guard" or is a `canActivate` call.
    if q.contains("guard") || q.ends_with(".canactivate") || q == "canactivate" {
        return QueryShape::GuardRollout;
    }

    // Shared-type / DTO / payload: name suggests a contract, DTO, schema or
    // payload definition in a canonical boundary repo.
    if q.contains("dto")
        || q.contains("payload")
        || q.contains("schema")
        || q.contains("contract")
        || q.contains("record")
        || q.ends_with("type")
        || q.ends_with("interface")
    {
        return QueryShape::SharedTypeRollout;
    }

    // Event: bare topic-style names (dotted event names, `.created`, `.updated`).
    if q.contains('.') && !q.starts_with('/') {
        return QueryShape::EventRollout;
    }

    QueryShape::GenericSymbolImpact
}

fn is_route_shaped(query: &str) -> bool {
    let Some((head, _)) = query.split_once(' ') else {
        return false;
    };
    matches!(
        head.trim(),
        "get" | "post" | "put" | "patch" | "delete" | "head" | "options"
    )
}

/// Lexicographic comparator key for candidate resolution ranking.
///
/// Fields are ordered from highest to lowest priority.  Derive `Ord` gives
/// lexicographic comparison automatically; the stable symbol-ID byte fallback
/// is applied by sorting on `(CandidateKey, id_bytes)` at the call site.
///
/// ## Field priority (descending)
///
/// 1. `canonical_boundary` — candidate lives in a canonical shared-contract
///    source (virtual node, repo named `shared-contracts`, or similar).
/// 2. `consumer_repo_evidence` — at least one structural cross-repo edge
///    connects this candidate to a node in a different repo.
/// 3. `query_shape_match` — candidate's node kind aligns with the classified
///    query shape (e.g. `SharedSymbol` for `SharedTypeRollout`).
/// 4. `exact_symbol_match` — the candidate's symbol name exactly matches the
///    query string.
/// 5. `structural_repo_span` — number of distinct repos reached via structural
///    edges (decayed by 0.5 for candidates without consumer-repo evidence).
/// 6. `advisory_span` — sum of advisory / co-change file counts.
/// 7. `lexical_score` — composite lexical / search relevance score (scaled).
/// 8. stable symbol-ID byte order is applied at the call site as a final
///    tie-break; it is NOT a field here to keep this type FFI-friendly.
#[expect(
    clippy::struct_excessive_bools,
    reason = "CandidateKey is a comparator tuple newtype; each bool is a distinct priority field, not a state machine"
)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CandidateKey {
    /// Priority 1 (highest): canonical boundary source.
    pub canonical_boundary: bool,
    /// Priority 2: structural cross-repo proof exists.
    pub consumer_repo_evidence: bool,
    /// Priority 3: query-shape match.
    pub query_shape_match: bool,
    /// Priority 4: exact symbol name match.
    pub exact_symbol_match: bool,
    /// Priority 5: structural repo span (may be decayed).
    pub structural_repo_span: u32,
    /// Priority 6: advisory (co-change) span.
    pub advisory_span: u32,
    /// Priority 7: lexical / search score (scaled to a comparable integer).
    pub lexical_score: u32,
}

impl CandidateKey {
    /// Construct a key for descending sort: all fields are negated / inverted so
    /// that `BTreeMap` or `sort()` ascending order yields the highest-ranked
    /// candidate first.
    ///
    /// `structural_repo_span`, `advisory_span`, and `lexical_score` are
    /// subtracted from `u32::MAX` so that larger values sort earlier.
    #[must_use]
    #[expect(
        clippy::fn_params_excessive_bools,
        reason = "each bool is a distinct priority field in the 8-field comparator; this is a deliberate comparator factory, not a state machine"
    )]
    pub fn for_descending_sort(
        canonical_boundary: bool,
        consumer_repo_evidence: bool,
        query_shape_match: bool,
        exact_symbol_match: bool,
        structural_repo_span: u32,
        advisory_span: u32,
        lexical_score: u32,
    ) -> Self {
        Self {
            // Booleans: `true` > `false` under `Ord`; flip to put `true` first.
            canonical_boundary: !canonical_boundary,
            consumer_repo_evidence: !consumer_repo_evidence,
            query_shape_match: !query_shape_match,
            exact_symbol_match: !exact_symbol_match,
            // Numeric fields: larger is better → invert so ascending sort is correct.
            structural_repo_span: u32::MAX - structural_repo_span,
            advisory_span: u32::MAX - advisory_span,
            lexical_score: u32::MAX - lexical_score,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackItem {
    pub repo: String,
    pub file_path: String,
    pub symbol_name: String,
    pub canonical: Option<Canonical>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pack {
    pub mode: PackMode,
    pub items: Vec<PackItem>,
}

pub trait PackAssembler {
    fn assemble(&self, mode: PackMode, nodes: &[NodeData]) -> Pack;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SimplePackAssembler;

impl PackAssembler for SimplePackAssembler {
    fn assemble(&self, mode: PackMode, nodes: &[NodeData]) -> Pack {
        // Collect all items first, then apply mode-specific ordering.
        let mut items = nodes
            .iter()
            .map(|node| PackItem {
                repo: node.repo.clone(),
                file_path: node.file_path.clone(),
                symbol_name: node.name.clone(),
                canonical: canonical_for_node(node),
            })
            .collect::<Vec<_>>();

        // Mode-specific priority: re-order items so that the most contextually
        // relevant nodes for the operating mode appear first.
        match mode {
            // Planning: broader scope first — canonical (shared) symbols before
            // plain file nodes, then alphabetical by repo and file path.
            PackMode::Planning => items.sort_by(|left, right| {
                let left_canonical = u8::from(left.canonical.is_some());
                let right_canonical = u8::from(right.canonical.is_some());
                right_canonical
                    .cmp(&left_canonical)
                    .then(left.repo.cmp(&right.repo))
                    .then(left.file_path.cmp(&right.file_path))
            }),
            // Debug: tight call chain — sort by file path ascending so the
            // nearest callee appears first in a depth-first reading order.
            PackMode::Debug => items.sort_by(|left, right| {
                left.file_path
                    .cmp(&right.file_path)
                    .then(left.symbol_name.cmp(&right.symbol_name))
            }),
            // Fix: direct dependencies — surface shared/canonical symbols last
            // (they are background context) and local symbols first.
            PackMode::Fix => items.sort_by(|left, right| {
                let left_canonical = u8::from(left.canonical.is_some());
                let right_canonical = u8::from(right.canonical.is_some());
                left_canonical
                    .cmp(&right_canonical)
                    .then(left.repo.cmp(&right.repo))
                    .then(left.file_path.cmp(&right.file_path))
            }),
            // Review: balanced — repo then file path, no special weighting.
            PackMode::Review => items.sort_by(|left, right| {
                left.repo
                    .cmp(&right.repo)
                    .then(left.file_path.cmp(&right.file_path))
                    .then(left.symbol_name.cmp(&right.symbol_name))
            }),
            // ChangeImpact: blast-radius maximisation — shared/canonical
            // symbols at the top (they propagate change furthest), then stable
            // alphabetical order so the output is reproducible.
            PackMode::ChangeImpact => items.sort_by(|left, right| {
                let left_canonical = u8::from(left.canonical.is_some());
                let right_canonical = u8::from(right.canonical.is_some());
                right_canonical
                    .cmp(&left_canonical)
                    .then(left.repo.cmp(&right.repo))
                    .then(left.file_path.cmp(&right.file_path))
                    .then(left.symbol_name.cmp(&right.symbol_name))
            }),
        }

        Pack { mode, items }
    }
}

#[cfg(test)]
mod tests {
    use gather_step_core::NodeKind;

    use super::{CandidateKey, QueryShape, classify_query_shape};

    // ── QueryShape classifier ──────────────────────────────────────────────────

    #[test]
    fn classifier_detects_event_kind_from_node_kinds() {
        for kind in [
            NodeKind::Topic,
            NodeKind::Queue,
            NodeKind::Subject,
            NodeKind::Stream,
            NodeKind::Event,
        ] {
            assert_eq!(
                classify_query_shape(&[kind], "anything"),
                QueryShape::EventRollout,
                "kind {kind:?} must map to EventRollout"
            );
        }
    }

    #[test]
    fn classifier_detects_route_kind_from_node_kinds() {
        assert_eq!(
            classify_query_shape(&[NodeKind::Route], "anything"),
            QueryShape::RouteApiRollout
        );
    }

    #[test]
    fn classifier_detects_shared_symbol_kind() {
        assert_eq!(
            classify_query_shape(&[NodeKind::SharedSymbol], "anything"),
            QueryShape::SharedTypeRollout
        );
        assert_eq!(
            classify_query_shape(&[NodeKind::PayloadContract], "anything"),
            QueryShape::SharedTypeRollout
        );
    }

    #[test]
    fn classifier_falls_through_to_text_for_empty_kinds() {
        assert_eq!(
            classify_query_shape(&[], "GET /orders"),
            QueryShape::RouteApiRollout
        );
        assert_eq!(
            classify_query_shape(&[], "order.created"),
            QueryShape::EventRollout
        );
        assert_eq!(
            classify_query_shape(&[], "UserAuthGuard"),
            QueryShape::GuardRollout
        );
        assert_eq!(
            classify_query_shape(&[], "CreateOrderDto"),
            QueryShape::SharedTypeRollout
        );
        assert_eq!(
            classify_query_shape(&[], "plainFn"),
            QueryShape::GenericSymbolImpact
        );
    }

    #[test]
    fn classifier_guard_text_beats_type_suffix() {
        // "AuthGuard" contains "guard" — must detect GuardRollout before
        // SharedTypeRollout even though it ends with a capital letter.
        assert_eq!(
            classify_query_shape(&[], "AuthGuard"),
            QueryShape::GuardRollout
        );
    }

    // ── CandidateKey ordering ──────────────────────────────────────────────────

    #[test]
    fn candidate_key_canonical_boundary_beats_everything() {
        let canonical = CandidateKey::for_descending_sort(
            true,  // canonical_boundary
            false, // consumer_repo_evidence
            false, // query_shape_match
            false, // exact_symbol_match
            0,     // structural_repo_span
            0,     // advisory_span
            0,     // lexical_score
        );
        let non_canonical = CandidateKey::for_descending_sort(
            false, // canonical_boundary
            true,  // consumer_repo_evidence — all other fields higher
            true,  // query_shape_match
            true,  // exact_symbol_match
            100,   // structural_repo_span
            100,   // advisory_span
            100,   // lexical_score
        );
        assert!(
            canonical < non_canonical,
            "canonical must sort before non-canonical (ascending sort on for_descending_sort)"
        );
    }

    #[test]
    fn candidate_key_consumer_evidence_is_second_priority() {
        let with_evidence = CandidateKey::for_descending_sort(
            false, // same canonical
            true,  // consumer_repo_evidence
            false, false, 0, 0, 0,
        );
        let without_evidence = CandidateKey::for_descending_sort(
            false, false, true, // query_shape higher
            true, // exact_match higher
            100, 100, 100,
        );
        assert!(
            with_evidence < without_evidence,
            "consumer_repo_evidence must dominate query_shape_match and exact_symbol_match"
        );
    }

    #[test]
    fn candidate_key_query_shape_is_third_priority() {
        let with_shape = CandidateKey::for_descending_sort(
            false, false, true, // query_shape
            false, 0, 0, 0,
        );
        let without_shape_but_exact = CandidateKey::for_descending_sort(
            false, false, false, // no query_shape
            true,  // exact_symbol_match higher
            100, 100, 100,
        );
        assert!(
            with_shape < without_shape_but_exact,
            "query_shape_match must rank above exact_symbol_match"
        );
    }

    #[test]
    fn candidate_key_stable_across_equivalent_fields() {
        // Two keys with identical boolean fields — numeric fields decide.
        let higher_span = CandidateKey::for_descending_sort(true, true, true, true, 10, 5, 50);
        let lower_span = CandidateKey::for_descending_sort(true, true, true, true, 5, 5, 50);
        assert!(
            higher_span < lower_span,
            "larger structural_repo_span must sort first"
        );
    }
}
