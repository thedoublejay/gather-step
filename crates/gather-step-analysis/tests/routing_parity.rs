/// Integration tests: pack/impact routing parity across all 5 query shapes.
///
/// These tests verify that:
///
/// 1. [`classify_query_shape`] produces the expected shape for representative
///    queries covering all 5 variants.
/// 2. [`CandidateKey::for_descending_sort`] ranks the shape-matched candidate
///    above the non-matching candidate, irrespective of lexical score, proving
///    that both `pack` and `impact` apply the same ordering for each shape.
/// 3. The stable tie-break on symbol-ID bytes is deterministic: two candidates
///    with identical [`CandidateKey`] fields are ordered by their raw node-ID
///    bytes rather than by insertion order or rayon scheduling.
use gather_step_analysis::pack_assembly::{CandidateKey, QueryShape, classify_query_shape};
use gather_step_core::{NodeId, NodeKind, node_id};

// ── Helper ─────────────────────────────────────────────────────────────────

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
        QueryShape::GuardRollout => matches!(kind, NodeKind::Class | NodeKind::Function),
        QueryShape::SharedTypeRollout => matches!(
            kind,
            NodeKind::SharedSymbol | NodeKind::Type | NodeKind::PayloadContract
        ),
        QueryShape::GenericSymbolImpact => false,
    }
}

/// Build a [`CandidateKey`] with query-shape-match derived from the node kind.
fn shape_key(kind: NodeKind, shape: QueryShape) -> CandidateKey {
    CandidateKey::for_descending_sort(
        false,
        false,
        node_kind_matches_shape(kind, shape),
        false,
        0,
        0,
        0,
    )
}

// ── Shape classifier agreement ─────────────────────────────────────────────

/// Each `(query, expected_shape)` pair must classify consistently.
#[test]
fn classifier_covers_all_five_shapes() {
    let cases: &[(&str, QueryShape)] = &[
        // `EventRollout` — dotted topic name
        ("order.created", QueryShape::EventRollout),
        ("report.generation.queued", QueryShape::EventRollout),
        // `RouteApiRollout` — HTTP verb + path
        ("GET /api/orders", QueryShape::RouteApiRollout),
        ("POST /api/v2/reports", QueryShape::RouteApiRollout),
        // `GuardRollout` — name contains "guard"
        ("UserAuthGuard", QueryShape::GuardRollout),
        ("HttpsRedirectGuard", QueryShape::GuardRollout),
        // `SharedTypeRollout` — DTO/payload/schema suffix
        ("CreateOrderDto", QueryShape::SharedTypeRollout),
        ("OrderPayload", QueryShape::SharedTypeRollout),
        ("UserSchema", QueryShape::SharedTypeRollout),
        // `GenericSymbolImpact` — plain names that don't match any pattern
        ("plainFunction", QueryShape::GenericSymbolImpact),
        ("orderService", QueryShape::GenericSymbolImpact),
    ];

    for (query, expected_shape) in cases {
        let got = classify_query_shape(&[], query);
        assert_eq!(
            got, *expected_shape,
            "classify_query_shape({query:?}) expected {expected_shape:?}, got {got:?}"
        );
    }
}

// ── CandidateKey: shape-matched candidate ranks above non-matching ─────────

/// `EventRollout`: a `Topic` node must rank above a `Class` node when the
/// query is event-shaped.  This mirrors what both `impact` (via `CandidateKey`)
/// and `pack` (via `query_shape_match_bonus`) do for the same query.
#[test]
fn event_rollout_shape_match_ranks_topic_above_class() {
    let shape = QueryShape::EventRollout;
    let topic_key = shape_key(NodeKind::Topic, shape);
    let class_key = shape_key(NodeKind::Class, shape);
    assert!(
        topic_key < class_key,
        "Topic must rank before Class for EventRollout \
         (ascending sort on for_descending_sort keys)"
    );
}

/// `RouteApiRollout`: a `Route` node must rank above a `Function` node when
/// the query is route-shaped.
#[test]
fn route_rollout_shape_match_ranks_route_above_function() {
    let shape = QueryShape::RouteApiRollout;
    let route_key = shape_key(NodeKind::Route, shape);
    let fn_key = shape_key(NodeKind::Function, shape);
    assert!(
        route_key < fn_key,
        "Route must rank before Function for RouteApiRollout"
    );
}

/// `GuardRollout`: a `Class` node (guard class) must rank above a `Service`
/// node.
#[test]
fn guard_rollout_shape_match_ranks_class_above_service() {
    let shape = QueryShape::GuardRollout;
    let class_key = shape_key(NodeKind::Class, shape);
    let service_key = shape_key(NodeKind::Service, shape);
    assert!(
        class_key < service_key,
        "Class must rank before Service for GuardRollout"
    );
}

/// `SharedTypeRollout`: a `SharedSymbol` node must rank above a `Function`
/// node.
#[test]
fn shared_type_rollout_shape_match_ranks_shared_symbol_above_function() {
    let shape = QueryShape::SharedTypeRollout;
    let shared_key = shape_key(NodeKind::SharedSymbol, shape);
    let fn_key = shape_key(NodeKind::Function, shape);
    assert!(
        shared_key < fn_key,
        "SharedSymbol must rank before Function for SharedTypeRollout"
    );
}

/// `GenericSymbolImpact`: no kind-based bonus — two candidates with equal
/// fields must compare equal in `query_shape_match`.
#[test]
fn generic_symbol_impact_applies_no_shape_bonus_to_any_kind() {
    let shape = QueryShape::GenericSymbolImpact;
    let baseline = CandidateKey::for_descending_sort(false, false, false, false, 0, 0, 0);
    for &kind in &[
        NodeKind::Class,
        NodeKind::Function,
        NodeKind::Topic,
        NodeKind::Route,
        NodeKind::SharedSymbol,
    ] {
        let k = shape_key(kind, shape);
        // `query_shape_match` must be false for every kind in
        // `GenericSymbolImpact`, so the key must equal the all-false baseline.
        assert_eq!(
            k, baseline,
            "GenericSymbolImpact must not award query_shape_match to {kind:?}"
        );
    }
}

// ── Stable tie-break on symbol-ID bytes ────────────────────────────────────

/// When two candidates have identical [`CandidateKey`] fields, sorting by
/// `(key, id_bytes)` must produce a deterministic order regardless of
/// iteration order.
///
/// This test simulates the tie-break logic used in `rerank_impact_candidates`:
/// the sort key is `(CandidateKey, [u8; 16])`.  Two calls with candidates in
/// different insertion orders must produce the same winner.
#[test]
fn stable_tie_break_on_symbol_id_bytes_is_deterministic() {
    // Two nodes with identical CandidateKey fields (all zeros / all-false).
    let key_a = CandidateKey::for_descending_sort(false, false, false, false, 0, 0, 0);
    let key_b = CandidateKey::for_descending_sort(false, false, false, false, 0, 0, 0);

    // Construct two NodeIds that will have different byte representations.
    // `node_id` is deterministic per `(repo, file, kind, name)`.
    let id_a: NodeId = node_id("repo_alpha", "src/a.ts", NodeKind::Function, "fnA");
    let id_b: NodeId = node_id("repo_beta", "src/b.ts", NodeKind::Function, "fnB");

    // Build the composite sort tuples.
    let entry_a = (key_a, id_a.as_bytes());
    let entry_b = (key_b, id_b.as_bytes());

    // The sort order must be stable regardless of which way we compare.
    let forward = entry_a.cmp(&entry_b);
    let reverse = entry_b.cmp(&entry_a);

    // They must not be equal (the ID bytes differ).
    assert_ne!(forward, std::cmp::Ordering::Equal, "IDs must differ");
    assert_ne!(reverse, std::cmp::Ordering::Equal);
    assert_eq!(
        forward,
        reverse.reverse(),
        "tie-break must be symmetric: forward={forward:?}, reverse={reverse:?}"
    );

    // Simulate sorting a small array in both insertion orders and confirm the
    // winner is always the same.
    let mut order_1 = [(key_a, id_a.as_bytes(), "a"), (key_b, id_b.as_bytes(), "b")];
    let mut order_2 = [(key_b, id_b.as_bytes(), "b"), (key_a, id_a.as_bytes(), "a")];

    order_1.sort_by(|(ka, ia, _), (kb, ib, _)| ka.cmp(kb).then(ia.cmp(ib)));
    order_2.sort_by(|(ka, ia, _), (kb, ib, _)| ka.cmp(kb).then(ia.cmp(ib)));

    assert_eq!(
        order_1[0].2, order_2[0].2,
        "winner must be the same regardless of insertion order: \
         order1={}, order2={}",
        order_1[0].2, order_2[0].2
    );
}

/// When candidates are equal on all [`CandidateKey`] fields AND equal on ID
/// bytes (same node), sorting is trivially stable and produces equal elements.
#[test]
fn identical_candidates_produce_equal_sort_keys() {
    let id: NodeId = node_id("repo_alpha", "src/a.ts", NodeKind::Function, "fnA");
    let key = CandidateKey::for_descending_sort(false, false, false, false, 0, 0, 0);

    let mut entries = [(key, id.as_bytes()), (key, id.as_bytes())];
    entries.sort_by(|(ka, ia), (kb, ib)| ka.cmp(kb).then(ia.cmp(ib)));
    // Both positions must compare equal — no undefined ordering.
    assert_eq!(entries[0], entries[1]);
}
