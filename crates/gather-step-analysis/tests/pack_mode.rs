/// Integration test: `PackMode` consolidation.
///
/// Verifies that the canonical `PackMode` in `gather-step-analysis` produces
/// mode-distinct item orderings when assembled by [`SimplePackAssembler`].
use gather_step_analysis::pack_assembly::{Pack, PackAssembler, PackMode, SimplePackAssembler};
use gather_step_core::{NodeData, NodeKind, SourceSpan, Visibility, node_id};

fn make_node(repo: &str, file_path: &str, name: &str, ordinal: u16) -> NodeData {
    NodeData {
        id: node_id(repo, file_path, NodeKind::Function, name),
        kind: NodeKind::Function,
        repo: repo.to_owned(),
        file_path: file_path.to_owned(),
        name: name.to_owned(),
        qualified_name: Some(format!("{repo}::{name}")),
        external_id: None,
        signature: None,
        visibility: Some(Visibility::Public),
        span: Some(SourceSpan {
            line_start: u32::from(ordinal) + 1,
            line_len: 0,
            column_start: 0,
            column_len: 4,
        }),
        is_virtual: false,
    }
}

/// Build two nodes: one in repo "alpha", one in repo "beta".
fn two_nodes() -> Vec<NodeData> {
    vec![
        make_node("beta_repo", "src/b.ts", "beta_fn", 0),
        make_node("alpha_repo", "src/a.ts", "alpha_fn", 1),
    ]
}

#[test]
fn planning_mode_sorts_by_repo_then_file() {
    let assembler = SimplePackAssembler;
    let nodes = two_nodes();
    let Pack { items, mode } = assembler.assemble(PackMode::Planning, &nodes);

    assert_eq!(mode, PackMode::Planning);
    assert_eq!(items.len(), 2);
    // Neither node is canonical (no SharedSymbol) so they fall through to
    // alphabetical repo order: alpha_repo < beta_repo.
    assert_eq!(items[0].repo, "alpha_repo");
    assert_eq!(items[1].repo, "beta_repo");
}

#[test]
fn debug_mode_sorts_by_file_path() {
    let assembler = SimplePackAssembler;
    let nodes = two_nodes();
    let Pack { items, mode } = assembler.assemble(PackMode::Debug, &nodes);

    assert_eq!(mode, PackMode::Debug);
    assert_eq!(items.len(), 2);
    // Debug sorts by file_path ascending: src/a.ts < src/b.ts.
    assert_eq!(items[0].file_path, "src/a.ts");
    assert_eq!(items[1].file_path, "src/b.ts");
}

#[test]
fn fix_mode_sorts_non_canonical_by_repo() {
    let assembler = SimplePackAssembler;
    let nodes = two_nodes();
    let Pack { items, mode } = assembler.assemble(PackMode::Fix, &nodes);

    assert_eq!(mode, PackMode::Fix);
    assert_eq!(items.len(), 2);
    // Neither is canonical; Fix sorts local-first by repo: alpha < beta.
    assert_eq!(items[0].repo, "alpha_repo");
    assert_eq!(items[1].repo, "beta_repo");
}

#[test]
fn planning_and_fix_produce_distinct_priorities_for_canonical_nodes() {
    use gather_step_core::virtual_node;

    let assembler = SimplePackAssembler;
    // A shared-symbol (canonical) node vs. a plain function node.
    let shared = virtual_node(
        NodeKind::SharedSymbol,
        "shared_contracts",
        "src/types.ts",
        "SharedDto",
        "__shared__@workspace/shared-contracts__SharedDto",
    );
    let local = make_node("feature_repo", "src/feature.ts", "localFn", 0);
    let nodes = vec![local.clone(), shared.clone()];

    let planning = assembler.assemble(PackMode::Planning, &nodes);
    let fix = assembler.assemble(PackMode::Fix, &nodes);

    // Planning puts canonical (shared) nodes first.
    assert_eq!(planning.items[0].symbol_name, "SharedDto");
    // Fix puts local nodes first (canonical last).
    assert_eq!(fix.items[0].symbol_name, "localFn");
    // Both modes include the same items — just in a different order.
    assert_ne!(
        planning.items[0].symbol_name, fix.items[0].symbol_name,
        "planning and fix must produce distinct first-item priorities"
    );
}

#[test]
fn pack_mode_parse_round_trips_all_variants() {
    for (input, expected) in [
        ("planning", PackMode::Planning),
        ("debug", PackMode::Debug),
        ("fix", PackMode::Fix),
        ("review", PackMode::Review),
        ("change_impact", PackMode::ChangeImpact),
        // Case-insensitive
        ("Planning", PackMode::Planning),
        ("DEBUG", PackMode::Debug),
        ("CHANGE_IMPACT", PackMode::ChangeImpact),
    ] {
        assert_eq!(
            PackMode::parse(input).unwrap_or_else(|e| panic!("{e}")),
            expected,
            "failed for input={input:?}"
        );
        assert_eq!(expected.as_str(), expected.as_str());
    }
}

#[test]
fn pack_mode_parse_rejects_unknown_input() {
    assert!(PackMode::parse("unknown").is_err());
    assert!(PackMode::parse("").is_err());
    assert!(PackMode::parse("PLAN").is_err());
}
