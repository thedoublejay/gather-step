//! Pure-function tests for value-mirror convergence (v5.1, Task 4).
//!
//! These exercise [`converge_value_mirrors`] with no store: candidates are
//! built by hand and the emitted nodes/edges are asserted directly.

use gather_step_analysis::value_mirror::{converge_value_mirrors, emit_value_mirrors_per_repo};
use gather_step_core::{EdgeKind, NodeId, NodeKind};
use gather_step_parser::{ValueMirrorCandidate, ValueMirrorKind, ValueMirrorSurface};

fn node(n: u8) -> NodeId {
    NodeId([n; 16])
}

/// A `Literal` candidate in `repo` with the given owner/file nodes.
fn cand(repo: &str, value: &str, authoritative: bool, owner: NodeId) -> ValueMirrorCandidate {
    ValueMirrorCandidate {
        value: value.to_owned(),
        kind: ValueMirrorKind::Literal,
        repo: repo.to_owned(),
        file_path: format!("src/{repo}.ts"),
        line: 1,
        authoritative,
        owner_node_id: owner,
        file_node_id: owner,
        surface: ValueMirrorSurface::Array,
    }
}

#[test]
fn enum_and_fe_array_in_two_repos_converge() {
    let cands = vec![
        cand("service-api", "orders.statusCheck.triggered", true, node(1)),
        cand("frontend", "orders.statusCheck.triggered", false, node(2)),
    ];
    let out = converge_value_mirrors(&cands);
    assert_eq!(
        out.nodes
            .iter()
            .filter(|n| n.kind == NodeKind::ValueMirror)
            .count(),
        1
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::MirrorsValueFrom && e.source == node(2))
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::Defines && e.source == node(1))
    );
}

#[test]
fn no_authoritative_candidate_yields_nothing() {
    // Two bare arrays mirroring the same value, neither authoritative.
    let cands = vec![
        cand("frontend", "orders.statusCheck.triggered", false, node(1)),
        cand("mobile", "orders.statusCheck.triggered", false, node(2)),
    ];
    let out = converge_value_mirrors(&cands);
    assert!(out.nodes.is_empty());
    assert!(out.edges.is_empty());
}

#[test]
fn same_repo_only_yields_nothing() {
    // Authoritative + mirror, but both in the same repo — fails the ≥2-repo gate.
    let cands = vec![
        cand("service-api", "orders.statusCheck.triggered", true, node(1)),
        cand(
            "service-api",
            "orders.statusCheck.triggered",
            false,
            node(2),
        ),
    ];
    let out = converge_value_mirrors(&cands);
    assert!(out.nodes.is_empty());
    assert!(out.edges.is_empty());
}

#[test]
fn mode_b_enum_ref_resolves_across_three_repos() {
    // Repo A: an enum-member REF (`[EventType.StatusChanged]`).
    let referencing_candidate = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "repo-a".to_owned(),
        file_path: "src/a.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    // Repo B: the authoritative enum-member DEF mapping to the string value.
    let authoritative_def = ValueMirrorCandidate {
        value: "orders.statusCheck.triggered".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "EventType".to_owned(),
            member: "StatusChanged".to_owned(),
        },
        repo: "repo-b".to_owned(),
        file_path: "src/b.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Array,
    };
    // Repo C: a plain literal mirror of the same string value.
    let literal = cand("repo-c", "orders.statusCheck.triggered", false, node(3));

    let out = converge_value_mirrors(&[referencing_candidate, authoritative_def, literal]);

    // All three converge on a single ValueMirror node.
    assert_eq!(
        out.nodes
            .iter()
            .filter(|n| n.kind == NodeKind::ValueMirror)
            .count(),
        1
    );
    // The enum DEF is the authoritative source → Defines.
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::Defines && e.source == node(2))
    );
    // The enum REF and the literal mirror → MirrorsValueFrom.
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::MirrorsValueFrom && e.source == node(1))
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::MirrorsValueFrom && e.source == node(3))
    );
}

#[test]
fn unresolvable_enum_ref_is_dropped() {
    // An enum REF whose DEF is absent cannot resolve, plus an authoritative
    // literal in another repo. The ref is dropped (cannot converge), so the
    // remaining literal is alone in its group → no node, no edges.
    let enum_ref = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "repo-a".to_owned(),
        file_path: "src/a.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    let literal = cand("repo-b", "some.other.value.entirely", true, node(2));

    let out = converge_value_mirrors(&[enum_ref, literal]);
    assert!(out.nodes.is_empty());
    assert!(out.edges.is_empty());
}

// --- Per-repo emission (what the indexer calls; no cross-repo gate) ---

#[test]
fn per_repo_emits_single_repo_candidate_with_no_gate() {
    // A lone authoritative literal in one repo. The gated fn emits nothing
    // (fails the ≥2-repo gate); the per-repo fn emits the shared node + edge so
    // a second repo indexed later converges via the shared id.
    let cands = vec![cand(
        "backend",
        "orders.statusCheck.triggered",
        true,
        node(1),
    )];

    assert!(converge_value_mirrors(&cands).nodes.is_empty());

    let out = emit_value_mirrors_per_repo(&cands);
    assert_eq!(
        out.nodes
            .iter()
            .filter(|n| n.kind == NodeKind::ValueMirror)
            .count(),
        1
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::Defines && e.source == node(1))
    );
}

#[test]
fn per_repo_dedups_node_but_emits_edge_per_owner() {
    // Two same-repo candidates mirroring one value collapse to one node with an
    // edge each — proving in-call node dedup and one-edge-per-owner emission.
    let cands = vec![
        cand("backend", "orders.statusCheck.triggered", true, node(1)),
        cand("backend", "orders.statusCheck.triggered", false, node(2)),
    ];
    let out = emit_value_mirrors_per_repo(&cands);
    assert_eq!(out.nodes.len(), 1);
    assert_eq!(out.edges.len(), 2);
    assert!(out.edges.iter().all(|e| e.target == out.nodes[0].id));
}

#[test]
fn per_repo_resolves_mode_b_intra_repo_and_drops_cross_repo_ref() {
    // An enum DEF + matching enum REF in the SAME repo resolve intra-repo and
    // both emit. A second enum REF whose DEF lives in a DIFFERENT repo cannot
    // resolve at index time (per-repo) and is dropped.
    let enum_def = ValueMirrorCandidate {
        value: "orders.statusCheck.triggered".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "EventType".to_owned(),
            member: "StatusChanged".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/events.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    let intra_repo_ref = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/allowlist.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Array,
    };
    // This ref's DEF is NOT in this candidate set (it lives in another repo) →
    // unresolvable intra-repo → dropped.
    let unresolvable_ref = ValueMirrorCandidate {
        value: "OtherMember".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "OtherEnum".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/other.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(3),
        file_node_id: node(3),
        surface: ValueMirrorSurface::Array,
    };

    let out = emit_value_mirrors_per_repo(&[enum_def, intra_repo_ref, unresolvable_ref]);
    assert_eq!(out.nodes.len(), 1);
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::Defines && e.source == node(1))
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::MirrorsValueFrom && e.source == node(2))
    );
    // The cross-repo ref produced no edge.
    assert!(out.edges.iter().all(|e| e.source != node(3)));
}

// --- Enum-subset array (v5.1, Task 16): intra-repo enum-ref array converges ---

/// An authoritative `EnumMemberDef` + an `EnumMemberRef` array mirror of the
/// SAME `enum_qn`, both in ONE repo (no second repo), converge: one shared
/// `__value__` node + a `MirrorsValueFrom` edge for the ref. The ≥2-repo gate is
/// relaxed because the mirror is a pure enum-subset of its own enum.
#[test]
fn intra_repo_enum_subset_array_converges_against_own_enum() {
    let enum_def = ValueMirrorCandidate {
        value: "orders.statusCheck.triggered".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "EventType".to_owned(),
            member: "StatusChanged".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/events.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    let subset_ref = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/allowlist.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Array,
    };

    let out = converge_value_mirrors(&[enum_def, subset_ref]);

    assert_eq!(
        out.nodes
            .iter()
            .filter(|n| n.kind == NodeKind::ValueMirror)
            .count(),
        1
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::Defines && e.source == node(1))
    );
    assert!(
        out.edges
            .iter()
            .any(|e| e.kind == EdgeKind::MirrorsValueFrom && e.source == node(2))
    );
}

/// Negative (a): a same-repo `Literal` (Mode A) mirror group does NOT get the
/// enum-subset relaxation — bare arrays still need ≥2 repos.
#[test]
fn intra_repo_literal_mirror_group_stays_zero() {
    let cands = vec![
        cand("backend", "orders.statusCheck.triggered", true, node(1)),
        cand("backend", "orders.statusCheck.triggered", false, node(2)),
    ];
    let out = converge_value_mirrors(&cands);
    assert!(out.nodes.is_empty());
    assert!(out.edges.is_empty());
}

/// Negative (b): a same-repo group mixing an `EnumMemberRef` AND a `Literal`
/// mirror surface is NOT purely enum-ref → not relaxed → 0.
#[test]
fn intra_repo_mixed_enum_ref_and_literal_stays_zero() {
    let enum_def = ValueMirrorCandidate {
        value: "orders.statusCheck.triggered".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "EventType".to_owned(),
            member: "StatusChanged".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/events.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    let subset_ref = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/allowlist.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Array,
    };
    let literal = cand("backend", "orders.statusCheck.triggered", false, node(3));

    let out = converge_value_mirrors(&[enum_def, subset_ref, literal]);
    assert!(out.nodes.is_empty());
    assert!(out.edges.is_empty());
}

/// Negative (c): a same-repo group whose `EnumMemberRef`s carry TWO different
/// `enum_qn`s that resolve to the same value must NOT relax (and must not
/// false-link). Two authoritative defs (different enums, same value) anchor it.
#[test]
fn intra_repo_enum_refs_from_two_enum_qns_stays_zero() {
    let def_a = ValueMirrorCandidate {
        value: "shared.value".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "Status".to_owned(),
            member: "A".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/status.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    let def_b = ValueMirrorCandidate {
        value: "shared.value".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "Mode".to_owned(),
            member: "B".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/mode.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Array,
    };
    let ref_a = ValueMirrorCandidate {
        value: "A".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "Status".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/allow_a.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(3),
        file_node_id: node(3),
        surface: ValueMirrorSurface::Array,
    };
    let ref_b = ValueMirrorCandidate {
        value: "B".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "Mode".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/allow_b.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(4),
        file_node_id: node(4),
        surface: ValueMirrorSurface::Array,
    };

    let out = converge_value_mirrors(&[def_a, def_b, ref_a, ref_b]);
    assert!(out.nodes.is_empty());
    assert!(out.edges.is_empty());
}

// --- Guard surface (v5.1, Task 11): intra-repo guard↔enum convergence ---

/// A `Guard` enum-member REF + its authoritative `EnumMemberDef`, BOTH in the
/// same repo, converge under the gated fn (gate relaxed for guards) and emit a
/// `GuardsEnumValue` edge carrying `guard_has_default`.
#[test]
fn intra_repo_guard_ref_converges_with_guards_enum_value_edge() {
    let guard_ref = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "repo-a".to_owned(),
        file_path: "src/guard.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Guard { has_default: false },
    };
    let enum_def = ValueMirrorCandidate {
        value: "orders.statusCheck.triggered".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "EventType".to_owned(),
            member: "StatusChanged".to_owned(),
        },
        repo: "repo-a".to_owned(),
        file_path: "src/events.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Array,
    };

    let out = converge_value_mirrors(&[guard_ref, enum_def]);

    // One shared __value__ node despite both candidates being in one repo.
    assert_eq!(
        out.nodes
            .iter()
            .filter(|n| n.kind == NodeKind::ValueMirror)
            .count(),
        1
    );
    // The guard endpoint emits GuardsEnumValue with guard_has_default == Some(false).
    let guard_edge = out
        .edges
        .iter()
        .find(|e| e.kind == EdgeKind::GuardsEnumValue && e.source == node(1))
        .expect("guard endpoint must emit a GuardsEnumValue edge");
    assert_eq!(guard_edge.metadata.guard_has_default, Some(false));
    // The authoritative enum def keeps Defines with no guard flag.
    let def_edge = out
        .edges
        .iter()
        .find(|e| e.kind == EdgeKind::Defines && e.source == node(2))
        .expect("authoritative enum def must emit a Defines edge");
    assert_eq!(def_edge.metadata.guard_has_default, None);
}

/// A `Guard { has_default: true }` candidate through the per-repo indexer path
/// emits `GuardsEnumValue` with `guard_has_default == Some(true)`.
#[test]
fn per_repo_guard_emits_guards_enum_value_edge() {
    let enum_def = ValueMirrorCandidate {
        value: "orders.statusCheck.triggered".to_owned(),
        kind: ValueMirrorKind::EnumMemberDef {
            enum_qn: "EventType".to_owned(),
            member: "StatusChanged".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/events.ts".to_owned(),
        line: 1,
        authoritative: true,
        owner_node_id: node(1),
        file_node_id: node(1),
        surface: ValueMirrorSurface::Array,
    };
    let guard_ref = ValueMirrorCandidate {
        value: "StatusChanged".to_owned(),
        kind: ValueMirrorKind::EnumMemberRef {
            enum_qn: "EventType".to_owned(),
        },
        repo: "backend".to_owned(),
        file_path: "src/guard.ts".to_owned(),
        line: 1,
        authoritative: false,
        owner_node_id: node(2),
        file_node_id: node(2),
        surface: ValueMirrorSurface::Guard { has_default: true },
    };

    let out = emit_value_mirrors_per_repo(&[enum_def, guard_ref]);
    let guard_edge = out
        .edges
        .iter()
        .find(|e| e.kind == EdgeKind::GuardsEnumValue && e.source == node(2))
        .expect("per-repo guard endpoint must emit a GuardsEnumValue edge");
    assert_eq!(guard_edge.metadata.guard_has_default, Some(true));
}

/// Part-1 regression: a cross-repo array+enum group still converges as
/// `MirrorsValueFrom` with `guard_has_default == None` (no guard surface).
#[test]
fn cross_repo_array_keeps_mirrors_value_from_with_no_guard_flag() {
    let cands = vec![
        cand("service-api", "orders.statusCheck.triggered", true, node(1)),
        cand("frontend", "orders.statusCheck.triggered", false, node(2)),
    ];
    let out = converge_value_mirrors(&cands);
    let mirror_edge = out
        .edges
        .iter()
        .find(|e| e.kind == EdgeKind::MirrorsValueFrom && e.source == node(2))
        .expect("array mirror endpoint keeps MirrorsValueFrom");
    assert_eq!(mirror_edge.metadata.guard_has_default, None);
}
