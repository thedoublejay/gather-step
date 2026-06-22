//! Value-mirror convergence (v5.1, Task 4).
//!
//! The parser captures [`ValueMirrorCandidate`]s — string literals, enum
//! members, and enum-member references that look like a shared transport value
//! duplicated across repos. This module converges them: candidates that share
//! a resolved value and span at least two repos (with at least one
//! authoritative definition) collapse onto a single deterministic
//! [`gather_step_core::NodeKind::ValueMirror`] virtual node. Each owner gets an
//! edge to that node — [`gather_step_core::EdgeKind::Defines`] from
//! authoritative owners, otherwise
//! [`gather_step_core::EdgeKind::MirrorsValueFrom`].
//!
//! Mode B (`EnumMemberRef`) candidates are resolved to the member's string
//! value via the authoritative `EnumMemberDef`s captured anywhere in the
//! candidate set; unresolvable refs are dropped because they cannot converge.
//!
//! ## Two entry points: workspace-ideal vs. per-repo
//!
//! [`converge_value_mirrors`] is the **workspace-ideal contract**: given *all*
//! candidates across *all* repos, it emits a node + edges only for groups that
//! have an authoritative definition AND span ≥2 repos. That gate is only
//! meaningful when every repo's candidates are in scope at once.
//!
//! Indexing, however, is strictly **per-repo** — a single `index_repo` call
//! sees only one repo's candidates and can never satisfy the ≥2-repo gate. So
//! the indexer instead calls [`emit_value_mirrors_per_repo`], which drops the
//! cross-repo gate and emits a `__value__<canonical>` node (keyed on the shared
//! deterministic id) plus an edge per resolved candidate. Repos then converge
//! *structurally in the graph store* via the shared node id — exactly how Kafka
//! topics and shared symbols already converge cross-repo. The "is this value
//! actually mirrored across repos / add-and-forget" precision call moves to
//! pr-review / query time, not index time. Single-repo orphan `__value__` nodes
//! are acceptable (same as orphan Kafka events); the specificity gate on
//! candidates bounds the count.
//!
//! ### Mode-B limitation at index time (per-repo)
//!
//! Because [`emit_value_mirrors_per_repo`] resolves enum-member refs only
//! against `EnumMemberDef`s captured **in the same repo**, an `EnumMemberRef`
//! whose backing `EnumMemberDef` lives in a *different* repo cannot be resolved
//! at index time and is dropped. This is a known forward-looking limitation: a
//! backend repo's enum def + its own enum-ref allowlist resolve intra-repo and
//! emit `__value__`/`MirrorsValueFrom`; a frontend repo's literal-string mirror
//! emits to the same shared id and converges. Mode-A literals need no enum, so
//! they always converge cross-repo. Cross-repo Mode-B resolution is deferred to
//! a future workspace pass (the gated [`converge_value_mirrors`] contract).
//!
//! The functions live in the parser crate (next to [`ValueMirrorCandidate`])
//! rather than `gather-step-analysis` so that `gather-step-storage`'s indexer
//! can call them during pass-2 materialization: analysis depends on storage, so
//! placing them there would force a dependency cycle. `gather-step-analysis`
//! re-exports them to keep the documented public API stable.

use rustc_hash::{FxHashMap, FxHashSet};

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, ResolverStrategy,
    VIRTUAL_NODE_REPO, value_mirror_qn, virtual_node,
};

use crate::ts_js_oxc::{ValueMirrorCandidate, ValueMirrorKind, ValueMirrorSurface};

/// Nodes and edges produced by converging a set of value-mirror candidates.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ValueMirrorConvergence {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

/// Build the deterministic `ValueMirror` virtual node for a canonical value qn.
fn value_mirror_node(qn: &str) -> NodeData {
    virtual_node(NodeKind::ValueMirror, VIRTUAL_NODE_REPO, qn, qn, qn)
}

/// Build an owner → value-node edge tagged with the value-mirror resolver.
///
/// `guard_has_default` is `Some(_)` only for `GuardsEnumValue` guard endpoints;
/// `None` for `Defines`/`MirrorsValueFrom` array endpoints. `enum_qn` carries
/// the owning enum's bare name for enum-ref surfaces and authoritative enum
/// `Defines` edges (Task 17 enum-scoped completeness); `None` for Mode-A
/// literal mirror edges.
fn value_mirror_edge(
    owner: NodeId,
    value_node: NodeId,
    kind: EdgeKind,
    owner_file: NodeId,
    guard_has_default: Option<bool>,
    enum_qn: Option<String>,
) -> EdgeData {
    let mut metadata = EdgeMetadata::default();
    metadata.set_resolver_strategy(ResolverStrategy::ValueMirror);
    metadata.guard_has_default = guard_has_default;
    metadata.enum_qn = enum_qn;
    EdgeData {
        source: owner,
        target: value_node,
        kind,
        metadata,
        owner_file,
        is_cross_file: true,
    }
}

/// Group candidates by their resolved canonical value qn.
///
/// Mode-B (`EnumMemberRef`) candidates resolve to the member's string value via
/// the authoritative `EnumMemberDef`s present in `candidates`. Both the
/// `EnumMemberRef.enum_qn` (the bare receiver name, e.g. `EventType`) and the
/// `EnumMemberDef.enum_qn` (the bare enum declaration name) use the same
/// representation, so the `(enum_qn, member)` join is direct with no
/// normalization. Unresolvable refs are dropped (they cannot converge).
fn group_by_resolved_value(
    candidates: &[ValueMirrorCandidate],
) -> FxHashMap<String, Vec<&ValueMirrorCandidate>> {
    let auth_value: FxHashMap<(&str, &str), &str> = candidates
        .iter()
        .filter_map(|c| match &c.kind {
            ValueMirrorKind::EnumMemberDef { enum_qn, member } => {
                Some(((enum_qn.as_str(), member.as_str()), c.value.as_str()))
            }
            _ => None,
        })
        .collect();

    let resolved = |c: &ValueMirrorCandidate| -> Option<String> {
        match &c.kind {
            ValueMirrorKind::Literal | ValueMirrorKind::EnumMemberDef { .. } => {
                Some(c.value.clone())
            }
            ValueMirrorKind::EnumMemberRef { enum_qn } => auth_value
                .get(&(enum_qn.as_str(), c.value.as_str()))
                .map(|v| (*v).to_owned()),
        }
    };

    let mut by_qn: FxHashMap<String, Vec<&ValueMirrorCandidate>> = FxHashMap::default();
    for c in candidates {
        if let Some(v) = resolved(c) {
            by_qn.entry(value_mirror_qn(&v)).or_default().push(c);
        }
    }
    by_qn
}

/// Emit a `ValueMirror` node (deduped by id) + an owner→node edge per candidate
/// in `group`. A `Guard` surface emits `GuardsEnumValue` (carrying
/// `guard_has_default`); otherwise authoritative owners get `Defines` and the
/// rest `MirrorsValueFrom` (both with `guard_has_default = None`).
fn emit_group(qn: &str, group: &[&ValueMirrorCandidate], out: &mut ValueMirrorConvergence) {
    let vnode = value_mirror_node(qn);
    for c in group {
        let (kind, guard_has_default) = match c.surface {
            ValueMirrorSurface::Guard { has_default } => {
                (EdgeKind::GuardsEnumValue, Some(has_default))
            }
            ValueMirrorSurface::Array if c.authoritative => (EdgeKind::Defines, None),
            ValueMirrorSurface::Array => (EdgeKind::MirrorsValueFrom, None),
        };
        // Carry the owning enum's bare name on authoritative enum `Defines`
        // edges and on enum-member-ref surfaces, so pr-review can scope the
        // add-and-forget completeness check to the same enum (Task 17). Mode-A
        // literal mirrors have no enum and stay `None`.
        let enum_qn = match &c.kind {
            ValueMirrorKind::EnumMemberDef { enum_qn, .. }
            | ValueMirrorKind::EnumMemberRef { enum_qn } => Some(enum_qn.clone()),
            ValueMirrorKind::Literal => None,
        };
        out.edges.push(value_mirror_edge(
            c.owner_node_id,
            vnode.id,
            kind,
            c.file_node_id,
            guard_has_default,
            enum_qn,
        ));
    }
    out.nodes.push(vnode);
}

/// Workspace-ideal convergence (the documented contract).
///
/// Given *all* candidates across *all* repos, emit a node + its edges only for
/// groups that have at least one authoritative candidate AND either span at
/// least two distinct repos OR contain a `Guard` surface (a guard switch keyed
/// on an enum is a real intra-repo consumer). Unresolvable `EnumMemberRef`s are
/// dropped.
///
/// This is **not** what the indexer calls — indexing is per-repo and a single
/// repo can never satisfy the ≥2-repo gate. See [`emit_value_mirrors_per_repo`]
/// and the module docs. This entry point exists as the workspace-pass contract.
#[must_use]
pub fn converge_value_mirrors(candidates: &[ValueMirrorCandidate]) -> ValueMirrorConvergence {
    let mut out = ValueMirrorConvergence::default();
    for (qn, group) in group_by_resolved_value(candidates) {
        let has_auth = group.iter().any(|c| c.authoritative);
        let has_guard = group
            .iter()
            .any(|c| matches!(c.surface, ValueMirrorSurface::Guard { .. }));
        let repos: FxHashSet<&str> = group.iter().map(|c| c.repo.as_str()).collect();
        // An intra-repo enum-subset array converges against its own enum: the
        // group's non-authoritative mirror surfaces are all `EnumMemberRef` of a
        // single `enum_qn`. A `Literal` mirror surface, or refs from more than
        // one `enum_qn`, disqualifies it (no Mode-A relax, no false-link).
        let is_enum_subset_array = {
            let mut ref_enums: FxHashSet<&str> = FxHashSet::default();
            let mut all_refs = false;
            let purely_enum_ref =
                group
                    .iter()
                    .filter(|c| !c.authoritative)
                    .all(|c| match &c.kind {
                        ValueMirrorKind::EnumMemberRef { enum_qn } => {
                            ref_enums.insert(enum_qn.as_str());
                            all_refs = true;
                            true
                        }
                        _ => false,
                    });
            purely_enum_ref && all_refs && ref_enums.len() == 1
        };
        // A guard surface converges intra-repo (a guard switch keyed on an enum
        // is a real consumer in its own repo); bare arrays still need ≥2 repos
        // unless they are a pure enum-subset of their own enum.
        if !has_auth || (repos.len() < 2 && !has_guard && !is_enum_subset_array) {
            continue;
        }
        emit_group(&qn, &group, &mut out);
    }
    out
}

/// Per-repo emission for the indexer (no cross-repo gate).
///
/// Resolves Mode-B enum refs **intra-repo only** (the `candidates` passed are a
/// single repo's), groups by canonical value, and emits a `__value__` node
/// (keyed on the shared deterministic id) + an edge per candidate for every
/// group. There is deliberately NO ≥2-repo or has-auth gate: a single repo
/// cannot see other repos, so repos converge structurally in the graph store
/// via the shared node id — repo A emits the node + a `Defines` edge, repo B
/// emits the same-id node (idempotent) + a `MirrorsValueFrom` edge. Nodes are
/// deduped by id within the call. Precision (is this actually mirrored
/// cross-repo / add-and-forget) is a query-time concern.
#[must_use]
pub fn emit_value_mirrors_per_repo(candidates: &[ValueMirrorCandidate]) -> ValueMirrorConvergence {
    let mut out = ValueMirrorConvergence::default();
    for (qn, group) in group_by_resolved_value(candidates) {
        emit_group(&qn, &group, &mut out);
    }
    out
}
