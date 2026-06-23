//! Value-mirror risk detection — v5.1 Task 5.
//!
//! Task 4 materializes "mirrored values" as a graph: a shared deterministic
//! `__value__<canonical>` [`NodeKind::ValueMirror`] node, with
//! [`EdgeKind::Defines`] edges from authoritative owners (enum/union members,
//! named consts) and [`EdgeKind::MirrorsValueFrom`] edges from non-authoritative
//! mirror owners (allowlist arrays, FE category maps). Emission is per-repo via
//! shared ids, so the persisted PR-branch (head) graph already has these edges
//! from every indexed repo converging on the shared `__value__` nodes.
//!
//! This module is the cross-repo PRECISION layer that Task 4 deferred to
//! query/PR-review time. It surfaces two risks, both as
//! [`RemovedSurfaceRisk`] entries:
//!
//! - **`value_mirror_incomplete` (add-and-forget, primary).** For each
//!   AUTHORITATIVE value ADDED in the PR (a `Defines` edge owner→`__value__`
//!   present in the head graph but not the baseline), find the owner group (the
//!   enum/union/owner = the `Defines` edge source). Gather that group's EXISTING
//!   `MirrorsValueFrom` mirror surfaces in the head graph. For each surface that
//!   mirrors ≥1 OTHER member of the same group but NOT the newly-added value,
//!   emit a risk naming the mirror surface (repo + file) and the missing value.
//!   This is a SET DIFFERENCE over established mirror surfaces — a new value has
//!   no `MirrorsValueFrom` edge yet, so no edge-walk on the new value is needed.
//!
//! - **`enum_guard_incomplete` (add-and-forget, guard surfaces — v5.1 Task 14).**
//!   Identical add-and-forget set-difference as `value_mirror_incomplete`, but
//!   over `GuardsEnumValue` surfaces (a `switch`/`if` over an enum, captured by
//!   Tasks 11–13). A guard surface that handles ≥1 OTHER member of the owner
//!   group but NOT the newly-added value is flagged — unless its edge carries
//!   `guard_has_default == Some(true)`, in which case an explicit
//!   `default:`/`case _:` already handles new members and the surface is exempt.
//!
//! - **`value_mirror` (modified mirrored value, secondary).** For each
//!   authoritative value CHANGED or REMOVED in the PR (a `Defines` edge
//!   owner→`__value__` present in the baseline but not the head) that still has
//!   surviving `MirrorsValueFrom` edges in the head graph, emit a risk per
//!   un-updated mirror surface. This is the edge-walk case.
//!
//! # Precision guards
//!
//! - A single-repo orphan `__value__` node (no cross-repo mirror) is fine: it
//!   simply has no `MirrorsValueFrom`/`GuardsEnumValue` edges, so it produces no
//!   risk.
//! - A mirror/guard surface that covers NO other member of the owner group is
//!   not flagged for a newly-added member (it is not an "established" surface of
//!   that group), avoiding a false positive on unrelated surfaces.
//! - A guard surface with an explicit default/catch-all branch
//!   (`guard_has_default == Some(true)`) is exempt: the default already handles
//!   any newly-added member, so a missing case is not a risk.
//!
//! # Cost
//!
//! Detection scans every persisted [`NodeKind::ValueMirror`] node in the
//! baseline and head graphs (a type-bucketed index lookup), not just the PR's
//! changed surface — a deliberate consequence of Task 4's ungated per-repo
//! emission, which keeps precision here rather than at index time. The kind is
//! narrow (canonical enum/union/const values), and the two linear passes are
//! dwarfed by the full PR-branch reindex `pr-review` performs in the same run,
//! so the cost is bounded in practice.

use std::collections::BTreeSet;

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::GraphStore;
use rustc_hash::FxHashMap;

use crate::pr_review::delta_report::{RemovedSurfaceRisk, RiskSeverity};

/// Minimum number of shared canonical values for two differently-named enums to
/// be treated as a hand-mirrored copy of the same set (H3). Set deliberately
/// high (medium FP-risk): two enums sharing only 1–2 values are coincidental,
/// not hand-copies, and must NOT be linked.
const HAND_MIRROR_MIN_SHARED_VALUES: usize = 3;

/// Trailing identifier suffixes stripped (case-insensitive) before comparing two
/// enum names for hand-mirror correspondence (H3). `OrderStatusEnum` ↔
/// `OrderStatusType` collapse to the same stem.
const HAND_MIRROR_NAME_SUFFIXES: [&str; 3] = ["enum", "type", "status"];

/// A suffix-stripped name match is only a SECONDARY signal: it links two enums
/// solely when they ALSO share at least this many canonical values. This keeps
/// the gate strict — a name coincidence with zero value overlap never links.
const HAND_MIRROR_NAME_MATCH_MIN_SHARED_VALUES: usize = 1;

/// Append value-mirror risks (`value_mirror_incomplete` + `value_mirror`) to
/// `risks`, derived from the value-mirror graph diff between `baseline` (PR
/// base) and `review` (PR head).
pub fn extend_with_value_mirror_risks<S: GraphStore>(
    baseline: &S,
    review: &S,
    risks: &mut Vec<RemovedSurfaceRisk>,
) -> Result<()> {
    // Hand-mirror correspondence index (H3): canonical value set per enum_qn in
    // the head graph. Built once so the per-surface enum-scope check can decide
    // whether two differently-named enums are really the same hand-copied set.
    let enum_value_sets = enum_value_sets_by_qn(review)?;

    // ── Added authoritative values (add-and-forget completeness check) ────────
    for added in added_authoritative_values(baseline, review)? {
        let AddedValue {
            owner_id,
            value_node,
            owner_enum_qn,
        } = added;
        let group = owner_defines_value_nodes(review, owner_id)?;
        // The new value's id, to exclude it from "other members" and to test
        // whether each surface already mirrors it.
        let new_value_id = value_node.id;

        // Gather the group's established mirror + guard surfaces from OTHER
        // members. Mirror surfaces (`MirrorsValueFrom`) and guard surfaces
        // (`GuardsEnumValue`) share the same `(owner, repo, file)` set-difference;
        // only the edge kind (→ risk label) and the default-exemption differ.
        let mut surfaces: Vec<MirrorSurface> = Vec::new();
        for member in &group {
            if member.id == new_value_id {
                continue;
            }
            for surface in surfaces_of(review, member.id)? {
                if !surfaces.iter().any(|s| s.same_surface(&surface)) {
                    surfaces.push(surface);
                }
            }
        }

        for surface in surfaces {
            // Enum-scoped matching (Task 17): when the added member's owner enum
            // has a known `enum_qn`, an enum-ref surface is only relevant if it
            // mirrors/guards the SAME enum. A surface carrying a different
            // `enum_qn` shares only a coincidental string value, so skip it —
            // UNLESS the two enums are hand-mirrored copies of the same set (H3),
            // in which case the surface's enum is a hand-copy that should track
            // the new member too. Mode-A literal surfaces (`enum_qn == None`)
            // keep value-only matching and are always considered.
            if let (Some(owner_qn), Some(surface_qn)) = (&owner_enum_qn, &surface.enum_qn)
                && owner_qn != surface_qn
                && !enums_hand_mirror_correspond(owner_qn, surface_qn, &enum_value_sets)
            {
                continue;
            }
            // Default-exemption: a guard with an explicit default/catch-all
            // branch already handles new members, so a missing case is not a
            // risk there.
            if surface.kind == SurfaceKind::Guard && surface.guard_has_default == Some(true) {
                continue;
            }
            // Precision: only flag a surface that does NOT already cover the new
            // value. (A surface updated in the same PR would have the edge.)
            if review_surface_edge_exists(review, &surface, new_value_id)? {
                continue;
            }
            let value = canonical_value_label(&value_node);
            let (kind, detail) = match surface.kind {
                SurfaceKind::Mirror => (
                    "value_mirror_incomplete",
                    format!(
                        "mirror surface {}::{} mirrors other members of this group \
                         but is missing the newly-added value {value}",
                        surface.repo, surface.file
                    ),
                ),
                SurfaceKind::Guard => (
                    "enum_guard_incomplete",
                    format!(
                        "guard surface {}::{} switches on other members of this enum \
                         but is missing a case for the newly-added value {value}",
                        surface.repo, surface.file
                    ),
                ),
            };
            risks.push(RemovedSurfaceRisk {
                kind: kind.to_owned(),
                identity: value_node
                    .qualified_name
                    .clone()
                    .unwrap_or_else(|| value.clone()),
                repo: Some(surface.repo.clone()),
                surviving_consumers: vec![],
                severity: RiskSeverity::Medium,
                detail: Some(detail),
            });
        }
    }

    // ── Modified / removed authoritative values (edge-walk) ───────────────────
    for value_node in removed_authoritative_values(baseline, review)? {
        for surface in mirror_surfaces_of(review, value_node.id)? {
            let value = canonical_value_label(&value_node);
            risks.push(RemovedSurfaceRisk {
                kind: "value_mirror".to_owned(),
                identity: value_node
                    .qualified_name
                    .clone()
                    .unwrap_or_else(|| value.clone()),
                repo: Some(surface.repo.clone()),
                surviving_consumers: vec![],
                severity: RiskSeverity::High,
                detail: Some(format!(
                    "authoritative value {value} was changed/removed but mirror \
                     surface {}::{} still mirrors it and was not updated",
                    surface.repo, surface.file
                )),
            });
        }
    }

    Ok(())
}

/// Whether a surface mirrors a value (`MirrorsValueFrom`) or guards on it
/// (`GuardsEnumValue`). Drives the risk label and the default-exemption.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SurfaceKind {
    Mirror,
    Guard,
}

/// One surface converging on a `__value__` node: the source node (allowlist
/// array / FE map owner, or a `switch`/`if` guard) plus its owning file.
/// `(owner, repo, file)` identifies it. `guard_has_default` is the
/// `GuardsEnumValue` edge's exemption flag (`None` for mirror surfaces).
#[derive(Clone)]
struct MirrorSurface {
    owner: NodeId,
    repo: String,
    file: String,
    kind: SurfaceKind,
    guard_has_default: Option<bool>,
    /// Owning enum's bare name for enum-ref surfaces (the edge's `enum_qn`);
    /// `None` for Mode-A literal mirror surfaces. Drives Task 17's enum-scoped
    /// completeness matching.
    enum_qn: Option<String>,
}

impl MirrorSurface {
    fn same_surface(&self, other: &Self) -> bool {
        // `enum_qn` is part of the surface identity: two surfaces in the same
        // file/owner that reference *different* enums must stay distinct, or
        // Task-17 enum-scoped completeness collapses into false negatives/positives.
        self.owner == other.owner
            && self.repo == other.repo
            && self.file == other.file
            && self.enum_qn == other.enum_qn
    }

    fn edge_kind(&self) -> EdgeKind {
        match self.kind {
            SurfaceKind::Mirror => EdgeKind::MirrorsValueFrom,
            SurfaceKind::Guard => EdgeKind::GuardsEnumValue,
        }
    }
}

/// An authoritative value newly added in the PR: the owning enum/union owner,
/// the added `ValueMirror` node, and the owner enum's bare `enum_qn` (read from
/// the `Defines` edge metadata; `None` for non-enum owners). The `enum_qn`
/// scopes Task 17's enum-aware completeness matching.
struct AddedValue {
    owner_id: NodeId,
    value_node: NodeData,
    owner_enum_qn: Option<String>,
}

/// Added authoritative values = `Defines` edges (owner → `ValueMirror`) present
/// in `review` but NOT in `baseline`.
fn added_authoritative_values<S: GraphStore>(baseline: &S, review: &S) -> Result<Vec<AddedValue>> {
    let mut out: Vec<AddedValue> = Vec::new();
    for value_node in review.nodes_by_type(NodeKind::ValueMirror)? {
        for edge in review.get_incoming(value_node.id)? {
            if edge.kind != EdgeKind::Defines {
                continue;
            }
            // Present in baseline? Then not newly added.
            let in_baseline = baseline
                .get_incoming(value_node.id)?
                .into_iter()
                .any(|b| b.kind == EdgeKind::Defines && b.source == edge.source);
            if in_baseline {
                continue;
            }
            out.push(AddedValue {
                owner_id: edge.source,
                value_node: value_node.clone(),
                owner_enum_qn: edge.metadata.enum_qn.clone(),
            });
        }
    }
    Ok(out)
}

/// Removed/changed authoritative values = `Defines` edges (owner → `ValueMirror`)
/// present in `baseline` but NOT in `review`. Returns the value nodes (deduped)
/// that still exist in the review graph (so we can walk their surviving
/// mirrors). A value node fully gone from review has no surviving mirrors.
fn removed_authoritative_values<S: GraphStore>(baseline: &S, review: &S) -> Result<Vec<NodeData>> {
    let mut out: Vec<NodeData> = Vec::new();
    for value_node in baseline.nodes_by_type(NodeKind::ValueMirror)? {
        let Some(review_value) = review.get_node(value_node.id)? else {
            continue;
        };
        for edge in baseline.get_incoming(value_node.id)? {
            if edge.kind != EdgeKind::Defines {
                continue;
            }
            let still_defined = review
                .get_incoming(value_node.id)?
                .into_iter()
                .any(|r| r.kind == EdgeKind::Defines && r.source == edge.source);
            if still_defined {
                continue;
            }
            if !out.iter().any(|v| v.id == review_value.id) {
                out.push(review_value.clone());
            }
        }
    }
    Ok(out)
}

/// All `ValueMirror` nodes that `owner` `Defines` in `store` (the owner group's
/// members), deduped by id.
fn owner_defines_value_nodes<S: GraphStore>(store: &S, owner: NodeId) -> Result<Vec<NodeData>> {
    let mut out: Vec<NodeData> = Vec::new();
    for edge in store.get_outgoing(owner)? {
        if edge.kind != EdgeKind::Defines {
            continue;
        }
        let Some(node) = store.get_node(edge.target)? else {
            continue;
        };
        if node.kind != NodeKind::ValueMirror {
            continue;
        }
        if !out.iter().any(|n| n.id == node.id) {
            out.push(node);
        }
    }
    Ok(out)
}

/// Mirror surfaces for a `ValueMirror` node = the `MirrorsValueFrom` source
/// nodes (deduped by `(owner, repo, file)`). Used by the edge-walk
/// (modified/removed) branch, which is mirror-only.
fn mirror_surfaces_of<S: GraphStore>(store: &S, value_node: NodeId) -> Result<Vec<MirrorSurface>> {
    surfaces_of_kind(store, value_node, EdgeKind::MirrorsValueFrom)
}

/// All mirror (`MirrorsValueFrom`) and guard (`GuardsEnumValue`) surfaces for a
/// `ValueMirror` node, deduped by `(owner, repo, file)`. Used by the
/// add-and-forget set-difference, which covers both surface kinds.
fn surfaces_of<S: GraphStore>(store: &S, value_node: NodeId) -> Result<Vec<MirrorSurface>> {
    let mut out = surfaces_of_kind(store, value_node, EdgeKind::MirrorsValueFrom)?;
    for surface in surfaces_of_kind(store, value_node, EdgeKind::GuardsEnumValue)? {
        if !out.iter().any(|s| s.same_surface(&surface)) {
            out.push(surface);
        }
    }
    Ok(out)
}

/// Surfaces for a `ValueMirror` node reached via a single `edge_kind`, deduped
/// by `(owner, repo, file)`. `guard_has_default` is read from the edge metadata
/// for `GuardsEnumValue` edges (`None` otherwise).
fn surfaces_of_kind<S: GraphStore>(
    store: &S,
    value_node: NodeId,
    edge_kind: EdgeKind,
) -> Result<Vec<MirrorSurface>> {
    let kind = match edge_kind {
        EdgeKind::GuardsEnumValue => SurfaceKind::Guard,
        _ => SurfaceKind::Mirror,
    };
    let mut out: Vec<MirrorSurface> = Vec::new();
    for edge in store.get_incoming(value_node)? {
        if edge.kind != edge_kind {
            continue;
        }
        let Some(source) = store.get_node(edge.source)? else {
            continue;
        };
        let surface = MirrorSurface {
            owner: source.id,
            repo: source.repo.clone(),
            file: if source.file_path.is_empty() {
                source.name.clone()
            } else {
                source.file_path.clone()
            },
            kind,
            guard_has_default: edge.metadata.guard_has_default,
            enum_qn: edge.metadata.enum_qn.clone(),
        };
        if !out.iter().any(|s| s.same_surface(&surface)) {
            out.push(surface);
        }
    }
    Ok(out)
}

/// Does `surface` already cover `value_node` in `store` (via its own edge kind)?
fn review_surface_edge_exists<S: GraphStore>(
    store: &S,
    surface: &MirrorSurface,
    value_node: NodeId,
) -> Result<bool> {
    let edge_kind = surface.edge_kind();
    Ok(store
        .get_incoming(value_node)?
        .into_iter()
        .any(|e| e.kind == edge_kind && e.source == surface.owner))
}

/// Human-readable canonical value for a `ValueMirror` node. The node id/name is
/// the `__value__<canonical>` qualified name (Task 4 lowercases + canonicalizes
/// the value via `value_mirror_qn`), so the label is the canonical value with
/// the `__value__` prefix stripped — that is the only form the persisted graph
/// carries.
fn canonical_value_label(value_node: &NodeData) -> String {
    let raw = value_node
        .qualified_name
        .as_deref()
        .filter(|qn| !qn.is_empty())
        .unwrap_or(&value_node.name);
    raw.strip_prefix("__value__").unwrap_or(raw).to_owned()
}

/// Canonical value set per `enum_qn` in `store`, derived from authoritative
/// `Defines` edges (enum owner → `ValueMirror`). The set uses the same
/// canonical value labels as the risk output ([`canonical_value_label`]), so
/// "shared values" between two enums is an exact canonical-form comparison.
/// Drives H3 hand-mirror correspondence.
fn enum_value_sets_by_qn<S: GraphStore>(store: &S) -> Result<FxHashMap<String, BTreeSet<String>>> {
    let mut out: FxHashMap<String, BTreeSet<String>> = FxHashMap::default();
    for value_node in store.nodes_by_type(NodeKind::ValueMirror)? {
        let label = canonical_value_label(&value_node);
        for edge in store.get_incoming(value_node.id)? {
            if edge.kind != EdgeKind::Defines {
                continue;
            }
            if let Some(enum_qn) = &edge.metadata.enum_qn {
                out.entry(enum_qn.clone())
                    .or_default()
                    .insert(label.clone());
            }
        }
    }
    Ok(out)
}

/// Strip a single trailing [`HAND_MIRROR_NAME_SUFFIXES`] token (case-insensitive)
/// and lowercase the result, yielding a stem for hand-mirror name comparison.
/// `OrderStatusEnum` → `orderstatus`, `OrderStatusType` → `orderstatus`.
fn hand_mirror_name_stem(enum_qn: &str) -> String {
    // One-shot owned lowercase: the stem is returned as an owned `String` and
    // matched against suffixes, so an in-place or compare-only variant won't do.
    #[expect(
        clippy::disallowed_methods,
        reason = "owned lowercase stem is the function's return value"
    )]
    let lower = enum_qn.to_ascii_lowercase();
    for suffix in HAND_MIRROR_NAME_SUFFIXES {
        if lower.len() > suffix.len()
            && let Some(stem) = lower.strip_suffix(suffix)
        {
            return stem.to_owned();
        }
    }
    lower
}

/// Whether two differently-named enums are a hand-mirrored copy of the same set
/// (H3). STRICT FP gate: link when they share ≥ [`HAND_MIRROR_MIN_SHARED_VALUES`]
/// canonical values, OR their suffix-stripped names match AND they still share
/// ≥ [`HAND_MIRROR_NAME_MATCH_MIN_SHARED_VALUES`] value (suffix-name-match is a
/// secondary signal only — never links on a bare name coincidence). Two enums
/// sharing 1–2 values with non-matching names are NOT linked.
fn enums_hand_mirror_correspond(
    owner_qn: &str,
    surface_qn: &str,
    enum_value_sets: &FxHashMap<String, BTreeSet<String>>,
) -> bool {
    let (Some(owner_values), Some(surface_values)) = (
        enum_value_sets.get(owner_qn),
        enum_value_sets.get(surface_qn),
    ) else {
        return false;
    };
    let shared = owner_values.intersection(surface_values).count();
    if shared >= HAND_MIRROR_MIN_SHARED_VALUES {
        return true;
    }
    let names_match = hand_mirror_name_stem(owner_qn) == hand_mirror_name_stem(surface_qn);
    names_match && shared >= HAND_MIRROR_NAME_MATCH_MIN_SHARED_VALUES
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, SourceSpan, node_id, value_mirror_qn,
        virtual_node,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use crate::pr_review::delta_report::RemovedSurfaceRisk;

    use super::extend_with_value_mirror_risks;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-value-mirror-{label}-{}-{id}.redb",
                std::process::id()
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

    fn open_store(label: &str) -> (TempDb, GraphStoreDb) {
        let tmp = TempDb::new(label);
        let db = GraphStoreDb::open(tmp.path()).expect("store should open");
        (tmp, db)
    }

    fn file_node(repo: &str, path: &str) -> NodeData {
        NodeData {
            id: node_id(repo, path, NodeKind::File, path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: path.to_owned(),
            name: path.to_owned(),
            qualified_name: Some(format!("{repo}::{path}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
            ai_role: None,
        }
    }

    /// An owner symbol node (enum declaration, allowlist const, FE map const).
    fn owner_node(repo: &str, file: &str, name: &str, kind: NodeKind) -> NodeData {
        NodeData {
            id: node_id(repo, file, kind, name),
            kind,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: None,
            visibility: None,
            span: Some(SourceSpan {
                line_start: 1,
                line_len: 1,
                column_start: 0,
                column_len: 0,
            }),
            is_virtual: false,
            ai_role: None,
        }
    }

    fn value_node(value: &str) -> NodeData {
        let qn = value_mirror_qn(value);
        virtual_node(
            NodeKind::ValueMirror,
            "__virtual__",
            qn.clone(),
            qn.clone(),
            qn,
        )
    }

    fn defines_edge(owner: &NodeData, value: &NodeData, file: &NodeData) -> EdgeData {
        EdgeData {
            source: owner.id,
            target: value.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata::default(),
            owner_file: file.id,
            is_cross_file: true,
        }
    }

    fn mirrors_edge(owner: &NodeData, value: &NodeData, file: &NodeData) -> EdgeData {
        EdgeData {
            source: owner.id,
            target: value.id,
            kind: EdgeKind::MirrorsValueFrom,
            metadata: EdgeMetadata::default(),
            owner_file: file.id,
            is_cross_file: true,
        }
    }

    /// A `Defines` edge from an authoritative enum owner to a value, carrying the
    /// owner enum's bare `enum_qn` (Task 17 enum-ref edges). Mirrors the parser's
    /// `value_mirror_edge` stamping for enum-member surfaces.
    fn defines_edge_qn(
        owner: &NodeData,
        value: &NodeData,
        file: &NodeData,
        enum_qn: &str,
    ) -> EdgeData {
        EdgeData {
            source: owner.id,
            target: value.id,
            kind: EdgeKind::Defines,
            metadata: EdgeMetadata {
                enum_qn: Some(enum_qn.to_owned()),
                ..EdgeMetadata::default()
            },
            owner_file: file.id,
            is_cross_file: true,
        }
    }

    /// A `MirrorsValueFrom` edge from an enum-subset array surface to a value,
    /// carrying the referenced enum's bare `enum_qn` (Mode B). A Mode-A literal
    /// mirror would leave `enum_qn = None` (use [`mirrors_edge`]).
    fn mirrors_edge_qn(
        owner: &NodeData,
        value: &NodeData,
        file: &NodeData,
        enum_qn: &str,
    ) -> EdgeData {
        EdgeData {
            source: owner.id,
            target: value.id,
            kind: EdgeKind::MirrorsValueFrom,
            metadata: EdgeMetadata {
                enum_qn: Some(enum_qn.to_owned()),
                ..EdgeMetadata::default()
            },
            owner_file: file.id,
            is_cross_file: true,
        }
    }

    /// A `GuardsEnumValue` edge from a `switch`/`if` guard surface to a value,
    /// carrying the `guard_has_default` exemption flag (Task 11–14).
    fn guards_edge(
        owner: &NodeData,
        value: &NodeData,
        file: &NodeData,
        has_default: bool,
    ) -> EdgeData {
        EdgeData {
            source: owner.id,
            target: value.id,
            kind: EdgeKind::GuardsEnumValue,
            metadata: EdgeMetadata {
                guard_has_default: Some(has_default),
                ..EdgeMetadata::default()
            },
            owner_file: file.id,
            is_cross_file: true,
        }
    }

    /// PRIMARY: an enum already mirrors members {m1,m2} into an allowlist
    /// surface; the PR adds m3 to the enum without adding it to the surface →
    /// `value_mirror_incomplete` naming the surface + m3.
    #[test]
    fn new_enum_member_missing_from_established_mirror_surface_is_flagged() {
        let (_tb, baseline) = open_store("primary-baseline");
        let (_tr, review) = open_store("primary-review");

        // Owner enum (authoritative) in the backend repo.
        let enum_file = file_node("service-log", "src/events.ts");
        let event_type = owner_node("service-log", "src/events.ts", "EventType", NodeKind::Type);

        // Allowlist mirror surface in the same/another repo (FE category map).
        let allow_file = file_node("web-frontend", "src/roles.ts");
        let allowlist = owner_node(
            "web-frontend",
            "src/roles.ts",
            "ALLOWED_VALUES",
            NodeKind::DataField,
        );

        let m1 = value_node("orders.review.submitted");
        let m2 = value_node("orders.review.approved");
        let m3 = value_node("orders.statusCheck.triggered");

        // Baseline: enum defines m1+m2, allowlist mirrors m1+m2.
        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    allow_file.clone(),
                    allowlist.clone(),
                    m1.clone(),
                    m2.clone(),
                ],
                &[
                    defines_edge(&event_type, &m1, &enum_file),
                    defines_edge(&event_type, &m2, &enum_file),
                    mirrors_edge(&allowlist, &m1, &allow_file),
                    mirrors_edge(&allowlist, &m2, &allow_file),
                ],
            )
            .expect("baseline insert");

        // Review (head): enum now also defines m3; allowlist UNCHANGED (still
        // only mirrors m1+m2 — the add-and-forget).
        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    allow_file.clone(),
                    allowlist.clone(),
                    m1.clone(),
                    m2.clone(),
                    m3.clone(),
                ],
                &[
                    defines_edge(&event_type, &m1, &enum_file),
                    defines_edge(&event_type, &m2, &enum_file),
                    defines_edge(&event_type, &m3, &enum_file),
                    mirrors_edge(&allowlist, &m1, &allow_file),
                    mirrors_edge(&allowlist, &m2, &allow_file),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| r.kind == "value_mirror_incomplete")
            .expect("expected an incomplete-mirror risk");
        let detail = risk.detail.as_deref().expect("detail should be set");
        assert!(
            detail.contains("ALLOWED_VALUES") || detail.contains("src/roles.ts"),
            "detail must name the mirror surface, got: {detail}"
        );
        // Task 4 canonicalizes the value into the `__value__<canonical>` id
        // (lowercased by `value_mirror_qn`), so the persisted graph — and thus
        // the risk detail — carries the canonical lowercased form.
        assert!(
            detail.contains("orders.statuscheck.triggered"),
            "detail must name the missing (canonical) value, got: {detail}"
        );
        assert_eq!(risk.repo.as_deref(), Some("web-frontend"));
    }

    /// PRECISION: a `__value__` node with no other group member mirrored by a
    /// surface (single-repo orphan, no `MirrorsValueFrom` edges) must NOT be
    /// flagged when a new member is added.
    #[test]
    fn orphan_value_with_no_other_mirrored_members_is_not_flagged() {
        let (_tb, baseline) = open_store("orphan-baseline");
        let (_tr, review) = open_store("orphan-review");

        let enum_file = file_node("service-log", "src/events.ts");
        let event_type = owner_node("service-log", "src/events.ts", "EventType", NodeKind::Type);

        let m1 = value_node("solo.value.one");
        let m2 = value_node("solo.value.two");

        // Baseline: enum defines m1 only; NO mirror surfaces anywhere.
        baseline
            .bulk_insert(
                &[enum_file.clone(), event_type.clone(), m1.clone()],
                &[defines_edge(&event_type, &m1, &enum_file)],
            )
            .expect("baseline insert");

        // Review: enum adds m2; still no mirror surfaces.
        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    m1.clone(),
                    m2.clone(),
                ],
                &[
                    defines_edge(&event_type, &m1, &enum_file),
                    defines_edge(&event_type, &m2, &enum_file),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r.kind != "value_mirror_incomplete"),
            "an orphan value with no mirrored sibling must not be flagged: {risks:?}"
        );
    }

    /// PRECISION: when the PR adds the new member to the mirror surface too,
    /// there is no incompleteness → no risk.
    #[test]
    fn new_member_also_added_to_surface_is_not_flagged() {
        let (_tb, baseline) = open_store("complete-baseline");
        let (_tr, review) = open_store("complete-review");

        let enum_file = file_node("service-log", "src/events.ts");
        let event_type = owner_node("service-log", "src/events.ts", "EventType", NodeKind::Type);
        let allow_file = file_node("web-frontend", "src/roles.ts");
        let allowlist = owner_node(
            "web-frontend",
            "src/roles.ts",
            "ALLOWED_VALUES",
            NodeKind::DataField,
        );

        let m1 = value_node("complete.value.one");
        let m2 = value_node("complete.value.two");

        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    allow_file.clone(),
                    allowlist.clone(),
                    m1.clone(),
                ],
                &[
                    defines_edge(&event_type, &m1, &enum_file),
                    mirrors_edge(&allowlist, &m1, &allow_file),
                ],
            )
            .expect("baseline insert");

        // Review: enum adds m2 AND the allowlist mirrors m2 too.
        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    allow_file.clone(),
                    allowlist.clone(),
                    m1.clone(),
                    m2.clone(),
                ],
                &[
                    defines_edge(&event_type, &m1, &enum_file),
                    defines_edge(&event_type, &m2, &enum_file),
                    mirrors_edge(&allowlist, &m1, &allow_file),
                    mirrors_edge(&allowlist, &m2, &allow_file),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r.kind != "value_mirror_incomplete"),
            "a surface that also mirrors the new value must not be flagged: {risks:?}"
        );
    }

    /// SECONDARY: a PR removes an authoritative value's `Defines` edge while a
    /// mirror surface still mirrors it → `value_mirror` edge-walk risk.
    #[test]
    fn modified_authoritative_value_with_surviving_mirror_is_flagged() {
        let (_tb, baseline) = open_store("modified-baseline");
        let (_tr, review) = open_store("modified-review");

        let enum_file = file_node("service-log", "src/events.ts");
        let event_type = owner_node("service-log", "src/events.ts", "EventType", NodeKind::Type);
        let allow_file = file_node("web-frontend", "src/roles.ts");
        let allowlist = owner_node(
            "web-frontend",
            "src/roles.ts",
            "ALLOWED_VALUES",
            NodeKind::DataField,
        );

        let m1 = value_node("removed.value.one");

        // Baseline: enum defines m1, allowlist mirrors m1.
        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    allow_file.clone(),
                    allowlist.clone(),
                    m1.clone(),
                ],
                &[
                    defines_edge(&event_type, &m1, &enum_file),
                    mirrors_edge(&allowlist, &m1, &allow_file),
                ],
            )
            .expect("baseline insert");

        // Review: enum NO LONGER defines m1 (renamed/changed), but the value
        // node + the allowlist mirror survive (un-updated).
        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    event_type.clone(),
                    allow_file.clone(),
                    allowlist.clone(),
                    m1.clone(),
                ],
                &[mirrors_edge(&allowlist, &m1, &allow_file)],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| r.kind == "value_mirror")
            .expect("expected a value_mirror edge-walk risk");
        let detail = risk.detail.as_deref().expect("detail should be set");
        assert!(
            detail.contains("removed.value.one"),
            "detail must name the changed value, got: {detail}"
        );
        assert!(
            detail.contains("ALLOWED_VALUES") || detail.contains("src/roles.ts"),
            "detail must name the surviving mirror, got: {detail}"
        );
    }

    /// Build the `add-status-forget-switch` graph: a `Status` enum with members
    /// `Active`+`Done`, a `switch` guard surface covering both (no default), and
    /// the PR adds `Cancelled` to the enum without adding a case to the switch.
    /// `has_default` controls the guard's exemption flag; `cover_cancelled`
    /// makes the (review) switch also cover the new member.
    fn build_add_status_forget_switch(
        baseline: &GraphStoreDb,
        review: &GraphStoreDb,
        has_default: bool,
        cover_cancelled: bool,
    ) {
        let enum_file = file_node("web-frontend", "src/status.ts");
        let status = owner_node("web-frontend", "src/status.ts", "Status", NodeKind::Type);

        let switch_file = file_node("web-frontend", "src/RunStatusCell.tsx");
        let switch = owner_node(
            "web-frontend",
            "src/RunStatusCell.tsx",
            "renderStatus",
            NodeKind::Function,
        );

        let active = value_node("status.active");
        let done = value_node("status.done");
        let cancelled = value_node("status.cancelled");

        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    status.clone(),
                    switch_file.clone(),
                    switch.clone(),
                    active.clone(),
                    done.clone(),
                ],
                &[
                    defines_edge(&status, &active, &enum_file),
                    defines_edge(&status, &done, &enum_file),
                    guards_edge(&switch, &active, &switch_file, has_default),
                    guards_edge(&switch, &done, &switch_file, has_default),
                ],
            )
            .expect("baseline insert");

        let review_nodes = vec![
            enum_file.clone(),
            status.clone(),
            switch_file.clone(),
            switch.clone(),
            active.clone(),
            done.clone(),
            cancelled.clone(),
        ];
        let mut review_edges = vec![
            defines_edge(&status, &active, &enum_file),
            defines_edge(&status, &done, &enum_file),
            defines_edge(&status, &cancelled, &enum_file),
            guards_edge(&switch, &active, &switch_file, has_default),
            guards_edge(&switch, &done, &switch_file, has_default),
        ];
        if cover_cancelled {
            review_edges.push(guards_edge(&switch, &cancelled, &switch_file, has_default));
        }
        review
            .bulk_insert(&review_nodes, &review_edges)
            .expect("review insert");
    }

    /// Task 14 PRIMARY: a `switch` guard (no default) covers {Active, Done}; the
    /// PR adds `Cancelled` to the enum without a new case → `enum_guard_incomplete`
    /// naming the guard file + the missing `Cancelled`.
    #[test]
    fn new_enum_member_missing_from_switch_guard_is_flagged() {
        let (_tb, baseline) = open_store("guard-primary-baseline");
        let (_tr, review) = open_store("guard-primary-review");
        build_add_status_forget_switch(&baseline, &review, false, false);

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| r.kind == "enum_guard_incomplete")
            .expect("expected an enum-guard completeness risk");
        let detail = risk.detail.as_deref().expect("detail should be set");
        assert!(
            detail.contains("status.cancelled"),
            "detail must name the missing (canonical) value, got: {detail}"
        );
        assert!(
            detail.contains("renderStatus") || detail.contains("src/RunStatusCell.tsx"),
            "detail must name the guard surface, got: {detail}"
        );
        assert_eq!(risk.repo.as_deref(), Some("web-frontend"));
    }

    /// Task 14 EXEMPTION: same fixture but the switch has a `default` branch
    /// (`guard_has_default == Some(true)`) → NO `enum_guard_incomplete` risk.
    #[test]
    fn switch_guard_with_default_is_exempt() {
        let (_tb, baseline) = open_store("guard-default-baseline");
        let (_tr, review) = open_store("guard-default-review");
        build_add_status_forget_switch(&baseline, &review, true, false);

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r.kind != "enum_guard_incomplete"),
            "a guard with a default branch must be exempt: {risks:?}"
        );
    }

    /// Task 14 NO-OP: the switch already covers all three members (the PR added
    /// the `Cancelled` case too) → NO risk.
    #[test]
    fn switch_guard_covering_all_members_is_not_flagged() {
        let (_tb, baseline) = open_store("guard-complete-baseline");
        let (_tr, review) = open_store("guard-complete-review");
        build_add_status_forget_switch(&baseline, &review, false, true);

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r.kind != "enum_guard_incomplete"),
            "a guard covering all members must not be flagged: {risks:?}"
        );
    }

    /// Task 14 NO-OP: a `switch` whose owner enum has no other mirrored/guarded
    /// member (a non-enum / orphan switch) → NO risk, because the set-difference
    /// only flags surfaces established on ≥1 OTHER member of the owner group.
    #[test]
    fn non_enum_switch_guard_is_not_flagged() {
        let (_tb, baseline) = open_store("guard-nonenum-baseline");
        let (_tr, review) = open_store("guard-nonenum-review");

        let enum_file = file_node("web-frontend", "src/status.ts");
        let status = owner_node("web-frontend", "src/status.ts", "Status", NodeKind::Type);
        let switch_file = file_node("web-frontend", "src/Other.tsx");
        let switch = owner_node(
            "web-frontend",
            "src/Other.tsx",
            "renderOther",
            NodeKind::Function,
        );

        let active = value_node("status.active");
        let cancelled = value_node("status.cancelled");

        // Baseline: enum defines `active`; the switch guards an UNRELATED value
        // (no overlap with the enum's members).
        let unrelated = value_node("unrelated.kind");
        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    status.clone(),
                    switch_file.clone(),
                    switch.clone(),
                    active.clone(),
                    unrelated.clone(),
                ],
                &[
                    defines_edge(&status, &active, &enum_file),
                    guards_edge(&switch, &unrelated, &switch_file, false),
                ],
            )
            .expect("baseline insert");

        // Review: enum adds `cancelled`; the switch still only guards the
        // unrelated value → it is not an established guard of the `Status` group.
        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    status.clone(),
                    switch_file.clone(),
                    switch.clone(),
                    active.clone(),
                    cancelled.clone(),
                    unrelated.clone(),
                ],
                &[
                    defines_edge(&status, &active, &enum_file),
                    defines_edge(&status, &cancelled, &enum_file),
                    guards_edge(&switch, &unrelated, &switch_file, false),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r.kind != "enum_guard_incomplete"),
            "a switch not guarding any member of the changed enum must not be \
             flagged: {risks:?}"
        );
    }

    /// Task 17 STEP 1 (the motivating real-world shape): an enum `{Pending, Completed, Failed}`
    /// and a `valueOptions` array mirroring all three (enum-ref, SAME repo); the
    /// PR adds `Cancelled` to the enum without adding it to the array →
    /// `value_mirror_incomplete` naming the array file + the missing `Cancelled`.
    #[test]
    fn new_enum_member_missing_from_enum_subset_array_is_flagged() {
        let (_tb, baseline) = open_store("array-baseline");
        let (_tr, review) = open_store("array-review");

        let enum_file = file_node("web-frontend", "src/status.ts");
        let status = owner_node("web-frontend", "src/status.ts", "Status", NodeKind::Type);

        let array_file = file_node("web-frontend", "src/columns.tsx");
        let value_options = owner_node(
            "web-frontend",
            "src/columns.tsx",
            "valueOptions",
            NodeKind::DataField,
        );

        let pending = value_node("status.pending");
        let completed = value_node("status.completed");
        let failed = value_node("status.failed");
        let cancelled = value_node("status.cancelled");

        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    status.clone(),
                    array_file.clone(),
                    value_options.clone(),
                    pending.clone(),
                    completed.clone(),
                    failed.clone(),
                ],
                &[
                    defines_edge_qn(&status, &pending, &enum_file, "Status"),
                    defines_edge_qn(&status, &completed, &enum_file, "Status"),
                    defines_edge_qn(&status, &failed, &enum_file, "Status"),
                    mirrors_edge_qn(&value_options, &pending, &array_file, "Status"),
                    mirrors_edge_qn(&value_options, &completed, &array_file, "Status"),
                    mirrors_edge_qn(&value_options, &failed, &array_file, "Status"),
                ],
            )
            .expect("baseline insert");

        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    status.clone(),
                    array_file.clone(),
                    value_options.clone(),
                    pending.clone(),
                    completed.clone(),
                    failed.clone(),
                    cancelled.clone(),
                ],
                &[
                    defines_edge_qn(&status, &pending, &enum_file, "Status"),
                    defines_edge_qn(&status, &completed, &enum_file, "Status"),
                    defines_edge_qn(&status, &failed, &enum_file, "Status"),
                    defines_edge_qn(&status, &cancelled, &enum_file, "Status"),
                    mirrors_edge_qn(&value_options, &pending, &array_file, "Status"),
                    mirrors_edge_qn(&value_options, &completed, &array_file, "Status"),
                    mirrors_edge_qn(&value_options, &failed, &array_file, "Status"),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| r.kind == "value_mirror_incomplete")
            .expect("expected an incomplete-mirror risk for the enum-subset array");
        let detail = risk.detail.as_deref().expect("detail should be set");
        assert!(
            detail.contains("valueOptions") || detail.contains("src/columns.tsx"),
            "detail must name the array surface, got: {detail}"
        );
        assert!(
            detail.contains("status.cancelled"),
            "detail must name the missing (canonical) value, got: {detail}"
        );
        assert_eq!(risk.repo.as_deref(), Some("web-frontend"));
    }

    /// Task 17 STEP 2b (NEGATIVE — `add-status-unrelated-enum-shares-value`):
    /// enum `A = {Pending, Cancelled}` with a complete array surface, and an
    /// UNRELATED enum `B = {Pending}` (different `enum_qn`, shares the string
    /// value `pending`) with its own array listing only Pending. The PR adds
    /// `Cancelled` to A. B's surface must NOT be flagged — pre-fix the shared
    /// `__value__pending` node would value-merge B's surface into A's group and
    /// falsely flag it; the `enum_qn` scoping removes that false positive.
    #[test]
    fn unrelated_enum_sharing_a_value_is_not_cross_flagged() {
        let (_tb, baseline) = open_store("unrelated-baseline");
        let (_tr, review) = open_store("unrelated-review");

        // Enum A and its complete array surface.
        let a_file = file_node("web-frontend", "src/a.ts");
        let enum_a = owner_node("web-frontend", "src/a.ts", "A", NodeKind::Type);
        let a_array_file = file_node("web-frontend", "src/aArray.tsx");
        let a_array = owner_node(
            "web-frontend",
            "src/aArray.tsx",
            "aOptions",
            NodeKind::DataField,
        );

        // Enum B and its own array surface (only Pending).
        let b_file = file_node("web-frontend", "src/b.ts");
        let enum_b = owner_node("web-frontend", "src/b.ts", "B", NodeKind::Type);
        let b_array_file = file_node("web-frontend", "src/bArray.tsx");
        let b_array = owner_node(
            "web-frontend",
            "src/bArray.tsx",
            "bOptions",
            NodeKind::DataField,
        );

        // Shared canonical value `pending` (both enums define it); A also has
        // `cancelled`.
        let pending = value_node("pending");
        let cancelled = value_node("cancelled");

        baseline
            .bulk_insert(
                &[
                    a_file.clone(),
                    enum_a.clone(),
                    a_array_file.clone(),
                    a_array.clone(),
                    b_file.clone(),
                    enum_b.clone(),
                    b_array_file.clone(),
                    b_array.clone(),
                    pending.clone(),
                ],
                &[
                    // A defines pending; A's array mirrors pending.
                    defines_edge_qn(&enum_a, &pending, &a_file, "A"),
                    mirrors_edge_qn(&a_array, &pending, &a_array_file, "A"),
                    // B defines pending; B's array mirrors pending.
                    defines_edge_qn(&enum_b, &pending, &b_file, "B"),
                    mirrors_edge_qn(&b_array, &pending, &b_array_file, "B"),
                ],
            )
            .expect("baseline insert");

        // Review: A adds `cancelled` to the enum AND to A's array (A stays
        // complete). B is untouched.
        review
            .bulk_insert(
                &[
                    a_file.clone(),
                    enum_a.clone(),
                    a_array_file.clone(),
                    a_array.clone(),
                    b_file.clone(),
                    enum_b.clone(),
                    b_array_file.clone(),
                    b_array.clone(),
                    pending.clone(),
                    cancelled.clone(),
                ],
                &[
                    defines_edge_qn(&enum_a, &pending, &a_file, "A"),
                    defines_edge_qn(&enum_a, &cancelled, &a_file, "A"),
                    mirrors_edge_qn(&a_array, &pending, &a_array_file, "A"),
                    mirrors_edge_qn(&a_array, &cancelled, &a_array_file, "A"),
                    defines_edge_qn(&enum_b, &pending, &b_file, "B"),
                    mirrors_edge_qn(&b_array, &pending, &b_array_file, "B"),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r
                .detail
                .as_deref()
                .is_none_or(|d| !d.contains("bOptions") && !d.contains("src/bArray.tsx"))),
            "B's surface (unrelated enum sharing the value `pending`) must not be \
             flagged: {risks:?}"
        );
    }

    /// Task 17 STEP 4a (cross-repo, COVERED): a shared `common-lib`-style enum
    /// with the SAME `enum_qn` is imported by two repos; the owning repo adds a
    /// member; the other repo's enum-subset array (same `enum_qn`) is missing it
    /// → FLAGGED. Enum-scoped matching is safe here because both surfaces carry
    /// the identical `enum_qn`.
    #[test]
    fn cross_repo_shared_enum_array_missing_member_is_flagged() {
        let (_tb, baseline) = open_store("xrepo-shared-baseline");
        let (_tr, review) = open_store("xrepo-shared-review");

        // Shared enum owned by the backend repo.
        let enum_file = file_node("service-log", "src/RunStatus.ts");
        let run_status = owner_node(
            "service-log",
            "src/RunStatus.ts",
            "RunStatus",
            NodeKind::Type,
        );

        // FE array referencing the SAME enum_qn (e.g. imported from common-lib).
        let array_file = file_node("web-frontend", "src/runColumns.tsx");
        let value_options = owner_node(
            "web-frontend",
            "src/runColumns.tsx",
            "runStatusOptions",
            NodeKind::DataField,
        );

        let active = value_node("runStatus.active");
        let done = value_node("runStatus.done");
        let cancelled = value_node("runStatus.cancelled");

        baseline
            .bulk_insert(
                &[
                    enum_file.clone(),
                    run_status.clone(),
                    array_file.clone(),
                    value_options.clone(),
                    active.clone(),
                    done.clone(),
                ],
                &[
                    defines_edge_qn(&run_status, &active, &enum_file, "RunStatus"),
                    defines_edge_qn(&run_status, &done, &enum_file, "RunStatus"),
                    mirrors_edge_qn(&value_options, &active, &array_file, "RunStatus"),
                    mirrors_edge_qn(&value_options, &done, &array_file, "RunStatus"),
                ],
            )
            .expect("baseline insert");

        review
            .bulk_insert(
                &[
                    enum_file.clone(),
                    run_status.clone(),
                    array_file.clone(),
                    value_options.clone(),
                    active.clone(),
                    done.clone(),
                    cancelled.clone(),
                ],
                &[
                    defines_edge_qn(&run_status, &active, &enum_file, "RunStatus"),
                    defines_edge_qn(&run_status, &done, &enum_file, "RunStatus"),
                    defines_edge_qn(&run_status, &cancelled, &enum_file, "RunStatus"),
                    mirrors_edge_qn(&value_options, &active, &array_file, "RunStatus"),
                    mirrors_edge_qn(&value_options, &done, &array_file, "RunStatus"),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| r.kind == "value_mirror_incomplete")
            .expect("expected an incomplete-mirror risk for the cross-repo array");
        let detail = risk.detail.as_deref().expect("detail should be set");
        assert!(
            detail.contains("runStatusOptions") || detail.contains("src/runColumns.tsx"),
            "detail must name the FE array surface, got: {detail}"
        );
        assert!(
            detail.contains("runstatus.cancelled"),
            "detail must name the missing (canonical) value, got: {detail}"
        );
        assert_eq!(risk.repo.as_deref(), Some("web-frontend"));
    }

    /// Build the cross-repo hand-mirror fixture: a BE enum `OrderStatusEnum`
    /// and a FE hand-copy enum `OrderStatus` (DIFFERENT `enum_qn`, same string
    /// values) with a FE array surface referencing the FE enum's `enum_qn`. The
    /// baseline shares `shared_members` between the two enums; BE then adds
    /// `cancelled` (FE untouched). Returns the FE array's owner name + file so a
    /// test can assert whether it is flagged.
    fn build_cross_repo_hand_mirror(
        baseline: &GraphStoreDb,
        review: &GraphStoreDb,
        shared_members: &[&str],
    ) {
        let be_file = file_node("service-log", "src/status.ts");
        let be_enum = owner_node(
            "service-log",
            "src/status.ts",
            "OrderStatusEnum",
            NodeKind::Type,
        );

        let fe_enum_file = file_node("web-frontend", "src/status.ts");
        let fe_enum = owner_node(
            "web-frontend",
            "src/status.ts",
            "OrderStatus",
            NodeKind::Type,
        );
        let fe_array_file = file_node("web-frontend", "src/statusColumns.tsx");
        let fe_array = owner_node(
            "web-frontend",
            "src/statusColumns.tsx",
            "statusOptions",
            NodeKind::DataField,
        );

        let cancelled = value_node("status.cancelled");
        let shared: Vec<NodeData> = shared_members.iter().map(|v| value_node(v)).collect();

        let mut base_nodes = vec![
            be_file.clone(),
            be_enum.clone(),
            fe_enum_file.clone(),
            fe_enum.clone(),
            fe_array_file.clone(),
            fe_array.clone(),
        ];
        base_nodes.extend(shared.iter().cloned());

        let mut base_edges = Vec::new();
        for member in &shared {
            // BE enum (authoritative) + FE hand-copy enum both define the shared
            // members; the FE array mirrors them under the FE enum_qn.
            base_edges.push(defines_edge_qn(
                &be_enum,
                member,
                &be_file,
                "OrderStatusEnum",
            ));
            base_edges.push(defines_edge_qn(
                &fe_enum,
                member,
                &fe_enum_file,
                "OrderStatus",
            ));
            base_edges.push(mirrors_edge_qn(
                &fe_array,
                member,
                &fe_array_file,
                "OrderStatus",
            ));
        }
        baseline
            .bulk_insert(&base_nodes, &base_edges)
            .expect("baseline insert");

        let mut review_nodes = base_nodes.clone();
        review_nodes.push(cancelled.clone());
        let mut review_edges = base_edges.clone();
        // BE gains `cancelled`; FE hand-copy enum + array are untouched.
        review_edges.push(defines_edge_qn(
            &be_enum,
            &cancelled,
            &be_file,
            "OrderStatusEnum",
        ));
        review
            .bulk_insert(&review_nodes, &review_edges)
            .expect("review insert");
    }

    /// H3 POSITIVE: a FE enum `OrderStatus` is a hand-copy of the BE enum
    /// `OrderStatusEnum` (DIFFERENT `enum_qn`) sharing ≥3 canonical values. BE
    /// adds `cancelled`. Hand-mirror correspondence links the two sets, so the
    /// un-updated FE array surface IS flagged.
    #[test]
    fn cross_repo_hand_mirror_sharing_three_values_is_flagged() {
        let (_tb, baseline) = open_store("xrepo-handmirror-3-baseline");
        let (_tr, review) = open_store("xrepo-handmirror-3-review");
        build_cross_repo_hand_mirror(
            &baseline,
            &review,
            &["status.active", "status.done", "status.pending"],
        );

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        let risk = risks
            .iter()
            .find(|r| {
                r.kind == "value_mirror_incomplete"
                    && r.detail.as_deref().is_some_and(|d| {
                        d.contains("statusOptions") || d.contains("src/statusColumns.tsx")
                    })
            })
            .expect("hand-mirror sharing ≥3 values must flag the FE array surface");
        let detail = risk.detail.as_deref().expect("detail should be set");
        assert!(
            detail.contains("status.cancelled"),
            "detail must name the missing (canonical) value, got: {detail}"
        );
    }

    /// H3 NEGATIVE (the required strict-gate test): two enums sharing EXACTLY 2
    /// canonical values with non-matching suffix-stripped names. The shared set
    /// is below the ≥3 hand-mirror bar, so the FE surface must NOT be
    /// cross-flagged when the BE enum gains a member.
    #[test]
    fn cross_repo_hand_mirror_sharing_two_values_is_not_flagged() {
        let (_tb, baseline) = open_store("xrepo-handmirror-2-baseline");
        let (_tr, review) = open_store("xrepo-handmirror-2-review");
        // `OrderStatusEnum` stem → `orderstatus`; `OrderStatus` stem → `order`
        // (one suffix stripped each): names do NOT match, so the secondary
        // name-match signal is inert and only the ≥3 value bar applies — which
        // 2 shared values fails.
        build_cross_repo_hand_mirror(&baseline, &review, &["status.active", "status.done"]);

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().all(|r| r.detail.as_deref().is_none_or(
                |d| !d.contains("statusOptions") && !d.contains("src/statusColumns.tsx")
            )),
            "two enums sharing only 2 values must NOT be hand-mirror linked: {risks:?}"
        );
    }

    /// H3 SECONDARY SIGNAL: two enums whose names match after stripping the
    /// `Enum`/`Type` suffix (`StatusType` ↔ `StatusEnum` → `status`) and that
    /// share 1 value are linked — the FE array surface IS flagged. Confirms the
    /// suffix-name-match path activates only with value overlap present.
    #[test]
    fn suffix_name_match_with_value_overlap_is_flagged() {
        let (_tb, baseline) = open_store("suffix-match-baseline");
        let (_tr, review) = open_store("suffix-match-review");

        // BE enum `StatusEnum` (authoritative).
        let be_file = file_node("service-log", "src/status.ts");
        let be_enum = owner_node("service-log", "src/status.ts", "StatusEnum", NodeKind::Type);

        // FE enum `StatusType` (hand-copy, different enum_qn, same stem) + array.
        let fe_enum_file = file_node("web-frontend", "src/status.ts");
        let fe_enum = owner_node(
            "web-frontend",
            "src/status.ts",
            "StatusType",
            NodeKind::Type,
        );
        let fe_array_file = file_node("web-frontend", "src/statusColumns.tsx");
        let fe_array = owner_node(
            "web-frontend",
            "src/statusColumns.tsx",
            "statusOptions",
            NodeKind::DataField,
        );

        let active = value_node("status.active");
        let cancelled = value_node("status.cancelled");

        baseline
            .bulk_insert(
                &[
                    be_file.clone(),
                    be_enum.clone(),
                    fe_enum_file.clone(),
                    fe_enum.clone(),
                    fe_array_file.clone(),
                    fe_array.clone(),
                    active.clone(),
                ],
                &[
                    defines_edge_qn(&be_enum, &active, &be_file, "StatusEnum"),
                    defines_edge_qn(&fe_enum, &active, &fe_enum_file, "StatusType"),
                    mirrors_edge_qn(&fe_array, &active, &fe_array_file, "StatusType"),
                ],
            )
            .expect("baseline insert");

        review
            .bulk_insert(
                &[
                    be_file.clone(),
                    be_enum.clone(),
                    fe_enum_file.clone(),
                    fe_enum.clone(),
                    fe_array_file.clone(),
                    fe_array.clone(),
                    active.clone(),
                    cancelled.clone(),
                ],
                &[
                    defines_edge_qn(&be_enum, &active, &be_file, "StatusEnum"),
                    defines_edge_qn(&be_enum, &cancelled, &be_file, "StatusEnum"),
                    defines_edge_qn(&fe_enum, &active, &fe_enum_file, "StatusType"),
                    mirrors_edge_qn(&fe_array, &active, &fe_array_file, "StatusType"),
                ],
            )
            .expect("review insert");

        let mut risks: Vec<RemovedSurfaceRisk> = Vec::new();
        extend_with_value_mirror_risks(&baseline, &review, &mut risks).expect("should succeed");

        assert!(
            risks.iter().any(|r| {
                r.kind == "value_mirror_incomplete"
                    && r.detail.as_deref().is_some_and(|d| {
                        d.contains("statusOptions") || d.contains("src/statusColumns.tsx")
                    })
            }),
            "suffix-name-matched enums sharing ≥1 value must be hand-mirror linked: {risks:?}"
        );
    }
}
