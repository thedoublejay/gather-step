//! Parity gate — compare two [`DeltaReport`]s for surface-level equivalence.
//!
//! Phase 5 Task 4: an automated check that the overlay engine matches the
//! temp-index engine output for every supported surface.  Implemented as a
//! deterministic comparator used by unit tests; a fixture-driven end-to-end
//! test is included but gated with `#[ignore]` until overlay extraction is
//! wired (Phase 5 follow-up).

use crate::pr_review::delta_report::DeltaReport;

// ─── Public API ───────────────────────────────────────────────────────────────

/// The result of comparing two [`DeltaReport`]s for parity.
pub struct ParityDiff {
    /// Human-readable explanations of each discrepancy found.
    pub differences: Vec<String>,
}

impl ParityDiff {
    /// `true` when the two reports are considered equivalent across all
    /// supported surfaces.
    pub fn is_match(&self) -> bool {
        self.differences.is_empty()
    }
}

/// Compare two [`DeltaReport`]s for surface-level parity.
///
/// Ignores volatile fields (`elapsed_ms`, `run_id`, file-system paths under
/// the cache root).  For surfaces marked `unavailable: true` on the overlay
/// side, parity is not checked — it is not expected to match.  For surfaces
/// the overlay claims to support, the lists must be set-equal (comparisons
/// are order-independent; all collections are sorted before comparing).
pub fn compare_for_parity(temp_index: &DeltaReport, overlay: &DeltaReport) -> ParityDiff {
    let mut differences: Vec<String> = Vec::new();

    // ── Routes ────────────────────────────────────────────────────────────────
    if !overlay.routes.unavailable {
        compare_route_lists(
            "routes.added",
            &temp_index.routes.added,
            &overlay.routes.added,
            &mut differences,
        );
        compare_route_lists(
            "routes.removed",
            &temp_index.routes.removed,
            &overlay.routes.removed,
            &mut differences,
        );
        compare_route_changed_lists(
            "routes.changed",
            &temp_index.routes.changed,
            &overlay.routes.changed,
            &mut differences,
        );
    }

    // ── Symbols ───────────────────────────────────────────────────────────────
    if !overlay.symbols.unavailable {
        compare_symbol_lists(
            "symbols.added",
            &temp_index.symbols.added,
            &overlay.symbols.added,
            &mut differences,
        );
        compare_symbol_lists(
            "symbols.removed",
            &temp_index.symbols.removed,
            &overlay.symbols.removed,
            &mut differences,
        );
        compare_symbol_changed_lists(
            "symbols.changed",
            &temp_index.symbols.changed,
            &overlay.symbols.changed,
            &mut differences,
        );
    }

    // ── Payload contracts ─────────────────────────────────────────────────────
    if !overlay.payload_contracts.unavailable {
        compare_payload_lists(
            "payload_contracts.added",
            &temp_index.payload_contracts.added,
            &overlay.payload_contracts.added,
            &mut differences,
        );
        compare_payload_lists(
            "payload_contracts.removed",
            &temp_index.payload_contracts.removed,
            &overlay.payload_contracts.removed,
            &mut differences,
        );
    }

    // ── Events ────────────────────────────────────────────────────────────────
    if !overlay.events.unavailable {
        compare_event_lists(
            "events.added",
            &temp_index.events.added,
            &overlay.events.added,
            &mut differences,
        );
        compare_event_lists(
            "events.removed",
            &temp_index.events.removed,
            &overlay.events.removed,
            &mut differences,
        );
    }

    // ── Decorators ────────────────────────────────────────────────────────────
    if !overlay.decorators.unavailable {
        compare_decorator_lists(
            "decorators.added",
            &temp_index.decorators.added,
            &overlay.decorators.added,
            &mut differences,
        );
        compare_decorator_lists(
            "decorators.removed",
            &temp_index.decorators.removed,
            &overlay.decorators.removed,
            &mut differences,
        );
    }

    // ── Contract alignments ───────────────────────────────────────────────────
    if !overlay.contract_alignments.unavailable {
        compare_alignment_lists(
            "contract_alignments.findings",
            &temp_index.contract_alignments.findings,
            &overlay.contract_alignments.findings,
            &mut differences,
        );
    }

    // ── Removed-surface risks ─────────────────────────────────────────────────
    // Risks are derived from routes/symbols/events; if those surfaces are
    // available on the overlay, risks should match too.
    let risks_surface_available = !overlay.routes.unavailable
        && !overlay.symbols.unavailable
        && !overlay.events.unavailable;
    if risks_surface_available {
        compare_risk_lists(
            &temp_index.removed_surface_risks,
            &overlay.removed_surface_risks,
            &mut differences,
        );
    }

    ParityDiff { differences }
}

// ─── Per-surface comparison helpers ──────────────────────────────────────────

use crate::pr_review::delta_report::{
    ContractAlignmentFinding, DecoratorDelta, EventDelta, PayloadContractDelta, RemovedSurfaceRisk,
    RouteDelta, RouteDeltaChange, SymbolDelta, SymbolDeltaChange,
};

fn route_key(r: &RouteDelta) -> String {
    format!("{} {}", r.method, r.path)
}

fn compare_route_lists(
    label: &str,
    a: &[RouteDelta],
    b: &[RouteDelta],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(route_key).collect();
    let mut bk: Vec<String> = b.iter().map(route_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn compare_route_changed_lists(
    label: &str,
    a: &[RouteDeltaChange],
    b: &[RouteDeltaChange],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a
        .iter()
        .map(|c| format!("{} {}", c.method, c.path))
        .collect();
    let mut bk: Vec<String> = b
        .iter()
        .map(|c| format!("{} {}", c.method, c.path))
        .collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn symbol_key(s: &SymbolDelta) -> String {
    format!("{}::{}", s.repo, s.qualified_name)
}

fn compare_symbol_lists(
    label: &str,
    a: &[SymbolDelta],
    b: &[SymbolDelta],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(symbol_key).collect();
    let mut bk: Vec<String> = b.iter().map(symbol_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn compare_symbol_changed_lists(
    label: &str,
    a: &[SymbolDeltaChange],
    b: &[SymbolDeltaChange],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a
        .iter()
        .map(|c| format!("{}::{}", c.repo, c.qualified_name))
        .collect();
    let mut bk: Vec<String> = b
        .iter()
        .map(|c| format!("{}::{}", c.repo, c.qualified_name))
        .collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn payload_key(p: &PayloadContractDelta) -> String {
    format!("{}::{}::{}", p.repo, p.target_qualified_name, p.side)
}

fn compare_payload_lists(
    label: &str,
    a: &[PayloadContractDelta],
    b: &[PayloadContractDelta],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(payload_key).collect();
    let mut bk: Vec<String> = b.iter().map(payload_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn event_key(e: &EventDelta) -> String {
    format!("{}:{}", e.event_kind, e.event_name)
}

fn compare_event_lists(
    label: &str,
    a: &[EventDelta],
    b: &[EventDelta],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(event_key).collect();
    let mut bk: Vec<String> = b.iter().map(event_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn decorator_key(d: &DecoratorDelta) -> String {
    format!(
        "{}::{}::{}",
        d.repo,
        d.decorator_name,
        d.target_qualified_name.as_deref().unwrap_or("__none__")
    )
}

fn compare_decorator_lists(
    label: &str,
    a: &[DecoratorDelta],
    b: &[DecoratorDelta],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(decorator_key).collect();
    let mut bk: Vec<String> = b.iter().map(decorator_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn alignment_key(f: &ContractAlignmentFinding) -> String {
    f.identity.clone()
}

fn compare_alignment_lists(
    label: &str,
    a: &[ContractAlignmentFinding],
    b: &[ContractAlignmentFinding],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(alignment_key).collect();
    let mut bk: Vec<String> = b.iter().map(alignment_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "{label}: temp-index has {ak:?}, overlay has {bk:?}"
        ));
    }
}

fn risk_key(r: &RemovedSurfaceRisk) -> String {
    format!("{}:{}", r.kind, r.identity)
}

fn compare_risk_lists(
    a: &[RemovedSurfaceRisk],
    b: &[RemovedSurfaceRisk],
    diffs: &mut Vec<String>,
) {
    let mut ak: Vec<String> = a.iter().map(risk_key).collect();
    let mut bk: Vec<String> = b.iter().map(risk_key).collect();
    ak.sort();
    bk.sort();
    if ak != bk {
        diffs.push(format!(
            "removed_surface_risks: temp-index has {ak:?}, overlay has {bk:?}"
        ));
        return;
    }
    // Also check severity for matching keys.
    let mut a_sorted = a.to_vec();
    let mut b_sorted = b.to_vec();
    a_sorted.sort_by_key(risk_key);
    b_sorted.sort_by_key(risk_key);
    for (ra, rb) in a_sorted.iter().zip(b_sorted.iter()) {
        if ra.severity != rb.severity {
            diffs.push(format!(
                "removed_surface_risks[{}]: severity mismatch: temp-index={:?}, overlay={:?}",
                risk_key(ra),
                ra.severity,
                rb.severity,
            ));
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::pr_review::delta_report::{
        CleanupPolicy, ContractAlignments, DecoratorDeltas, DeltaReport, EventDeltas,
        PayloadContractDeltas, ReviewMetadata, RiskSeverity, RemovedSurfaceRisk,
        RouteDelta, RouteDeltas, SafetyMetadata, SymbolDeltas,
    };

    use super::compare_for_parity;

    fn empty_report() -> DeltaReport {
        DeltaReport {
            schema_version: 5,
            metadata: ReviewMetadata {
                workspace: PathBuf::from("/tmp/ws"),
                base_input: "main".to_owned(),
                base_sha: "a".repeat(40),
                head_input: "HEAD".to_owned(),
                head_sha: "b".repeat(40),
                checkout_mode: "head".to_owned(),
                changed_repos: vec![],
                indexed_repos: vec![],
                elapsed_ms: 0,
                warnings: vec![],
            },
            safety: SafetyMetadata {
                baseline_registry_path: PathBuf::from("/tmp/reg.json"),
                baseline_storage_path: PathBuf::from("/tmp/storage"),
                review_registry_path: PathBuf::from("/tmp/rev/reg.json"),
                review_storage_path: PathBuf::from("/tmp/rev/storage"),
                review_root: PathBuf::from("/tmp/rev"),
                run_id: "test-run".to_owned(),
                cleanup_policy: CleanupPolicy::RemoveOnExit,
                cache_key: "hash:aaa:bbb".to_owned(),
            },
            changed_files: vec![],
            changed_files_truncated: false,
            routes: RouteDeltas::default(),
            symbols: SymbolDeltas::default(),
            payload_contracts: PayloadContractDeltas::default(),
            events: EventDeltas::default(),
            removed_surface_risks: vec![],
            contract_alignments: ContractAlignments::default(),
            decorators: DecoratorDeltas::default(),
            suggested_followups: vec![],
            unsupported_surfaces: vec![],
        }
    }

    fn make_route(method: &str, path: &str) -> RouteDelta {
        RouteDelta {
            method: method.to_owned(),
            path: path.to_owned(),
            repo: None,
            file: None,
            line: None,
            handler_qualified_name: None,
            impact: None,
        }
    }

    // ── Test 1: identical routes → match ──────────────────────────────────────

    #[test]
    fn parity_match_when_both_engines_emit_same_routes() {
        let route = make_route("GET", "/orders");

        let mut ti = empty_report();
        ti.routes.added.push(route.clone());

        let mut ov = empty_report();
        ov.routes.added.push(route);

        let diff = compare_for_parity(&ti, &ov);
        assert!(diff.is_match(), "expected match but got: {:?}", diff.differences);
    }

    // ── Test 2: differing routes → mismatch ──────────────────────────────────

    #[test]
    fn parity_mismatch_when_routes_differ() {
        let mut ti = empty_report();
        ti.routes.added.push(make_route("GET", "/orders"));

        let mut ov = empty_report();
        ov.routes.added.push(make_route("GET", "/orders"));
        ov.routes.added.push(make_route("POST", "/orders"));

        let diff = compare_for_parity(&ti, &ov);
        assert!(!diff.is_match(), "expected mismatch");
        assert!(
            diff.differences.iter().any(|d| d.contains("POST /orders")),
            "differences should mention the extra route: {:?}",
            diff.differences
        );
    }

    // ── Test 3: unavailable surface skipped ───────────────────────────────────

    #[test]
    fn parity_skips_unavailable_surfaces() {
        use crate::pr_review::delta_report::{AlignmentConfidence, ContractAlignmentFinding};

        // temp-index has 5 contract alignments.
        let mut ti = empty_report();
        for i in 0..5 {
            ti.contract_alignments.findings.push(ContractAlignmentFinding {
                identity: format!("Contract{i}"),
                members: vec![],
                confidence: AlignmentConfidence::High,
                touched_by_pr: false,
            });
        }

        // overlay marks contract_alignments as unavailable.
        let mut ov = empty_report();
        ov.contract_alignments.unavailable = true;
        // (findings list is empty — it was skipped)

        let diff = compare_for_parity(&ti, &ov);
        assert!(
            diff.is_match(),
            "unavailable surface must be skipped in parity check: {:?}",
            diff.differences
        );
    }

    // ── Test 4: severity mismatch in risks ────────────────────────────────────

    #[test]
    fn parity_detects_severity_mismatch_in_risks() {
        fn make_risk(identity: &str, severity: RiskSeverity) -> RemovedSurfaceRisk {
            RemovedSurfaceRisk {
                kind: "shared_symbol".to_owned(),
                identity: identity.to_owned(),
                repo: Some("backend".to_owned()),
                surviving_consumers: vec![],
                severity,
            }
        }

        let mut ti = empty_report();
        ti.removed_surface_risks
            .push(make_risk("UpdateLabelProject", RiskSeverity::High));

        let mut ov = empty_report();
        ov.removed_surface_risks
            .push(make_risk("UpdateLabelProject", RiskSeverity::Medium));

        let diff = compare_for_parity(&ti, &ov);
        assert!(!diff.is_match(), "severity mismatch should be detected");
        assert!(
            diff.differences.iter().any(|d| d.contains("severity mismatch")),
            "difference message should mention severity: {:?}",
            diff.differences
        );
    }

    // ── End-to-end fixture test (Phase 5 follow-up) ────────────────────────────

    #[test]
    #[ignore = "Phase 5 follow-up: enable when DiffOverlayStore-backed extraction is wired"]
    fn overlay_engine_matches_temp_index_on_route_fixture() {
        // Run both engines on the same 2-commit fixture, compare.
        //
        // Sketch (to be filled in when overlay extraction lands):
        //
        //   let fixture = prepare_two_commit_fixture();
        //   let ti_report = run_with_engine(&fixture, ReviewEngine::TempIndex);
        //   let ov_report = run_with_engine(&fixture, ReviewEngine::Overlay);
        //   let diff = compare_for_parity(&ti_report, &ov_report);
        //   assert!(diff.is_match(), "{:?}", diff.differences);
    }
}
