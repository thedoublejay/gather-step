//! Contract-alignment extraction — Phase 3 Task 3.
//!
//! Finds related payload contracts across repos and reports them as alignment
//! findings with a confidence level.  Two contracts are considered "aligned"
//! when their canonical identity (the `target_qualified_name` stripped of
//! common type suffixes) matches and at least two records exist in the cluster
//! with at least one non-`unknown` role.
//!
//! # Identity derivation
//!
//! Strip trailing suffixes (case-insensitive) from `target_qualified_name`:
//! `Dto`, `Payload`, `Request`, `Response`, `Body`, `Input`.
//!
//! # Role inference
//!
//! - `Dto` / `Body` / `Input` / `Response` → `backend_dto`
//! - `Payload` / `Request` from a frontend-hinted repo → `frontend_payload`
//! - `Payload` / `Request` from other repos → `backend_dto`
//! - Everything else → `unknown` (filtered from emission unless other roles
//!   are present)
//!
//! # Confidence levels
//!
//! | Jaccard overlap | Confidence |
//! |-----------------|-----------|
//! | 100 %           | `High`    |
//! | ≥ 70 %          | `Medium`  |
//! | < 70 %          | `Low`     |
//!
//! # Output cap
//!
//! At most 50 findings.  Untouched-by-PR findings are pruned first.

use anyhow::Result;
use gather_step_storage::{MetadataStore, PayloadContractQuery};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::pr_review::delta_report::{
    AlignmentConfidence, ContractAlignmentFinding, ContractAlignmentMember, ContractAlignments,
    PayloadContractDeltas,
};

/// Maximum number of alignment findings emitted.
const MAX_FINDINGS: usize = 50;

/// Suffixes stripped when computing a canonical identity (longest first).
const STRIP_SUFFIXES: &[&str] = &["payload", "request", "response", "input", "body", "dto"];

/// `(repo, file_path, qualified_name, field_names)` — one row per contract record.
type MemberRaw = (String, String, String, Vec<String>);

/// Extract contract alignment findings from the review metadata store.
///
/// `payload_changes` is used to set `touched_by_pr` on each finding.
pub fn extract_contract_alignments<M: MetadataStore>(
    review_metadata: &M,
    payload_changes: &PayloadContractDeltas,
) -> Result<ContractAlignments> {
    let records = review_metadata.payload_contracts_for_query(PayloadContractQuery::default())?;

    // Build a set of (repo, file, target_qn) for PR-touched contracts (any side).
    let mut touched: FxHashSet<(String, String, String)> = FxHashSet::default();
    for c in &payload_changes.added {
        touched.insert((
            c.repo.clone(),
            c.file.clone(),
            c.target_qualified_name.clone(),
        ));
    }
    for c in &payload_changes.removed {
        touched.insert((
            c.repo.clone(),
            c.file.clone(),
            c.target_qualified_name.clone(),
        ));
    }
    for c in &payload_changes.changed {
        touched.insert((
            c.repo.clone(),
            c.file.clone(),
            c.target_qualified_name.clone(),
        ));
    }

    // Group records by canonical identity.
    // Stored as (repo, file_path, qn, field_names).
    //
    // The type alias is hoisted to module level to avoid the
    // `items_after_statements` lint.
    let mut by_identity: FxHashMap<String, Vec<MemberRaw>> = FxHashMap::default();

    for rec in &records {
        let Some(qn) = rec.record.contract_target_qualified_name.as_deref() else {
            continue;
        };
        let identity = canonical_identity(qn);
        let field_names: Vec<String> = rec
            .record
            .contract
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect();
        by_identity.entry(identity).or_default().push((
            rec.record.repo.clone(),
            rec.record.file_path.clone(),
            qn.to_owned(),
            field_names,
        ));
    }

    let mut findings: Vec<ContractAlignmentFinding> = Vec::new();

    for (identity, members_raw) in &by_identity {
        // Need ≥2 records.
        if members_raw.len() < 2 {
            continue;
        }

        // Build members with role inference.
        let members: Vec<ContractAlignmentMember> = members_raw
            .iter()
            .map(|(repo, file, qn, _)| ContractAlignmentMember {
                role: infer_role(qn, repo),
                repo: repo.clone(),
                qualified_name: qn.clone(),
                file: if file.is_empty() {
                    None
                } else {
                    Some(file.clone())
                },
            })
            .collect();

        // Must have ≥2 distinct roles (skip all-unknown clusters).
        let non_unknown_count = members.iter().filter(|m| m.role != "unknown").count();
        if non_unknown_count < 1 {
            continue;
        }

        let confidence = compute_confidence(members_raw);

        let touched_by_pr = members_raw.iter().any(|(repo, file, qn, _)| {
            touched
                .iter()
                .any(|(tr, tf, tqn)| tr == repo && tf == file && tqn == qn)
        });

        findings.push(ContractAlignmentFinding {
            identity: identity.clone(),
            members,
            confidence,
            touched_by_pr,
        });
    }

    // Sort: touched_by_pr desc, confidence desc, identity asc.
    findings.sort_by(|a, b| {
        b.touched_by_pr
            .cmp(&a.touched_by_pr)
            .then_with(|| b.confidence.cmp(&a.confidence))
            .then_with(|| a.identity.cmp(&b.identity))
    });

    if findings.len() > MAX_FINDINGS {
        findings.truncate(MAX_FINDINGS);
    }

    Ok(ContractAlignments {
        findings,
        unavailable: false,
    })
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Derive a canonical identity by stripping well-known type suffixes.
fn canonical_identity(qn: &str) -> String {
    // Strip any leading virtual-node prefix (e.g. "__topic__kafka__") first
    // by taking the last `__`-separated segment.
    let name = if let Some(pos) = qn.rfind("__") {
        if pos + 2 < qn.len() {
            &qn[pos + 2..]
        } else {
            qn
        }
    } else {
        qn
    };

    // Use ASCII case-insensitive comparison to avoid heap allocations.
    for suffix in STRIP_SUFFIXES {
        if name.len() > suffix.len()
            && name[name.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
        {
            return name[..name.len() - suffix.len()].to_owned();
        }
    }
    name.to_owned()
}

/// Returns `true` if `s` ends with `suffix` (ASCII case-insensitive).
fn ends_with_ignore_ascii(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len() && s[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
}

/// Returns `true` if `s` contains `needle` (ASCII case-insensitive).
fn contains_ignore_ascii(s: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let nl = needle.len();
    if s.len() < nl {
        return false;
    }
    (0..=s.len() - nl).any(|i| s[i..i + nl].eq_ignore_ascii_case(needle))
}

/// Infer the role of a contract member from its `target_qualified_name` and repo.
fn infer_role(qn: &str, repo: &str) -> String {
    let is_frontend_repo = contains_ignore_ascii(repo, "front")
        || contains_ignore_ascii(repo, "web")
        || contains_ignore_ascii(repo, "client");

    if ends_with_ignore_ascii(qn, "dto")
        || ends_with_ignore_ascii(qn, "body")
        || ends_with_ignore_ascii(qn, "input")
        || ends_with_ignore_ascii(qn, "response")
    {
        "backend_dto".to_owned()
    } else if ends_with_ignore_ascii(qn, "payload") || ends_with_ignore_ascii(qn, "request") {
        if is_frontend_repo {
            "frontend_payload".to_owned()
        } else {
            "backend_dto".to_owned()
        }
    } else if is_frontend_repo {
        "frontend_payload".to_owned()
    } else {
        "unknown".to_owned()
    }
}

/// Compute alignment confidence from the minimum pairwise Jaccard similarity.
fn compute_confidence(members: &[(String, String, String, Vec<String>)]) -> AlignmentConfidence {
    if members.len() < 2 {
        return AlignmentConfidence::Low;
    }

    // Scaled ×1000; starts at maximum (all pairs identical) and shrinks.
    let mut min_scaled_pct: usize = 1000;

    for i in 0..members.len() {
        for j in (i + 1)..members.len() {
            let a: FxHashSet<&str> = members[i].3.iter().map(String::as_str).collect();
            let b: FxHashSet<&str> = members[j].3.iter().map(String::as_str).collect();

            let union = a.union(&b).count();
            if union == 0 {
                // Both empty — treat as identical.
                continue;
            }

            let intersection = a.intersection(&b).count();
            // Use integer arithmetic (scaled ×1000) to avoid f64 cast precision
            // loss while keeping threshold comparisons exact.
            // scaled_pct = intersection * 1000 / union → ≥700 means ≥70 %.
            let scaled_pct = intersection * 1000 / union;

            if scaled_pct < min_scaled_pct {
                min_scaled_pct = scaled_pct;
            }
        }
    }

    // 1000 → 100 %, 700 → 70 %.
    if min_scaled_pct >= 1000 {
        AlignmentConfidence::High
    } else if min_scaled_pct >= 700 {
        AlignmentConfidence::Medium
    } else {
        AlignmentConfidence::Low
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use crate::pr_review::delta_report::{PayloadContractDeltaChange, PayloadFieldSummary};
    use gather_step_core::{
        NodeKind, PayloadContractDoc, PayloadContractRecord, PayloadField, PayloadInferenceKind,
        PayloadSide, ref_node_id,
    };
    use gather_step_storage::{MetadataStoreDb, PayloadContractStoreRecord};

    use super::*;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-align-{label}-{}-{id}.sqlite3",
                std::process::id()
            ));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(
                    self.path
                        .with_extension(format!("sqlite3{suffix}").trim_start_matches('.')),
                );
                // Also try removing plain path + suffix.
                let p = format!("{}{}", self.path.display(), suffix);
                let _ = std::fs::remove_file(&p);
            }
        }
    }

    fn open_store(label: &str) -> (TempDb, MetadataStoreDb) {
        let td = TempDb::new(label);
        let db = MetadataStoreDb::open(&td.path).expect("open store");
        (td, db)
    }

    fn make_record(
        repo: &str,
        file: &str,
        target_qn: &str,
        side: PayloadSide,
        fields: Vec<PayloadField>,
    ) -> PayloadContractStoreRecord {
        let target_id = ref_node_id(NodeKind::Topic, target_qn);
        let source_id = ref_node_id(NodeKind::Function, &format!("{repo}::handler"));
        let contract_id = ref_node_id(
            NodeKind::PayloadContract,
            &format!("__pc__{repo}__{target_qn}"),
        );
        PayloadContractStoreRecord {
            record: PayloadContractRecord {
                payload_contract_node_id: contract_id,
                contract_target_node_id: target_id,
                contract_target_kind: NodeKind::Topic,
                contract_target_qualified_name: Some(target_qn.to_owned()),
                repo: repo.to_owned(),
                file_path: file.to_owned(),
                source_symbol_node_id: source_id,
                line_start: Some(10),
                side,
                inference_kind: PayloadInferenceKind::LiteralObject,
                confidence: 900,
                source_type_name: None,
                contract: PayloadContractDoc {
                    content_type: "application/json".to_owned(),
                    schema_format: "normalized_object".to_owned(),
                    side,
                    inference_kind: PayloadInferenceKind::LiteralObject,
                    confidence: 900,
                    fields,
                    source_type_name: None,
                },
            },
        }
    }

    fn field(name: &str) -> PayloadField {
        PayloadField {
            name: name.to_owned(),
            type_name: "string".to_owned(),
            optional: false,
            confidence: 900,
        }
    }

    fn insert(store: &MetadataStoreDb, rec: PayloadContractStoreRecord) {
        let repo = rec.record.repo.clone();
        let file = rec.record.file_path.clone();
        store
            .replace_payload_contracts_for_files(&repo, &[file], &[rec])
            .expect("insert");
    }

    /// Two records with same identity and identical fields → 1 finding, High confidence.
    #[test]
    fn alignment_groups_dto_payload_pair_with_high_confidence() {
        let (_td, store) = open_store("high-conf");

        let fields = vec![field("id"), field("name"), field("value")];
        insert(
            &store,
            make_record(
                "backend",
                "src/dto.ts",
                "UpdateLabelProjectDto",
                PayloadSide::Producer,
                fields.clone(),
            ),
        );
        insert(
            &store,
            make_record(
                "frontend",
                "src/payload.ts",
                "UpdateLabelProjectPayload",
                PayloadSide::Consumer,
                fields,
            ),
        );

        let deltas = PayloadContractDeltas::default();
        let result = extract_contract_alignments(&store, &deltas).expect("should succeed");

        assert_eq!(result.findings.len(), 1, "expected 1 finding");
        let f = &result.findings[0];
        assert_eq!(f.members.len(), 2);
        assert_eq!(f.confidence, AlignmentConfidence::High);
        assert!(!f.touched_by_pr);
    }

    /// 6 fields each, 5 shared, 1 unique each.
    /// Jaccard: intersection=5, union=7 → 5/7 = 71.4 % → Medium.
    #[test]
    fn alignment_with_partial_overlap_is_medium_confidence() {
        let (_td, store) = open_store("medium-conf");

        // 6 fields each, 5 shared, 1 unique each → union=7, intersection=5 → 71.4 % → Medium
        insert(
            &store,
            make_record(
                "backend",
                "src/dto.ts",
                "CreateOrderDto",
                PayloadSide::Producer,
                vec![
                    field("a"),
                    field("b"),
                    field("c"),
                    field("d"),
                    field("e"),
                    field("x"),
                ],
            ),
        );
        insert(
            &store,
            make_record(
                "frontend",
                "src/payload.ts",
                "CreateOrderPayload",
                PayloadSide::Consumer,
                vec![
                    field("a"),
                    field("b"),
                    field("c"),
                    field("d"),
                    field("e"),
                    field("y"),
                ],
            ),
        );

        let deltas = PayloadContractDeltas::default();
        let result = extract_contract_alignments(&store, &deltas).expect("should succeed");

        assert_eq!(result.findings.len(), 1);
        assert_eq!(
            result.findings[0].confidence,
            AlignmentConfidence::Medium,
            "5/7 Jaccard → 71.4 % → Medium"
        );
    }

    /// `touched_by_pr` is `true` when a member appears in `payload_changes.changed`.
    #[test]
    fn alignment_marks_pr_touched_when_member_changed() {
        let (_td, store) = open_store("touched");

        let fields = vec![field("id"), field("name")];
        insert(
            &store,
            make_record(
                "backend",
                "src/dto.ts",
                "DeleteItemDto",
                PayloadSide::Producer,
                fields.clone(),
            ),
        );
        insert(
            &store,
            make_record(
                "frontend",
                "src/types.ts",
                "DeleteItemPayload",
                PayloadSide::Consumer,
                fields,
            ),
        );

        let mut deltas = PayloadContractDeltas::default();
        deltas.changed.push(PayloadContractDeltaChange {
            repo: "backend".to_owned(),
            file: "src/dto.ts".to_owned(),
            target_qualified_name: "DeleteItemDto".to_owned(),
            side: "producer".to_owned(),
            fields_added: vec![PayloadFieldSummary {
                name: "extra".to_owned(),
                type_name: Some("string".to_owned()),
                optional: false,
            }],
            fields_removed: vec![],
            fields_optional_to_required: vec![],
            fields_required_to_optional: vec![],
            fields_type_changed: vec![],
            impact: None,
        });

        let result = extract_contract_alignments(&store, &deltas).expect("should succeed");
        assert_eq!(result.findings.len(), 1);
        assert!(
            result.findings[0].touched_by_pr,
            "should be marked as touched by PR"
        );
    }

    /// Single record — no alignment finding emitted.
    #[test]
    fn alignment_skips_clusters_with_only_one_member() {
        let (_td, store) = open_store("single");

        insert(
            &store,
            make_record(
                "backend",
                "src/dto.ts",
                "OnlyOneDto",
                PayloadSide::Producer,
                vec![field("id")],
            ),
        );

        let deltas = PayloadContractDeltas::default();
        let result = extract_contract_alignments(&store, &deltas).expect("should succeed");
        assert!(
            result.findings.is_empty(),
            "single member must not produce an alignment finding"
        );
    }
}
