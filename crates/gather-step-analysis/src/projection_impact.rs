use std::collections::{BTreeMap, BTreeSet};

use gather_step_core::{EdgeData, EdgeKind, NodeData, NodeId, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum ProjectionImpactError {
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionImpactRequest {
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    #[serde(default = "default_max_results")]
    pub max_results: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionImpactReport {
    pub target: String,
    pub resolved: bool,
    pub ambiguity: Option<String>,
    pub candidates: Vec<ProjectionField>,
    pub source_fields: Vec<ProjectionField>,
    pub projected_fields: Vec<ProjectionField>,
    pub derivation_edges: Vec<ProjectionDerivation>,
    pub readers: Vec<ProjectionEvidence>,
    pub writers: Vec<ProjectionEvidence>,
    pub filters: Vec<ProjectionEvidence>,
    pub indexes: Vec<ProjectionEvidence>,
    pub backfills: Vec<ProjectionEvidence>,
    pub risk_hints: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub confidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectionField {
    pub repo: String,
    pub field_path: String,
    pub qualified_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectionDerivation {
    pub source: ProjectionField,
    pub projected: ProjectionField,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProjectionEvidence {
    pub repo: String,
    pub file_path: String,
    pub field_path: String,
    pub edge_kind: EdgeKind,
    pub confidence: Option<u16>,
}

const FIELD_EDGE_KINDS: [EdgeKind; 6] = [
    EdgeKind::ReadsField,
    EdgeKind::WritesField,
    EdgeKind::DerivesFieldFrom,
    EdgeKind::FiltersOnField,
    EdgeKind::IndexesField,
    EdgeKind::BackfillsField,
];

fn default_max_results() -> usize {
    20
}

pub fn projection_impact<S: GraphStore>(
    store: &S,
    request: ProjectionImpactRequest,
) -> Result<ProjectionImpactReport, ProjectionImpactError> {
    let max_results = request.max_results.clamp(1, 100);
    let candidates = resolve_field_candidates(store, &request.target, request.repo.as_deref())?;
    let selected = candidates
        .iter()
        .take(max_results)
        .cloned()
        .collect::<Vec<_>>();
    let selected_ids = selected.iter().map(|node| node.id).collect::<BTreeSet<_>>();

    let mut source_ids = BTreeSet::new();
    let mut projected_ids = BTreeSet::new();
    let mut derivations = BTreeSet::new();

    for candidate in &selected {
        let incoming = store.get_incoming(candidate.id)?;
        for edge in incoming
            .iter()
            .filter(|edge| edge.kind == EdgeKind::DerivesFieldFrom)
        {
            source_ids.insert(edge.source);
            projected_ids.insert(edge.target);
            derivations.insert((edge.source, edge.target));
        }

        let outgoing = store.get_outgoing(candidate.id)?;
        for edge in outgoing
            .iter()
            .filter(|edge| edge.kind == EdgeKind::DerivesFieldFrom)
        {
            source_ids.insert(edge.source);
            projected_ids.insert(edge.target);
            derivations.insert((edge.source, edge.target));
        }

        if !incoming
            .iter()
            .chain(outgoing.iter())
            .any(|edge| edge.kind == EdgeKind::DerivesFieldFrom)
        {
            source_ids.insert(candidate.id);
        }
    }

    let mut relevant_ids = selected_ids;
    relevant_ids.extend(source_ids.iter().copied());
    relevant_ids.extend(projected_ids.iter().copied());

    let mut evidence = EvidenceBuckets::default();
    for field_id in &relevant_ids {
        for edge in store.get_incoming(*field_id)? {
            if !FIELD_EDGE_KINDS.contains(&edge.kind) || edge.kind == EdgeKind::DerivesFieldFrom {
                continue;
            }
            if let Some(field) = store.get_node(*field_id)?
                && let Some(item) = evidence_item(store, &field, &edge)?
            {
                evidence.push(edge.kind, item);
            }
        }
    }

    let mut report = ProjectionImpactReport {
        target: request.target,
        resolved: !selected.is_empty(),
        ambiguity: (selected.len() > 1).then(|| "multiple_field_candidates".to_owned()),
        candidates: nodes_to_fields(&selected),
        source_fields: ids_to_fields(store, &source_ids)?,
        projected_fields: ids_to_fields(store, &projected_ids)?,
        derivation_edges: derivations_to_report(store, &derivations)?,
        readers: evidence.readers.into_values().collect(),
        writers: evidence.writers.into_values().collect(),
        filters: evidence.filters.into_values().collect(),
        indexes: evidence.indexes.into_values().collect(),
        backfills: evidence.backfills.into_values().collect(),
        risk_hints: Vec::new(),
        missing_evidence: Vec::new(),
        confidence: "low".to_owned(),
    };

    populate_risks_and_confidence(&mut report);
    Ok(report)
}

fn resolve_field_candidates<S: GraphStore>(
    store: &S,
    target: &str,
    repo: Option<&str>,
) -> Result<Vec<NodeData>, ProjectionImpactError> {
    let target = target.trim();
    if target.is_empty() {
        return Ok(Vec::new());
    }
    let qualified_suffix = format!("::{target}");
    let mut exact = Vec::new();
    let mut fuzzy = Vec::new();
    for node in store.nodes_by_type(NodeKind::DataField)? {
        if repo.is_some_and(|repo| node.repo != repo) {
            continue;
        }
        let qualified = node.qualified_name.as_deref().unwrap_or_default();
        let external = node.external_id.as_deref().unwrap_or_default();
        if node.name.eq_ignore_ascii_case(target)
            || ends_with_ascii_case(qualified, &qualified_suffix)
        {
            exact.push(node);
        } else if contains_ascii_case(&node.name, target)
            || contains_ascii_case(qualified, target)
            || contains_ascii_case(external, target)
        {
            fuzzy.push(node);
        }
    }
    exact.sort_by_key(field_node_sort_key);
    fuzzy.sort_by_key(field_node_sort_key);
    if exact.is_empty() {
        Ok(fuzzy)
    } else {
        Ok(exact)
    }
}

fn field_node_sort_key(node: &NodeData) -> (String, String, String) {
    (
        node.repo.clone(),
        node.name.clone(),
        node.qualified_name.clone().unwrap_or_default(),
    )
}

fn ids_to_fields<S: GraphStore>(
    store: &S,
    ids: &BTreeSet<NodeId>,
) -> Result<Vec<ProjectionField>, ProjectionImpactError> {
    let mut fields = Vec::new();
    for id in ids {
        if let Some(node) = store.get_node(*id)? {
            fields.push(field_from_node(&node));
        }
    }
    fields.sort();
    Ok(fields)
}

fn nodes_to_fields(nodes: &[NodeData]) -> Vec<ProjectionField> {
    let mut fields = nodes.iter().map(field_from_node).collect::<Vec<_>>();
    fields.sort();
    fields
}

fn field_from_node(node: &NodeData) -> ProjectionField {
    ProjectionField {
        repo: node.repo.clone(),
        field_path: node.name.clone(),
        qualified_name: node.qualified_name.clone(),
    }
}

fn derivations_to_report<S: GraphStore>(
    store: &S,
    derivations: &BTreeSet<(NodeId, NodeId)>,
) -> Result<Vec<ProjectionDerivation>, ProjectionImpactError> {
    let mut result = Vec::new();
    for (source, projected) in derivations {
        let Some(source) = store.get_node(*source)? else {
            continue;
        };
        let Some(projected) = store.get_node(*projected)? else {
            continue;
        };
        result.push(ProjectionDerivation {
            source: field_from_node(&source),
            projected: field_from_node(&projected),
        });
    }
    result.sort();
    Ok(result)
}

fn evidence_item<S: GraphStore>(
    store: &S,
    field: &NodeData,
    edge: &EdgeData,
) -> Result<Option<ProjectionEvidence>, ProjectionImpactError> {
    let Some(owner) = store.get_node(edge.owner_file)? else {
        return Ok(None);
    };
    Ok(Some(ProjectionEvidence {
        repo: owner.repo,
        file_path: owner.file_path,
        field_path: field.name.clone(),
        edge_kind: edge.kind,
        confidence: edge.metadata.confidence,
    }))
}

#[derive(Default)]
struct EvidenceBuckets {
    readers: BTreeMap<(String, String, String), ProjectionEvidence>,
    writers: BTreeMap<(String, String, String), ProjectionEvidence>,
    filters: BTreeMap<(String, String, String), ProjectionEvidence>,
    indexes: BTreeMap<(String, String, String), ProjectionEvidence>,
    backfills: BTreeMap<(String, String, String), ProjectionEvidence>,
}

impl EvidenceBuckets {
    fn push(&mut self, kind: EdgeKind, item: ProjectionEvidence) {
        let key = (
            item.repo.clone(),
            item.file_path.clone(),
            item.field_path.clone(),
        );
        match kind {
            EdgeKind::ReadsField => {
                self.readers.insert(key, item);
            }
            EdgeKind::WritesField => {
                self.writers.insert(key, item);
            }
            EdgeKind::FiltersOnField => {
                self.filters.insert(key, item);
            }
            EdgeKind::IndexesField => {
                self.indexes.insert(key, item);
            }
            EdgeKind::BackfillsField => {
                self.backfills.insert(key, item);
            }
            _ => {}
        }
    }
}

fn populate_risks_and_confidence(report: &mut ProjectionImpactReport) {
    if !report.resolved {
        report
            .risk_hints
            .push("field_candidate_not_found".to_owned());
        report.missing_evidence.push("data_field".to_owned());
        "low".clone_into(&mut report.confidence);
        return;
    }

    if report.ambiguity.is_some() {
        report.risk_hints.push("needs_disambiguation".to_owned());
    }
    if report.derivation_edges.is_empty() {
        report
            .risk_hints
            .push("projection_chain_unproven".to_owned());
        report.missing_evidence.push("derivation_edge".to_owned());
    }
    if !report.projected_fields.is_empty() && report.writers.is_empty() {
        report
            .risk_hints
            .push("projection_writer_missing".to_owned());
        report.missing_evidence.push("writer".to_owned());
    }
    if !report.projected_fields.is_empty() && report.backfills.is_empty() {
        report.risk_hints.push("backfill_unproven".to_owned());
        report.missing_evidence.push("backfill".to_owned());
    }
    if !report.projected_fields.is_empty() && report.indexes.is_empty() {
        report
            .risk_hints
            .push("index_or_search_mapping_unproven".to_owned());
        report
            .missing_evidence
            .push("index_or_search_mapping".to_owned());
    }
    if !report.filters.is_empty() {
        report
            .risk_hints
            .push("filter_contract_impacted".to_owned());
    }
    if !report.derivation_edges.is_empty() {
        report
            .risk_hints
            .push("deployed_owner_unchecked".to_owned());
    }

    report.risk_hints.sort();
    report.risk_hints.dedup();
    report.missing_evidence.sort();
    report.missing_evidence.dedup();

    report.confidence = if !report.derivation_edges.is_empty()
        && (!report.writers.is_empty() || !report.readers.is_empty())
    {
        "high".to_owned()
    } else if !report.derivation_edges.is_empty() || !report.candidates.is_empty() {
        "medium".to_owned()
    } else {
        "low".to_owned()
    };
}

fn contains_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn ends_with_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack.len() >= needle.len()
        && haystack.as_bytes()[haystack.len() - needle.len()..]
            .eq_ignore_ascii_case(needle.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{TempDb, file_node};
    use gather_step_core::{EdgeMetadata, node_id, virtual_node};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    fn field(repo: &str, name: &str) -> NodeData {
        virtual_node(
            NodeKind::DataField,
            repo,
            "<data-field>",
            name,
            format!("data-field::{repo}::{name}"),
        )
    }

    fn edge(source: NodeId, target: NodeId, owner_file: NodeId, kind: EdgeKind) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata {
                confidence: Some(900),
                ..EdgeMetadata::default()
            },
            owner_file,
            is_cross_file: false,
        }
    }

    fn store_with_projection() -> GraphStoreDb {
        let temp = TempDb::new("projection-impact", "chain");
        let store = temp.open();
        let file = file_node("svc", "src/task.ts");
        let backfill = file_node("svc", "migrations/backfill-subtasks.ts");
        let index = file_node("svc", "src/task.index.ts");
        let subtasks = field("svc", "subtasks");
        let subtask_ids = field("svc", "subtaskIds");
        store
            .bulk_insert(
                &[
                    file.clone(),
                    backfill.clone(),
                    index.clone(),
                    subtasks.clone(),
                    subtask_ids.clone(),
                ],
                &[
                    edge(
                        subtasks.id,
                        subtask_ids.id,
                        file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                    edge(file.id, subtasks.id, file.id, EdgeKind::ReadsField),
                    edge(file.id, subtask_ids.id, file.id, EdgeKind::WritesField),
                    edge(file.id, subtask_ids.id, file.id, EdgeKind::FiltersOnField),
                    edge(index.id, subtask_ids.id, index.id, EdgeKind::IndexesField),
                    edge(
                        backfill.id,
                        subtask_ids.id,
                        backfill.id,
                        EdgeKind::BackfillsField,
                    ),
                ],
            )
            .expect("projection graph should write");
        std::mem::forget(temp);
        store
    }

    #[test]
    fn reports_projection_chain_and_runtime_surfaces() {
        let store = store_with_projection();
        let report = projection_impact(
            &store,
            ProjectionImpactRequest {
                target: "subtaskIds".to_owned(),
                repo: Some("svc".to_owned()),
                max_results: 20,
            },
        )
        .expect("projection impact should load");

        assert!(report.resolved);
        assert_eq!(report.source_fields[0].field_path, "subtasks");
        assert_eq!(report.projected_fields[0].field_path, "subtaskIds");
        assert_eq!(report.derivation_edges.len(), 1);
        assert_eq!(report.writers.len(), 1);
        assert_eq!(report.indexes.len(), 1);
        assert_eq!(report.backfills.len(), 1);
        assert!(
            report
                .risk_hints
                .contains(&"filter_contract_impacted".to_owned())
        );
    }

    #[test]
    fn unresolved_target_reports_missing_field() {
        let temp = TempDb::new("projection-impact", "empty");
        let store = temp.open();
        let file = file_node("svc", "src/task.ts");
        store
            .bulk_insert(&[file], &[])
            .expect("empty graph should write");

        let report = projection_impact(
            &store,
            ProjectionImpactRequest {
                target: "subtaskIds".to_owned(),
                repo: Some("svc".to_owned()),
                max_results: 20,
            },
        )
        .expect("projection impact should load");

        assert!(!report.resolved);
        assert!(
            report
                .risk_hints
                .contains(&"field_candidate_not_found".to_owned())
        );
    }

    #[test]
    fn target_can_be_source_field() {
        let store = store_with_projection();
        let report = projection_impact(
            &store,
            ProjectionImpactRequest {
                target: "subtasks".to_owned(),
                repo: Some("svc".to_owned()),
                max_results: 20,
            },
        )
        .expect("projection impact should load");

        assert_eq!(report.source_fields[0].field_path, "subtasks");
        assert_eq!(report.projected_fields[0].field_path, "subtaskIds");
    }

    #[test]
    fn field_node_ids_are_stable_for_test_setup() {
        let field = field("svc", "subtaskIds");
        assert_ne!(
            field.id,
            node_id("svc", "src/task.ts", NodeKind::DataField, "subtaskIds")
        );
    }
}
