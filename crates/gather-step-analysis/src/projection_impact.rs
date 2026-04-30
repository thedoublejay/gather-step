use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    #[serde(default)]
    pub evidence_verbosity: ProjectionEvidenceVerbosity,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEvidenceVerbosity {
    Summary,
    #[default]
    Full,
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
const MAX_DERIVATION_DEPTH: usize = 4;

type FieldKey = (String, String);
type LogicalDerivation = (FieldKey, FieldKey);

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

    let mut chains_by_signature = BTreeMap::<BTreeSet<LogicalDerivation>, DerivationChain>::new();
    for candidate in &selected {
        let chain = collect_derivation_chain(store, candidate.id, max_results)?;
        if !chain.derivations.is_empty() {
            let signature = logical_derivation_signature(store, &chain.derivations)?;
            chains_by_signature
                .entry(signature)
                .and_modify(|existing| existing.merge(&chain))
                .or_insert(chain);
        }
    }

    let ambiguous =
        chains_by_signature.len() > 1 || (chains_by_signature.is_empty() && selected.len() > 1);
    if ambiguous {
        let mut report = ProjectionImpactReport {
            target: request.target,
            resolved: !selected.is_empty(),
            ambiguity: Some("multiple_field_candidates".to_owned()),
            candidates: nodes_to_fields(&selected),
            source_fields: Vec::new(),
            projected_fields: Vec::new(),
            derivation_edges: Vec::new(),
            readers: Vec::new(),
            writers: Vec::new(),
            filters: Vec::new(),
            indexes: Vec::new(),
            backfills: Vec::new(),
            risk_hints: Vec::new(),
            missing_evidence: Vec::new(),
            confidence: "low".to_owned(),
        };
        populate_risks_and_confidence(&mut report);
        return Ok(report);
    }

    let mut source_ids = BTreeSet::new();
    let mut projected_ids = BTreeSet::new();
    let mut derivations = BTreeSet::new();
    let mut relevant_ids = selected_ids.clone();
    let descendant_ids = descendant_field_ids(store, &selected)?;
    relevant_ids.extend(descendant_ids);
    if let Some(chain) = chains_by_signature.values().next() {
        derivations.extend(chain.derivations.iter().copied());
        relevant_ids.extend(chain.node_ids.iter().copied());
        for (source, projected) in &chain.derivations {
            source_ids.insert(*source);
            projected_ids.insert(*projected);
        }
    }

    let projected_keys = field_keys_for_ids(store, &projected_ids)?;
    relevant_ids.extend(equivalent_field_ids(store, &projected_keys)?);
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
        ambiguity: None,
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
    apply_evidence_verbosity(&mut report, request.evidence_verbosity);
    Ok(report)
}

#[derive(Clone, Debug, Default)]
struct DerivationChain {
    node_ids: BTreeSet<NodeId>,
    derivations: BTreeSet<(NodeId, NodeId)>,
}

impl DerivationChain {
    fn merge(&mut self, other: &Self) {
        self.node_ids.extend(other.node_ids.iter().copied());
        self.derivations.extend(other.derivations.iter().copied());
    }
}

fn collect_derivation_chain<S: GraphStore>(
    store: &S,
    seed: NodeId,
    max_results: usize,
) -> Result<DerivationChain, ProjectionImpactError> {
    let mut chain = DerivationChain::default();
    let mut visited = BTreeSet::new();
    let mut frontier = VecDeque::from([(seed, 0usize)]);

    while let Some((field_id, depth)) = frontier.pop_front() {
        if !visited.insert(field_id) {
            continue;
        }
        chain.node_ids.insert(field_id);
        if depth >= MAX_DERIVATION_DEPTH {
            continue;
        }

        let incoming = store.get_incoming(field_id)?;
        let outgoing = store.get_outgoing(field_id)?;
        for edge in incoming
            .iter()
            .chain(outgoing.iter())
            .filter(|edge| edge.kind == EdgeKind::DerivesFieldFrom)
        {
            chain.derivations.insert((edge.source, edge.target));
            chain.node_ids.insert(edge.source);
            chain.node_ids.insert(edge.target);
            if chain.derivations.len() >= max_results {
                return Ok(chain);
            }
            if edge.source != field_id {
                frontier.push_back((edge.source, depth + 1));
            }
            if edge.target != field_id {
                frontier.push_back((edge.target, depth + 1));
            }
        }
    }

    Ok(chain)
}

fn logical_derivation_signature<S: GraphStore>(
    store: &S,
    derivations: &BTreeSet<(NodeId, NodeId)>,
) -> Result<BTreeSet<LogicalDerivation>, ProjectionImpactError> {
    let mut signature = BTreeSet::new();
    for (source_id, target_id) in derivations {
        let Some(source) = store.get_node(*source_id)? else {
            continue;
        };
        let Some(target) = store.get_node(*target_id)? else {
            continue;
        };
        signature.insert((field_key(&source), field_key(&target)));
    }
    Ok(signature)
}

fn field_keys_for_ids<S: GraphStore>(
    store: &S,
    ids: &BTreeSet<NodeId>,
) -> Result<BTreeSet<FieldKey>, ProjectionImpactError> {
    let mut keys = BTreeSet::new();
    for id in ids {
        if let Some(node) = store.get_node(*id)? {
            keys.insert(field_key(&node));
        }
    }
    Ok(keys)
}

fn equivalent_field_ids<S: GraphStore>(
    store: &S,
    keys: &BTreeSet<FieldKey>,
) -> Result<BTreeSet<NodeId>, ProjectionImpactError> {
    let mut ids = BTreeSet::new();
    if keys.is_empty() {
        return Ok(ids);
    }
    for node in store.nodes_by_type(NodeKind::DataField)? {
        if keys.contains(&field_key(&node)) {
            ids.insert(node.id);
        }
    }
    Ok(ids)
}

fn descendant_field_ids<S: GraphStore>(
    store: &S,
    fields: &[NodeData],
) -> Result<BTreeSet<NodeId>, ProjectionImpactError> {
    let prefixes = fields
        .iter()
        .map(|field| (field.repo.clone(), format!("{}.", field.name)))
        .collect::<BTreeSet<_>>();
    if prefixes.is_empty() {
        return Ok(BTreeSet::new());
    }

    let mut ids = BTreeSet::new();
    for node in store.nodes_by_type(NodeKind::DataField)? {
        if prefixes
            .iter()
            .any(|(repo, prefix)| node.repo == *repo && starts_with_ascii_case(&node.name, prefix))
        {
            ids.insert(node.id);
        }
    }
    Ok(ids)
}

fn field_key(node: &NodeData) -> FieldKey {
    (node.repo.clone(), node.name.clone())
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
    let has_field_evidence = !report.readers.is_empty()
        || !report.writers.is_empty()
        || !report.filters.is_empty()
        || !report.indexes.is_empty()
        || !report.backfills.is_empty();
    let has_projection_context =
        !report.source_fields.is_empty() || !report.projected_fields.is_empty();
    if !report.source_fields.is_empty() && !report.projected_fields.is_empty() {
        report.risk_hints.push("source_field_unreviewed".to_owned());
    }
    if report.derivation_edges.is_empty() && has_projection_context {
        report
            .risk_hints
            .push("projection_chain_unproven".to_owned());
        report.missing_evidence.push("derivation_edge".to_owned());
    } else if report.derivation_edges.is_empty() && has_field_evidence {
        report
            .risk_hints
            .push("direct_field_access_observed".to_owned());
    } else if report.derivation_edges.is_empty() && report.ambiguity.is_some() {
        report
            .risk_hints
            .push("projection_chain_unproven".to_owned());
        report.missing_evidence.push("derivation_edge".to_owned());
    } else if report.derivation_edges.is_empty() {
        report.risk_hints.push("field_evidence_missing".to_owned());
        report.missing_evidence.push("reader_or_writer".to_owned());
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
    if !report.projected_fields.is_empty()
        && (!report.readers.is_empty() || !report.filters.is_empty())
        && report.backfills.is_empty()
        && report.indexes.is_empty()
    {
        report.risk_hints.push("frontend_only_focus".to_owned());
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

    report.confidence = if (!report.derivation_edges.is_empty() || has_field_evidence)
        && report.ambiguity.is_none()
    {
        "high".to_owned()
    } else if !report.derivation_edges.is_empty() || !report.candidates.is_empty() {
        "medium".to_owned()
    } else {
        "low".to_owned()
    };
}

fn apply_evidence_verbosity(
    report: &mut ProjectionImpactReport,
    verbosity: ProjectionEvidenceVerbosity,
) {
    const SUMMARY_EVIDENCE_LIMIT: usize = 3;

    if verbosity == ProjectionEvidenceVerbosity::Full {
        return;
    }

    report.readers.truncate(SUMMARY_EVIDENCE_LIMIT);
    report.writers.truncate(SUMMARY_EVIDENCE_LIMIT);
    report.filters.truncate(SUMMARY_EVIDENCE_LIMIT);
    report.indexes.truncate(SUMMARY_EVIDENCE_LIMIT);
    report.backfills.truncate(SUMMARY_EVIDENCE_LIMIT);
}

fn contains_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn starts_with_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack.len() >= needle.len()
        && haystack.as_bytes()[..needle.len()].eq_ignore_ascii_case(needle.as_bytes())
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

    fn field_in_file(repo: &str, file_path: &str, name: &str) -> NodeData {
        virtual_node(
            NodeKind::DataField,
            repo,
            file_path,
            name,
            format!("data-field::{repo}::{file_path}::{name}"),
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

    fn request(target: &str) -> ProjectionImpactRequest {
        ProjectionImpactRequest {
            target: target.to_owned(),
            repo: Some("svc".to_owned()),
            max_results: 20,
            evidence_verbosity: ProjectionEvidenceVerbosity::Full,
        }
    }

    #[test]
    fn reports_projection_chain_and_runtime_surfaces() {
        let store = store_with_projection();
        let report = projection_impact(&store, request("subtaskIds"))
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
        assert!(
            report
                .risk_hints
                .contains(&"source_field_unreviewed".to_owned())
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

        let report = projection_impact(&store, request("subtaskIds"))
            .expect("projection impact should load");

        assert!(!report.resolved);
        assert!(
            report
                .risk_hints
                .contains(&"field_candidate_not_found".to_owned())
        );
    }

    #[test]
    fn direct_field_evidence_reports_parent_and_nested_access() {
        let temp = TempDb::new("projection-impact", "direct-field");
        let store = temp.open();
        let file = file_node("svc", "src/alerts.ts");
        let workflow = field_in_file("svc", "src/alerts.ts", "Alert.workflow");
        let task_ids = field_in_file("svc", "src/alerts.ts", "Alert.workflow.taskIds");
        store
            .bulk_insert(
                &[file.clone(), workflow.clone(), task_ids.clone()],
                &[edge(file.id, task_ids.id, file.id, EdgeKind::WritesField)],
            )
            .expect("direct field graph should write");

        let report = projection_impact(&store, request("Alert.workflow"))
            .expect("projection impact should load");

        assert!(report.resolved);
        assert_eq!(report.candidates[0].field_path, "Alert.workflow");
        assert_eq!(report.writers[0].field_path, "Alert.workflow.taskIds");
        assert!(report.derivation_edges.is_empty());
        assert!(report.missing_evidence.is_empty());
        assert_eq!(report.confidence, "high");
        assert!(
            report
                .risk_hints
                .contains(&"direct_field_access_observed".to_owned())
        );
    }

    #[test]
    fn frontend_only_projection_reports_frontend_focus_risk() {
        let temp = TempDb::new("projection-impact", "frontend-only");
        let store = temp.open();
        let projection_file = file_node("svc", "src/projection.ts");
        let reader_file = file_node("svc", "web/render.tsx");
        let source = field_in_file("svc", "src/projection.ts", "items");
        let projected = field_in_file("svc", "src/projection.ts", "itemIds");
        store
            .bulk_insert(
                &[
                    projection_file.clone(),
                    reader_file.clone(),
                    source.clone(),
                    projected.clone(),
                ],
                &[
                    edge(
                        source.id,
                        projected.id,
                        projection_file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                    edge(
                        reader_file.id,
                        projected.id,
                        reader_file.id,
                        EdgeKind::ReadsField,
                    ),
                ],
            )
            .expect("frontend-only graph should write");

        let report =
            projection_impact(&store, request("itemIds")).expect("projection impact should load");

        assert!(
            report
                .risk_hints
                .contains(&"frontend_only_focus".to_owned())
        );
        assert!(
            report
                .risk_hints
                .contains(&"source_field_unreviewed".to_owned())
        );
    }

    #[test]
    fn summary_evidence_verbosity_truncates_large_evidence_lists() {
        let temp = TempDb::new("projection-impact", "summary-verbosity");
        let store = temp.open();
        let projection_file = file_node("svc", "src/projection.ts");
        let source = field_in_file("svc", "src/projection.ts", "items");
        let projected = field_in_file("svc", "src/projection.ts", "itemIds");
        let reader_files = (0..4)
            .map(|index| file_node("svc", &format!("web/render-{index}.tsx")))
            .collect::<Vec<_>>();
        let mut nodes = vec![projection_file.clone(), source.clone(), projected.clone()];
        nodes.extend(reader_files.iter().cloned());
        let mut edges = vec![edge(
            source.id,
            projected.id,
            projection_file.id,
            EdgeKind::DerivesFieldFrom,
        )];
        edges.extend(
            reader_files
                .iter()
                .map(|file| edge(file.id, projected.id, file.id, EdgeKind::ReadsField)),
        );
        store
            .bulk_insert(&nodes, &edges)
            .expect("projection graph should write");

        let mut summary_request = request("itemIds");
        summary_request.evidence_verbosity = ProjectionEvidenceVerbosity::Summary;
        let report =
            projection_impact(&store, summary_request).expect("projection impact should load");

        assert_eq!(report.readers.len(), 3);
        assert!(
            report
                .risk_hints
                .contains(&"frontend_only_focus".to_owned())
        );
    }

    #[test]
    fn target_can_be_source_field() {
        let store = store_with_projection();
        let report =
            projection_impact(&store, request("subtasks")).expect("projection impact should load");

        assert_eq!(report.source_fields[0].field_path, "subtasks");
        assert_eq!(report.projected_fields[0].field_path, "subtaskIds");
    }

    #[test]
    fn follows_bounded_multi_hop_projection_chain() {
        let temp = TempDb::new("projection-impact", "multi-hop");
        let store = temp.open();
        let file = file_node("svc", "src/projection.ts");
        let raw_total = field_in_file("svc", "src/projection.ts", "rawTotal");
        let normalized_total = field_in_file("svc", "src/projection.ts", "normalizedTotal");
        let invoice_total = field_in_file("svc", "src/projection.ts", "invoiceTotal");
        store
            .bulk_insert(
                &[
                    file.clone(),
                    raw_total.clone(),
                    normalized_total.clone(),
                    invoice_total.clone(),
                ],
                &[
                    edge(
                        raw_total.id,
                        normalized_total.id,
                        file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                    edge(
                        normalized_total.id,
                        invoice_total.id,
                        file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                ],
            )
            .expect("multi-hop graph should write");

        let report = projection_impact(&store, request("invoiceTotal"))
            .expect("projection impact should load");
        let source_fields = report
            .source_fields
            .iter()
            .map(|field| field.field_path.as_str())
            .collect::<BTreeSet<_>>();
        let projected_fields = report
            .projected_fields
            .iter()
            .map(|field| field.field_path.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(report.derivation_edges.len(), 2);
        assert!(source_fields.contains("rawTotal"));
        assert!(source_fields.contains("normalizedTotal"));
        assert!(projected_fields.contains("normalizedTotal"));
        assert!(projected_fields.contains("invoiceTotal"));
    }

    #[test]
    fn isolated_index_copy_does_not_become_source_field() {
        let temp = TempDb::new("projection-impact", "isolated-copy");
        let store = temp.open();
        let projection_file = file_node("svc", "src/projection.ts");
        let index_file = file_node("svc", "src/search-index.ts");
        let line_items = field_in_file("svc", "src/projection.ts", "lineItems");
        let line_item_total = field_in_file("svc", "src/projection.ts", "lineItemTotal");
        let indexed_line_item_total = field_in_file("svc", "src/search-index.ts", "lineItemTotal");
        store
            .bulk_insert(
                &[
                    projection_file.clone(),
                    index_file.clone(),
                    line_items.clone(),
                    line_item_total.clone(),
                    indexed_line_item_total.clone(),
                ],
                &[
                    edge(
                        line_items.id,
                        line_item_total.id,
                        projection_file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                    edge(
                        index_file.id,
                        indexed_line_item_total.id,
                        index_file.id,
                        EdgeKind::IndexesField,
                    ),
                ],
            )
            .expect("projection graph should write");

        let report = projection_impact(&store, request("lineItemTotal"))
            .expect("projection impact should load");

        assert_eq!(report.ambiguity, None);
        assert_eq!(report.source_fields.len(), 1);
        assert_eq!(report.source_fields[0].field_path, "lineItems");
        assert!(
            report
                .source_fields
                .iter()
                .all(|field| field.field_path != "lineItemTotal")
        );
        assert_eq!(report.indexes.len(), 1);
        assert_eq!(report.indexes[0].file_path, "src/search-index.ts");
    }

    #[test]
    fn ambiguous_same_name_chains_do_not_merge_evidence() {
        let temp = TempDb::new("projection-impact", "ambiguous");
        let store = temp.open();
        let account_file = file_node("svc", "src/account.ts");
        let billing_file = file_node("svc", "src/billing.ts");
        let account_status = field_in_file("svc", "src/account.ts", "status");
        let account_projection = field_in_file("svc", "src/account.ts", "accountStatus");
        let billing_status = field_in_file("svc", "src/billing.ts", "status");
        let billing_projection = field_in_file("svc", "src/billing.ts", "billingStatus");
        store
            .bulk_insert(
                &[
                    account_file.clone(),
                    billing_file.clone(),
                    account_status.clone(),
                    account_projection.clone(),
                    billing_status.clone(),
                    billing_projection.clone(),
                ],
                &[
                    edge(
                        account_status.id,
                        account_projection.id,
                        account_file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                    edge(
                        billing_status.id,
                        billing_projection.id,
                        billing_file.id,
                        EdgeKind::DerivesFieldFrom,
                    ),
                ],
            )
            .expect("ambiguous graph should write");

        let report =
            projection_impact(&store, request("status")).expect("projection impact should load");

        assert_eq!(
            report.ambiguity.as_deref(),
            Some("multiple_field_candidates")
        );
        assert!(
            report
                .risk_hints
                .contains(&"needs_disambiguation".to_owned())
        );
        assert!(report.derivation_edges.is_empty());
        assert!(report.source_fields.is_empty());
        assert!(report.projected_fields.is_empty());
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
