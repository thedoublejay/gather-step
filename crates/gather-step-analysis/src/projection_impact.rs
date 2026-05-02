use std::collections::{BTreeMap, BTreeSet, VecDeque};

use gather_step_core::{
    EdgeData, EdgeKind, MIGRATION_FILTERS_METADATA_PREFIX, NodeData, NodeId, NodeKind,
    PayloadContractRecord, ResolverStrategy,
};
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_source: Option<String>,
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
const MIGRATION_COLLECTION_PREFIX: &str = "__migration_collection__";
const OPTIONALITY_MIN_CONFIDENCE: u16 = 750;

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
        apply_deployment_topology_risk(store, &mut report)?;
        return Ok(report);
    }

    let mut source_ids = BTreeSet::new();
    let mut projected_ids = BTreeSet::new();
    let mut derivations = BTreeSet::new();
    let mut relevant_ids = selected_ids.clone();
    if let Some(chain) = chains_by_signature.values().next() {
        derivations.extend(chain.derivations.iter().copied());
        relevant_ids.extend(chain.node_ids.iter().copied());
        for (source, projected) in &chain.derivations {
            source_ids.insert(*source);
            projected_ids.insert(*projected);
        }
    }

    // `descendant_field_ids` and `equivalent_field_ids` both walk
    // `nodes_by_type(DataField)` independently — fold them into a single pass
    // so a workspace with millions of `DataField` nodes pays the cost once per
    // request rather than twice.
    let projected_keys = field_keys_for_ids(store, &projected_ids)?;
    let (descendant_ids, equivalent_ids) =
        descendant_and_equivalent_field_ids(store, &selected, &projected_keys)?;
    relevant_ids.extend(descendant_ids);
    relevant_ids.extend(equivalent_ids);
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
    apply_deployment_topology_risk(store, &mut report)?;
    apply_evidence_verbosity(&mut report, request.evidence_verbosity);
    Ok(report)
}

pub fn projection_impact_with_payload_contracts<S: GraphStore>(
    store: &S,
    request: ProjectionImpactRequest,
    payload_contracts: &[PayloadContractRecord],
) -> Result<ProjectionImpactReport, ProjectionImpactError> {
    let evidence_verbosity = request.evidence_verbosity;
    let mut full_request = request;
    full_request.evidence_verbosity = ProjectionEvidenceVerbosity::Full;
    let mut report = projection_impact(store, full_request)?;
    apply_optional_payload_filter_risk(store, &mut report, payload_contracts)?;
    apply_evidence_verbosity(&mut report, evidence_verbosity);
    Ok(report)
}

pub fn apply_optional_payload_filter_risk<S: GraphStore>(
    store: &S,
    report: &mut ProjectionImpactReport,
    payload_contracts: &[PayloadContractRecord],
) -> Result<(), ProjectionImpactError> {
    if !report.resolved || payload_contracts.is_empty() {
        return Ok(());
    }

    let report_repos = report_repos(report);
    let target_fields = report_logical_fields(report);
    let filter_fields = report_filter_fields(store, report, &report_repos)?;
    if target_fields.is_empty() || filter_fields.is_empty() {
        return Ok(());
    }

    let has_mismatch = payload_contracts.iter().any(|record| {
        (report_repos.is_empty() || report_repos.contains(&record.repo))
            && record.confidence >= OPTIONALITY_MIN_CONFIDENCE
            && record.contract.fields.iter().any(|field| {
                field.optional
                    && field.confidence >= OPTIONALITY_MIN_CONFIDENCE
                    && target_fields
                        .iter()
                        .any(|target| logical_field_matches_payload(target, &field.name))
                    && filter_fields
                        .iter()
                        .any(|filter| filter_field_matches_payload(filter, &field.name))
            })
    });

    if has_mismatch {
        report
            .risk_hints
            .push("optional_payload_filter_mismatch".to_owned());
        report
            .missing_evidence
            .push("runtime_shape_probe".to_owned());
        report.risk_hints.sort();
        report.risk_hints.dedup();
        report.missing_evidence.sort();
        report.missing_evidence.dedup();
    }
    Ok(())
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

fn descendant_and_equivalent_field_ids<S: GraphStore>(
    store: &S,
    descendant_anchors: &[NodeData],
    equivalence_keys: &BTreeSet<FieldKey>,
) -> Result<(BTreeSet<NodeId>, BTreeSet<NodeId>), ProjectionImpactError> {
    let prefixes = descendant_anchors
        .iter()
        .map(|field| (field.repo.clone(), format!("{}.", field.name)))
        .collect::<BTreeSet<_>>();

    let mut descendants = BTreeSet::new();
    let mut equivalents = BTreeSet::new();
    if prefixes.is_empty() && equivalence_keys.is_empty() {
        return Ok((descendants, equivalents));
    }

    for node in store.nodes_by_type(NodeKind::DataField)? {
        if !equivalence_keys.is_empty() && equivalence_keys.contains(&field_key(&node)) {
            equivalents.insert(node.id);
        }
        if !prefixes.is_empty()
            && prefixes.iter().any(|(repo, prefix)| {
                node.repo == *repo && starts_with_ascii_case(&node.name, prefix)
            })
        {
            descendants.insert(node.id);
        }
    }
    Ok((descendants, equivalents))
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
        evidence_source: evidence_source_from_resolver(edge.metadata.resolver.as_deref()),
    }))
}

fn evidence_source_from_resolver(resolver: Option<&str>) -> Option<String> {
    Some(
        match resolver.and_then(ResolverStrategy::from_str)? {
            ResolverStrategy::FieldDirect => "direct_field_access",
            ResolverStrategy::FieldLocalAlias => "local_alias_field_access",
            _ => return None,
        }
        .to_owned(),
    )
}

fn report_repos(report: &ProjectionImpactReport) -> BTreeSet<String> {
    let mut repos = report
        .candidates
        .iter()
        .chain(report.source_fields.iter())
        .chain(report.projected_fields.iter())
        .map(|field| field.repo.clone())
        .collect::<BTreeSet<_>>();
    repos.extend(
        report
            .readers
            .iter()
            .chain(report.writers.iter())
            .chain(report.filters.iter())
            .chain(report.indexes.iter())
            .chain(report.backfills.iter())
            .map(|evidence| evidence.repo.clone()),
    );
    repos
}

fn report_logical_fields(report: &ProjectionImpactReport) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    fields.extend(
        report
            .candidates
            .iter()
            .chain(report.source_fields.iter())
            .chain(report.projected_fields.iter())
            .map(|field| field.field_path.clone()),
    );
    fields.extend(
        report
            .readers
            .iter()
            .chain(report.writers.iter())
            .chain(report.filters.iter())
            .chain(report.indexes.iter())
            .chain(report.backfills.iter())
            .map(|evidence| evidence.field_path.clone()),
    );
    fields
}

fn report_filter_fields<S: GraphStore>(
    store: &S,
    report: &ProjectionImpactReport,
    report_repos: &BTreeSet<String>,
) -> Result<BTreeSet<String>, ProjectionImpactError> {
    let mut fields = report
        .filters
        .iter()
        .map(|evidence| evidence.field_path.clone())
        .collect::<BTreeSet<_>>();

    for node in store.nodes_by_type(NodeKind::Entity)? {
        if !node
            .external_id
            .as_deref()
            .is_some_and(|external_id| external_id.starts_with(MIGRATION_COLLECTION_PREFIX))
        {
            continue;
        }
        for edge in store.get_incoming(node.id)? {
            if edge.kind != EdgeKind::MigratesCollection {
                continue;
            }
            let owner = store.get_node(edge.owner_file)?;
            if let Some(owner) = owner.as_ref()
                && !report_repos.is_empty()
                && !report_repos.contains(&owner.repo)
            {
                continue;
            }
            for filter in decode_migration_filters(edge.metadata.drift_kind.as_deref()) {
                fields.extend(field_paths_from_filter_literal(&filter));
            }
        }
    }

    Ok(fields)
}

fn logical_field_matches_payload(field_path: &str, payload_field: &str) -> bool {
    field_path == payload_field
        || field_path.ends_with(&format!(".{payload_field}"))
        || field_path.contains(&format!(".{payload_field}."))
        || field_path.starts_with(&format!("{payload_field}."))
}

fn filter_field_matches_payload(filter_field: &str, payload_field: &str) -> bool {
    filter_field == payload_field
        || filter_field.starts_with(&format!("{payload_field}."))
        || payload_field.starts_with(&format!("{filter_field}."))
}

fn decode_migration_filters(value: Option<&str>) -> Vec<String> {
    let Some(raw) = value.and_then(|value| value.strip_prefix(MIGRATION_FILTERS_METADATA_PREFIX))
    else {
        return Vec::new();
    };
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn field_paths_from_filter_literal(filter: &str) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let bytes = filter.as_bytes();
    let mut cursor = 0;

    while cursor < bytes.len() {
        cursor = skip_non_filter_key(bytes, cursor);
        if cursor >= bytes.len() {
            break;
        }

        let (raw_key, next_cursor) = if bytes[cursor] == b'\'' || bytes[cursor] == b'"' {
            quoted_filter_key(filter, cursor).unwrap_or_else(|| (String::new(), cursor + 1))
        } else {
            unquoted_filter_key(filter, cursor).unwrap_or_else(|| (String::new(), cursor + 1))
        };
        cursor = next_cursor;

        let after_key = skip_ascii_ws_bytes(bytes, cursor);
        if after_key >= bytes.len() || bytes[after_key] != b':' {
            continue;
        }
        if let Some(field) = normalize_filter_field_path(&raw_key) {
            fields.insert(field);
        }
    }

    fields
}

fn skip_non_filter_key(bytes: &[u8], mut cursor: usize) -> usize {
    while cursor < bytes.len()
        && bytes[cursor] != b'\''
        && bytes[cursor] != b'"'
        && !is_filter_key_start(bytes[cursor])
    {
        cursor += 1;
    }
    cursor
}

fn quoted_filter_key(filter: &str, cursor: usize) -> Option<(String, usize)> {
    let bytes = filter.as_bytes();
    let quote = *bytes.get(cursor)?;
    let mut end = cursor + 1;
    while end < bytes.len() && bytes[end] != quote {
        end += 1;
    }
    if end >= bytes.len() {
        return None;
    }
    Some((filter[cursor + 1..end].to_owned(), end + 1))
}

fn unquoted_filter_key(filter: &str, cursor: usize) -> Option<(String, usize)> {
    let bytes = filter.as_bytes();
    if !is_filter_key_start(*bytes.get(cursor)?) {
        return None;
    }
    let mut end = cursor + 1;
    while end < bytes.len() && is_filter_key_continue(bytes[end]) {
        end += 1;
    }
    Some((filter[cursor..end].to_owned(), end))
}

fn normalize_filter_field_path(raw_key: &str) -> Option<String> {
    let key = raw_key.trim();
    if key.is_empty() || key.starts_with('$') || key.contains(' ') {
        return None;
    }
    let field = key.trim_start_matches("this.");
    if field.split('.').all(|part| {
        !part.is_empty()
            && part
                .as_bytes()
                .iter()
                .all(|byte| is_filter_key_continue(*byte))
    }) {
        Some(field.to_owned())
    } else {
        None
    }
}

fn is_filter_key_start(byte: u8) -> bool {
    byte == b'$' || byte == b'_' || byte.is_ascii_alphabetic()
}

fn is_filter_key_continue(byte: u8) -> bool {
    byte == b'$' || byte == b'_' || byte == b'.' || byte.is_ascii_alphanumeric()
}

fn skip_ascii_ws_bytes(bytes: &[u8], mut cursor: usize) -> usize {
    while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
        cursor += 1;
    }
    cursor
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

fn apply_deployment_topology_risk<S: GraphStore>(
    store: &S,
    report: &mut ProjectionImpactReport,
) -> Result<(), ProjectionImpactError> {
    if !report
        .risk_hints
        .iter()
        .any(|hint| hint == "deployed_owner_unchecked")
    {
        return Ok(());
    }

    if has_deployment_topology_for_report(store, report)? {
        report
            .risk_hints
            .retain(|hint| hint != "deployed_owner_unchecked");
        report
            .risk_hints
            .push("deployed_owner_topology_observed".to_owned());
    } else {
        report
            .missing_evidence
            .push("deployment_topology".to_owned());
    }

    report.risk_hints.sort();
    report.risk_hints.dedup();
    report.missing_evidence.sort();
    report.missing_evidence.dedup();
    Ok(())
}

fn has_deployment_topology_for_report<S: GraphStore>(
    store: &S,
    report: &ProjectionImpactReport,
) -> Result<bool, ProjectionImpactError> {
    let repos = report_repos(report);
    if repos.is_empty() {
        return Ok(false);
    }

    let service_targets = report_service_targets(report);
    for deployment in store.nodes_by_type(NodeKind::Deployment)? {
        if !repos.contains(&deployment.repo) {
            continue;
        }
        for edge in store.get_incoming(deployment.id)? {
            if edge.kind != EdgeKind::DeployedAs {
                continue;
            }
            if store.get_node(edge.source)?.is_some_and(|source| {
                source.kind == NodeKind::Service
                    && if service_targets.is_empty() {
                        service_matches_report_repo(&source, &repos)
                    } else {
                        service_matches_report_targets(&source, &service_targets)
                    }
            }) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn report_service_targets(report: &ProjectionImpactReport) -> BTreeSet<String> {
    let mut targets = BTreeSet::new();
    for evidence in report
        .readers
        .iter()
        .chain(report.writers.iter())
        .chain(report.filters.iter())
        .chain(report.indexes.iter())
        .chain(report.backfills.iter())
    {
        add_service_targets_from_path(&evidence.file_path, &mut targets);
    }
    targets
}

fn add_service_targets_from_path(path: &str, targets: &mut BTreeSet<String>) {
    let parts = path
        .split(['/', '\\'])
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>();
    for pair in parts.windows(2) {
        if matches!(
            pair[0],
            "apps" | "app" | "services" | "service" | "packages" | "package" | "crates" | "crate"
        ) {
            let target = canonical_projection_part(pair[1]);
            if !target.is_empty() {
                targets.insert(target);
            }
        }
    }
}

fn service_matches_report_targets(service: &NodeData, targets: &BTreeSet<String>) -> bool {
    let service_name = canonical_projection_part(&service.name);
    targets.iter().any(|target| {
        service_name == *target
            || [
                service.qualified_name.as_deref(),
                service.external_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|identifier| identifier_matches_service_target(identifier, target))
    })
}

fn identifier_matches_service_target(identifier: &str, target: &str) -> bool {
    let identifier = canonical_projection_part(identifier);
    identifier
        .split('_')
        .filter(|part| !part.is_empty())
        .any(|part| part == target)
}

fn service_matches_report_repo(service: &NodeData, repos: &BTreeSet<String>) -> bool {
    let service_name = canonical_projection_part(&service.name);
    repos.iter().any(|repo| {
        let repo_name = canonical_projection_part(repo);
        service_name == repo_name
            || [
                service.qualified_name.as_deref(),
                service.external_id.as_deref(),
            ]
            .into_iter()
            .flatten()
            .any(|identifier| identifier.contains(&format!("__{repo_name}__{repo_name}")))
    })
}

fn canonical_projection_part(value: &str) -> String {
    let mut normalized = String::new();
    let mut previous_was_separator = false;
    for ch in value.trim().chars() {
        let next = if ch.is_ascii_alphanumeric() {
            previous_was_separator = false;
            ch.to_ascii_lowercase()
        } else if matches!(ch, '.' | '-' | ':') {
            previous_was_separator = false;
            ch
        } else if !previous_was_separator {
            previous_was_separator = true;
            '_'
        } else {
            continue;
        };
        normalized.push(next);
    }
    normalized.trim_matches('_').replace("__", "_")
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
    use gather_step_core::{
        EdgeMetadata, PayloadContractDoc, PayloadField, PayloadInferenceKind, PayloadSide, node_id,
        virtual_node,
    };
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

    fn edge_with_resolver(
        source: NodeId,
        target: NodeId,
        owner_file: NodeId,
        kind: EdgeKind,
        resolver: ResolverStrategy,
    ) -> EdgeData {
        EdgeData {
            source,
            target,
            kind,
            metadata: EdgeMetadata {
                confidence: Some(900),
                resolver: Some(resolver.as_str().to_owned()),
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

    fn payload_contract(repo: &str, file_path: &str, field_name: &str) -> PayloadContractRecord {
        let target = node_id(repo, file_path, NodeKind::Function, "publish");
        let source = node_id(repo, file_path, NodeKind::Class, "AlertPayload");
        PayloadContractRecord {
            payload_contract_node_id: node_id(
                repo,
                file_path,
                NodeKind::DataField,
                "payload-contract",
            ),
            contract_target_node_id: target,
            contract_target_kind: NodeKind::Function,
            contract_target_qualified_name: Some("publish".to_owned()),
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            source_symbol_node_id: source,
            line_start: Some(1),
            side: PayloadSide::Producer,
            inference_kind: PayloadInferenceKind::TypedParameter,
            confidence: 900,
            source_type_name: Some("AlertPayload".to_owned()),
            contract: PayloadContractDoc {
                content_type: "application/json".to_owned(),
                schema_format: "typescript".to_owned(),
                side: PayloadSide::Producer,
                inference_kind: PayloadInferenceKind::TypedParameter,
                confidence: 900,
                fields: vec![PayloadField {
                    name: field_name.to_owned(),
                    type_name: "object".to_owned(),
                    optional: true,
                    confidence: 900,
                }],
                source_type_name: Some("AlertPayload".to_owned()),
            },
        }
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
    fn deployment_topology_replaces_unchecked_deployment_risk() {
        let store = store_with_projection();
        let deployment_file = file_node("svc", "compose.yaml");
        let service = virtual_node(
            NodeKind::Service,
            "svc",
            "compose.yaml",
            "svc",
            "__service__svc__svc",
        );
        let deployment = virtual_node(
            NodeKind::Deployment,
            "svc",
            "compose.yaml",
            "svc",
            "__deployment__svc__svc",
        );
        store
            .bulk_insert(
                &[deployment_file.clone(), service.clone(), deployment.clone()],
                &[edge(
                    service.id,
                    deployment.id,
                    deployment_file.id,
                    EdgeKind::DeployedAs,
                )],
            )
            .expect("deployment topology graph should write");

        let report = projection_impact(&store, request("subtaskIds"))
            .expect("projection impact should load");

        assert!(
            report
                .risk_hints
                .contains(&"deployed_owner_topology_observed".to_owned())
        );
        assert!(
            !report
                .risk_hints
                .contains(&"deployed_owner_unchecked".to_owned())
        );
        assert!(
            !report
                .missing_evidence
                .contains(&"deployment_topology".to_owned())
        );
    }

    #[test]
    fn unrelated_deployment_topology_keeps_unchecked_deployment_risk() {
        let store = store_with_projection();
        let deployment_file = file_node("svc", "compose.yaml");
        let service = virtual_node(
            NodeKind::Service,
            "svc",
            "compose.yaml",
            "worker",
            "__service__svc__worker",
        );
        let deployment = virtual_node(
            NodeKind::Deployment,
            "svc",
            "compose.yaml",
            "worker",
            "__deployment__svc__worker",
        );
        store
            .bulk_insert(
                &[deployment_file.clone(), service.clone(), deployment.clone()],
                &[edge(
                    service.id,
                    deployment.id,
                    deployment_file.id,
                    EdgeKind::DeployedAs,
                )],
            )
            .expect("deployment topology graph should write");

        let report = projection_impact(&store, request("subtaskIds"))
            .expect("projection impact should load");

        assert!(
            !report
                .risk_hints
                .contains(&"deployed_owner_topology_observed".to_owned())
        );
        assert!(
            report
                .risk_hints
                .contains(&"deployed_owner_unchecked".to_owned())
        );
        assert!(
            report
                .missing_evidence
                .contains(&"deployment_topology".to_owned())
        );
    }

    #[test]
    fn deployment_topology_matches_service_path_owner() {
        let temp = TempDb::new("projection-impact", "service-path-owner");
        let store = temp.open();
        let deployment_file = file_node("platform", "compose.yaml");
        let service = virtual_node(
            NodeKind::Service,
            "platform",
            "compose.yaml",
            "api",
            "__service__platform__api",
        );
        let deployment = virtual_node(
            NodeKind::Deployment,
            "platform",
            "compose.yaml",
            "api",
            "__deployment__platform__api",
        );
        store
            .bulk_insert(
                &[deployment_file.clone(), service.clone(), deployment.clone()],
                &[edge(
                    service.id,
                    deployment.id,
                    deployment_file.id,
                    EdgeKind::DeployedAs,
                )],
            )
            .expect("deployment topology graph should write");
        let mut report = deployment_path_report("platform", "services/api/src/task.ts");

        apply_deployment_topology_risk(&store, &mut report)
            .expect("deployment topology risk should apply");

        assert!(
            report
                .risk_hints
                .contains(&"deployed_owner_topology_observed".to_owned())
        );
        assert!(
            !report
                .risk_hints
                .contains(&"deployed_owner_unchecked".to_owned())
        );
        std::mem::forget(temp);
    }

    #[test]
    fn repo_named_deployment_does_not_clear_service_path_mismatch() {
        let temp = TempDb::new("projection-impact", "service-path-mismatch");
        let store = temp.open();
        let deployment_file = file_node("platform", "compose.yaml");
        let service = virtual_node(
            NodeKind::Service,
            "platform",
            "compose.yaml",
            "platform",
            "__service__platform__platform",
        );
        let deployment = virtual_node(
            NodeKind::Deployment,
            "platform",
            "compose.yaml",
            "platform",
            "__deployment__platform__platform",
        );
        store
            .bulk_insert(
                &[deployment_file.clone(), service.clone(), deployment.clone()],
                &[edge(
                    service.id,
                    deployment.id,
                    deployment_file.id,
                    EdgeKind::DeployedAs,
                )],
            )
            .expect("deployment topology graph should write");
        let mut report = deployment_path_report("platform", "services/api/src/task.ts");

        apply_deployment_topology_risk(&store, &mut report)
            .expect("deployment topology risk should apply");

        assert!(
            !report
                .risk_hints
                .contains(&"deployed_owner_topology_observed".to_owned())
        );
        assert!(
            report
                .risk_hints
                .contains(&"deployed_owner_unchecked".to_owned())
        );
        assert!(
            report
                .missing_evidence
                .contains(&"deployment_topology".to_owned())
        );
        std::mem::forget(temp);
    }

    fn deployment_path_report(repo: &str, file_path: &str) -> ProjectionImpactReport {
        ProjectionImpactReport {
            target: "subtaskIds".to_owned(),
            resolved: true,
            ambiguity: None,
            candidates: vec![ProjectionField {
                repo: repo.to_owned(),
                field_path: "subtaskIds".to_owned(),
                qualified_name: None,
            }],
            source_fields: Vec::new(),
            projected_fields: vec![ProjectionField {
                repo: repo.to_owned(),
                field_path: "subtaskIds".to_owned(),
                qualified_name: None,
            }],
            derivation_edges: Vec::new(),
            readers: vec![ProjectionEvidence {
                repo: repo.to_owned(),
                file_path: file_path.to_owned(),
                field_path: "subtaskIds".to_owned(),
                edge_kind: EdgeKind::ReadsField,
                confidence: Some(900),
                evidence_source: None,
            }],
            writers: Vec::new(),
            filters: Vec::new(),
            indexes: Vec::new(),
            backfills: Vec::new(),
            risk_hints: vec!["deployed_owner_unchecked".to_owned()],
            missing_evidence: Vec::new(),
            confidence: "high".to_owned(),
        }
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
                &[edge_with_resolver(
                    file.id,
                    task_ids.id,
                    file.id,
                    EdgeKind::WritesField,
                    ResolverStrategy::FieldLocalAlias,
                )],
            )
            .expect("direct field graph should write");

        let report = projection_impact(&store, request("Alert.workflow"))
            .expect("projection impact should load");

        assert!(report.resolved);
        assert_eq!(report.candidates[0].field_path, "Alert.workflow");
        assert_eq!(report.writers[0].field_path, "Alert.workflow.taskIds");
        assert_eq!(
            report.writers[0].evidence_source.as_deref(),
            Some("local_alias_field_access")
        );
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
    fn optionality_mismatch_risk_joins_payload_contract_and_migration_filter() {
        let temp = TempDb::new("projection-impact", "optionality-mismatch");
        let store = temp.open();
        let source_file = file_node("svc", "src/alerts.ts");
        let migration_file = file_node("svc", "migrations/20260430-alerts.ts");
        let workflow = field_in_file("svc", "src/alerts.ts", "Alert.workflow");
        let collection = virtual_node(
            NodeKind::Entity,
            "svc",
            "migrations/20260430-alerts.ts",
            "alerts",
            "__migration_collection__alerts",
        );
        let migration_filter = serde_json::to_string(&vec!["{ workflow: { $type: 'object' } }"])
            .expect("filter metadata should serialize");
        store
            .bulk_insert(
                &[
                    source_file.clone(),
                    migration_file.clone(),
                    workflow.clone(),
                    collection.clone(),
                ],
                &[
                    edge_with_resolver(
                        source_file.id,
                        workflow.id,
                        source_file.id,
                        EdgeKind::ReadsField,
                        ResolverStrategy::FieldDirect,
                    ),
                    EdgeData {
                        source: migration_file.id,
                        target: collection.id,
                        kind: EdgeKind::MigratesCollection,
                        metadata: EdgeMetadata {
                            drift_kind: Some(format!(
                                "{MIGRATION_FILTERS_METADATA_PREFIX}{migration_filter}"
                            )),
                            ..EdgeMetadata::default()
                        },
                        owner_file: migration_file.id,
                        is_cross_file: false,
                    },
                ],
            )
            .expect("optionality graph should write");

        let report = projection_impact_with_payload_contracts(
            &store,
            request("Alert.workflow"),
            &[payload_contract("svc", "src/contracts.ts", "workflow")],
        )
        .expect("projection impact should load");

        assert!(
            report
                .risk_hints
                .contains(&"optional_payload_filter_mismatch".to_owned())
        );
        assert!(
            report
                .missing_evidence
                .contains(&"runtime_shape_probe".to_owned())
        );
    }

    #[test]
    fn optionality_mismatch_uses_full_filter_evidence_before_summary_truncation() {
        let temp = TempDb::new("projection-impact", "optionality-summary-filters");
        let store = temp.open();
        let source_file = file_node("svc", "src/alerts.ts");
        let workflow = field("svc", "workflow");
        let filter_files = (0..4)
            .map(|index| file_node("svc", &format!("src/filter-{index}.ts")))
            .collect::<Vec<_>>();
        let filter_fields = [
            field("svc", "workflow.alpha"),
            field("svc", "workflow.beta"),
            field("svc", "workflow.gamma"),
            field("svc", "workflow.taskIds"),
        ];
        let mut nodes = vec![source_file.clone(), workflow.clone()];
        nodes.extend(filter_files.iter().cloned());
        nodes.extend(filter_fields.iter().cloned());
        let edges = filter_files
            .iter()
            .zip(filter_fields.iter())
            .map(|(file, field)| edge(file.id, field.id, file.id, EdgeKind::FiltersOnField))
            .collect::<Vec<_>>();
        store
            .bulk_insert(&nodes, &edges)
            .expect("summary optionality graph should write");

        let mut summary_request = request("workflow");
        summary_request.evidence_verbosity = ProjectionEvidenceVerbosity::Summary;
        let report = projection_impact_with_payload_contracts(
            &store,
            summary_request,
            &[payload_contract(
                "svc",
                "src/contracts.ts",
                "workflow.taskIds",
            )],
        )
        .expect("projection impact should load");

        assert_eq!(report.filters.len(), 3);
        assert!(
            !report
                .filters
                .iter()
                .any(|filter| filter.field_path == "workflow.taskIds")
        );
        assert!(
            report
                .risk_hints
                .contains(&"optional_payload_filter_mismatch".to_owned())
        );
        assert!(
            report
                .missing_evidence
                .contains(&"runtime_shape_probe".to_owned())
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
