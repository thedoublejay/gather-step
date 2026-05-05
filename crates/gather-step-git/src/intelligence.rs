use std::collections::BTreeMap;

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, ResolverStrategy, ref_node_id,
};
use gather_step_storage::{GraphStore, GraphStoreError, MetadataStore, MetadataStoreError};
use hashbrown::HashMap;
use thiserror::Error;
use tracing::warn;

use crate::{
    AnalyticsOptions, AnalyticsReport, BusFactorRisk, CommitFact, CommitFileChangeKind,
    CommitFileDelta, OwnershipOptions, OwnershipSummary, analyze_history, analyze_ownership,
    bus_factor_risks, persist_ownership_into_file_analytics, redact_email,
};

const DEFAULT_MAX_CO_CHANGE_EDGES_PER_FILE: usize = 8;

#[derive(Clone, Debug)]
pub struct RepoIntelligenceOptions {
    pub analytics: AnalyticsOptions,
    pub ownership: OwnershipOptions,
    pub max_co_change_edges_per_file: usize,
}

impl RepoIntelligenceOptions {
    #[must_use]
    pub fn with_defaults() -> Self {
        Self {
            analytics: AnalyticsOptions::default(),
            ownership: OwnershipOptions::default(),
            max_co_change_edges_per_file: DEFAULT_MAX_CO_CHANGE_EDGES_PER_FILE,
        }
    }
}

impl Default for RepoIntelligenceOptions {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RepoIntelligenceReport {
    pub analytics: AnalyticsReport,
    pub ownership: Vec<OwnershipSummary>,
    pub bus_factor_risks: Vec<BusFactorRisk>,
}

#[derive(Debug, Error)]
pub enum RepoIntelligenceError {
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataStoreError),
}

pub fn refresh_repo_intelligence<G: GraphStore, M: MetadataStore>(
    graph: &G,
    metadata: &M,
    repo: &str,
    computed_at_unix: i64,
    options: &RepoIntelligenceOptions,
) -> Result<RepoIntelligenceReport, RepoIntelligenceError> {
    let commits = metadata.get_commits_by_repo(repo, i64::MIN, i64::MAX)?;
    let deltas = metadata.get_commit_file_deltas_for_repo(repo)?;
    // Load the repo's nodes once and reuse them for both the file-path
    // filter set and the `materialize_summary_edges` step. The previous
    // shape made two `nodes_by_repo` calls (one here, one inside
    // `materialize_summary_edges`); for repos with thousands of nodes
    // that doubled the redb scan cost on every refresh.
    let repo_nodes = graph.nodes_by_repo(repo)?;
    let live_file_paths = repo_nodes
        .iter()
        .filter(|node| node.kind == NodeKind::File)
        .map(|node| node.file_path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let facts = build_commit_facts(&commits, &deltas);

    let mut analytics = analyze_history(&facts, computed_at_unix, &options.analytics);
    analytics
        .hotspots
        .retain(|record| live_file_paths.contains(&record.file_path));
    analytics.co_changes.retain(|record| {
        live_file_paths.contains(&record.file_a) && live_file_paths.contains(&record.file_b)
    });
    analytics.persist(metadata, repo)?;

    let ownership = analyze_ownership(&commits, &deltas, &options.ownership)
        .into_iter()
        .filter(|summary| live_file_paths.contains(&summary.file_path))
        .collect::<Vec<_>>();
    persist_ownership_into_file_analytics(metadata, repo, &ownership)?;

    materialize_summary_edges(
        graph,
        &repo_nodes,
        &analytics,
        &ownership,
        options.max_co_change_edges_per_file,
    )?;

    Ok(RepoIntelligenceReport {
        bus_factor_risks: bus_factor_risks(&ownership, &options.ownership),
        analytics,
        ownership,
    })
}

fn build_commit_facts(
    commits: &[gather_step_storage::CommitRecord],
    deltas: &[gather_step_storage::CommitFileDeltaRecord],
) -> Vec<CommitFact> {
    // Pre-allocate with the number of deltas as an upper bound on the number of
    // distinct SHAs so the map never needs to rehash during iteration.
    let mut deltas_by_sha = HashMap::<String, Vec<CommitFileDelta>>::with_capacity(deltas.len());
    for delta in deltas {
        deltas_by_sha
            .entry(delta.sha.clone())
            .or_default()
            .push(CommitFileDelta {
                file_path: delta.file_path.clone(),
                change_kind: match delta.change_kind {
                    gather_step_storage::CommitFileChangeKind::Added => CommitFileChangeKind::Added,
                    gather_step_storage::CommitFileChangeKind::Modified => {
                        CommitFileChangeKind::Modified
                    }
                    gather_step_storage::CommitFileChangeKind::Deleted => {
                        CommitFileChangeKind::Deleted
                    }
                    gather_step_storage::CommitFileChangeKind::Renamed => {
                        CommitFileChangeKind::Renamed
                    }
                    gather_step_storage::CommitFileChangeKind::Copied => {
                        CommitFileChangeKind::Copied
                    }
                    gather_step_storage::CommitFileChangeKind::TypeChanged => {
                        CommitFileChangeKind::TypeChanged
                    }
                },
                insertions: delta
                    .insertions
                    .and_then(|value| storage_count_to_u64(value, &delta.sha, &delta.file_path)),
                deletions: delta
                    .deletions
                    .and_then(|value| storage_count_to_u64(value, &delta.sha, &delta.file_path)),
                old_path: delta.old_path.clone(),
            });
    }

    let mut facts = commits
        .iter()
        .map(|commit| CommitFact {
            repo: commit.repo.clone(),
            sha: commit.sha.clone(),
            author_email: commit.author_email.clone(),
            author_date_unix: commit.date,
            message: commit.message.clone(),
            classification: commit.classification.clone(),
            pr_number: commit.pr_number.and_then(|value| u64::try_from(value).ok()),
            has_decision_signal: commit.has_decision_signal,
            parent_count: 0,
            file_deltas: deltas_by_sha.remove(&commit.sha).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    facts.sort_by_key(|fact| fact.author_date_unix);
    facts
}

fn materialize_summary_edges<G: GraphStore>(
    graph: &G,
    repo_nodes: &[gather_step_core::NodeData],
    analytics: &AnalyticsReport,
    ownership: &[OwnershipSummary],
    max_co_change_edges_per_file: usize,
) -> Result<(), GraphStoreError> {
    let file_nodes = repo_nodes
        .iter()
        .filter(|node| node.kind == NodeKind::File)
        .cloned()
        .collect::<Vec<_>>();
    // Borrow file_path strings from `file_nodes` rather than cloning whole
    // NodeData values: the map only needs the NodeId at each lookup site.
    let file_by_path = file_nodes
        .iter()
        .map(|node| (node.file_path.as_str(), node.id))
        .collect::<HashMap<_, _>>();

    let mut author_nodes = BTreeMap::<String, NodeData>::new();
    let mut edges = Vec::new();

    for summary in ownership {
        let Some(&file_id) = file_by_path.get(summary.file_path.as_str()) else {
            continue;
        };
        for contribution in &summary.contributions {
            // Only the NodeId is needed; avoid cloning the full NodeData.
            let author_id = author_nodes
                .entry(contribution.author_email.clone())
                .or_insert_with(|| author_node(&contribution.author_email))
                .id;
            edges.push(EdgeData {
                source: file_id,
                target: author_id,
                kind: EdgeKind::OwnedBy,
                metadata: EdgeMetadata {
                    weight: Some(percent_to_weight(contribution.ownership_pct)),
                    confidence: Some(percent_to_confidence(contribution.ownership_pct)),
                    timestamp_unix: None,
                    drift_kind: None,
                    resolver: Some(ResolverStrategy::HistoryOwnership.as_str().to_owned()),
                },
                owner_file: file_id,
                is_cross_file: true,
            });
        }
    }

    let mut emitted_per_file = HashMap::<String, usize>::default();
    for pair in &analytics.co_changes {
        let Some(&src_id) = file_by_path.get(pair.file_a.as_str()) else {
            continue;
        };
        let Some(&tgt_id) = file_by_path.get(pair.file_b.as_str()) else {
            continue;
        };
        if emitted_per_file
            .get(&pair.file_a)
            .copied()
            .unwrap_or_default()
            >= max_co_change_edges_per_file
            || emitted_per_file
                .get(&pair.file_b)
                .copied()
                .unwrap_or_default()
                >= max_co_change_edges_per_file
        {
            continue;
        }

        let metadata = EdgeMetadata {
            weight: Some(pair.occurrences),
            confidence: Some(score_to_confidence(pair.strength)),
            timestamp_unix: Some(pair.last_seen_unix),
            drift_kind: None,
            resolver: Some(ResolverStrategy::CoChange.as_str().to_owned()),
        };
        edges.push(EdgeData {
            source: src_id,
            target: tgt_id,
            kind: EdgeKind::CoChangesWith,
            metadata: metadata.clone(),
            owner_file: src_id,
            is_cross_file: true,
        });
        edges.push(EdgeData {
            source: tgt_id,
            target: src_id,
            kind: EdgeKind::CoChangesWith,
            metadata,
            owner_file: tgt_id,
            is_cross_file: true,
        });
        *emitted_per_file.entry(pair.file_a.clone()).or_default() += 1;
        *emitted_per_file.entry(pair.file_b.clone()).or_default() += 1;
    }
    let owner_files = file_nodes.iter().map(|file| file.id).collect::<Vec<_>>();
    graph.bulk_insert(&author_nodes.into_values().collect::<Vec<_>>(), &[])?;
    graph.replace_edges_for_owners_by_kind(
        &owner_files,
        &[EdgeKind::OwnedBy, EdgeKind::CoChangesWith],
        &edges,
    )?;
    Ok(())
}

/// Converts a stored `i64` insertion/deletion count to `u64`, logging when
/// a negative value is dropped. Storage uses `i64` for `SQLite` compatibility,
/// but a negative count indicates corrupted data — silently dropping it
/// would skew downstream analytics with zero attribution rather than
/// surfacing the integrity issue.
fn storage_count_to_u64(value: i64, sha: &str, file_path: &str) -> Option<u64> {
    if let Ok(value) = u64::try_from(value) {
        Some(value)
    } else {
        warn!(
            sha = sha,
            file_path = file_path,
            value,
            "negative line-count in commit_file_deltas; dropping value (data integrity)",
        );
        None
    }
}

/// Synthetic repo name for virtual cross-repo nodes (e.g. authors, shared
/// symbols). Pinned as a constant so changes ripple through every emitter
/// and consumer in lockstep instead of drifting via copy-paste.
const VIRTUAL_NODE_REPO: &str = "__virtual__";
/// Path prefix for synthesized author nodes. The author email is appended
/// after the slash.
const AUTHOR_FILE_PREFIX: &str = "__authors__/";

fn author_node(author_email: &str) -> NodeData {
    let redacted = redact_email(author_email);
    NodeData {
        id: ref_node_id(NodeKind::Author, &redacted),
        kind: NodeKind::Author,
        repo: VIRTUAL_NODE_REPO.to_owned(),
        file_path: format!("{AUTHOR_FILE_PREFIX}{redacted}"),
        name: redacted.clone(),
        qualified_name: Some(redacted.clone()),
        external_id: Some(redacted),
        signature: None,
        visibility: None,
        span: None,
        is_virtual: true,
    }
}

/// Test-support re-export of [`author_node`].
///
/// Exposes the private emitter for integration-test assertions without
/// making it part of the stable public API.  The `_for_test` suffix signals
/// that this function is not intended for production call sites.
#[doc(hidden)]
#[must_use]
pub fn author_node_for_test(author_email: &str) -> NodeData {
    author_node(author_email)
}

/// Scale factor applied to percent/score values before quantising into the
/// integer-typed graph edge metadata fields. Three decimal places of
/// precision is enough to distinguish ownership shares without inflating
/// edge metadata size.
const EDGE_METADATA_SCALE: f64 = 1_000.0;

/// Quantises a non-negative bounded f64 (percent or score) to a `u64`
/// suitable for the graph edge metadata. NaN, negatives, and inf clamp to 0;
/// values exceeding `max` clamp to `max`. Centralising this here means the
/// three call sites (ownership weight, ownership confidence, co-change
/// confidence) cannot drift in their handling of edge cases.
#[expect(
    clippy::cast_possible_truncation,
    reason = "bounded scaling intentionally quantizes into graph edge metadata"
)]
#[expect(
    clippy::cast_sign_loss,
    reason = "negative and NaN values are clamped to zero before conversion"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "max is used as a comparison ceiling, not for precise arithmetic"
)]
fn quantise_to_edge_metadata(value: f64, max: u64) -> u64 {
    let scaled = (value.max(0.0) * EDGE_METADATA_SCALE).round();
    let max_f = max as f64;
    if !scaled.is_finite() || scaled <= 0.0 {
        0
    } else if scaled >= max_f {
        max
    } else {
        scaled as u64
    }
}

fn percent_to_weight(percent: f64) -> u32 {
    let clamped = percent.clamp(0.0, 1.0);
    u32::try_from(quantise_to_edge_metadata(clamped, u64::from(u32::MAX))).unwrap_or(u32::MAX)
}

fn percent_to_confidence(percent: f64) -> u16 {
    let clamped = percent.clamp(0.0, 1.0);
    u16::try_from(quantise_to_edge_metadata(clamped, u64::from(u16::MAX))).unwrap_or(u16::MAX)
}

fn score_to_confidence(score: f64) -> u16 {
    u16::try_from(quantise_to_edge_metadata(score, u64::from(u16::MAX))).unwrap_or(u16::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeKind, node_id};
    use gather_step_storage::{
        CommitFileChangeKind as StoredChangeKind, CommitFileDeltaRecord, CommitRecord, GraphStore,
        GraphStoreDb, MetadataStore, MetadataStoreDb,
    };

    use super::{RepoIntelligenceOptions, refresh_repo_intelligence};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempPaths {
        graph: PathBuf,
        metadata: PathBuf,
    }

    impl TempPaths {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = env::temp_dir().join(format!(
                "gather-step-intelligence-{name}-{}-{id}",
                process::id()
            ));
            Self {
                graph: root.with_extension("redb"),
                metadata: root.with_extension("sqlite"),
            }
        }
    }

    impl Drop for TempPaths {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.graph);
            for suffix in ["", "-wal", "-shm"] {
                let _ = fs::remove_file(PathBuf::from(format!(
                    "{}{}",
                    self.metadata.display(),
                    suffix
                )));
            }
        }
    }

    #[test]
    fn refresh_repo_intelligence_persists_analytics_and_materializes_summary_edges() {
        let temp = TempPaths::new("refresh");
        let graph = GraphStoreDb::open(&temp.graph).expect("open graph");
        let metadata = MetadataStoreDb::open(&temp.metadata).expect("open metadata");
        graph
            .bulk_insert(
                &[
                    file("service-a", "src/a.rs"),
                    file("service-a", "src/b.rs"),
                    file("service-a", "src/routes.rs"),
                ],
                &[],
            )
            .expect("graph write");

        metadata
            .insert_commits(&[
                CommitRecord {
                    sha: "a1".to_owned(),
                    repo: "service-a".to_owned(),
                    author_email: "alice@example.com".to_owned(),
                    date: 100,
                    message: "feat: add".to_owned(),
                    classification: Some("feat".to_owned()),
                    files_changed: 2,
                    insertions: 4,
                    deletions: 0,
                    has_decision_signal: false,
                    pr_number: None,
                },
                CommitRecord {
                    sha: "b2".to_owned(),
                    repo: "service-a".to_owned(),
                    author_email: "bob@example.com".to_owned(),
                    date: 200,
                    message: "fix: patch".to_owned(),
                    classification: Some("fix".to_owned()),
                    files_changed: 2,
                    insertions: 3,
                    deletions: 1,
                    has_decision_signal: false,
                    pr_number: None,
                },
            ])
            .expect("commits insert");
        metadata
            .upsert_commit_file_deltas(&[
                delta(
                    "service-a",
                    "a1",
                    "src/a.rs",
                    StoredChangeKind::Modified,
                    2,
                    0,
                ),
                delta(
                    "service-a",
                    "a1",
                    "src/b.rs",
                    StoredChangeKind::Modified,
                    2,
                    0,
                ),
                delta(
                    "service-a",
                    "b2",
                    "src/a.rs",
                    StoredChangeKind::Modified,
                    1,
                    1,
                ),
                delta(
                    "service-a",
                    "b2",
                    "src/b.rs",
                    StoredChangeKind::Modified,
                    2,
                    0,
                ),
            ])
            .expect("delta insert");

        let report = refresh_repo_intelligence(
            &graph,
            &metadata,
            "service-a",
            300,
            &RepoIntelligenceOptions::default(),
        )
        .expect("refresh should succeed");

        assert!(!report.analytics.hotspots.is_empty());
        assert!(
            metadata
                .get_file_analytics("service-a", "src/a.rs")
                .expect("analytics lookup")
                .is_some()
        );

        let a_file = graph
            .nodes_by_file("service-a", "src/a.rs")
            .expect("file lookup")
            .into_iter()
            .find(|node| node.kind == NodeKind::File)
            .expect("file node");
        let owned_by = graph
            .edges_by_owner(a_file.id)
            .expect("owned edges")
            .into_iter()
            .filter(|edge| edge.kind == EdgeKind::OwnedBy)
            .count();
        let co_change = graph
            .edges_by_owner(a_file.id)
            .expect("co-change edges")
            .into_iter()
            .filter(|edge| edge.kind == EdgeKind::CoChangesWith)
            .count();

        assert!(owned_by > 0);
        assert!(co_change > 0);
    }

    #[test]
    fn refresh_repo_intelligence_preserves_non_analytics_edges_for_same_owner_file() {
        let temp = TempPaths::new("preserve-semantic-edges");
        let graph = GraphStoreDb::open(&temp.graph).expect("open graph");
        let metadata = MetadataStoreDb::open(&temp.metadata).expect("open metadata");
        let file = file("service-a", "src/events.rs");
        let producer = function("service-a", "src/events.rs", "emitReportQueued", 0);
        let event = gather_step_core::virtual_node(
            NodeKind::Event,
            "service-a",
            "src/events.rs",
            "csv.generation.queued",
            "__event__kafka__csv.generation.queued".to_owned(),
        );
        graph
            .bulk_insert(
                &[file.clone(), producer.clone(), event.clone()],
                &[EdgeData {
                    source: producer.id,
                    target: event.id,
                    kind: EdgeKind::ProducesEventFor,
                    metadata: EdgeMetadata::default(),
                    owner_file: file.id,
                    is_cross_file: false,
                }],
            )
            .expect("graph write");

        metadata
            .insert_commits(&[CommitRecord {
                sha: "a1".to_owned(),
                repo: "service-a".to_owned(),
                author_email: "alice@example.com".to_owned(),
                date: 100,
                message: "feat: add".to_owned(),
                classification: Some("feat".to_owned()),
                files_changed: 1,
                insertions: 2,
                deletions: 0,
                has_decision_signal: false,
                pr_number: None,
            }])
            .expect("commits insert");
        metadata
            .upsert_commit_file_deltas(&[delta(
                "service-a",
                "a1",
                "src/events.rs",
                StoredChangeKind::Modified,
                2,
                0,
            )])
            .expect("delta insert");

        refresh_repo_intelligence(
            &graph,
            &metadata,
            "service-a",
            300,
            &RepoIntelligenceOptions::default(),
        )
        .expect("refresh should succeed");

        let incoming = graph
            .get_incoming(event.id)
            .expect("incoming edges should load");
        assert!(
            incoming
                .iter()
                .any(|edge| edge.kind == EdgeKind::ProducesEventFor && edge.source == producer.id),
            "repo intelligence refresh must not delete existing ProducesEventFor edges for the same owner file"
        );
    }

    fn file(repo: &str, file_path: &str) -> gather_step_core::NodeData {
        gather_step_core::NodeData {
            id: node_id(repo, file_path, NodeKind::File, file_path),
            kind: NodeKind::File,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: file_path.to_owned(),
            qualified_name: None,
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn function(
        repo: &str,
        file_path: &str,
        name: &str,
        _ordinal: u16,
    ) -> gather_step_core::NodeData {
        gather_step_core::NodeData {
            id: node_id(repo, file_path, NodeKind::Function, name),
            kind: NodeKind::Function,
            repo: repo.to_owned(),
            file_path: file_path.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(format!("{repo}::{name}")),
            external_id: None,
            signature: Some(format!("{name}()")),
            visibility: None,
            span: None,
            is_virtual: false,
        }
    }

    fn delta(
        repo: &str,
        sha: &str,
        file_path: &str,
        change_kind: StoredChangeKind,
        insertions: i64,
        deletions: i64,
    ) -> CommitFileDeltaRecord {
        CommitFileDeltaRecord {
            repo: repo.to_owned(),
            sha: sha.to_owned(),
            file_path: file_path.to_owned(),
            change_kind,
            insertions: Some(insertions),
            deletions: Some(deletions),
            old_path: None,
        }
    }
}
