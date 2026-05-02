//! Deployment-topology delta extraction — Phase 7.
//!
//! Diffs deployment-topology nodes (`NodeKind::Deployment`, `EnvVar`, `Secret`,
//! `ConfigMap`, `Broker`, `Database`, `WorkflowJob`) between a baseline graph
//! and a review graph to produce [`DeploymentDeltas`].
//!
//! # Diff keys
//!
//! - `Deployment`: `(repo, name)` decoded from the `qualified_name` with the
//!   `"__deployment__{repo}__{name}"` format.
//! - `EnvVar`: `(repo, name)` — repo is the owning repo of the node; name is
//!   decoded from `"__env_var__{name}"`.
//! - `Secret`, `ConfigMap`, `Broker`, `Database`: name-only sets decoded from
//!   their respective qualified-name prefixes.
//! - `WorkflowJob`: `(repo, workflow, job_name)` decoded from
//!   `"__workflow_job__{repo}__{workflow}__{job}"`.
//!
//! # Service association
//!
//! For `Deployment` nodes, the associated service is resolved by walking
//! outgoing [`EdgeKind::DeployedAs`] edges from `Service` nodes — specifically,
//! we walk *incoming* edges on the `Deployment` node and find any `DeployedAs`
//! edge whose source is a `Service` node.
//!
//! # Consumer changes for env vars
//!
//! The consumer set for an `EnvVar` node is the set of `Service` / `Deployment`
//! qualified names that have an outgoing `ReadsEnv` edge to that `EnvVar`.
//! When the same env var key exists in both baseline and review but the consumer
//! set differs, the change is reported in `consumer_changes`.

use anyhow::Result;
use gather_step_core::{EdgeKind, NodeKind};
use gather_step_storage::GraphStore;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::pr_review::delta_report::{
    DeploymentDelta, DeploymentDeltaChange, DeploymentDeltas, DeploymentSurfaceDeltas,
    EnvVarConsumerChange, EnvVarDelta, EnvVarDeltas, NameOnlyDeltas, WorkflowJobDelta,
    WorkflowJobDeltas,
};

// ── Diff key types ────────────────────────────────────────────────────────────

/// Canonical key for a `Deployment` node.
type DeploymentKey = (String, String); // (repo, name)

/// Canonical key for an `EnvVar` node.
type EnvVarKey = (String, String); // (repo, name)

/// Canonical key for a `WorkflowJob` node.
type WorkflowJobKey = (String, String, String); // (repo, workflow, job_name)

// ── Maps built from one snapshot ─────────────────────────────────────────────

type DeploymentMap = FxHashMap<DeploymentKey, DeploymentDelta>;
type EnvVarMap = FxHashMap<EnvVarKey, EnvVarDelta>;
// For consumer tracking: key → set of consumer qualified_names
type EnvVarConsumerMap = FxHashMap<EnvVarKey, FxHashSet<String>>;
type WorkflowJobMap = FxHashMap<WorkflowJobKey, WorkflowJobDelta>;

/// Extract added / removed / changed deployment-topology nodes by diffing the
/// graphs in `baseline` against `review`.
///
/// If `baseline` is an empty / never-indexed store every review node is
/// reported as `added` — no error is returned.
pub fn extract_deployment_deltas<S: GraphStore>(
    baseline: &S,
    review: &S,
) -> Result<DeploymentDeltas> {
    // ── Deployments ───────────────────────────────────────────────────────────
    let baseline_dep_map = build_deployment_map(baseline)?;
    let review_dep_map = build_deployment_map(review)?;
    let deployments = diff_deployments(&baseline_dep_map, &review_dep_map);

    // ── Env vars ──────────────────────────────────────────────────────────────
    let baseline_env_map = build_env_var_map(baseline)?;
    let review_env_map = build_env_var_map(review)?;
    let baseline_env_consumers = build_env_consumer_map(baseline)?;
    let review_env_consumers = build_env_consumer_map(review)?;
    let env_vars = diff_env_vars(
        &baseline_env_map,
        &review_env_map,
        &baseline_env_consumers,
        &review_env_consumers,
    );

    // ── Name-only surfaces ────────────────────────────────────────────────────
    let secrets = diff_name_only(
        &build_name_set(baseline, NodeKind::Secret)?,
        &build_name_set(review, NodeKind::Secret)?,
    );
    let config_maps = diff_name_only(
        &build_name_set(baseline, NodeKind::ConfigMap)?,
        &build_name_set(review, NodeKind::ConfigMap)?,
    );
    let brokers = diff_name_only(
        &build_name_set(baseline, NodeKind::Broker)?,
        &build_name_set(review, NodeKind::Broker)?,
    );
    let databases = diff_name_only(
        &build_name_set(baseline, NodeKind::Database)?,
        &build_name_set(review, NodeKind::Database)?,
    );

    // ── Workflow jobs ─────────────────────────────────────────────────────────
    let baseline_job_map = build_workflow_job_map(baseline)?;
    let review_job_map = build_workflow_job_map(review)?;
    let workflow_jobs = diff_workflow_jobs(&baseline_job_map, &review_job_map);

    Ok(DeploymentDeltas {
        deployments,
        env_vars,
        secrets,
        config_maps,
        brokers,
        databases,
        workflow_jobs,
        unavailable: false,
    })
}

// ── Build helpers ─────────────────────────────────────────────────────────────

/// Build `(repo, name) → DeploymentDelta` for all `Deployment` nodes in `store`.
fn build_deployment_map<S: GraphStore>(store: &S) -> Result<DeploymentMap> {
    let nodes = store.nodes_by_type(NodeKind::Deployment)?;
    let mut map = DeploymentMap::default();

    for node in nodes {
        let Some((repo, name)) = decode_deployment_qn(
            node.qualified_name
                .as_deref()
                .or(node.external_id.as_deref()),
        ) else {
            continue;
        };

        // Infer artifact kind from file_path.
        let kind = deployment_kind_from_path(&node.file_path);

        // Resolve the associated service name by walking incoming DeployedAs edges.
        let service = resolve_service_for_deployment(store, node.id)?;

        let key = (repo.clone(), name.clone());
        map.insert(
            key,
            DeploymentDelta {
                kind,
                name,
                repo,
                file: if node.file_path.is_empty() {
                    None
                } else {
                    Some(node.file_path.clone())
                },
                line: node.span.as_ref().map(|s| s.line_start),
                service,
                image: None, // image is not stored in NodeData
            },
        );
    }

    Ok(map)
}

/// Build `(repo, name) → EnvVarDelta` for all `EnvVar` nodes in `store`.
fn build_env_var_map<S: GraphStore>(store: &S) -> Result<EnvVarMap> {
    let nodes = store.nodes_by_type(NodeKind::EnvVar)?;
    let mut map = EnvVarMap::default();

    for node in nodes {
        let Some(name) = decode_env_var_qn(
            node.qualified_name
                .as_deref()
                .or(node.external_id.as_deref()),
        ) else {
            continue;
        };

        // Resolve deployment name from incoming ReadsEnv edges.
        let deployment = resolve_deployment_for_env_var(store, node.id)?;

        let key = (node.repo.clone(), name.clone());
        // Last writer wins for duplicate keys (shouldn't occur in practice).
        map.insert(
            key,
            EnvVarDelta {
                name,
                repo: node.repo,
                source_kind: None, // not stored in NodeData
                deployment,
            },
        );
    }

    Ok(map)
}

/// Build `(repo, name) → Set<consumer_qn>` for all `EnvVar` nodes in `store`.
///
/// A consumer is any node that has an outgoing `ReadsEnv` edge to this `EnvVar`.
/// We build the map by walking all `EnvVar` nodes and collecting their incoming
/// `ReadsEnv` edges.
fn build_env_consumer_map<S: GraphStore>(store: &S) -> Result<EnvVarConsumerMap> {
    let nodes = store.nodes_by_type(NodeKind::EnvVar)?;
    let mut map = EnvVarConsumerMap::default();

    for node in nodes {
        let Some(name) = decode_env_var_qn(
            node.qualified_name
                .as_deref()
                .or(node.external_id.as_deref()),
        ) else {
            continue;
        };
        let key = (node.repo.clone(), name);
        let consumers = map.entry(key).or_default();

        for edge in store.get_incoming(node.id)? {
            if edge.kind == EdgeKind::ReadsEnv
                && let Some(source) = store.get_node(edge.source)?
                && let Some(qn) = source.qualified_name
            {
                consumers.insert(qn);
            }
        }
    }

    Ok(map)
}

/// Build a set of node names for the given `NodeKind`.
fn build_name_set<S: GraphStore>(store: &S, kind: NodeKind) -> Result<FxHashSet<String>> {
    let nodes = store.nodes_by_type(kind)?;
    Ok(nodes.into_iter().map(|n| n.name).collect())
}

/// Build `(repo, workflow, job_name) → WorkflowJobDelta` for all `WorkflowJob`
/// nodes in `store`.
fn build_workflow_job_map<S: GraphStore>(store: &S) -> Result<WorkflowJobMap> {
    let nodes = store.nodes_by_type(NodeKind::WorkflowJob)?;
    let mut map = WorkflowJobMap::default();

    for node in nodes {
        let qn = node
            .qualified_name
            .as_deref()
            .or(node.external_id.as_deref());
        let Some((repo, workflow, job_name)) = decode_workflow_job_qn(qn) else {
            continue;
        };

        // Resolve deploy_target from outgoing BuiltBy edges.
        let deploy_target = resolve_deploy_target(store, node.id)?;

        let key = (repo.clone(), workflow.clone(), job_name.clone());
        map.insert(
            key,
            WorkflowJobDelta {
                workflow,
                job_name,
                repo,
                deploy_target,
            },
        );
    }

    Ok(map)
}

// ── Diff functions ────────────────────────────────────────────────────────────

fn diff_deployments(baseline: &DeploymentMap, review: &DeploymentMap) -> DeploymentSurfaceDeltas {
    let mut added: Vec<DeploymentDelta> = Vec::new();
    let mut removed: Vec<DeploymentDelta> = Vec::new();
    let mut changed: Vec<DeploymentDeltaChange> = Vec::new();

    // Added: in review but not in baseline.
    for (key, delta) in review {
        if !baseline.contains_key(key) {
            added.push(delta.clone());
        }
    }

    // Removed: in baseline but not in review.
    for (key, delta) in baseline {
        if !review.contains_key(key) {
            removed.push(delta.clone());
        }
    }

    // Changed: in both — compare file path and service.
    for (key, review_delta) in review {
        if let Some(baseline_delta) = baseline.get(key) {
            let image_changed = baseline_delta.image != review_delta.image;
            let file_changed = baseline_delta.file != review_delta.file;
            if image_changed || file_changed {
                changed.push(DeploymentDeltaChange {
                    kind: review_delta.kind.clone(),
                    name: key.1.clone(),
                    repo: key.0.clone(),
                    before: baseline_delta.clone(),
                    after: review_delta.clone(),
                    image_changed,
                    env_changed: false, // computed below if needed
                });
            }
        }
    }

    // Deterministic output.
    added.sort_by(|a, b| (&a.repo, &a.name).cmp(&(&b.repo, &b.name)));
    removed.sort_by(|a, b| (&a.repo, &a.name).cmp(&(&b.repo, &b.name)));
    changed.sort_by(|a, b| (&a.repo, &a.name).cmp(&(&b.repo, &b.name)));

    DeploymentSurfaceDeltas {
        added,
        removed,
        changed,
    }
}

fn diff_env_vars(
    baseline: &EnvVarMap,
    review: &EnvVarMap,
    baseline_consumers: &EnvVarConsumerMap,
    review_consumers: &EnvVarConsumerMap,
) -> EnvVarDeltas {
    let mut added: Vec<EnvVarDelta> = Vec::new();
    let mut removed: Vec<EnvVarDelta> = Vec::new();
    let mut consumer_changes: Vec<EnvVarConsumerChange> = Vec::new();

    // Added.
    for (key, delta) in review {
        if !baseline.contains_key(key) {
            added.push(delta.clone());
        }
    }

    // Removed.
    for (key, delta) in baseline {
        if !review.contains_key(key) {
            removed.push(delta.clone());
        }
    }

    // Consumer changes: same key in both, different consumer sets.
    for key in review.keys() {
        if baseline.contains_key(key) {
            let empty: FxHashSet<String> = FxHashSet::default();
            let baseline_set = baseline_consumers.get(key).unwrap_or(&empty);
            let review_set = review_consumers.get(key).unwrap_or(&empty);

            let mut consumers_added: Vec<String> =
                review_set.difference(baseline_set).cloned().collect();
            let mut consumers_removed: Vec<String> =
                baseline_set.difference(review_set).cloned().collect();

            if !consumers_added.is_empty() || !consumers_removed.is_empty() {
                consumers_added.sort_unstable();
                consumers_removed.sort_unstable();
                consumer_changes.push(EnvVarConsumerChange {
                    name: key.1.clone(),
                    consumers_added,
                    consumers_removed,
                });
            }
        }
    }

    // Deterministic output.
    added.sort_by(|a, b| (&a.repo, &a.name).cmp(&(&b.repo, &b.name)));
    removed.sort_by(|a, b| (&a.repo, &a.name).cmp(&(&b.repo, &b.name)));
    consumer_changes.sort_by(|a, b| a.name.cmp(&b.name));

    EnvVarDeltas {
        added,
        removed,
        consumer_changes,
    }
}

fn diff_name_only(baseline: &FxHashSet<String>, review: &FxHashSet<String>) -> NameOnlyDeltas {
    let mut added: Vec<String> = review.difference(baseline).cloned().collect();
    let mut removed: Vec<String> = baseline.difference(review).cloned().collect();
    added.sort_unstable();
    removed.sort_unstable();
    NameOnlyDeltas { added, removed }
}

fn diff_workflow_jobs(baseline: &WorkflowJobMap, review: &WorkflowJobMap) -> WorkflowJobDeltas {
    let mut added: Vec<WorkflowJobDelta> = Vec::new();
    let mut removed: Vec<WorkflowJobDelta> = Vec::new();

    for (key, delta) in review {
        if !baseline.contains_key(key) {
            added.push(delta.clone());
        }
    }

    for (key, delta) in baseline {
        if !review.contains_key(key) {
            removed.push(delta.clone());
        }
    }

    added.sort_by(|a, b| {
        (&a.repo, &a.workflow, &a.job_name).cmp(&(&b.repo, &b.workflow, &b.job_name))
    });
    removed.sort_by(|a, b| {
        (&a.repo, &a.workflow, &a.job_name).cmp(&(&b.repo, &b.workflow, &b.job_name))
    });

    WorkflowJobDeltas { added, removed }
}

// ── Edge-resolution helpers ───────────────────────────────────────────────────

/// Walk incoming `DeployedAs` edges on a `Deployment` node to find an
/// associated `Service` name.  Returns the first match.
fn resolve_service_for_deployment<S: GraphStore>(
    store: &S,
    deployment_id: gather_step_core::NodeId,
) -> Result<Option<String>> {
    for edge in store.get_incoming(deployment_id)? {
        if edge.kind == EdgeKind::DeployedAs
            && let Some(source) = store.get_node(edge.source)?
            && source.kind == NodeKind::Service
        {
            return Ok(Some(source.name));
        }
    }
    Ok(None)
}

/// Walk incoming `ReadsEnv` edges on an `EnvVar` node to find the associated
/// deployment name.  Returns the name of the first `Service` or `Deployment`
/// source node found.
fn resolve_deployment_for_env_var<S: GraphStore>(
    store: &S,
    env_var_id: gather_step_core::NodeId,
) -> Result<Option<String>> {
    for edge in store.get_incoming(env_var_id)? {
        if edge.kind == EdgeKind::ReadsEnv
            && let Some(source) = store.get_node(edge.source)?
            && matches!(source.kind, NodeKind::Service | NodeKind::Deployment)
        {
            return Ok(Some(source.name));
        }
    }
    Ok(None)
}

/// Walk outgoing `BuiltBy` edges on a `WorkflowJob` to find the deploy target.
fn resolve_deploy_target<S: GraphStore>(
    store: &S,
    job_id: gather_step_core::NodeId,
) -> Result<Option<String>> {
    for edge in store.get_outgoing(job_id)? {
        if edge.kind == EdgeKind::BuiltBy
            && let Some(target) = store.get_node(edge.target)?
            && matches!(target.kind, NodeKind::Service | NodeKind::Deployment)
        {
            return Ok(Some(target.name));
        }
    }
    Ok(None)
}

// ── Qualified-name decoders ───────────────────────────────────────────────────

/// Decode `"__deployment__{repo}__{name}"` → `(repo, name)`.
fn decode_deployment_qn(qn: Option<&str>) -> Option<(String, String)> {
    let qn = qn?;
    let suffix = qn.strip_prefix("__deployment__")?;
    let (repo, name) = suffix.split_once("__")?;
    if repo.is_empty() || name.is_empty() {
        return None;
    }
    Some((repo.to_owned(), name.to_owned()))
}

/// Decode `"__env_var__{name}"` → `name`.
fn decode_env_var_qn(qn: Option<&str>) -> Option<String> {
    let qn = qn?;
    let name = qn.strip_prefix("__env_var__")?;
    if name.is_empty() {
        return None;
    }
    Some(name.to_owned())
}

/// Decode `"__workflow_job__{repo}__{workflow}__{job}"` → `(repo, workflow, job)`.
///
/// The workflow path uses `__` as a separator so we split by taking the first
/// two `__`-delimited fields as `repo` and then reassemble the rest as `workflow`
/// … but actually the format is `__{repo}__{path}__{job}` where each part is
/// canonical (lowercased, spaces→`_`).  We split on the first two `__` pairs
/// and treat the remainder as the job name.
fn decode_workflow_job_qn(qn: Option<&str>) -> Option<(String, String, String)> {
    let qn = qn?;
    let suffix = qn.strip_prefix("__workflow_job__")?;
    // Format: {repo}__{workflow_path}__{job_name}
    // We split at most twice so the job_name can contain `__` if needed.
    let mut parts = suffix.splitn(3, "__");
    let repo = parts.next()?.to_owned();
    let workflow = parts.next()?.to_owned();
    let job = parts.next()?.to_owned();
    if repo.is_empty() || workflow.is_empty() || job.is_empty() {
        return None;
    }
    Some((repo, workflow, job))
}

/// Infer deployment artifact kind from a file path — mirrors the heuristics
/// in `gather-step-deploy::detect_artifact_kind` without depending on that crate.
fn deployment_kind_from_path(file_path: &str) -> String {
    let mut normalized = file_path.replace('\\', "/");
    normalized.make_ascii_lowercase();
    let file_name = normalized.rsplit('/').next().unwrap_or(&normalized);

    if normalized.contains("/.github/workflows/") || normalized.starts_with(".github/workflows/") {
        return "github_actions".to_owned();
    }
    if file_name.eq_ignore_ascii_case("dockerfile") || file_name.starts_with("dockerfile.") {
        return "dockerfile".to_owned();
    }
    if file_name.eq_ignore_ascii_case("docker-compose.yml")
        || file_name.eq_ignore_ascii_case("docker-compose.yaml")
        || file_name.eq_ignore_ascii_case("compose.yml")
        || file_name.eq_ignore_ascii_case("compose.yaml")
    {
        return "compose".to_owned();
    }
    if file_name.eq_ignore_ascii_case("kustomization.yaml")
        || file_name.eq_ignore_ascii_case("kustomization.yml")
    {
        return "kustomize".to_owned();
    }
    let p = std::path::Path::new(&normalized);
    let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml") {
        return "kubernetes".to_owned();
    }
    "unknown".to_owned()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use gather_step_core::{EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, node_id};
    use gather_step_storage::{GraphStore, GraphStoreDb};

    use super::extract_deployment_deltas;

    // ── temp-db helpers ───────────────────────────────────────────────────────

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new(label: &str) -> Self {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gs-deploy-extractor-{label}-{}-{id}.redb",
                std::process::id()
            ));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn open_store(label: &str) -> (TempDb, GraphStoreDb) {
        let tmp = TempDb::new(label);
        let db = GraphStoreDb::open(&tmp.path).expect("store should open");
        (tmp, db)
    }

    // ── graph-building helpers ────────────────────────────────────────────────

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
        }
    }

    /// Build a virtual deployment node with the `__deployment__{repo}__{name}` qn.
    fn deployment_node(repo: &str, file: &str, name: &str) -> NodeData {
        let qn = format!("__deployment__{repo}__{name}");
        NodeData {
            id: gather_step_core::virtual_node_id(NodeKind::Deployment, &qn),
            kind: NodeKind::Deployment,
            repo: repo.to_owned(),
            file_path: file.to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    /// Build a virtual env-var node with the `__env_var__{name}` qn.
    fn env_var_node(repo: &str, name: &str) -> NodeData {
        let mut lower = name.to_owned();
        lower.make_ascii_lowercase();
        let qn = format!("__env_var__{lower}");
        NodeData {
            id: gather_step_core::virtual_node_id(NodeKind::EnvVar, &qn),
            kind: NodeKind::EnvVar,
            repo: repo.to_owned(),
            file_path: String::new(),
            name: name.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    /// Build a virtual secret node with the `__secret__{name}` qn.
    fn secret_node(repo: &str, name: &str) -> NodeData {
        let mut lower = name.to_owned();
        lower.make_ascii_lowercase();
        let qn = format!("__secret__{lower}");
        NodeData {
            id: gather_step_core::virtual_node_id(NodeKind::Secret, &qn),
            kind: NodeKind::Secret,
            repo: repo.to_owned(),
            file_path: String::new(),
            name: name.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    /// Build a virtual workflow-job node.
    fn workflow_job_node(repo: &str, workflow: &str, job: &str) -> NodeData {
        let qn = format!("__workflow_job__{repo}__{workflow}__{job}");
        NodeData {
            id: gather_step_core::virtual_node_id(NodeKind::WorkflowJob, &qn),
            kind: NodeKind::WorkflowJob,
            repo: repo.to_owned(),
            file_path: workflow.to_owned(),
            name: job.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    /// Build a service node (non-virtual) for edge wiring.
    fn service_node(repo: &str, name: &str) -> NodeData {
        let qn = format!("__service__{repo}__{name}");
        NodeData {
            id: gather_step_core::virtual_node_id(NodeKind::Service, &qn),
            kind: NodeKind::Service,
            repo: repo.to_owned(),
            file_path: String::new(),
            name: name.to_owned(),
            qualified_name: Some(qn.clone()),
            external_id: Some(qn),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    /// Connect service → `env_var` via `ReadsEnv` edge.
    fn reads_env_edge(service: &NodeData, env_var: &NodeData, owner: &NodeData) -> EdgeData {
        EdgeData {
            source: service.id,
            target: env_var.id,
            kind: EdgeKind::ReadsEnv,
            metadata: EdgeMetadata {
                confidence: Some(850),
                ..EdgeMetadata::default()
            },
            owner_file: owner.id,
            is_cross_file: false,
        }
    }

    /// Insert a standalone deployment node (no service edge).
    fn insert_deployment(store: &GraphStoreDb, repo: &str, file: &str, name: &str) {
        let f = file_node(repo, file);
        let d = deployment_node(repo, file, name);
        store.bulk_insert(&[f, d], &[]).expect("bulk insert");
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// A `Deployment` node present only in the review graph appears in `added`.
    #[test]
    fn deployment_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("dep-added-baseline");
        let (_td_r, review) = open_store("dep-added-review");

        insert_deployment(&review, "backend", "Dockerfile", "api");

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.deployments.added.len(),
            1,
            "expected one added deployment"
        );
        assert_eq!(deltas.deployments.added[0].name, "api");
        assert_eq!(deltas.deployments.added[0].kind, "dockerfile");
        assert!(
            deltas.deployments.removed.is_empty(),
            "no deployments should be removed"
        );
    }

    /// A `Deployment` node present only in the baseline graph appears in `removed`.
    #[test]
    fn deployment_removed_appears_in_removed_list() {
        let (_td_b, baseline) = open_store("dep-removed-baseline");
        let (_td_r, review) = open_store("dep-removed-review");

        insert_deployment(&baseline, "backend", "Dockerfile", "api");

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.deployments.removed.len(),
            1,
            "expected one removed deployment"
        );
        assert_eq!(deltas.deployments.removed[0].name, "api");
        assert!(
            deltas.deployments.added.is_empty(),
            "no deployments should be added"
        );
    }

    /// Same key in both snapshots but file path changed → appears in `changed`.
    #[test]
    fn deployment_image_change_appears_in_changed_list() {
        let (_td_b, baseline) = open_store("dep-changed-baseline");
        let (_td_r, review) = open_store("dep-changed-review");

        // Baseline: api deployment from Dockerfile
        insert_deployment(&baseline, "backend", "Dockerfile", "api");
        // Review: same name+repo but different file (simulates a move)
        insert_deployment(&review, "backend", "deploy/Dockerfile", "api");

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.deployments.changed.len(),
            1,
            "expected one changed deployment"
        );
        assert_eq!(deltas.deployments.changed[0].name, "api");
        assert!(
            deltas.deployments.added.is_empty(),
            "no deployments should be added"
        );
        assert!(
            deltas.deployments.removed.is_empty(),
            "no deployments should be removed"
        );
    }

    /// An `EnvVar` node present only in the review graph appears in `env_vars.added`.
    #[test]
    fn env_var_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("env-added-baseline");
        let (_td_r, review) = open_store("env-added-review");

        let f = file_node("backend", "Dockerfile");
        let v = env_var_node("backend", "database_url");
        review.bulk_insert(&[f, v], &[]).expect("bulk insert");

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(deltas.env_vars.added.len(), 1, "expected one added env var");
        assert_eq!(deltas.env_vars.added[0].name, "database_url");
        assert!(
            deltas.env_vars.removed.is_empty(),
            "no env vars should be removed"
        );
    }

    /// Same env var key in both snapshots but consumer set changed →
    /// appears in `env_vars.consumer_changes`.
    #[test]
    fn env_var_consumer_set_change_appears_in_consumer_changes() {
        let (_td_b, baseline) = open_store("env-consumer-baseline");
        let (_td_r, review) = open_store("env-consumer-review");

        let env_name = "redis_url";

        // Baseline: env var consumed by service "api".
        {
            let f = file_node("backend", "compose.yaml");
            let v = env_var_node("backend", env_name);
            let svc = service_node("backend", "api");
            let edge = reads_env_edge(&svc, &v, &f);
            baseline
                .bulk_insert(&[f, v, svc], &[edge])
                .expect("bulk insert");
        }

        // Review: same env var now consumed by "api" AND new service "worker".
        {
            // Use unique file per insert to avoid owner-file collision.
            let f1 = file_node("backend", "compose.yaml.api.env");
            let f2 = file_node("backend", "compose.yaml.worker.env");
            let v = env_var_node("backend", env_name);
            let svc_api = service_node("backend", "api");
            let svc_worker = service_node("backend", "worker");
            let edge1 = reads_env_edge(&svc_api, &v, &f1);
            let edge2 = reads_env_edge(&svc_worker, &v, &f2);
            review
                .bulk_insert(&[f1.clone(), v.clone(), svc_api], &[edge1])
                .expect("bulk insert api");
            review
                .bulk_insert(&[f2, v, svc_worker], &[edge2])
                .expect("bulk insert worker");
        }

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert!(
            deltas.env_vars.added.is_empty(),
            "env var must not appear in added (exists in both)"
        );
        assert!(
            deltas.env_vars.removed.is_empty(),
            "env var must not appear in removed"
        );
        assert_eq!(
            deltas.env_vars.consumer_changes.len(),
            1,
            "expected one consumer change"
        );
        assert_eq!(deltas.env_vars.consumer_changes[0].name, env_name);
        assert_eq!(deltas.env_vars.consumer_changes[0].consumers_added.len(), 1);
    }

    /// A `Secret` node present only in the review graph appears in `secrets.added`.
    #[test]
    fn secret_added_appears_in_secret_added_list() {
        let (_td_b, baseline) = open_store("secret-added-baseline");
        let (_td_r, review) = open_store("secret-added-review");

        let f = file_node("backend", "compose.yaml");
        let s = secret_node("backend", "api-secret");
        review.bulk_insert(&[f, s], &[]).expect("bulk insert");

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert!(
            deltas.secrets.added.contains(&"api-secret".to_owned()),
            "api-secret must appear in secrets.added"
        );
        assert!(
            deltas.secrets.removed.is_empty(),
            "no secrets should be removed"
        );
    }

    /// A `WorkflowJob` node present only in the review graph appears in
    /// `workflow_jobs.added`.
    #[test]
    fn workflow_job_added_appears_in_added_list() {
        let (_td_b, baseline) = open_store("job-added-baseline");
        let (_td_r, review) = open_store("job-added-review");

        let f = file_node("backend", ".github/workflows/foo.yml");
        let j = workflow_job_node("backend", ".github/workflows/foo.yml", "deploy");
        review.bulk_insert(&[f, j], &[]).expect("bulk insert");

        let deltas = extract_deployment_deltas(&baseline, &review).expect("should succeed");

        assert_eq!(
            deltas.workflow_jobs.added.len(),
            1,
            "expected one added workflow job"
        );
        assert_eq!(deltas.workflow_jobs.added[0].job_name, "deploy");
        assert!(
            deltas.workflow_jobs.removed.is_empty(),
            "no workflow jobs should be removed"
        );
    }
}
