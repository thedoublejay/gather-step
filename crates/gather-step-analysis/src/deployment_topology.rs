use std::collections::BTreeMap;

use gather_step_core::{EdgeData, EdgeKind, NodeData, NodeKind};
use gather_step_storage::{GraphStore, GraphStoreError};
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum DeploymentTopologyError {
    #[error(transparent)]
    Graph(#[from] GraphStoreError),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeploymentTopologyQuery {
    WhereDeployed { service: String },
    ServiceEnv { service: String },
    EnvVarConsumers { env_var: String },
    UndeployedServices,
    DeployedButNoCode,
    SharedInfra,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentTopologyReport {
    pub query: DeploymentTopologyQuery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
    pub deployments: Vec<DeploymentTopologyNode>,
    pub services: Vec<DeploymentTopologyNode>,
    pub env_vars: Vec<DeploymentTopologyNode>,
    pub shared_infra: Vec<DeploymentTopologyNode>,
    pub workflow_jobs: Vec<DeploymentTopologyNode>,
    pub edges: Vec<DeploymentTopologyEdge>,
    pub missing_evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeploymentTopologyNode {
    pub repo: String,
    pub kind: NodeKind,
    pub name: String,
    pub file_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qualified_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeploymentTopologyEdge {
    pub source: DeploymentTopologyNode,
    pub target: DeploymentTopologyNode,
    pub kind: EdgeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<u16>,
}

pub fn deployment_topology<S: GraphStore>(
    store: &S,
    query: DeploymentTopologyQuery,
    repo: Option<&str>,
    limit: usize,
) -> Result<DeploymentTopologyReport, DeploymentTopologyError> {
    let limit = limit.clamp(1, 100);
    let mut report = DeploymentTopologyReport {
        query: query.clone(),
        repo: repo.map(ToOwned::to_owned),
        deployments: Vec::new(),
        services: Vec::new(),
        env_vars: Vec::new(),
        shared_infra: Vec::new(),
        workflow_jobs: Vec::new(),
        edges: Vec::new(),
        missing_evidence: Vec::new(),
    };

    match query {
        DeploymentTopologyQuery::WhereDeployed { service } => {
            let services = find_named_nodes(store, NodeKind::Service, &service, repo)?;
            if services.is_empty() {
                report
                    .missing_evidence
                    .push(format!("no indexed service matched `{service}`"));
            }
            for service_node in services.into_iter().take(limit) {
                if report.edges.len() >= limit {
                    break;
                }
                let edge_count = report.edges.len();
                collect_outgoing(
                    store,
                    &mut report,
                    &service_node,
                    &[EdgeKind::DeployedAs],
                    Some(NodeKind::Deployment),
                    limit,
                )?;
                if report.edges.len() == edge_count {
                    report.missing_evidence.push(format!(
                        "service `{}` has no indexed deployment edge",
                        service_node.name
                    ));
                }
                report.services.push(node_item(&service_node));
            }
        }
        DeploymentTopologyQuery::ServiceEnv { service } => {
            let services = find_named_nodes(store, NodeKind::Service, &service, repo)?;
            if services.is_empty() {
                report
                    .missing_evidence
                    .push(format!("no indexed service matched `{service}`"));
            }
            for service_node in services.into_iter().take(limit) {
                if report.edges.len() >= limit {
                    break;
                }
                let edge_count = report.edges.len();
                collect_outgoing(
                    store,
                    &mut report,
                    &service_node,
                    &[EdgeKind::ReadsEnv],
                    Some(NodeKind::EnvVar),
                    limit,
                )?;
                if report.edges.len() == edge_count {
                    report.missing_evidence.push(format!(
                        "service `{}` has no indexed env var edge",
                        service_node.name
                    ));
                }
                report.services.push(node_item(&service_node));
            }
        }
        DeploymentTopologyQuery::EnvVarConsumers { env_var } => {
            let env_vars = find_named_nodes(store, NodeKind::EnvVar, &env_var, repo)?;
            if env_vars.is_empty() {
                report
                    .missing_evidence
                    .push(format!("no indexed env var matched `{env_var}`"));
            }
            for env_node in env_vars.into_iter().take(limit) {
                if report.edges.len() >= limit {
                    break;
                }
                let edge_count = report.edges.len();
                collect_incoming(
                    store,
                    &mut report,
                    &env_node,
                    &[EdgeKind::ReadsEnv],
                    Some(NodeKind::Service),
                    limit,
                )?;
                if report.edges.len() == edge_count {
                    report.missing_evidence.push(format!(
                        "env var `{}` has no indexed service consumers",
                        env_node.name
                    ));
                }
                report.env_vars.push(node_item(&env_node));
            }
        }
        DeploymentTopologyQuery::UndeployedServices => {
            for service_node in filtered_nodes(store, NodeKind::Service, repo)? {
                if report.services.len() >= limit {
                    break;
                }
                let has_deployment = store
                    .get_outgoing(service_node.id)?
                    .iter()
                    .any(|edge| edge.kind == EdgeKind::DeployedAs);
                if !has_deployment {
                    report.services.push(node_item(&service_node));
                }
            }
            if report.services.is_empty() {
                report
                    .missing_evidence
                    .push("no undeployed services found in indexed topology".to_owned());
            }
        }
        DeploymentTopologyQuery::DeployedButNoCode => {
            for deployment in filtered_nodes(store, NodeKind::Deployment, repo)? {
                if report.deployments.len() >= limit {
                    break;
                }
                let has_service = store
                    .get_incoming(deployment.id)?
                    .iter()
                    .any(|edge| edge.kind == EdgeKind::DeployedAs);
                if !has_service {
                    report.deployments.push(node_item(&deployment));
                }
            }
            if report.deployments.is_empty() {
                report.missing_evidence.push(
                    "no deployed-without-service-code evidence found in indexed topology"
                        .to_owned(),
                );
            }
        }
        DeploymentTopologyQuery::SharedInfra => {
            let mut infra = filtered_nodes(store, NodeKind::Database, repo)?;
            infra.extend(filtered_nodes(store, NodeKind::Broker, repo)?);
            for infra_node in infra.into_iter().take(limit) {
                if report.edges.len() >= limit {
                    break;
                }
                collect_incoming(
                    store,
                    &mut report,
                    &infra_node,
                    &[EdgeKind::UsesDatabase, EdgeKind::UsesBroker],
                    Some(NodeKind::Service),
                    limit,
                )?;
                report.shared_infra.push(node_item(&infra_node));
            }
            if report.shared_infra.is_empty() {
                report
                    .missing_evidence
                    .push("no shared infrastructure nodes found in indexed topology".to_owned());
            }
        }
    }

    dedupe_and_sort(&mut report);
    Ok(report)
}

fn collect_outgoing<S: GraphStore>(
    store: &S,
    report: &mut DeploymentTopologyReport,
    source: &NodeData,
    edge_kinds: &[EdgeKind],
    target_kind: Option<NodeKind>,
    limit: usize,
) -> Result<(), DeploymentTopologyError> {
    for edge in store.get_outgoing(source.id)? {
        if report.edges.len() >= limit {
            break;
        }
        if !edge_kinds.contains(&edge.kind) {
            continue;
        }
        let Some(target) = store.get_node(edge.target)? else {
            continue;
        };
        if target_kind.is_some_and(|kind| target.kind != kind) {
            continue;
        }
        push_edge(report, source, &target, &edge);
    }
    Ok(())
}

fn collect_incoming<S: GraphStore>(
    store: &S,
    report: &mut DeploymentTopologyReport,
    target: &NodeData,
    edge_kinds: &[EdgeKind],
    source_kind: Option<NodeKind>,
    limit: usize,
) -> Result<(), DeploymentTopologyError> {
    for edge in store.get_incoming(target.id)? {
        if report.edges.len() >= limit {
            break;
        }
        if !edge_kinds.contains(&edge.kind) {
            continue;
        }
        let Some(source) = store.get_node(edge.source)? else {
            continue;
        };
        if source_kind.is_some_and(|kind| source.kind != kind) {
            continue;
        }
        push_edge(report, &source, target, &edge);
    }
    Ok(())
}

fn push_edge(
    report: &mut DeploymentTopologyReport,
    source: &NodeData,
    target: &NodeData,
    edge: &EdgeData,
) {
    for node in [source, target] {
        match node.kind {
            NodeKind::Deployment => report.deployments.push(node_item(node)),
            NodeKind::EnvVar => report.env_vars.push(node_item(node)),
            NodeKind::WorkflowJob => report.workflow_jobs.push(node_item(node)),
            NodeKind::Database | NodeKind::Broker => report.shared_infra.push(node_item(node)),
            NodeKind::Service => report.services.push(node_item(node)),
            _ => {}
        }
    }
    report.edges.push(DeploymentTopologyEdge {
        source: node_item(source),
        target: node_item(target),
        kind: edge.kind,
        confidence: edge.metadata.confidence,
    });
}

fn find_named_nodes<S: GraphStore>(
    store: &S,
    kind: NodeKind,
    target: &str,
    repo: Option<&str>,
) -> Result<Vec<NodeData>, DeploymentTopologyError> {
    let target = target.trim();
    let canonical_target = canonical_topology_part(target);
    Ok(filtered_nodes(store, kind, repo)?
        .into_iter()
        .filter(|node| node_matches_target(node, target, &canonical_target))
        .collect())
}

fn node_matches_target(node: &NodeData, target: &str, canonical_target: &str) -> bool {
    node.name.eq_ignore_ascii_case(target)
        || node
            .qualified_name
            .as_deref()
            .is_some_and(|qualified_name| {
                identifier_matches_target(qualified_name, target, canonical_target)
            })
        || node.external_id.as_deref().is_some_and(|external_id| {
            identifier_matches_target(external_id, target, canonical_target)
        })
}

fn identifier_matches_target(value: &str, target: &str, canonical_target: &str) -> bool {
    value.eq_ignore_ascii_case(target)
        || value
            .rsplit("__")
            .next()
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(canonical_target))
}

fn filtered_nodes<S: GraphStore>(
    store: &S,
    kind: NodeKind,
    repo: Option<&str>,
) -> Result<Vec<NodeData>, DeploymentTopologyError> {
    Ok(store
        .nodes_by_type(kind)?
        .into_iter()
        .filter(|node| repo.is_none_or(|repo| node_matches_repo(node, repo)))
        .collect())
}

fn node_matches_repo(node: &NodeData, repo: &str) -> bool {
    if node.repo == repo {
        return true;
    }
    if !node.is_virtual {
        return false;
    }

    let repo_marker = format!("__{}__", canonical_topology_part(repo));
    [node.qualified_name.as_deref(), node.external_id.as_deref()]
        .into_iter()
        .flatten()
        .any(|value| value.contains(&repo_marker))
}

fn canonical_topology_part(value: &str) -> String {
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

fn node_item(node: &NodeData) -> DeploymentTopologyNode {
    DeploymentTopologyNode {
        repo: node.repo.clone(),
        kind: node.kind,
        name: node.name.clone(),
        file_path: node.file_path.clone(),
        qualified_name: node.qualified_name.clone(),
    }
}

fn dedupe_and_sort(report: &mut DeploymentTopologyReport) {
    report.deployments = dedupe_nodes(std::mem::take(&mut report.deployments));
    report.services = dedupe_nodes(std::mem::take(&mut report.services));
    report.env_vars = dedupe_nodes(std::mem::take(&mut report.env_vars));
    report.shared_infra = dedupe_nodes(std::mem::take(&mut report.shared_infra));
    report.workflow_jobs = dedupe_nodes(std::mem::take(&mut report.workflow_jobs));
    report.edges.sort();
    report.edges.dedup();
    report.missing_evidence.sort();
    report.missing_evidence.dedup();
}

fn dedupe_nodes(nodes: Vec<DeploymentTopologyNode>) -> Vec<DeploymentTopologyNode> {
    let mut by_key = BTreeMap::<(NodeKind, Option<String>, String), DeploymentTopologyNode>::new();
    for node in nodes {
        by_key
            .entry((node.kind, node.qualified_name.clone(), node.name.clone()))
            .or_insert(node);
    }
    by_key.into_values().collect()
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use gather_step_core::{
        EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeKind, node_id, ref_node_id,
    };
    use gather_step_storage::{GraphStore, GraphStoreDb};
    use pretty_assertions::assert_eq;

    use super::{DeploymentTopologyQuery, deployment_topology};

    fn node(kind: NodeKind, repo: &str, name: &str, qn: &str) -> NodeData {
        NodeData {
            id: ref_node_id(kind, qn),
            kind,
            repo: repo.to_owned(),
            file_path: "compose.yaml".to_owned(),
            name: name.to_owned(),
            qualified_name: Some(qn.to_owned()),
            external_id: Some(qn.to_owned()),
            signature: None,
            visibility: None,
            span: None,
            is_virtual: true,
        }
    }

    #[test]
    fn where_deployed_and_env_queries_return_stable_empty_arrays() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let graph_path = std::env::temp_dir().join(format!(
            "gather-step-deployment-topology-analysis-{unique}.redb"
        ));
        let graph = GraphStoreDb::open(&graph_path).expect("graph");
        let file = NodeData {
            id: node_id("backend", "compose.yaml", NodeKind::File, "compose.yaml"),
            kind: NodeKind::File,
            repo: "backend".to_owned(),
            file_path: "compose.yaml".to_owned(),
            name: "compose.yaml".to_owned(),
            qualified_name: Some("compose.yaml".to_owned()),
            external_id: None,
            signature: None,
            visibility: None,
            span: None,
            is_virtual: false,
        };
        let service = node(
            NodeKind::Service,
            "backend",
            "api",
            "__service__backend__api",
        );
        let deployment = node(
            NodeKind::Deployment,
            "backend",
            "api",
            "__deployment__backend__api",
        );
        let env = node(
            NodeKind::EnvVar,
            "backend",
            "DATABASE_URL",
            "__env_var__database_url",
        );
        let worker = node(
            NodeKind::Service,
            "backend",
            "api-worker",
            "__service__backend__api-worker",
        );
        let worker_deployment = node(
            NodeKind::Deployment,
            "backend",
            "api-worker",
            "__deployment__backend__api-worker",
        );
        let orphan = node(
            NodeKind::Service,
            "backend",
            "orphan",
            "__service__backend__orphan",
        );
        let unused_env = node(
            NodeKind::EnvVar,
            "backend",
            "UNUSED_PORT",
            "__env_var__unused_port",
        );
        let other_service = node(NodeKind::Service, "worker", "api", "__service__worker__api");
        let other_deployment = node(
            NodeKind::Deployment,
            "worker",
            "api",
            "__deployment__worker__api",
        );
        graph.insert_node(&file).expect("file");
        graph.insert_node(&service).expect("service");
        graph.insert_node(&deployment).expect("deployment");
        graph.insert_node(&env).expect("env");
        graph.insert_node(&worker).expect("worker service");
        graph
            .insert_node(&worker_deployment)
            .expect("worker deployment");
        graph.insert_node(&orphan).expect("orphan service");
        graph.insert_node(&unused_env).expect("unused env");
        graph.insert_node(&other_service).expect("other service");
        graph
            .insert_node(&other_deployment)
            .expect("other deployment");
        graph
            .insert_edge(&EdgeData {
                source: service.id,
                target: deployment.id,
                kind: EdgeKind::DeployedAs,
                metadata: EdgeMetadata {
                    confidence: Some(900),
                    ..EdgeMetadata::default()
                },
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("deploy edge");
        graph
            .insert_edge(&EdgeData {
                source: worker.id,
                target: worker_deployment.id,
                kind: EdgeKind::DeployedAs,
                metadata: EdgeMetadata {
                    confidence: Some(900),
                    ..EdgeMetadata::default()
                },
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("worker deploy edge");
        graph
            .insert_edge(&EdgeData {
                source: other_service.id,
                target: other_deployment.id,
                kind: EdgeKind::DeployedAs,
                metadata: EdgeMetadata {
                    confidence: Some(900),
                    ..EdgeMetadata::default()
                },
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("other deploy edge");
        graph
            .insert_edge(&EdgeData {
                source: service.id,
                target: env.id,
                kind: EdgeKind::ReadsEnv,
                metadata: EdgeMetadata {
                    confidence: Some(900),
                    ..EdgeMetadata::default()
                },
                owner_file: file.id,
                is_cross_file: false,
            })
            .expect("env edge");
        let report = deployment_topology(
            &graph,
            DeploymentTopologyQuery::WhereDeployed {
                service: "api".to_owned(),
            },
            Some("backend"),
            20,
        )
        .expect("where deployed");
        assert_eq!(report.deployments.len(), 1);
        assert_eq!(report.env_vars.len(), 0);
        assert_eq!(report.edges[0].kind, EdgeKind::DeployedAs);
        assert_eq!(
            report
                .services
                .iter()
                .map(|service| service.qualified_name.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("__service__backend__api")]
        );

        let report = deployment_topology(
            &graph,
            DeploymentTopologyQuery::ServiceEnv {
                service: "api".to_owned(),
            },
            Some("backend"),
            20,
        )
        .expect("service env");
        assert_eq!(report.env_vars.len(), 1);
        assert_eq!(report.edges[0].kind, EdgeKind::ReadsEnv);

        let report = deployment_topology(
            &graph,
            DeploymentTopologyQuery::WhereDeployed {
                service: "orphan".to_owned(),
            },
            Some("backend"),
            20,
        )
        .expect("orphan service");
        assert!(report.deployments.is_empty());
        assert!(
            report
                .missing_evidence
                .iter()
                .any(|message| message.contains("no indexed deployment edge"))
        );

        let report = deployment_topology(
            &graph,
            DeploymentTopologyQuery::EnvVarConsumers {
                env_var: "UNUSED_PORT".to_owned(),
            },
            Some("backend"),
            20,
        )
        .expect("unused env");
        assert!(report.services.is_empty());
        assert!(
            report
                .missing_evidence
                .iter()
                .any(|message| message.contains("no indexed service consumers"))
        );

        let report = deployment_topology(
            &graph,
            DeploymentTopologyQuery::WhereDeployed {
                service: "api".to_owned(),
            },
            None,
            1,
        )
        .expect("limited where deployed");
        assert_eq!(report.edges.len(), 1);

        drop(graph);
        let _ = std::fs::remove_file(graph_path);
    }
}
