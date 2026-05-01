#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use gather_step_core::{
    EdgeKind, NodeKind, broker_qn, config_map_qn, database_qn, deployment_qn, env_var_qn, secret_qn,
};
use serde::Deserialize;
use serde_yaml_ng::Value;
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentArtifactKind {
    Dockerfile,
    Compose,
    Kubernetes,
    Kustomize,
    Helm,
    GithubActions,
    EnvFile,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeploymentParseOutput {
    pub repo: String,
    pub path: String,
    pub artifact_kind: DeploymentArtifactKind,
    pub nodes: Vec<DeploymentNode>,
    pub edges: Vec<DeploymentEdge>,
    pub diagnostics: Vec<DeploymentDiagnostic>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeploymentNode {
    pub kind: NodeKind,
    pub name: String,
    pub qualified_name: String,
    pub confidence: u16,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeploymentEdge {
    pub source_kind: NodeKind,
    pub source_qualified_name: String,
    pub target_kind: NodeKind,
    pub target_qualified_name: String,
    pub kind: EdgeKind,
    pub confidence: u16,
    pub evidence: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeploymentDiagnostic {
    pub severity: DeploymentDiagnosticSeverity,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComposeEnvFileRef {
    pub service: String,
    pub path: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentDiagnosticSeverity {
    Info,
    Warning,
}

#[derive(Debug, Error)]
pub enum DeploymentParseError {
    #[error("failed to parse YAML deployment artifact `{path}`: {source}")]
    Yaml {
        path: String,
        #[source]
        source: serde_yaml_ng::Error,
    },
}

#[must_use]
pub fn detect_artifact_kind(path: &str, content: &str) -> DeploymentArtifactKind {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    let file_name = Path::new(&normalized)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");

    if normalized.contains("/.github/workflows/") || normalized.starts_with(".github/workflows/") {
        return DeploymentArtifactKind::GithubActions;
    }
    if file_name == "dockerfile" || file_name.starts_with("dockerfile.") {
        return DeploymentArtifactKind::Dockerfile;
    }
    if file_name == ".env" || file_name.starts_with(".env.") || file_name.ends_with(".env") {
        return DeploymentArtifactKind::EnvFile;
    }
    if file_name == "docker-compose.yml"
        || file_name == "docker-compose.yaml"
        || file_name == "compose.yml"
        || file_name == "compose.yaml"
    {
        return DeploymentArtifactKind::Compose;
    }
    if file_name == "kustomization.yaml" || file_name == "kustomization.yml" {
        return DeploymentArtifactKind::Kustomize;
    }
    if normalized.starts_with("charts/")
        || normalized.contains("/charts/")
        || file_name == "chart.yaml"
        || file_name == "values.yaml"
    {
        return DeploymentArtifactKind::Helm;
    }
    if matches!(
        file_name,
        "deployment.yaml"
            | "deployment.yml"
            | "statefulset.yaml"
            | "statefulset.yml"
            | "daemonset.yaml"
            | "daemonset.yml"
            | "service.yaml"
            | "service.yml"
            | "configmap.yaml"
            | "configmap.yml"
            | "secret.yaml"
            | "secret.yml"
    ) || content.contains("apiVersion:") && content.contains("kind:")
    {
        return DeploymentArtifactKind::Kubernetes;
    }

    DeploymentArtifactKind::Unknown
}

pub fn parse_deployment_artifact(
    repo: &str,
    path: &str,
    content: &str,
) -> Result<DeploymentParseOutput, DeploymentParseError> {
    let artifact_kind = detect_artifact_kind(path, content);
    parse_deployment_artifact_with_kind(repo, path, content, artifact_kind)
}

pub fn parse_deployment_artifact_with_kind(
    repo: &str,
    path: &str,
    content: &str,
    artifact_kind: DeploymentArtifactKind,
) -> Result<DeploymentParseOutput, DeploymentParseError> {
    let mut builder = OutputBuilder::new(repo, path, artifact_kind);

    match artifact_kind {
        DeploymentArtifactKind::Dockerfile => parse_dockerfile(&mut builder, content),
        DeploymentArtifactKind::Compose => parse_compose(&mut builder, content, 900)?,
        DeploymentArtifactKind::Kubernetes => parse_kubernetes(&mut builder, content, 900)?,
        DeploymentArtifactKind::Kustomize => parse_kustomize(&mut builder, content)?,
        DeploymentArtifactKind::Helm => parse_helm(&mut builder, content),
        DeploymentArtifactKind::GithubActions => parse_github_actions(&mut builder, content)?,
        DeploymentArtifactKind::EnvFile => parse_env_file(&mut builder, content),
        DeploymentArtifactKind::Unknown => builder.info("unsupported deployment artifact family"),
    }

    Ok(builder.finish())
}

struct OutputBuilder<'a> {
    repo: &'a str,
    path: &'a str,
    artifact_kind: DeploymentArtifactKind,
    nodes: BTreeMap<(NodeKind, String), DeploymentNode>,
    edges: BTreeMap<(NodeKind, String, NodeKind, String, EdgeKind), DeploymentEdge>,
    diagnostics: Vec<DeploymentDiagnostic>,
}

impl<'a> OutputBuilder<'a> {
    fn new(repo: &'a str, path: &'a str, artifact_kind: DeploymentArtifactKind) -> Self {
        Self {
            repo,
            path,
            artifact_kind,
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn service(&mut self, name: &str, confidence: u16, evidence: impl Into<String>) -> String {
        let qualified_name = service_qn(self.repo, name);
        self.node(
            NodeKind::Service,
            name,
            qualified_name.clone(),
            confidence,
            evidence,
        );
        qualified_name
    }

    fn deployment(&mut self, name: &str, confidence: u16, evidence: impl Into<String>) -> String {
        let qualified_name = deployment_qn(self.repo, name);
        self.node(
            NodeKind::Deployment,
            name,
            qualified_name.clone(),
            confidence,
            evidence,
        );
        qualified_name
    }

    fn node(
        &mut self,
        kind: NodeKind,
        name: &str,
        qualified_name: String,
        confidence: u16,
        evidence: impl Into<String>,
    ) {
        let key = (kind, qualified_name.clone());
        self.nodes.entry(key).or_insert_with(|| DeploymentNode {
            kind,
            name: name.to_owned(),
            qualified_name,
            confidence,
            evidence: evidence.into(),
        });
    }

    fn edge(
        &mut self,
        source: NodeRef,
        target: NodeRef,
        kind: EdgeKind,
        confidence: u16,
        evidence: impl Into<String>,
    ) {
        let key = (
            source.kind,
            source.qualified_name.clone(),
            target.kind,
            target.qualified_name.clone(),
            kind,
        );
        self.edges.entry(key).or_insert_with(|| DeploymentEdge {
            source_kind: source.kind,
            source_qualified_name: source.qualified_name,
            target_kind: target.kind,
            target_qualified_name: target.qualified_name,
            kind,
            confidence,
            evidence: evidence.into(),
        });
    }

    fn info(&mut self, message: impl Into<String>) {
        self.diagnostics.push(DeploymentDiagnostic {
            severity: DeploymentDiagnosticSeverity::Info,
            message: message.into(),
        });
    }

    fn warning(&mut self, message: impl Into<String>) {
        self.diagnostics.push(DeploymentDiagnostic {
            severity: DeploymentDiagnosticSeverity::Warning,
            message: message.into(),
        });
    }

    fn finish(self) -> DeploymentParseOutput {
        DeploymentParseOutput {
            repo: self.repo.to_owned(),
            path: self.path.to_owned(),
            artifact_kind: self.artifact_kind,
            nodes: self.nodes.into_values().collect(),
            edges: self.edges.into_values().collect(),
            diagnostics: self.diagnostics,
        }
    }
}

#[derive(Clone, Debug)]
struct NodeRef {
    kind: NodeKind,
    qualified_name: String,
}

impl NodeRef {
    fn new(kind: NodeKind, qualified_name: String) -> Self {
        Self {
            kind,
            qualified_name,
        }
    }
}

fn parse_dockerfile(builder: &mut OutputBuilder<'_>, content: &str) {
    let name = deployment_name_from_path(builder.repo, builder.path);
    let service_qn = builder.service(&name, 800, "dockerfile service");
    let deployment_qn = builder.deployment(&name, 850, "dockerfile deployment");
    builder.edge(
        NodeRef::new(NodeKind::Service, service_qn.clone()),
        NodeRef::new(NodeKind::Deployment, deployment_qn),
        EdgeKind::DeployedAs,
        850,
        "Dockerfile",
    );

    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("ENV ") {
            for name in docker_env_names(rest) {
                add_env_edge(builder, &service_qn, &name, 850, "Dockerfile ENV");
            }
        }
    }
}

fn parse_compose(
    builder: &mut OutputBuilder<'_>,
    content: &str,
    confidence: u16,
) -> Result<(), DeploymentParseError> {
    let root = parse_yaml(content, builder.path)?;
    let Some(services) = mapping_get(&root, "services").and_then(Value::as_mapping) else {
        builder.warning("compose file has no services mapping");
        return Ok(());
    };

    for (service_key, service_value) in services {
        let Some(service_name) = service_key.as_str() else {
            continue;
        };
        let service_qn = builder.service(service_name, confidence, "compose service");
        let deployment_qn = builder.deployment(service_name, confidence, "compose service");
        builder.edge(
            NodeRef::new(NodeKind::Service, service_qn.clone()),
            NodeRef::new(NodeKind::Deployment, deployment_qn),
            EdgeKind::DeployedAs,
            confidence,
            "compose service",
        );

        let image = mapping_get(service_value, "image").and_then(Value::as_str);
        add_infra_from_image(
            builder,
            &service_qn,
            service_name,
            image,
            confidence,
            "compose image",
        );
        collect_compose_env(service_value)
            .iter()
            .for_each(|name| add_env_edge(builder, &service_qn, name, confidence, "compose env"));

        for dependency in compose_depends_on(service_value) {
            let dependency_qn = builder.service(&dependency, confidence, "compose dependency");
            builder.edge(
                NodeRef::new(NodeKind::Service, service_qn.clone()),
                NodeRef::new(NodeKind::Service, dependency_qn),
                EdgeKind::BackedBy,
                confidence,
                "compose depends_on",
            );
        }
    }

    Ok(())
}

fn parse_kubernetes(
    builder: &mut OutputBuilder<'_>,
    content: &str,
    confidence: u16,
) -> Result<(), DeploymentParseError> {
    for document in serde_yaml_ng::Deserializer::from_str(content) {
        let value = Value::deserialize(document).map_err(|source| DeploymentParseError::Yaml {
            path: builder.path.to_owned(),
            source,
        })?;
        parse_kubernetes_document(builder, &value, confidence);
    }
    Ok(())
}

fn parse_helm(builder: &mut OutputBuilder<'_>, content: &str) {
    builder.info("helm parsing uses offline heuristics; template control flow is not rendered");
    if parse_kubernetes(builder, content, 650).is_err() {
        let name = deployment_name_from_path(builder.repo, builder.path);
        let service_qn = builder.service(&name, 600, "helm chart heuristic");
        let deployment_qn = builder.deployment(&name, 600, "helm chart heuristic");
        builder.edge(
            NodeRef::new(NodeKind::Service, service_qn),
            NodeRef::new(NodeKind::Deployment, deployment_qn),
            EdgeKind::DeployedAs,
            600,
            "helm chart heuristic",
        );
        builder.info("helm template YAML could not be parsed; env names were not inferred");
    }
}

fn parse_kustomize(
    builder: &mut OutputBuilder<'_>,
    content: &str,
) -> Result<(), DeploymentParseError> {
    let root = parse_yaml(content, builder.path)?;
    let kind = mapping_get(&root, "kind")
        .and_then(Value::as_str)
        .unwrap_or("Kustomization");
    if !kind.is_empty() && kind != "Kustomization" {
        builder.info(format!("unsupported kustomize kind `{kind}`"));
    }

    let name = kustomize_name_from_path(builder.repo, builder.path);
    let service_qn = builder.service(&name, 750, "kustomize application");
    let deployment_qn = builder.deployment(&name, 750, "kustomize application");
    builder.edge(
        NodeRef::new(NodeKind::Service, service_qn.clone()),
        NodeRef::new(NodeKind::Deployment, deployment_qn),
        EdgeKind::DeployedAs,
        750,
        "kustomize application",
    );

    for image in kustomize_images(&root) {
        add_infra_from_image(
            builder,
            &service_qn,
            &name,
            Some(&image),
            700,
            "kustomize image",
        );
    }

    for generator in kustomize_generators(&root, "configMapGenerator") {
        let qn = config_map_qn(&generator.name);
        builder.node(
            NodeKind::ConfigMap,
            &generator.name,
            qn.clone(),
            750,
            "kustomize configMapGenerator",
        );
        builder.edge(
            NodeRef::new(NodeKind::Service, service_qn.clone()),
            NodeRef::new(NodeKind::ConfigMap, qn),
            EdgeKind::BackedBy,
            750,
            "kustomize configMapGenerator",
        );
        for literal in generator.literals {
            add_env_edge(builder, &service_qn, &literal, 700, "kustomize literal");
        }
    }

    for generator in kustomize_generators(&root, "secretGenerator") {
        let qn = secret_qn(&generator.name);
        builder.node(
            NodeKind::Secret,
            &generator.name,
            qn.clone(),
            750,
            "kustomize secretGenerator",
        );
        builder.edge(
            NodeRef::new(NodeKind::Service, service_qn.clone()),
            NodeRef::new(NodeKind::Secret, qn),
            EdgeKind::BackedBy,
            750,
            "kustomize secretGenerator",
        );
    }

    Ok(())
}

fn parse_kubernetes_document(builder: &mut OutputBuilder<'_>, value: &Value, confidence: u16) {
    let kind = mapping_get(value, "kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let name = mapping_get(value, "metadata")
        .and_then(|metadata| mapping_get(metadata, "name"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    match kind {
        "Deployment" | "StatefulSet" | "DaemonSet" => {
            let service_qn = builder.service(name, confidence, "kubernetes workload");
            let deployment_qn = builder.deployment(name, confidence, "kubernetes workload");
            builder.edge(
                NodeRef::new(NodeKind::Service, service_qn.clone()),
                NodeRef::new(NodeKind::Deployment, deployment_qn),
                EdgeKind::DeployedAs,
                confidence,
                kind,
            );

            for env_name in kubernetes_env_names(value) {
                add_env_edge(
                    builder,
                    &service_qn,
                    &env_name,
                    confidence,
                    "kubernetes env",
                );
            }
            for config_name in kubernetes_env_from_refs(value, "configMapRef") {
                let qn = config_map_qn(&config_name);
                builder.node(
                    NodeKind::ConfigMap,
                    &config_name,
                    qn.clone(),
                    confidence,
                    "kubernetes envFrom configMapRef",
                );
                builder.edge(
                    NodeRef::new(NodeKind::Service, service_qn.clone()),
                    NodeRef::new(NodeKind::ConfigMap, qn),
                    EdgeKind::BackedBy,
                    confidence,
                    "kubernetes envFrom configMapRef",
                );
            }
            for secret_name in kubernetes_env_from_refs(value, "secretRef") {
                let qn = secret_qn(&secret_name);
                builder.node(
                    NodeKind::Secret,
                    &secret_name,
                    qn.clone(),
                    confidence,
                    "kubernetes envFrom secretRef",
                );
                builder.edge(
                    NodeRef::new(NodeKind::Service, service_qn.clone()),
                    NodeRef::new(NodeKind::Secret, qn),
                    EdgeKind::BackedBy,
                    confidence,
                    "kubernetes envFrom secretRef",
                );
            }
        }
        "ConfigMap" => {
            let qn = config_map_qn(name);
            builder.node(
                NodeKind::ConfigMap,
                name,
                qn,
                confidence,
                "kubernetes ConfigMap",
            );
        }
        "Secret" => {
            let qn = secret_qn(name);
            builder.node(NodeKind::Secret, name, qn, confidence, "kubernetes Secret");
        }
        "Service" => {
            builder.service(name, confidence, "kubernetes Service");
        }
        _ if !kind.is_empty() => builder.info(format!("unsupported kubernetes kind `{kind}`")),
        _ => {}
    }
}

fn parse_github_actions(
    builder: &mut OutputBuilder<'_>,
    content: &str,
) -> Result<(), DeploymentParseError> {
    let root = parse_yaml(content, builder.path)?;
    let Some(jobs) = mapping_get(&root, "jobs").and_then(Value::as_mapping) else {
        builder.warning("workflow has no jobs mapping");
        return Ok(());
    };

    for (job_key, job_value) in jobs {
        let Some(job_name) = job_key.as_str() else {
            continue;
        };
        let job_qn = workflow_job_qn(builder.repo, builder.path, job_name);
        builder.node(
            NodeKind::WorkflowJob,
            job_name,
            job_qn.clone(),
            850,
            "github actions job",
        );

        if workflow_job_is_deployish(job_value) {
            let deployment_name = format!(
                "{}:{job_name}",
                deployment_name_from_path(builder.repo, builder.path)
            );
            let deployment_qn =
                builder.deployment(&deployment_name, 750, "github actions deployment job");
            builder.edge(
                NodeRef::new(NodeKind::Deployment, deployment_qn),
                NodeRef::new(NodeKind::WorkflowJob, job_qn.clone()),
                EdgeKind::BuiltBy,
                750,
                "github actions deploy/build step",
            );
        }

        for needed_job in workflow_needs(job_value) {
            let needed_qn = workflow_job_qn(builder.repo, builder.path, &needed_job);
            builder.node(
                NodeKind::WorkflowJob,
                &needed_job,
                needed_qn.clone(),
                800,
                "github actions needs",
            );
            builder.edge(
                NodeRef::new(NodeKind::WorkflowJob, needed_qn),
                NodeRef::new(NodeKind::WorkflowJob, job_qn.clone()),
                EdgeKind::Triggers,
                800,
                "github actions needs",
            );
        }
    }

    Ok(())
}

fn parse_env_file(builder: &mut OutputBuilder<'_>, content: &str) {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("export ") {
            continue;
        }
        let Some((name, _value)) = trimmed.split_once('=') else {
            continue;
        };
        let name = name.trim();
        if is_env_name(name) {
            let qn = env_var_qn(name);
            builder.node(NodeKind::EnvVar, name, qn, 900, "env file name");
        }
    }
}

fn add_env_edge(
    builder: &mut OutputBuilder<'_>,
    service_qn: &str,
    env_name: &str,
    confidence: u16,
    evidence: &'static str,
) {
    if !is_env_name(env_name) {
        return;
    }
    let qn = env_var_qn(env_name);
    builder.node(NodeKind::EnvVar, env_name, qn.clone(), confidence, evidence);
    builder.edge(
        NodeRef::new(NodeKind::Service, service_qn.to_owned()),
        NodeRef::new(NodeKind::EnvVar, qn),
        EdgeKind::ReadsEnv,
        confidence,
        evidence,
    );
}

fn add_infra_from_image(
    builder: &mut OutputBuilder<'_>,
    service_qn: &str,
    service_name: &str,
    image: Option<&str>,
    confidence: u16,
    evidence: &'static str,
) {
    let haystack = format!(
        "{} {}",
        service_name.to_ascii_lowercase(),
        image.unwrap_or_default().to_ascii_lowercase()
    );
    let infra = if haystack.contains("postgres") || haystack.contains("mysql") {
        Some((NodeKind::Database, EdgeKind::UsesDatabase, "sql"))
    } else if haystack.contains("mongo") {
        Some((NodeKind::Database, EdgeKind::UsesDatabase, "mongo"))
    } else if haystack.contains("redis") {
        Some((NodeKind::Broker, EdgeKind::UsesBroker, "redis"))
    } else if haystack.contains("kafka") {
        Some((NodeKind::Broker, EdgeKind::UsesBroker, "kafka"))
    } else if haystack.contains("rabbit") {
        Some((NodeKind::Broker, EdgeKind::UsesBroker, "rabbitmq"))
    } else {
        None
    };

    let Some((kind, edge_kind, family)) = infra else {
        return;
    };
    let qn = match kind {
        NodeKind::Database => database_qn(family, service_name),
        NodeKind::Broker => broker_qn(family, service_name),
        _ => return,
    };
    builder.node(kind, service_name, qn.clone(), confidence, evidence);
    builder.edge(
        NodeRef::new(NodeKind::Service, service_qn.to_owned()),
        NodeRef::new(kind, qn),
        edge_kind,
        confidence,
        evidence,
    );
}

fn collect_compose_env(service_value: &Value) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let Some(environment) = mapping_get(service_value, "environment") else {
        return names;
    };
    match environment {
        Value::Mapping(map) => {
            for key in map.keys().filter_map(Value::as_str) {
                if is_env_name(key) {
                    names.insert(key.to_owned());
                }
            }
        }
        Value::Sequence(sequence) => {
            for item in sequence.iter().filter_map(Value::as_str) {
                let name = item.split_once('=').map_or(item, |(name, _)| name).trim();
                if is_env_name(name) {
                    names.insert(name.to_owned());
                }
            }
        }
        _ => {}
    }
    names
}

pub fn compose_env_file_refs(
    content: &str,
    path: &str,
) -> Result<Vec<ComposeEnvFileRef>, DeploymentParseError> {
    let root = parse_yaml(content, path)?;
    let Some(services) = mapping_get(&root, "services").and_then(Value::as_mapping) else {
        return Ok(Vec::new());
    };

    let mut refs = Vec::new();
    for (service_key, service_value) in services {
        let Some(service_name) = service_key.as_str() else {
            continue;
        };
        for path in collect_compose_env_files(service_value) {
            refs.push(ComposeEnvFileRef {
                service: service_name.to_owned(),
                path,
            });
        }
    }
    Ok(refs)
}

fn collect_compose_env_files(service_value: &Value) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    let Some(env_file) = mapping_get(service_value, "env_file") else {
        return paths;
    };
    match env_file {
        Value::String(path) => {
            insert_env_file_path(&mut paths, path);
        }
        Value::Sequence(sequence) => {
            for item in sequence {
                match item {
                    Value::String(path) => insert_env_file_path(&mut paths, path),
                    Value::Mapping(_) => {
                        if let Some(path) = mapping_get(item, "path").and_then(Value::as_str) {
                            insert_env_file_path(&mut paths, path);
                        }
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    paths
}

fn insert_env_file_path(paths: &mut BTreeSet<String>, path: &str) {
    let path = path.trim();
    if !path.is_empty() {
        paths.insert(path.to_owned());
    }
}

fn compose_depends_on(service_value: &Value) -> BTreeSet<String> {
    let mut dependencies = BTreeSet::new();
    let Some(depends_on) = mapping_get(service_value, "depends_on") else {
        return dependencies;
    };
    match depends_on {
        Value::Sequence(sequence) => {
            for dependency in sequence.iter().filter_map(Value::as_str) {
                dependencies.insert(dependency.to_owned());
            }
        }
        Value::Mapping(map) => {
            for dependency in map.keys().filter_map(Value::as_str) {
                dependencies.insert(dependency.to_owned());
            }
        }
        _ => {}
    }
    dependencies
}

fn kubernetes_env_names(value: &Value) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    visit_mappings(value, &mut |mapping| {
        let Some(env) = mapping
            .get(Value::String("env".to_owned()))
            .and_then(Value::as_sequence)
        else {
            return;
        };
        for item in env {
            if let Some(name) = mapping_get(item, "name").and_then(Value::as_str)
                && is_env_name(name)
            {
                names.insert(name.to_owned());
            }
        }
    });
    names
}

fn kubernetes_env_from_refs(value: &Value, ref_key: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    visit_mappings(value, &mut |mapping| {
        let Some(env_from) = mapping
            .get(Value::String("envFrom".to_owned()))
            .and_then(Value::as_sequence)
        else {
            return;
        };
        for item in env_from {
            if let Some(name) = mapping_get(item, ref_key)
                .and_then(|reference| mapping_get(reference, "name"))
                .and_then(Value::as_str)
            {
                names.insert(name.to_owned());
            }
        }
    });
    names
}

fn visit_mappings<F>(value: &Value, visitor: &mut F)
where
    F: FnMut(&serde_yaml_ng::Mapping),
{
    match value {
        Value::Mapping(mapping) => {
            visitor(mapping);
            for value in mapping.values() {
                visit_mappings(value, visitor);
            }
        }
        Value::Sequence(sequence) => {
            for value in sequence {
                visit_mappings(value, visitor);
            }
        }
        _ => {}
    }
}

fn docker_env_names(rest: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let parts = rest.split_whitespace().collect::<Vec<_>>();
    if parts.len() == 1 {
        if let Some((name, _)) = parts[0].split_once('=')
            && is_env_name(name)
        {
            names.insert(name.to_owned());
        } else if is_env_name(parts[0]) {
            names.insert(parts[0].to_owned());
        }
        return names;
    }
    let mut iter = parts.iter();
    while let Some(name) = iter.next() {
        if name.contains('=') {
            if let Some((name, _)) = name.split_once('=')
                && is_env_name(name)
            {
                names.insert(name.to_owned());
            }
        } else if is_env_name(name) {
            names.insert((*name).to_owned());
            let _ = iter.next();
        }
    }
    names
}

fn workflow_job_is_deployish(value: &Value) -> bool {
    let text = serde_json::to_string(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    [
        "deploy",
        "kubectl",
        "helm",
        "docker build",
        "docker/build-push-action",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn workflow_needs(value: &Value) -> BTreeSet<String> {
    let mut needs = BTreeSet::new();
    let Some(raw) = mapping_get(value, "needs") else {
        return needs;
    };
    match raw {
        Value::String(name) => {
            needs.insert(name.clone());
        }
        Value::Sequence(sequence) => {
            for name in sequence.iter().filter_map(Value::as_str) {
                needs.insert(name.to_owned());
            }
        }
        _ => {}
    }
    needs
}

fn parse_yaml(content: &str, path: &str) -> Result<Value, DeploymentParseError> {
    serde_yaml_ng::from_str(content).map_err(|source| DeploymentParseError::Yaml {
        path: path.to_owned(),
        source,
    })
}

fn mapping_get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_mapping()?.get(Value::String(key.to_owned()))
}

struct KustomizeGenerator {
    name: String,
    literals: BTreeSet<String>,
}

fn kustomize_images(value: &Value) -> BTreeSet<String> {
    let mut images = BTreeSet::new();
    let Some(sequence) = mapping_get(value, "images").and_then(Value::as_sequence) else {
        return images;
    };
    for item in sequence {
        match item {
            Value::String(image) => {
                if !image.trim().is_empty() {
                    images.insert(image.trim().to_owned());
                }
            }
            Value::Mapping(_) => {
                let Some(name) = mapping_get(item, "newName")
                    .or_else(|| mapping_get(item, "name"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let image = mapping_get(item, "newTag")
                    .and_then(Value::as_str)
                    .map_or_else(|| name.to_owned(), |tag| format!("{name}:{tag}"));
                images.insert(image);
            }
            _ => {}
        }
    }
    images
}

fn kustomize_generators(value: &Value, key: &str) -> Vec<KustomizeGenerator> {
    let Some(sequence) = mapping_get(value, key).and_then(Value::as_sequence) else {
        return Vec::new();
    };
    sequence
        .iter()
        .filter_map(|item| {
            let name = mapping_get(item, "name")
                .and_then(Value::as_str)?
                .trim()
                .to_owned();
            if name.is_empty() {
                return None;
            }
            let literals = mapping_get(item, "literals")
                .and_then(Value::as_sequence)
                .map(|sequence| {
                    sequence
                        .iter()
                        .filter_map(Value::as_str)
                        .filter_map(|literal| literal.split_once('=').map(|(name, _)| name.trim()))
                        .filter(|name| is_env_name(name))
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            Some(KustomizeGenerator { name, literals })
        })
        .collect()
}

fn deployment_name_from_path(repo: &str, path: &str) -> String {
    let normalized = path.replace('\\', "/");
    let mut parts = normalized.split('/').filter(|part| !part.is_empty());
    parts
        .next_back()
        .and_then(|file| {
            if file.eq_ignore_ascii_case("Dockerfile")
                || file.eq_ignore_ascii_case("docker-compose.yml")
                || file.eq_ignore_ascii_case("docker-compose.yaml")
                || file.eq_ignore_ascii_case("compose.yml")
                || file.eq_ignore_ascii_case("compose.yaml")
                || file.eq_ignore_ascii_case("kustomization.yaml")
                || file.eq_ignore_ascii_case("kustomization.yml")
            {
                parts.next_back()
            } else {
                Some(file)
            }
        })
        .filter(|part| !part.starts_with('.'))
        .unwrap_or(repo)
        .to_owned()
}

fn kustomize_name_from_path(repo: &str, path: &str) -> String {
    let normalized = path.replace('\\', "/");
    let mut parts = normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.last().is_some_and(|file| {
        file.eq_ignore_ascii_case("kustomization.yaml")
            || file.eq_ignore_ascii_case("kustomization.yml")
    }) {
        parts.pop();
    }
    for marker in ["overlays", "overlay", "base", "bases"] {
        if let Some(index) = parts
            .iter()
            .position(|part| part.eq_ignore_ascii_case(marker))
            && index > 0
        {
            return parts[index - 1].to_owned();
        }
    }
    parts.last().copied().unwrap_or(repo).to_owned()
}

fn workflow_job_qn(repo: &str, path: &str, job: &str) -> String {
    format!(
        "__workflow_job__{}__{}__{}",
        canonical_part(repo, "repo"),
        canonical_part(path, "workflow"),
        canonical_part(job, "job")
    )
}

pub fn deployment_service_qn(repo: &str, name: &str) -> String {
    format!(
        "__service__{}__{}",
        canonical_part(repo, "repo"),
        canonical_part(name, "service")
    )
}

fn service_qn(repo: &str, name: &str) -> String {
    deployment_service_qn(repo, name)
}

fn canonical_part(value: &str, fallback: &str) -> String {
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
    let normalized = normalized.trim_matches('_').replace("__", "_");
    if normalized.is_empty() {
        fallback.to_owned()
    } else {
        normalized
    }
}

fn is_env_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use gather_step_core::{EdgeKind, NodeKind};
    use pretty_assertions::assert_eq;

    use super::{
        DeploymentArtifactKind, compose_env_file_refs, detect_artifact_kind,
        parse_deployment_artifact,
    };

    #[test]
    fn dockerfile_parser_extracts_env_names_without_values() {
        let output = parse_deployment_artifact(
            "backend",
            "services/api/Dockerfile",
            "FROM node:22\nENV DATABASE_URL=postgres://secret\nENV API_TOKEN value\nENV FEATURE_FLAG\n",
        )
        .expect("dockerfile should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::Dockerfile);
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::EnvVar && node.qualified_name == "__env_var__database_url"
        }));
        assert!(
            output.nodes.iter().any(|node| node.kind == NodeKind::EnvVar
                && node.qualified_name == "__env_var__api_token")
        );
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::EnvVar && node.qualified_name == "__env_var__feature_flag"
        }));
        let serialized = serde_json::to_string(&output).expect("serialize output");
        assert!(!serialized.contains("postgres://secret"));
    }

    #[test]
    fn compose_parser_links_services_env_and_shared_infra() {
        let output = parse_deployment_artifact(
            "platform",
            "compose.yaml",
            r#"
services:
  api:
    image: platform-api
    environment:
      DATABASE_URL: postgres://secret
      REDIS_URL: redis://redis:6379
    depends_on:
      - postgres
      - redis
  postgres:
    image: postgres:16
  redis:
    image: redis:7
"#,
        )
        .expect("compose should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::Compose);
        assert!(output.edges.iter().any(|edge| {
            edge.kind == EdgeKind::ReadsEnv
                && edge.target_qualified_name == "__env_var__database_url"
        }));
        assert!(
            output
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::UsesDatabase)
        );
        assert!(
            output
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::UsesBroker)
        );
        let serialized = serde_json::to_string(&output).expect("serialize output");
        assert!(!serialized.contains("postgres://secret"));
    }

    #[test]
    fn compose_env_file_refs_are_structural_and_service_scoped() {
        let refs = compose_env_file_refs(
            r#"
services:
  api:
    env_file:
      - .env
      - path: config/api.env
        required: false
  worker:
    env_file: worker.env
"#,
            "compose.yaml",
        )
        .expect("compose should parse");

        assert_eq!(refs.len(), 3);
        assert!(
            refs.iter()
                .any(|item| item.service == "api" && item.path == ".env")
        );
        assert!(
            refs.iter()
                .any(|item| item.service == "api" && item.path == "config/api.env")
        );
        assert!(
            refs.iter()
                .any(|item| item.service == "worker" && item.path == "worker.env")
        );
    }

    #[test]
    fn kubernetes_parser_handles_multi_doc_workload_configmap_secret() {
        let output = parse_deployment_artifact(
            "backend",
            "deploy/api/deployment.yaml",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  template:
    spec:
      containers:
        - name: api
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: db-secret
                  key: url
          envFrom:
            - configMapRef:
                name: api-config
            - secretRef:
                name: api-secret
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: api-config
---
apiVersion: v1
kind: Secret
metadata:
  name: api-secret
"#,
        )
        .expect("kubernetes should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::Kubernetes);
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::Deployment && node.qualified_name == "__deployment__backend__api"
        }));
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::ConfigMap && node.qualified_name == "__config_map__api-config"
        }));
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::Secret && node.qualified_name == "__secret__api-secret"
        }));
    }

    #[test]
    fn kustomize_parser_handles_application_and_generators_without_values() {
        let output = parse_deployment_artifact(
            "platform-gitops",
            "kustomize/apps/api/overlays/prod/kustomization.yaml",
            r#"
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
images:
  - name: postgres
    newName: postgres
    newTag: "16"
configMapGenerator:
  - name: api-config
    literals:
      - DATABASE_URL=postgres://secret
secretGenerator:
  - name: api-secret
    literals:
      - API_TOKEN=secret
"#,
        )
        .expect("kustomization should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::Kustomize);
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::Deployment
                && node.qualified_name == "__deployment__platform-gitops__api"
        }));
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::ConfigMap && node.qualified_name == "__config_map__api-config"
        }));
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::Secret && node.qualified_name == "__secret__api-secret"
        }));
        assert!(output.edges.iter().any(|edge| {
            edge.kind == EdgeKind::ReadsEnv
                && edge.target_qualified_name == "__env_var__database_url"
        }));
        let serialized = serde_json::to_string(&output).expect("serialize output");
        assert!(!serialized.contains("postgres://secret"));
        assert!(!serialized.contains("API_TOKEN=secret"));
    }

    #[test]
    fn helm_template_parser_downgrades_confidence() {
        let output = parse_deployment_artifact(
            "backend",
            "charts/api/templates/deployment.yaml",
            r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "api.fullname" . }}
spec:
  template:
    spec:
      containers:
        - name: api
          env:
            - name: DATABASE_URL
              value: {{ .Values.databaseUrl }}
"#,
        )
        .expect("helm template should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::Helm);
        assert!(output.nodes.iter().any(|node| node.confidence <= 650));
        assert!(!output.diagnostics.is_empty());
        assert!(
            !output
                .nodes
                .iter()
                .any(|node| node.qualified_name == "__env_var__api")
        );
    }

    #[test]
    fn helm_template_fallback_does_not_treat_every_yaml_name_as_env() {
        let output = parse_deployment_artifact(
            "backend",
            "charts/api/templates/service.yaml",
            r#"
{{ if .Values.enabled }}
name: nginx
{{ end }}
"#,
        )
        .expect("helm template fallback should not fail parsing");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::Helm);
        assert!(
            !output
                .nodes
                .iter()
                .any(|node| node.qualified_name == "__env_var__nginx")
        );
    }

    #[test]
    fn github_actions_parser_extracts_jobs_and_needs_edges() {
        let output = parse_deployment_artifact(
            "backend",
            ".github/workflows/deploy.yml",
            r#"
name: Deploy
on:
  push:
jobs:
  build:
    steps:
      - run: docker build .
  deploy:
    needs: build
    steps:
      - run: helm upgrade api ./charts/api
"#,
        )
        .expect("workflow should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::GithubActions);
        assert!(
            output
                .nodes
                .iter()
                .any(|node| node.kind == NodeKind::WorkflowJob && node.name == "deploy")
        );
        assert!(
            output
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::Triggers)
        );
        assert!(
            output
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::BuiltBy)
        );
    }

    #[test]
    fn env_file_parser_records_names_only() {
        let output = parse_deployment_artifact(
            "backend",
            ".env.production",
            "DATABASE_URL=postgres://secret\n# ignored\nexport BAD=ignored\nAPI_TOKEN=value\n",
        )
        .expect("env file should parse");

        assert_eq!(output.artifact_kind, DeploymentArtifactKind::EnvFile);
        assert!(output.nodes.iter().any(|node| {
            node.kind == NodeKind::EnvVar && node.qualified_name == "__env_var__database_url"
        }));
        assert!(
            !output
                .nodes
                .iter()
                .any(|node| node.qualified_name == "__env_var__bad")
        );
        let serialized = serde_json::to_string(&output).expect("serialize output");
        assert!(!serialized.contains("postgres://secret"));
    }

    #[test]
    fn artifact_detection_is_path_scoped() {
        assert_eq!(
            detect_artifact_kind(".github/workflows/ci.yml", ""),
            DeploymentArtifactKind::GithubActions
        );
        assert_eq!(
            detect_artifact_kind("src/app.yaml", "name: not deploy"),
            DeploymentArtifactKind::Unknown
        );
        assert_eq!(
            detect_artifact_kind(
                "platform/templates/deployment.yaml",
                "apiVersion: apps/v1\nkind: Deployment\n"
            ),
            DeploymentArtifactKind::Kubernetes
        );
        assert_eq!(
            detect_artifact_kind("kustomize/apps/api/kustomization.yaml", ""),
            DeploymentArtifactKind::Kustomize
        );
        assert_eq!(
            detect_artifact_kind("platform/templates/name.yaml", "name: nginx"),
            DeploymentArtifactKind::Unknown
        );
    }
}
