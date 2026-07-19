use std::path::Path;

use gather_step_core::{
    EdgeData, EdgeKind, EdgeMetadata, NodeData, NodeId, NodeKind, virtual_node,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestDependency {
    pub package: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedPackageManifest {
    pub package_name: Option<String>,
    pub dependencies: Vec<ManifestDependency>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestExtraction {
    pub nodes: Vec<NodeData>,
    pub edges: Vec<EdgeData>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionMismatch {
    pub package: String,
    pub versions: Vec<(String, String)>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("failed to parse package manifest: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to parse Python project manifest: {0}")]
    Toml(#[from] toml::de::Error),
}

pub fn parse_package_manifest_str(raw: &str) -> Result<ParsedPackageManifest, ManifestError> {
    let manifest = serde_json::from_str::<serde_json::Value>(raw)?;
    let package_name = manifest
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);

    let mut dependencies = Vec::new();
    for section in ["dependencies", "devDependencies", "peerDependencies"] {
        if let Some(entries) = manifest.get(section).and_then(serde_json::Value::as_object) {
            for (package, version) in entries {
                let Some(version) = version.as_str() else {
                    continue;
                };
                dependencies.push(ManifestDependency {
                    package: package.clone(),
                    version: version.to_owned(),
                });
            }
        }
    }
    dependencies.sort_by(|left, right| {
        left.package
            .cmp(&right.package)
            .then(left.version.cmp(&right.version))
    });
    dependencies.dedup();

    Ok(ParsedPackageManifest {
        package_name,
        dependencies,
    })
}

pub fn parse_python_project_manifest_str(
    raw: &str,
) -> Result<ParsedPackageManifest, ManifestError> {
    let manifest = toml::from_str::<toml::Value>(raw)?;
    let project = manifest.get("project").and_then(toml::Value::as_table);
    let poetry = manifest
        .get("tool")
        .and_then(|tool| tool.get("poetry"))
        .and_then(toml::Value::as_table);
    let package_name = project
        .and_then(|project| project.get("name"))
        .and_then(toml::Value::as_str)
        .or_else(|| {
            poetry
                .and_then(|poetry| poetry.get("name"))
                .and_then(toml::Value::as_str)
        })
        .map(ToOwned::to_owned);

    let mut dependencies = Vec::new();
    if let Some(entries) = project
        .and_then(|project| project.get("dependencies"))
        .and_then(toml::Value::as_array)
    {
        dependencies.extend(
            entries
                .iter()
                .filter_map(toml::Value::as_str)
                .filter_map(parse_python_dependency),
        );
    }
    if let Some(groups) = project
        .and_then(|project| project.get("optional-dependencies"))
        .and_then(toml::Value::as_table)
    {
        for entries in groups.values().filter_map(toml::Value::as_array) {
            dependencies.extend(
                entries
                    .iter()
                    .filter_map(toml::Value::as_str)
                    .filter_map(parse_python_dependency),
            );
        }
    }
    if let Some(entries) = poetry
        .and_then(|poetry| poetry.get("dependencies"))
        .and_then(toml::Value::as_table)
    {
        for (package, constraint) in entries {
            if package.eq_ignore_ascii_case("python") {
                continue;
            }
            let version = constraint
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| {
                    constraint
                        .get("version")
                        .and_then(toml::Value::as_str)
                        .map(ToOwned::to_owned)
                })
                .unwrap_or_else(|| "*".to_owned());
            dependencies.push(ManifestDependency {
                package: normalize_python_package_name(package),
                version: sanitize_dependency_locator(&version),
            });
        }
    }
    sort_dependencies(&mut dependencies);
    Ok(ParsedPackageManifest {
        package_name,
        dependencies,
    })
}

#[must_use]
pub fn parse_requirements_manifest_str(raw: &str) -> ParsedPackageManifest {
    let mut dependencies = raw
        .lines()
        .filter_map(|line| line.split('#').next())
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('-'))
        .filter_map(parse_python_dependency)
        .collect::<Vec<_>>();
    sort_dependencies(&mut dependencies);
    ParsedPackageManifest {
        package_name: None,
        dependencies,
    }
}

fn parse_python_dependency(specification: &str) -> Option<ManifestDependency> {
    let specification = specification.trim();
    let package_end = specification
        .find(|character: char| {
            character.is_whitespace()
                || matches!(character, '<' | '>' | '=' | '!' | '~' | '[' | ';')
        })
        .unwrap_or(specification.len());
    let package = specification[..package_end].trim();
    if package.is_empty()
        || !package.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
    {
        return None;
    }
    let version = specification[package_end..]
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    Some(ManifestDependency {
        package: normalize_python_package_name(package),
        version: if version.is_empty() {
            "*".to_owned()
        } else {
            sanitize_dependency_locator(version)
        },
    })
}

/// PEP 503 name normalization: lowercase, collapse `[-_.]+` runs to `-`, so
/// spelling variants of one distribution unify on a single graph identity.
fn normalize_python_package_name(package: &str) -> String {
    let mut normalized = String::with_capacity(package.len());
    let mut in_separator = false;
    for character in package.chars() {
        if matches!(character, '-' | '_' | '.') {
            if !in_separator {
                normalized.push('-');
            }
            in_separator = true;
        } else {
            in_separator = false;
            normalized.push(character.to_ascii_lowercase());
        }
    }
    normalized
}

/// Strip credentials from PEP 508 direct-reference URLs so they never reach
/// searchable symbol names/identities.
fn sanitize_dependency_locator(version: &str) -> String {
    if !version.contains("://") && !version.contains('?') {
        return version.to_owned();
    }
    redact_sensitive_query_values(&strip_url_userinfo(version))
}

fn strip_url_userinfo(input: &str) -> String {
    let mut sanitized = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(scheme) = rest.find("://") {
        let authority_start = scheme + 3;
        sanitized.push_str(&rest[..authority_start]);
        let tail = &rest[authority_start..];
        let authority_end = tail
            .find(|character: char| {
                matches!(character, '/' | '?' | '#') || character.is_whitespace()
            })
            .unwrap_or(tail.len());
        let authority = &tail[..authority_end];
        match authority.rfind('@') {
            Some(at) => sanitized.push_str(&authority[at + 1..]),
            None => sanitized.push_str(authority),
        }
        rest = &tail[authority_end..];
    }
    sanitized.push_str(rest);
    sanitized
}

fn redact_sensitive_query_values(input: &str) -> String {
    let mut sanitized = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(question) = rest.find('?') {
        sanitized.push_str(&rest[..=question]);
        let tail = &rest[question + 1..];
        let query_end = tail
            .find(|character: char| character == '#' || character.is_whitespace())
            .unwrap_or(tail.len());
        let (query, remainder) = tail.split_at(query_end);
        let mut first = true;
        for parameter in query.split('&') {
            if !first {
                sanitized.push('&');
            }
            first = false;
            match parameter.split_once('=') {
                Some((key, _)) if is_sensitive_query_key(key) => {
                    sanitized.push_str(key);
                    sanitized.push_str("=redacted");
                }
                _ => sanitized.push_str(parameter),
            }
        }
        rest = remainder;
    }
    sanitized.push_str(rest);
    sanitized
}

fn is_sensitive_query_key(key: &str) -> bool {
    [
        "token",
        "secret",
        "password",
        "passwd",
        "credential",
        "auth",
        "key",
        "sig",
    ]
    .iter()
    .any(|needle| {
        key.as_bytes()
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
    })
}

fn sort_dependencies(dependencies: &mut Vec<ManifestDependency>) {
    dependencies.sort_by(|left, right| {
        left.package
            .cmp(&right.package)
            .then(left.version.cmp(&right.version))
    });
    dependencies.dedup();
}

pub fn extract_package_manifest(
    repo: &str,
    file_path: &str,
    owner_file: NodeId,
    owner_repo_node: NodeId,
    raw: &str,
) -> Result<ManifestExtraction, ManifestError> {
    let parsed = if file_path.ends_with("pyproject.toml") {
        parse_python_project_manifest_str(raw)?
    } else if Path::new(file_path)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            Path::new(name)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .is_some_and(|stem| stem.starts_with("requirements"))
                && Path::new(name)
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("txt"))
        })
    {
        parse_requirements_manifest_str(raw)
    } else {
        parse_package_manifest_str(raw)?
    };
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for dependency in parsed.dependencies {
        if !is_shared_dependency(&dependency.package) {
            continue;
        }

        let dependency_repo = virtual_node(
            NodeKind::Repo,
            repo,
            file_path,
            dependency.package.clone(),
            format!("__repo__{}", dependency.package),
        );
        let shared_symbol = virtual_node(
            NodeKind::SharedSymbol,
            repo,
            file_path,
            dependency.package.clone(),
            format!("__shared__{}__package", dependency.package),
        );
        let version_symbol = virtual_node(
            NodeKind::SharedSymbol,
            repo,
            file_path,
            format!("{}@{}", dependency.package, dependency.version),
            format!(
                "__shared__{}@{}__package",
                dependency.package, dependency.version
            ),
        );

        edges.push(manifest_edge(
            owner_repo_node,
            dependency_repo.id,
            EdgeKind::DependsOn,
            owner_file,
        ));
        edges.push(manifest_edge(
            owner_repo_node,
            shared_symbol.id,
            EdgeKind::CrossRepoDepends,
            owner_file,
        ));
        edges.push(manifest_edge(
            owner_repo_node,
            version_symbol.id,
            EdgeKind::UsesShared,
            owner_file,
        ));

        nodes.push(dependency_repo);
        nodes.push(shared_symbol);
        nodes.push(version_symbol);
    }

    Ok(ManifestExtraction { nodes, edges })
}

#[must_use]
pub fn detect_version_mismatches(
    repo_dependencies: &[(String, ManifestDependency)],
) -> Vec<VersionMismatch> {
    let mut by_package = std::collections::BTreeMap::<String, Vec<(String, String)>>::new();
    for (repo, dependency) in repo_dependencies {
        by_package
            .entry(dependency.package.clone())
            .or_default()
            .push((repo.clone(), dependency.version.clone()));
    }

    by_package
        .into_iter()
        .filter_map(|(package, mut versions)| {
            versions.sort();
            versions.dedup_by(|left, right| left.1 == right.1);
            (versions.len() > 1).then_some(VersionMismatch { package, versions })
        })
        .collect()
}

fn manifest_edge(source: NodeId, target: NodeId, kind: EdgeKind, owner_file: NodeId) -> EdgeData {
    EdgeData {
        source,
        target,
        kind,
        metadata: EdgeMetadata {
            confidence: Some(950),
            ..EdgeMetadata::default()
        },
        owner_file,
        is_cross_file: true,
    }
}

fn is_shared_dependency(package: &str) -> bool {
    let mut normalized = package.replace('_', "-");
    normalized.make_ascii_lowercase();
    package.starts_with("@workspace/")
        || package.starts_with("@shared/")
        || normalized.contains("shared")
        || normalized.contains("contract")
        || normalized.contains("schema")
        || normalized.contains("types")
}

#[cfg(test)]
mod tests {
    use gather_step_core::{NodeKind, node_id};
    use pretty_assertions::assert_eq;

    use super::{
        ManifestDependency, detect_version_mismatches, extract_package_manifest,
        parse_package_manifest_str, parse_python_project_manifest_str,
        parse_requirements_manifest_str,
    };

    #[test]
    fn parses_package_manifest_and_extracts_shared_dependencies() {
        let parsed = parse_package_manifest_str(
            r#"{ "name": "backend-service", "dependencies": { "@workspace/shared-contracts": "2.3.1", "express": "^5.0.0" } }"#,
        )
        .expect("manifest should parse");

        assert_eq!(parsed.package_name.as_deref(), Some("backend-service"));
        assert_eq!(parsed.dependencies.len(), 2);
    }

    #[test]
    fn creates_shared_symbol_and_dependency_edges() {
        let owner_file = node_id("backend", "package.json", NodeKind::File, "package.json");
        let repo_node = node_id("backend", "__repo__", NodeKind::Repo, "backend");
        let extraction = extract_package_manifest(
            "backend",
            "package.json",
            owner_file,
            repo_node,
            r#"{ "dependencies": { "@workspace/shared-contracts": "2.3.1" } }"#,
        )
        .expect("extraction should succeed");

        assert!(extraction.nodes.iter().any(|node| {
            node.kind == NodeKind::SharedSymbol
                && node.external_id.as_deref()
                    == Some("__shared__@workspace/shared-contracts@2.3.1__package")
        }));
        assert!(
            extraction
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::DependsOn)
        );
        assert!(
            extraction
                .edges
                .iter()
                .any(|edge| edge.kind == gather_step_core::EdgeKind::UsesShared)
        );
    }

    #[test]
    fn parses_pep_621_and_poetry_dependencies() {
        let pep621 = parse_python_project_manifest_str(
            r#"
[project]
name = "asset-api"
dependencies = [
  "shared-schemas>=2.3",
  "httpx>=0.28",
  "foundation-kit @ git+https://example.invalid/foundation-kit@1.4.0",
]

[project.optional-dependencies]
worker = ["shared-events==3.1"]
"#,
        )
        .expect("PEP 621 manifest should parse");
        assert_eq!(pep621.package_name.as_deref(), Some("asset-api"));
        assert!(pep621.dependencies.iter().any(|dependency| {
            dependency.package == "foundation-kit" && dependency.version.contains("1.4.0")
        }));
        assert!(
            pep621
                .dependencies
                .iter()
                .any(|dependency| dependency.package == "shared-events")
        );

        let poetry = parse_python_project_manifest_str(
            r#"
[tool.poetry]
name = "asset-worker"

[tool.poetry.dependencies]
python = "^3.12"
shared-contracts = { version = "^4.0" }
"#,
        )
        .expect("Poetry manifest should parse");
        assert_eq!(poetry.package_name.as_deref(), Some("asset-worker"));
        assert_eq!(poetry.dependencies.len(), 1);
        assert_eq!(poetry.dependencies[0].package, "shared-contracts");
    }

    #[test]
    fn parses_requirements_without_options_comments_or_environment_markers() {
        let parsed = parse_requirements_manifest_str(
            r#"
# runtime dependencies
shared-schemas==2.3.1
foundation_kit @ git+https://example.invalid/foundation-kit@1.4.0
httpx>=0.28 ; python_version >= "3.12"
-r requirements/dev.txt
"#,
        );

        assert_eq!(parsed.dependencies.len(), 3);
        assert!(
            parsed.dependencies.iter().any(|dependency| {
                dependency.package == "httpx" && dependency.version == ">=0.28"
            })
        );
    }

    #[test]
    fn extracts_shared_dependencies_from_python_manifest() {
        let owner_file = node_id(
            "asset-api",
            "pyproject.toml",
            NodeKind::File,
            "pyproject.toml",
        );
        let repo_node = node_id("asset-api", "__repo__", NodeKind::Repo, "asset-api");
        let extraction = extract_package_manifest(
            "asset-api",
            "pyproject.toml",
            owner_file,
            repo_node,
            r#"
[project]
name = "asset-api"
dependencies = ["shared-schemas==2.3.1", "httpx>=0.28"]
"#,
        )
        .expect("Python manifest extraction should succeed");

        assert!(extraction.nodes.iter().any(|node| {
            node.kind == NodeKind::SharedSymbol
                && node.external_id.as_deref() == Some("__shared__shared-schemas@==2.3.1__package")
        }));
        assert!(!extraction.nodes.iter().any(|node| node.name == "httpx"));
    }

    #[test]
    fn detects_version_mismatches_across_repos() {
        let mismatches = detect_version_mismatches(&[
            (
                "service-a".to_owned(),
                ManifestDependency {
                    package: "@workspace/shared-contracts".to_owned(),
                    version: "2.3.1".to_owned(),
                },
            ),
            (
                "service-b".to_owned(),
                ManifestDependency {
                    package: "@workspace/shared-contracts".to_owned(),
                    version: "2.4.0".to_owned(),
                },
            ),
        ]);

        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].package, "@workspace/shared-contracts");
    }

    #[test]
    fn pep_503_spelling_variants_unify_across_manifests() {
        let requirements = parse_requirements_manifest_str("Shared_Contracts==2.3.1\n");
        let pyproject = parse_python_project_manifest_str(
            "[project]\nname = \"asset-api\"\ndependencies = [\"shared.contracts==2.4.0\"]\n",
        )
        .expect("pyproject should parse");
        let poetry = parse_python_project_manifest_str(
            "[tool.poetry]\nname = \"asset-worker\"\n\n[tool.poetry.dependencies]\n\"Shared.Contracts\" = \"2.5.0\"\n",
        )
        .expect("poetry manifest should parse");

        for parsed in [&requirements, &pyproject, &poetry] {
            assert_eq!(parsed.dependencies[0].package, "shared-contracts");
        }

        let mismatches = detect_version_mismatches(&[
            ("service-a".to_owned(), requirements.dependencies[0].clone()),
            ("service-b".to_owned(), pyproject.dependencies[0].clone()),
        ]);
        assert_eq!(mismatches.len(), 1);
        assert_eq!(mismatches[0].package, "shared-contracts");
        assert_eq!(mismatches[0].versions.len(), 2);
    }

    #[test]
    fn direct_reference_credentials_never_reach_symbol_identities() {
        let bait_reference =
            "shared-contract @ https://alice:s3cr3t@example.com/pkg.whl?token=abc123&x=1";
        let manifests = [
            (
                "pyproject.toml",
                format!("[project]\nname = \"asset-api\"\ndependencies = [\"{bait_reference}\"]\n"),
            ),
            ("requirements.txt", format!("{bait_reference}\n")),
        ];

        for (file_path, raw) in manifests {
            let owner_file = node_id("asset-api", file_path, NodeKind::File, file_path);
            let repo_node = node_id("asset-api", "__repo__", NodeKind::Repo, "asset-api");
            let extraction =
                extract_package_manifest("asset-api", file_path, owner_file, repo_node, &raw)
                    .expect("extraction should succeed");

            assert!(
                !extraction.nodes.is_empty(),
                "{file_path}: shared dependency expected"
            );
            let leaks = |field: &str| field.contains("s3cr3t") || field.contains("abc123");
            for node in &extraction.nodes {
                assert!(
                    !leaks(&node.name),
                    "{file_path}: node.name leaks: {:?}",
                    node.name
                );
                assert!(
                    !node.qualified_name.as_deref().is_some_and(leaks),
                    "{file_path}: node.qualified_name leaks: {:?}",
                    node.qualified_name
                );
                assert!(
                    !node.external_id.as_deref().is_some_and(leaks),
                    "{file_path}: node.external_id leaks: {:?}",
                    node.external_id
                );
            }
            assert!(
                extraction.nodes.iter().any(|node| {
                    node.name.contains("example.com/pkg.whl") && node.name.contains("x=1")
                }),
                "{file_path}: credential-free locator must survive as the version"
            );
        }
    }

    /// `package.json` files sometimes contain credential fields in their JSON.
    /// The structured parser must not
    /// propagate those values into any emitted graph node's `name`,
    /// `qualified_name`, or `signature` fields.
    #[test]
    fn package_json_credential_field_does_not_reach_graph_nodes() {
        let credential_value = "credential-value-xyz";
        let raw = format!(
            r#"{{
                "name": "my-service",
                "_authCredential": "{credential_value}",
                "dependencies": {{
                    "_authCredential": "{credential_value}",
                    "@workspace/shared-contracts": "2.0.0"
                }}
            }}"#
        );

        let owner_file = node_id("my-service", "package.json", NodeKind::File, "package.json");
        let repo_node = node_id("my-service", "__repo__", NodeKind::Repo, "my-service");
        let extraction =
            extract_package_manifest("my-service", "package.json", owner_file, repo_node, &raw)
                .expect("extraction should succeed");

        let has_credential = |field: &str| field.contains(credential_value);
        for node in &extraction.nodes {
            assert!(
                !has_credential(&node.name),
                "The node.name field must not contain the credential value: {:?}.",
                node.name
            );
            assert!(
                !node.qualified_name.as_deref().is_some_and(has_credential),
                "The node.qualified_name field must not contain the credential value: {:?}.",
                node.qualified_name
            );
            assert!(
                !node.signature.as_deref().is_some_and(has_credential),
                "The node.signature field must not contain the credential value: {:?}.",
                node.signature
            );
        }
    }
}
