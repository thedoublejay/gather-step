//! PR-set manifest schema for multi-PR `pr-review`.
//!
//! The manifest is intentionally small: it names a review set and lists each
//! repo/ref pair that belongs to it. Later phases use this validated contract to
//! coordinate per-PR review runs and cross-PR analysis.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

pub const PR_SET_MANIFEST_VERSION: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrSetManifest {
    pub version: u32,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub prs: Vec<PrEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrEntry {
    pub id: String,
    pub repo: String,
    pub base: String,
    pub head: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("Failed to read PR-set manifest `{}`: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse PR-set manifest YAML: {source}")]
    Parse {
        #[source]
        source: serde_norway::Error,
    },
    #[error("Unsupported PR-set manifest version {found}; expected {expected}.")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("PR-set manifest id must not be empty.")]
    EmptyManifestId,
    #[error("PR-set manifest must contain at least one PR entry.")]
    EmptyEntries,
    #[error("PR entry at index {index} has an empty `{field}` field.")]
    EmptyEntryField { index: usize, field: &'static str },
    #[error("Duplicate PR entry id `{id}`.")]
    DuplicateEntryId { id: String },
    #[error("Duplicate PR entry repo/head pair `{repo}` / `{head}`.")]
    DuplicateRepoHead { repo: String, head: String },
    #[error("Duplicate PR entry repo/pr pair `{repo}` / #{pr}.")]
    DuplicateRepoPr { repo: String, pr: u64 },
    #[error("PR entry `{id}` depends on unknown entry `{depends_on}`.")]
    UnknownDependency { id: String, depends_on: String },
    #[error("PR entry `{id}` depends on itself.")]
    SelfDependency { id: String },
    #[error("PR-set manifest contains a dependency cycle involving `{id}`.")]
    DependencyCycle { id: String },
}

impl PrSetManifest {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ManifestError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_yaml_str(&raw)
    }

    pub fn from_yaml_str(raw: &str) -> Result<Self, ManifestError> {
        let manifest: Self =
            serde_norway::from_str(raw).map_err(|source| ManifestError::Parse { source })?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn to_yaml_string(&self) -> Result<String, ManifestError> {
        serde_norway::to_string(self).map_err(|source| ManifestError::Parse { source })
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.version != PR_SET_MANIFEST_VERSION {
            return Err(ManifestError::UnsupportedVersion {
                found: self.version,
                expected: PR_SET_MANIFEST_VERSION,
            });
        }
        if self.id.trim().is_empty() {
            return Err(ManifestError::EmptyManifestId);
        }
        if self.prs.is_empty() {
            return Err(ManifestError::EmptyEntries);
        }

        let mut ids = BTreeSet::new();
        let mut repo_heads = BTreeSet::new();
        let mut repo_prs = BTreeSet::new();

        for (index, entry) in self.prs.iter().enumerate() {
            validate_required_entry_field(index, "id", &entry.id)?;
            validate_required_entry_field(index, "repo", &entry.repo)?;
            validate_required_entry_field(index, "base", &entry.base)?;
            validate_required_entry_field(index, "head", &entry.head)?;

            if !ids.insert(entry.id.clone()) {
                return Err(ManifestError::DuplicateEntryId {
                    id: entry.id.clone(),
                });
            }

            let repo_head = (entry.repo.clone(), entry.head.clone());
            if !repo_heads.insert(repo_head) {
                return Err(ManifestError::DuplicateRepoHead {
                    repo: entry.repo.clone(),
                    head: entry.head.clone(),
                });
            }

            if let Some(pr) = entry.pr {
                let repo_pr = (entry.repo.clone(), pr);
                if !repo_prs.insert(repo_pr) {
                    return Err(ManifestError::DuplicateRepoPr {
                        repo: entry.repo.clone(),
                        pr,
                    });
                }
            }
        }

        self.validate_dependencies()
    }

    fn validate_dependencies(&self) -> Result<(), ManifestError> {
        let entries: BTreeMap<&str, &PrEntry> = self
            .prs
            .iter()
            .map(|entry| (entry.id.as_str(), entry))
            .collect();

        for entry in &self.prs {
            for depends_on in &entry.depends_on {
                if depends_on.trim().is_empty() {
                    return Err(ManifestError::UnknownDependency {
                        id: entry.id.clone(),
                        depends_on: depends_on.clone(),
                    });
                }
                if depends_on == &entry.id {
                    return Err(ManifestError::SelfDependency {
                        id: entry.id.clone(),
                    });
                }
                if !entries.contains_key(depends_on.as_str()) {
                    return Err(ManifestError::UnknownDependency {
                        id: entry.id.clone(),
                        depends_on: depends_on.clone(),
                    });
                }
            }
        }

        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for entry in &self.prs {
            detect_dependency_cycle(entry.id.as_str(), &entries, &mut visiting, &mut visited)?;
        }

        Ok(())
    }
}

fn validate_required_entry_field(
    index: usize,
    field: &'static str,
    value: &str,
) -> Result<(), ManifestError> {
    if value.trim().is_empty() {
        return Err(ManifestError::EmptyEntryField { index, field });
    }
    Ok(())
}

fn detect_dependency_cycle<'a>(
    id: &'a str,
    entries: &BTreeMap<&'a str, &'a PrEntry>,
    visiting: &mut BTreeSet<&'a str>,
    visited: &mut BTreeSet<&'a str>,
) -> Result<(), ManifestError> {
    if visited.contains(id) {
        return Ok(());
    }
    if !visiting.insert(id) {
        return Err(ManifestError::DependencyCycle { id: id.to_owned() });
    }

    let Some(entry) = entries.get(id) else {
        return Ok(());
    };

    for depends_on in &entry.depends_on {
        detect_dependency_cycle(depends_on, entries, visiting, visited)?;
    }

    visiting.remove(id);
    visited.insert(id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ManifestError, PR_SET_MANIFEST_VERSION, PrSetManifest};

    const REG_13863_EXAMPLE: &str = include_str!("../../../../../examples/pr-set/reg-13863.yaml");
    const STACKED_EXAMPLE: &str =
        include_str!("../../../../../examples/pr-set/stacked-in-one-repo.yaml");
    const DIVERGENT_BASES_EXAMPLE: &str =
        include_str!("../../../../../examples/pr-set/divergent-bases.yaml");

    #[test]
    fn pr_set_manifest_parses_reg_13863_example() {
        let manifest = PrSetManifest::from_yaml_str(REG_13863_EXAMPLE)
            .expect("REG-13863 example should parse");

        assert_eq!(manifest.version, PR_SET_MANIFEST_VERSION);
        assert_eq!(manifest.id, "REG-13863");
        assert_eq!(manifest.prs.len(), 3);
        assert!(
            manifest
                .prs
                .iter()
                .any(|entry| entry.id == "label-review-502"
                    && entry.repo == "label-review"
                    && entry.pr == Some(502))
        );

        let round_tripped = manifest
            .to_yaml_string()
            .expect("manifest should serialize to YAML");
        let reparsed = PrSetManifest::from_yaml_str(&round_tripped)
            .expect("serialized manifest should parse again");
        assert_eq!(reparsed, manifest);
    }

    #[test]
    fn pr_set_manifest_examples_validate_cleanly() {
        for raw in [REG_13863_EXAMPLE, STACKED_EXAMPLE, DIVERGENT_BASES_EXAMPLE] {
            PrSetManifest::from_yaml_str(raw).expect("example manifest should validate cleanly");
        }
    }

    #[test]
    fn manifest_parse_rejects_duplicate_entry_ids() {
        let err = PrSetManifest::from_yaml_str(
            "
version: 0
id: duplicate-ids
prs:
  - id: duplicate
    repo: api
    base: main
    head: feature/a
  - id: duplicate
    repo: web
    base: main
    head: feature/b
",
        )
        .expect_err("duplicate ids should be rejected");

        assert!(matches!(err, ManifestError::DuplicateEntryId { .. }));
    }

    #[test]
    fn manifest_parse_rejects_duplicate_repo_head_pairs() {
        let err = PrSetManifest::from_yaml_str(
            "
version: 0
id: duplicate-repo-head
prs:
  - id: api-1
    repo: api
    base: main
    head: feature/a
  - id: api-2
    repo: api
    base: release
    head: feature/a
",
        )
        .expect_err("duplicate repo/head pairs should be rejected");

        assert!(matches!(err, ManifestError::DuplicateRepoHead { .. }));
    }

    #[test]
    fn manifest_parse_rejects_duplicate_repo_pr_pairs() {
        let err = PrSetManifest::from_yaml_str(
            "
version: 0
id: duplicate-repo-pr
prs:
  - id: api-1
    repo: api
    base: main
    head: feature/a
    pr: 42
  - id: api-2
    repo: api
    base: main
    head: feature/b
    pr: 42
",
        )
        .expect_err("duplicate repo/pr pairs should be rejected");

        assert!(matches!(err, ManifestError::DuplicateRepoPr { .. }));
    }

    #[test]
    fn manifest_parse_rejects_dependency_cycle() {
        let err = PrSetManifest::from_yaml_str(
            "
version: 0
id: cycle
prs:
  - id: api
    repo: api
    base: main
    head: feature/a
    depends_on: [web]
  - id: web
    repo: web
    base: main
    head: feature/b
    depends_on: [api]
",
        )
        .expect_err("dependency cycles should be rejected");

        assert!(matches!(err, ManifestError::DependencyCycle { .. }));
    }

    #[test]
    fn manifest_parse_rejects_unknown_dependency() {
        let err = PrSetManifest::from_yaml_str(
            "
version: 0
id: unknown-dependency
prs:
  - id: api
    repo: api
    base: main
    head: feature/a
    depends_on: [web]
",
        )
        .expect_err("unknown dependencies should be rejected");

        assert!(matches!(err, ManifestError::UnknownDependency { .. }));
    }

    #[test]
    fn manifest_parse_rejects_unknown_version() {
        let err = PrSetManifest::from_yaml_str(
            "
version: 1
id: unknown-version
prs:
  - id: api
    repo: api
    base: main
    head: feature/a
",
        )
        .expect_err("unknown manifest versions should be rejected");

        assert!(matches!(err, ManifestError::UnsupportedVersion { .. }));
    }
}
