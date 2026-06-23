//! GitHub CLI resolver for PR-set manifests.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    process::Command,
};

use gather_step_core::GatherStepConfig;
use serde::Deserialize;

use super::manifest::{PR_SET_MANIFEST_VERSION, PrEntry, PrSetManifest};

const GH_LIMIT_PER_REPO: &str = "100";

#[derive(Debug, Clone)]
pub struct GhResolveOptions {
    pub query: String,
    pub set_id: Option<String>,
    pub allow_unknown_repos: bool,
}

pub fn resolve_pr_set_from_gh(
    workspace: &Path,
    config_path: &Path,
    options: &GhResolveOptions,
) -> Result<PrSetManifest, GhResolverError> {
    let config = GatherStepConfig::from_yaml_file(config_path)
        .map_err(|source| GhResolverError::Config { source })?;
    let lookup = RepoLookup::from_config(&config);
    let owner = config.github.as_ref().map(|github| github.owner.as_str());

    let responses = if let Some(owner) = owner {
        let mut all = Vec::new();
        let mut first_error = None;
        let mut successes = 0usize;
        for repo_name in lookup.local_repo_names() {
            let repo = format!("{owner}/{repo_name}");
            match gh_pr_list(workspace, Some(&repo), &options.query) {
                Ok(mut prs) => {
                    successes += 1;
                    all.append(&mut prs);
                }
                Err(error) => {
                    tracing::debug!(
                        error = %error,
                        repo = %repo,
                        "GitHub PR-set resolver skipped a repo whose PR list could not be loaded."
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if successes == 0
            && let Some(error) = first_error
        {
            return Err(error);
        }
        all
    } else {
        gh_pr_list(workspace, None, &options.query)?
    };

    manifest_from_gh_prs(&responses, &lookup, options)
}

/// Maximum accepted length of a `--from-gh` search query. GitHub's own search
/// query limit is well under this; the cap simply bounds what we hand to `gh`.
const MAX_GH_QUERY_LEN: usize = 512;

/// Reject queries that are over-long or contain control characters before they
/// reach the `gh` subprocess. The query runs under the caller's `gh` token.
fn validate_gh_query(query: &str) -> Result<(), GhResolverError> {
    if query.len() > MAX_GH_QUERY_LEN {
        return Err(GhResolverError::InvalidQuery {
            reason: "query exceeds the maximum length",
        });
    }
    if query.chars().any(char::is_control) {
        return Err(GhResolverError::InvalidQuery {
            reason: "query contains control characters",
        });
    }
    Ok(())
}

fn gh_pr_list(
    workspace: &Path,
    repo: Option<&str>,
    query: &str,
) -> Result<Vec<GhPr>, GhResolverError> {
    validate_gh_query(query)?;
    let mut cmd = Command::new("gh");
    cmd.arg("pr").arg("list");
    if let Some(repo) = repo {
        cmd.arg("--repo").arg(repo);
    }
    cmd.arg("--search").arg(query);
    cmd.arg("--json")
        .arg("number,headRefName,baseRefName,repoNameWithOwner");
    cmd.arg("--limit").arg(GH_LIMIT_PER_REPO);
    cmd.current_dir(workspace);

    let output = cmd
        .output()
        .map_err(|source| GhResolverError::Launch { source })?;
    if !output.status.success() {
        return Err(GhResolverError::Command {
            status: output.status.code(),
        });
    }
    serde_json::from_slice(&output.stdout).map_err(|source| GhResolverError::Parse { source })
}

fn manifest_from_gh_prs(
    prs: &[GhPr],
    lookup: &RepoLookup,
    options: &GhResolveOptions,
) -> Result<PrSetManifest, GhResolverError> {
    let mut deduped = BTreeMap::new();
    for pr in prs {
        deduped.insert((pr.repo_name_with_owner.clone(), pr.number), pr.clone());
    }

    let mut entries = Vec::new();
    for pr in deduped.into_values().take(200) {
        let repo = lookup
            .local_name_for_gh_repo(&pr.repo_name_with_owner)
            .or_else(|| {
                options
                    .allow_unknown_repos
                    .then(|| gh_repo_basename(&pr.repo_name_with_owner))
            });
        let Some(repo) = repo else {
            continue;
        };
        entries.push(PrEntry {
            id: format!("{}-{}", slugify_set_id(&repo), pr.number),
            repo,
            base: pr.base_ref_name,
            head: pr.head_ref_name,
            pr: Some(pr.number),
            depends_on: Vec::new(),
        });
    }

    if entries.is_empty() {
        return Err(GhResolverError::NoPullRequests {
            query: options.query.clone(),
        });
    }

    let manifest = PrSetManifest {
        version: PR_SET_MANIFEST_VERSION,
        id: options
            .set_id
            .clone()
            .unwrap_or_else(|| slugify_set_id(&options.query)),
        title: Some(format!("PR set for {}", options.query)),
        prs: entries,
    };
    manifest
        .validate()
        .map_err(|source| GhResolverError::Manifest { source })?;
    Ok(manifest)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GhPr {
    number: u64,
    head_ref_name: String,
    base_ref_name: String,
    repo_name_with_owner: String,
}

#[derive(Debug, Clone)]
struct RepoLookup {
    aliases: BTreeMap<String, String>,
}

impl RepoLookup {
    fn from_config(config: &GatherStepConfig) -> Self {
        let mut aliases = BTreeMap::new();
        for repo in &config.repos {
            aliases.insert(repo.name.clone(), repo.name.clone());
            if let Some(name) = Path::new(&repo.path)
                .file_name()
                .and_then(|name| name.to_str())
            {
                aliases.insert(name.to_owned(), repo.name.clone());
            }
            if let Some(github) = config.github.as_ref() {
                aliases.insert(format!("{}/{}", github.owner, repo.name), repo.name.clone());
                if let Some(name) = Path::new(&repo.path)
                    .file_name()
                    .and_then(|name| name.to_str())
                {
                    aliases.insert(format!("{}/{}", github.owner, name), repo.name.clone());
                }
            }
        }
        Self { aliases }
    }

    fn local_repo_names(&self) -> BTreeSet<String> {
        self.aliases.values().cloned().collect()
    }

    fn local_name_for_gh_repo(&self, repo_name_with_owner: &str) -> Option<String> {
        self.aliases.get(repo_name_with_owner).cloned().or_else(|| {
            self.aliases
                .get(&gh_repo_basename(repo_name_with_owner))
                .cloned()
        })
    }
}

fn gh_repo_basename(repo_name_with_owner: &str) -> String {
    repo_name_with_owner
        .rsplit('/')
        .next()
        .unwrap_or(repo_name_with_owner)
        .to_owned()
}

#[must_use]
pub fn slugify_set_id(input: &str) -> String {
    let mut out = String::new();
    let mut last_was_dash = false;
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash {
            out.push('-');
            last_was_dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_owned();
    if trimmed.is_empty() {
        "pr-set".to_owned()
    } else {
        trimmed
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GhResolverError {
    #[error("Failed to load the workspace config for GitHub PR-set resolution: {source}")]
    Config {
        #[source]
        source: gather_step_core::ConfigError,
    },
    #[error("Failed to launch `gh pr list`: {source}")]
    Launch {
        #[source]
        source: std::io::Error,
    },
    #[error("`gh pr list` exited with status {status:?}.")]
    Command { status: Option<i32> },
    #[error(
        "GitHub PR-set query is invalid: {reason}. Provide a shorter, single-line search query."
    )]
    InvalidQuery { reason: &'static str },
    #[error("Failed to parse `gh pr list` JSON: {source}")]
    Parse {
        #[source]
        source: serde_json::Error,
    },
    #[error("No pull requests matched query `{query}` after workspace repo filtering.")]
    NoPullRequests { query: String },
    #[error("Resolved PR-set manifest is invalid: {source}")]
    Manifest {
        #[source]
        source: super::manifest::ManifestError,
    },
}

#[cfg(test)]
mod tests {
    use super::{
        GhPr, GhResolveOptions, RepoLookup, manifest_from_gh_prs, slugify_set_id, validate_gh_query,
    };
    use gather_step_core::{GatherStepConfig, RepoConfig};

    #[test]
    fn gh_query_validation_rejects_control_chars_and_overlong_input() {
        assert!(validate_gh_query("is:open author:me").is_ok());
        assert!(validate_gh_query("ok\nmalicious").is_err());
        assert!(validate_gh_query("x\0y").is_err());
        assert!(validate_gh_query(&"a".repeat(1000)).is_err());
    }

    fn config() -> GatherStepConfig {
        GatherStepConfig {
            allow_listed_repos: vec![],
            repos: vec![
                RepoConfig {
                    name: "api-service".to_owned(),
                    path: "services/api-service".to_owned(),
                    depth: None,
                },
                RepoConfig {
                    name: "web-client".to_owned(),
                    path: "apps/web-client".to_owned(),
                    depth: None,
                },
            ],
            github: Some(gather_step_core::GithubConfig {
                owner: "acme".to_owned(),
                api_base_url: None,
                token_env: None,
            }),
            jira: None,
            indexing: gather_step_core::IndexingConfig::default(),
            deployment: gather_step_core::DeploymentConfig::default(),
        }
    }

    #[test]
    fn manifest_from_gh_prs_filters_to_configured_repos() {
        let prs = vec![
            GhPr {
                number: 42,
                head_ref_name: "feature/api".to_owned(),
                base_ref_name: "main".to_owned(),
                repo_name_with_owner: "acme/api-service".to_owned(),
            },
            GhPr {
                number: 7,
                head_ref_name: "feature/unknown".to_owned(),
                base_ref_name: "main".to_owned(),
                repo_name_with_owner: "acme/unknown".to_owned(),
            },
        ];
        let options = GhResolveOptions {
            query: "checkout refresh".to_owned(),
            set_id: Some("checkout-refresh".to_owned()),
            allow_unknown_repos: false,
        };
        let manifest = manifest_from_gh_prs(&prs, &RepoLookup::from_config(&config()), &options)
            .expect("manifest");

        assert_eq!(manifest.id, "checkout-refresh");
        assert_eq!(manifest.prs.len(), 1);
        assert_eq!(manifest.prs[0].repo, "api-service");
        assert_eq!(manifest.prs[0].pr, Some(42));
    }

    #[test]
    fn slugify_set_id_keeps_ids_neutral_and_filesystem_safe() {
        assert_eq!(slugify_set_id("Checkout Refresh!"), "checkout-refresh");
        assert_eq!(slugify_set_id("  "), "pr-set");
    }
}
