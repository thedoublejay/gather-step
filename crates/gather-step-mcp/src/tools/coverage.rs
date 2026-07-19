use std::collections::BTreeSet;

use gather_step_core::WorkspaceRegistry;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Disclosure attached to list-shaped graph responses.
///
/// Coverage is intentionally descriptive rather than a completeness claim. The
/// source scope is classified conservatively from indexed file paths, so
/// callers must still treat an empty result as a possible extraction gap.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct QueryCoverage {
    pub repos_considered: Vec<String>,
    pub matching_frameworks: Vec<String>,
    /// Index-time extractor provenance is not currently persisted. This list
    /// therefore remains empty rather than presenting the query path as an
    /// extractor that produced the indexed graph.
    pub extractors_run: Vec<String>,
    pub source_scopes: Vec<String>,
    /// Concrete graph edges represented by the response. Aggregate tools that
    /// cannot retain exact edge identity report zero and explain that limit in
    /// `limitations`.
    pub edges_contributed: usize,
    pub verdict: String,
    pub limitations: Vec<String>,
}

impl QueryCoverage {
    /// Build coverage from the registered workspace metadata.
    #[must_use]
    pub fn workspace(
        registry: &WorkspaceRegistry,
        query_path: impl Into<String>,
        edges_contributed: usize,
    ) -> Self {
        Self::scoped(registry, None, query_path, edges_contributed)
    }

    /// Build coverage for either one selected repo or the complete registry.
    #[must_use]
    pub fn scoped(
        registry: &WorkspaceRegistry,
        repo: Option<&str>,
        query_path: impl Into<String>,
        edges_contributed: usize,
    ) -> Self {
        let repos = registry
            .repos
            .keys()
            .filter(|name| repo.is_none_or(|selected| name.as_str() == selected));
        Self::for_repos(registry, repos, query_path, edges_contributed)
    }

    /// Build coverage for an explicit set of registered repositories that
    /// participated in a query or its returned evidence.
    #[must_use]
    pub fn for_repos<I, S>(
        registry: &WorkspaceRegistry,
        repos: I,
        query_path: impl Into<String>,
        edges_contributed: usize,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let repos_considered = repos
            .into_iter()
            .map(|repo| repo.as_ref().to_owned())
            .filter(|repo| registry.repos.contains_key(repo))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let matching_frameworks = repos_considered
            .iter()
            .filter_map(|name| registry.repos.get(name))
            .flat_map(|metadata| metadata.frameworks.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let query_path = query_path.into();
        let mut limitations = vec![
            "Source scope is classified from indexed file paths; virtual, external, or unresolved evidence may remain unknown."
                .to_owned(),
            "matching_frameworks lists registry-detected frameworks for the reported repo scope; it does not attribute an edge to a specific framework."
                .to_owned(),
            format!(
                "Query path `{query_path}` ran over the stored graph; index-time extractor provenance is not persisted, so extractors_run is empty."
            ),
        ];
        if edges_contributed == 0 {
            limitations.push(
                "No concrete edge contribution was recorded for this response; this can mean no match or an aggregate result whose exact edge identity is unavailable."
                    .to_owned(),
            );
        }

        Self {
            repos_considered,
            matching_frameworks,
            extractors_run: Vec::new(),
            source_scopes: vec!["unknown".to_owned()],
            edges_contributed,
            verdict: if edges_contributed == 0 {
                "possible_extraction_gap"
            } else {
                "ok"
            }
            .to_owned(),
            limitations,
        }
    }

    #[must_use]
    pub fn with_verdict(mut self, verdict: impl Into<String>) -> Self {
        self.verdict = verdict.into();
        self
    }

    #[must_use]
    pub fn with_source_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let scopes = scopes.into_iter().map(Into::into).collect::<BTreeSet<_>>();
        if !scopes.is_empty() {
            self.source_scopes = scopes.into_iter().collect();
        }
        self
    }

    #[must_use]
    pub fn with_limitation(mut self, limitation: impl Into<String>) -> Self {
        self.limitations.push(limitation.into());
        self
    }
}
