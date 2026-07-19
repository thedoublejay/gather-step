use std::collections::BTreeSet;

use gather_step_core::WorkspaceRegistry;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How a zero-edge result should be read when deriving the default verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroEdgeSemantics {
    /// Zero edges may mean the extractor missed evidence, so the verdict
    /// stays `possible_extraction_gap`. This is the conservative default.
    PossibleExtractionGap,
    /// The query counted its result exactly over indexed repos, so zero
    /// edges is a healthy `ok` answer rather than a gap signal. When no
    /// repos were considered the verdict still degrades to
    /// `possible_extraction_gap` because nothing was actually scanned.
    ExactZeroIsOk,
}

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
        Self::scoped_with_zero_semantics(
            registry,
            repo,
            query_path,
            edges_contributed,
            ZeroEdgeSemantics::PossibleExtractionGap,
        )
    }

    /// Like [`Self::scoped`], with explicit zero-edge semantics for callers
    /// whose result is an exact count where zero is a healthy answer.
    #[must_use]
    pub fn scoped_with_zero_semantics(
        registry: &WorkspaceRegistry,
        repo: Option<&str>,
        query_path: impl Into<String>,
        edges_contributed: usize,
        zero_semantics: ZeroEdgeSemantics,
    ) -> Self {
        let repos = registry
            .repos
            .keys()
            .filter(|name| repo.is_none_or(|selected| name.as_str() == selected));
        Self::for_repos_with_zero_semantics(
            registry,
            repos,
            query_path,
            edges_contributed,
            zero_semantics,
        )
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
        Self::for_repos_with_zero_semantics(
            registry,
            repos,
            query_path,
            edges_contributed,
            ZeroEdgeSemantics::PossibleExtractionGap,
        )
    }

    /// Like [`Self::for_repos`], with explicit zero-edge semantics for callers
    /// whose result is an exact count where zero is a healthy answer.
    #[must_use]
    pub fn for_repos_with_zero_semantics<I, S>(
        registry: &WorkspaceRegistry,
        repos: I,
        query_path: impl Into<String>,
        edges_contributed: usize,
        zero_semantics: ZeroEdgeSemantics,
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
        let zero_is_healthy =
            zero_semantics == ZeroEdgeSemantics::ExactZeroIsOk && !repos_considered.is_empty();
        if edges_contributed == 0 && !zero_is_healthy {
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
            verdict: if edges_contributed == 0 && !zero_is_healthy {
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

#[cfg(test)]
mod tests {
    use gather_step_core::{DepthLevel, RegisteredRepo, WorkspaceRegistry};

    use super::{QueryCoverage, ZeroEdgeSemantics};

    const ZERO_EDGE_LIMITATION_PREFIX: &str = "No concrete edge contribution was recorded";

    fn registry_with(repos: &[&str]) -> WorkspaceRegistry {
        let mut registry = WorkspaceRegistry::default();
        for repo in repos {
            registry.repos.insert(
                (*repo).to_owned(),
                RegisteredRepo::new(format!("/tmp/{repo}"), DepthLevel::Full),
            );
        }
        registry
    }

    fn has_zero_edge_limitation(coverage: &QueryCoverage) -> bool {
        coverage
            .limitations
            .iter()
            .any(|limitation| limitation.starts_with(ZERO_EDGE_LIMITATION_PREFIX))
    }

    #[test]
    fn zero_edges_over_indexed_repo_defaults_to_possible_extraction_gap() {
        let registry = registry_with(&["backend_standard"]);
        let coverage = QueryCoverage::scoped(&registry, None, "test_query", 0);

        assert_eq!(coverage.verdict, "possible_extraction_gap");
        assert!(has_zero_edge_limitation(&coverage));
    }

    #[test]
    fn nonzero_edges_default_to_ok_without_zero_edge_limitation() {
        let registry = registry_with(&["backend_standard"]);
        let coverage = QueryCoverage::scoped(&registry, None, "test_query", 3);

        assert_eq!(coverage.verdict, "ok");
        assert!(!has_zero_edge_limitation(&coverage));
    }

    #[test]
    fn exact_zero_over_indexed_repo_is_ok_without_zero_edge_limitation() {
        let registry = registry_with(&["backend_standard"]);
        let coverage = QueryCoverage::scoped_with_zero_semantics(
            &registry,
            None,
            "test_query",
            0,
            ZeroEdgeSemantics::ExactZeroIsOk,
        );

        assert_eq!(coverage.verdict, "ok");
        assert!(!has_zero_edge_limitation(&coverage));
    }

    #[test]
    fn exact_zero_without_considered_repos_stays_possible_extraction_gap() {
        let registry = registry_with(&[]);
        let coverage = QueryCoverage::scoped_with_zero_semantics(
            &registry,
            None,
            "test_query",
            0,
            ZeroEdgeSemantics::ExactZeroIsOk,
        );

        assert_eq!(coverage.verdict, "possible_extraction_gap");
        assert!(has_zero_edge_limitation(&coverage));
    }
}
