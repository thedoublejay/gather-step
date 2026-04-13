use std::fs;

use gather_step_analysis::{
    ConfidenceBand, build_overview, detect_conventions, find_dead_code_with_manifest,
};
use gather_step_git::{OwnershipOptions, analyze_ownership_for_file, redact_email};
use gather_step_parser::parse_package_manifest_str;
use gather_step_storage::MetadataStore;
use rmcp::schemars;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    budget::{BudgetedTool, ResponseBudget, apply_response_budget, response_schema_version},
    config::{McpContext, validate_input_length},
    error::McpServerError,
    output::redact::relativize_to_workspace,
    tools::search::{SearchRequest, search_symbols},
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RepoScopedRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeadCodeRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub min_confidence: Option<String>,
    /// Maximum number of findings to return. Defaults to 200 when omitted
    /// so a repo with thousands of unreachable files cannot produce a
    /// multi-megabyte JSON payload over MCP.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Default cap on dead-code findings returned to MCP clients. The full
/// report is still computed; only the response payload is truncated.
const DEFAULT_DEAD_CODE_LIMIT: usize = 200;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct WhoOwnsRequest {
    #[serde(default)]
    pub budget_bytes: Option<usize>,
    #[serde(default)]
    pub repo: Option<String>,
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct RepoIntelligenceMeta {
    pub budget: ResponseBudget,
    pub generation: i64,
    #[serde(default = "response_schema_version")]
    pub response_schema_version: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OwnershipEntry {
    pub author_email: String,
    pub contribution_score: f64,
    pub ownership_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct WhoOwnsData {
    pub bus_factor: u32,
    pub file_path: String,
    pub ownership: Vec<OwnershipEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_owner_email: Option<String>,
    pub top_owner_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct WhoOwnsResponse {
    pub data: WhoOwnsData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<RepoIntelligenceMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeadCodeItem {
    pub confidence: String,
    pub detector_basis: String,
    pub file_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_name: Option<String>,
    pub reason: String,
    pub repo: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeadCodeResponse {
    pub data: DeadCodeResponseData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<RepoIntelligenceMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct DeadCodeResponseData {
    pub coverage_limits: Vec<String>,
    pub findings: Vec<DeadCodeItem>,
    pub root_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ConventionItem {
    pub confidence: f64,
    pub description: String,
    pub examples: Vec<String>,
    pub pattern: String,
    pub repo: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ConventionResponse {
    pub data: ConventionResponseData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<RepoIntelligenceMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ConventionResponseData {
    pub findings: Vec<ConventionItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ModuleItem {
    pub file_count: usize,
    pub module: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OverviewResponse {
    pub data: OverviewResponseData,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<RepoIntelligenceMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct OverviewResponseData {
    pub dead_code_candidates: usize,
    pub entry_points: Vec<String>,
    pub frameworks: Vec<String>,
    pub git_history_available: bool,
    pub modules: Vec<ModuleItem>,
    pub node_counts: Vec<(String, usize)>,
    pub repo: String,
    pub top_hotspots: Vec<String>,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn who_owns_tool(
    ctx: &McpContext,
    request: WhoOwnsRequest,
) -> Result<WhoOwnsResponse, McpServerError> {
    validate_input_length("target", &request.target)?;
    let repo = resolve_repo(ctx, request.repo.as_deref())?;
    let file_path = resolve_file_target(ctx, &repo, &request.target)?;
    // Per-file ownership recompute uses the rename-aware history query
    // (`get_history_for_file_with_renames` in storage). It walks only
    // the file's contribution set instead of the entire repo's commit
    // log, so a single MCP call is bounded by the file's edit count
    // rather than the repo size. The persisted `file_analytics` row is
    // used as a fall-back when no recorded history is found (e.g. a
    // brand-new file that has not had analytics materialised yet).
    let full_summary = analyze_ownership_for_file(
        ctx.metadata(),
        &repo,
        &file_path,
        &OwnershipOptions::default(),
    )?;
    let stored_summary = ctx.metadata().get_file_analytics(&repo, &file_path)?;

    let (bus_factor, ownership, top_owner_email, top_owner_pct) =
        if let Some(summary) = full_summary {
            (
                summary.bus_factor,
                summary
                    .contributions
                    .into_iter()
                    .map(|contribution| OwnershipEntry {
                        // Redact raw email at the MCP output boundary.
                        author_email: redact_email(&contribution.author_email),
                        contribution_score: contribution.contribution_score,
                        ownership_pct: contribution.ownership_pct,
                    })
                    .collect(),
                // Redact the top_owner_email field too.
                summary.top_owner_email.as_deref().map(redact_email),
                summary.top_owner_pct,
            )
        } else if let Some(summary) = stored_summary {
            (
                u32::try_from(summary.bus_factor).unwrap_or_default(),
                summary
                    .top_owner_email
                    .as_deref()
                    .map_or_else(Vec::new, |author_email| {
                        vec![OwnershipEntry {
                            // Redact raw email at the MCP output boundary.
                            author_email: redact_email(author_email),
                            contribution_score: 0.0,
                            ownership_pct: summary.top_owner_pct,
                        }]
                    }),
                // Redact the top_owner_email field too.
                summary.top_owner_email.as_deref().map(redact_email),
                summary.top_owner_pct,
            )
        } else {
            return Err(McpServerError::InvalidInput(format!(
                "no ownership data found for `{file_path}` in repo `{repo}`"
            )));
        };

    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = WhoOwnsResponse {
        data: WhoOwnsData {
            bus_factor,
            file_path,
            ownership,
            top_owner_email,
            top_owner_pct,
        },
        meta: None,
    };
    let budget = apply_response_budget(
        BudgetedTool::RepoIntelligence,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.ownership.pop().is_some(),
    )?;
    let included = response.data.ownership.len();
    response.meta = Some(RepoIntelligenceMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

/// Complete a [`ResponseBudget`] emitted by [`apply_response_budget`] with the
/// post-truncation `items_included` count.
fn finalize_budget(mut budget: ResponseBudget, items_included: usize) -> ResponseBudget {
    budget.items_included = items_included;
    budget
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn get_dead_code_tool(
    ctx: &McpContext,
    request: DeadCodeRequest,
) -> Result<DeadCodeResponse, McpServerError> {
    let repo = resolve_repo(ctx, request.repo.as_deref())?;
    let (manifest, manifest_limits) = load_repo_package_manifest(ctx, &repo);
    let report = find_dead_code_with_manifest(ctx.graph(), &repo, manifest.as_ref())?;
    let min_confidence = request
        .min_confidence
        .as_deref()
        .map(parse_dead_code_confidence)
        .transpose()?;
    let limit = ctx
        .config
        .capped_limit(request.limit, DEFAULT_DEAD_CODE_LIMIT);
    let mut coverage_limits = report.coverage_limits;
    coverage_limits.extend(manifest_limits);
    let total_matching = report
        .findings
        .iter()
        .filter(|finding| min_confidence.is_none_or(|min_band| finding.confidence >= min_band))
        .count();
    let findings: Vec<DeadCodeItem> = report
        .findings
        .into_iter()
        .filter(|finding| min_confidence.is_none_or(|min_band| finding.confidence >= min_band))
        .take(limit)
        .map(|finding| DeadCodeItem {
            confidence: finding.confidence.as_str().to_owned(),
            detector_basis: finding.detector_basis.as_str().to_owned(),
            file_path: finding.file_path,
            package_name: finding.package_name,
            reason: finding.reason,
            repo: finding.repo,
            symbol_name: finding.symbol_name,
        })
        .collect();
    if findings.len() < total_matching {
        coverage_limits.push(format!(
            "Response truncated to {limit} of {total_matching} matching findings; pass `limit` \
             to widen."
        ));
    }
    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = DeadCodeResponse {
        data: DeadCodeResponseData {
            coverage_limits,
            findings,
            root_files: report.root_files,
        },
        meta: None,
    };
    let budget = apply_response_budget(
        BudgetedTool::RepoIntelligence,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.findings.pop().is_some() || payload.data.root_files.pop().is_some(),
    )?;
    let included = response.data.findings.len() + response.data.root_files.len();
    response.meta = Some(RepoIntelligenceMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

/// Loads `package.json` for a repo and parses it. Returns `(manifest, limits)`
/// where `limits` enumerates any read or parse failures so the caller can fold
/// them into the dead-code report's `coverage_limits` instead of silently
/// dropping zombie-dependency analysis.
///
/// A missing `package.json` is not a coverage limit — many repos legitimately
/// have no JS manifest. Only existing-but-broken files surface a warning.
fn load_repo_package_manifest(
    ctx: &McpContext,
    repo: &str,
) -> (
    Option<gather_step_parser::ParsedPackageManifest>,
    Vec<String>,
) {
    let Ok(registry) = ctx.registry_snapshot() else {
        return (None, Vec::new());
    };
    let Some(registered) = registry.repos.get(repo) else {
        return (None, Vec::new());
    };
    let manifest_path = registered.path.join("package.json");
    if !manifest_path.exists() {
        return (None, Vec::new());
    }
    let workspace_root = ctx.config.workspace_root();
    let manifest_display = relativize_to_workspace(&manifest_path, &workspace_root);
    match fs::read_to_string(&manifest_path) {
        Ok(raw) => match parse_package_manifest_str(&raw) {
            Ok(manifest) => (Some(manifest), Vec::new()),
            Err(error) => (
                None,
                vec![format!(
                    "package.json present at {manifest_display} but failed to parse ({error}); \
                     zombie-dependency detection skipped for this repo.",
                )],
            ),
        },
        Err(error) => (
            None,
            vec![format!(
                "package.json present at {manifest_display} but could not be read ({error}); \
                 zombie-dependency detection skipped for this repo.",
            )],
        ),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn get_conventions_tool(
    ctx: &McpContext,
    request: RepoScopedRequest,
) -> Result<ConventionResponse, McpServerError> {
    let repo = resolve_repo(ctx, request.repo.as_deref())?;
    let report = detect_conventions(ctx.graph(), &repo)?;
    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = ConventionResponse {
        data: ConventionResponseData {
            findings: report
                .findings
                .into_iter()
                .map(|finding| ConventionItem {
                    confidence: finding.confidence,
                    description: finding.description,
                    examples: finding.examples,
                    pattern: finding.pattern,
                    repo: finding.repo,
                })
                .collect(),
        },
        meta: None,
    };
    // Sort by confidence descending so pop drops the lowest-confidence convention first.
    response.data.findings.sort_by(|l, r| {
        r.confidence
            .partial_cmp(&l.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let budget = apply_response_budget(
        BudgetedTool::RepoIntelligence,
        request.budget_bytes,
        &mut response,
        |payload| payload.data.findings.pop().is_some(),
    )?;
    let included = response.data.findings.len();
    response.meta = Some(RepoIntelligenceMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "tool handlers deserialize owned request payloads from MCP calls"
)]
pub fn get_overview_tool(
    ctx: &McpContext,
    request: RepoScopedRequest,
) -> Result<OverviewResponse, McpServerError> {
    let repo = resolve_repo(ctx, request.repo.as_deref())?;
    let report = build_overview(ctx.graph(), ctx.metadata(), &repo)?;
    let registry = ctx.registry_snapshot()?;
    let frameworks = registry
        .repos
        .get(&repo)
        .map_or_else(Vec::new, |registered| registered.frameworks.clone());

    let generation = ctx.metadata().latest_indexed_at(None)?;
    let mut response = OverviewResponse {
        data: OverviewResponseData {
            dead_code_candidates: report.dead_code_candidates,
            entry_points: report.entry_points,
            frameworks,
            git_history_available: report.git_history_available,
            modules: report
                .modules
                .into_iter()
                .map(|module| ModuleItem {
                    file_count: module.file_count,
                    module: module.module,
                })
                .collect(),
            node_counts: report.node_counts.into_iter().collect(),
            repo: report.repo,
            top_hotspots: report.top_hotspots,
        },
        meta: None,
    };
    // Under budget pressure drop the noisiest lists first: hotspots → modules →
    // entry_points. Leaves headline counts (dead_code_candidates, framework set)
    // intact so the caller can still orient.
    let budget = apply_response_budget(
        BudgetedTool::RepoIntelligence,
        request.budget_bytes,
        &mut response,
        |payload| {
            payload.data.top_hotspots.pop().is_some()
                || payload.data.modules.pop().is_some()
                || payload.data.entry_points.pop().is_some()
        },
    )?;
    let included = response.data.top_hotspots.len()
        + response.data.modules.len()
        + response.data.entry_points.len();
    response.meta = Some(RepoIntelligenceMeta {
        budget: finalize_budget(budget, included),
        generation,
        response_schema_version: response_schema_version(),
    });
    Ok(response)
}

fn resolve_repo(ctx: &McpContext, requested: Option<&str>) -> Result<String, McpServerError> {
    let registry = ctx.registry_snapshot()?;
    if let Some(repo) = requested {
        validate_input_length("repo", repo)?;
        if registry.repos.contains_key(repo) {
            return Ok(repo.to_owned());
        }
        return Err(McpServerError::InvalidInput(format!(
            "repo `{repo}` is not registered"
        )));
    }

    if registry.repos.len() == 1
        && let Some((repo, _)) = registry.repos.iter().next()
    {
        return Ok(repo.clone());
    }

    Err(McpServerError::InvalidInput(
        "repo is required when multiple repos are registered".to_owned(),
    ))
}

fn resolve_file_target(
    ctx: &McpContext,
    repo: &str,
    target: &str,
) -> Result<String, McpServerError> {
    if target.contains('/') || target.contains('.') {
        // Even though `target` is only used as a graph lookup key (never as
        // a filesystem path), reject obvious traversal segments so error
        // messages are predictable and we don't echo `../../etc/passwd` style
        // strings back to the caller.
        if target
            .split('/')
            .any(|segment| segment == ".." || segment == ".")
        {
            return Err(McpServerError::InvalidInput(format!(
                "target `{target}` contains path traversal segments; pass a normalized path"
            )));
        }
        return Ok(target.to_owned());
    }
    let search = search_symbols(
        ctx,
        SearchRequest {
            budget_bytes: None,
            cursor: None,
            kind: None,
            language: None,
            limit: Some(10),
            query: target.to_owned(),
            repo: Some(repo.to_owned()),
        },
    )?;
    let exact_paths = search
        .data
        .results
        .into_iter()
        .filter(|item| item.exact_match && item.symbol_name == target)
        .map(|item| item.file_path)
        .collect::<std::collections::BTreeSet<_>>();
    match exact_paths.len() {
        0 => Err(McpServerError::InvalidInput(format!(
            "could not resolve `{target}` to a file in repo `{repo}`"
        ))),
        1 => Ok(exact_paths.into_iter().next().expect("set length checked")),
        _ => Err(McpServerError::InvalidInput(format!(
            "target `{target}` is ambiguous in repo `{repo}`; pass an explicit file path"
        ))),
    }
}

fn parse_dead_code_confidence(value: &str) -> Result<ConfidenceBand, McpServerError> {
    match value {
        "high" => Ok(ConfidenceBand::High),
        "medium" => Ok(ConfidenceBand::Medium),
        "low" => Ok(ConfidenceBand::Low),
        other => Err(McpServerError::InvalidInput(format!(
            "unsupported min_confidence `{other}`; expected one of: low, medium, high"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfidenceBand, parse_dead_code_confidence};
    use crate::error::McpServerError;
    use pretty_assertions::assert_eq;

    #[test]
    fn parse_dead_code_confidence_accepts_three_canonical_bands() {
        assert_eq!(
            parse_dead_code_confidence("low").expect("low parses"),
            ConfidenceBand::Low,
        );
        assert_eq!(
            parse_dead_code_confidence("medium").expect("medium parses"),
            ConfidenceBand::Medium,
        );
        assert_eq!(
            parse_dead_code_confidence("high").expect("high parses"),
            ConfidenceBand::High,
        );
    }

    #[test]
    fn parse_dead_code_confidence_rejects_unknown_strings() {
        let err = parse_dead_code_confidence("HIGH").expect_err("uppercase should not parse");
        assert!(matches!(err, McpServerError::InvalidInput(_)));
        let err = parse_dead_code_confidence("").expect_err("empty should not parse");
        assert!(matches!(err, McpServerError::InvalidInput(_)));
        let err = parse_dead_code_confidence("medi").expect_err("partial should not parse");
        assert!(matches!(err, McpServerError::InvalidInput(_)));
    }

    #[test]
    fn confidence_band_orders_low_lt_medium_lt_high() {
        // Filtering by `min_confidence` relies on this ordering, so test
        // it here too. If the enum's PartialOrd derive ever changes, the
        // dead-code MCP filter would silently flip its semantics.
        assert!(ConfidenceBand::Low < ConfidenceBand::Medium);
        assert!(ConfidenceBand::Medium < ConfidenceBand::High);
    }
}
