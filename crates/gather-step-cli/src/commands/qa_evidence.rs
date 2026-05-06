use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use gather_step_core::RegistryStore;
use gather_step_mcp::tools::{
    events::{TraceEventRequest, TraceRouteRequest, trace_event_tool, trace_route_tool},
    packs::{ContextPackRequest, ContextPackResponse, context_pack_tool},
};
use serde::Serialize;

use crate::app::AppContext;
use crate::command_render::RenderedCommand;
use crate::storage_context::StorageContext;

const QA_EVIDENCE_SCHEMA_VERSION: &str = "qa-evidence.v0.1";

#[derive(Debug, Args)]
pub struct QaEvidenceArgs {
    #[arg(long, help = "Read symbol registry JSON from this path")]
    pub registry: Option<PathBuf>,
    #[arg(long, help = "Read storage artifacts from this directory")]
    pub storage: Option<PathBuf>,
    #[arg(help = "Target symbol name or symbol_id")]
    pub target: Option<String>,
    #[arg(
        long,
        help = "Explicit symbol target; equivalent to the positional target"
    )]
    pub symbol: Option<String>,
    #[arg(long, help = "Resolve a route target from this HTTP method")]
    pub route_method: Option<String>,
    #[arg(long, help = "Resolve a route target from this HTTP path")]
    pub route_path: Option<String>,
    #[arg(long, help = "Resolve an event-like target from this subject")]
    pub event_target: Option<String>,
    #[arg(
        long,
        help = "Base ref for the implementation diff that Braingent owns"
    )]
    pub base: Option<String>,
    #[arg(
        long,
        help = "Head ref for the implementation diff that Braingent owns"
    )]
    pub head: Option<String>,
    #[arg(long, default_value_t = 6, help = "Maximum ranked pack items per mode")]
    pub limit: usize,
    #[arg(
        long,
        default_value_t = 2,
        help = "Traversal depth for caller/callee pack context"
    )]
    pub depth: usize,
    #[arg(long, help = "Optional response byte budget override for each pack")]
    pub budget_bytes: Option<usize>,
    #[arg(
        long,
        default_value_t = 50,
        help = "Maximum filesystem-derived feature/test evidence rows"
    )]
    pub scan_limit: usize,
}

#[derive(Debug, Serialize)]
struct QaEvidenceOutput {
    event: &'static str,
    schema_version: &'static str,
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_ref: Option<String>,
    manifest_summary: QaEvidenceSummary,
    rows: Vec<QaEvidenceRow>,
    gaps: Vec<QaEvidenceGap>,
}

#[derive(Debug, Serialize)]
struct QaEvidenceSummary {
    row_count: usize,
    gap_count: usize,
    pack_modes: Vec<&'static str>,
    truncated: bool,
    omitted_rows: usize,
    dropped_kinds: Vec<String>,
}

#[derive(Debug, Serialize)]
struct QaEvidenceRow {
    id: String,
    fact_kind: String,
    source_resolver: String,
    confidence: String,
    citation_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    repo: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    line_start: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<String>,
    surface: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct QaEvidenceGap {
    id: String,
    source_resolver: String,
    kind: String,
    message: String,
    blocks_complete_coverage: bool,
}

#[derive(Debug, Clone)]
struct FileLineEvidence {
    repo: String,
    file_path: String,
    line_start: Option<u32>,
    text: String,
}

pub fn run(app: &AppContext, args: QaEvidenceArgs) -> Result<()> {
    let ctx = if args.registry.is_some() || args.storage.is_some() {
        StorageContext::workspace_read_only_with_overrides(
            app,
            args.registry.clone(),
            args.storage.clone(),
        )
    } else {
        StorageContext::workspace_read_only(app)
    };
    run_rendered(app, &ctx, &args)?.emit(&app.output())
}

pub(crate) fn run_rendered(
    app: &AppContext,
    ctx: &StorageContext,
    args: &QaEvidenceArgs,
) -> Result<RenderedCommand> {
    let mcp_ctx = gather_step_mcp::McpContext::open(ctx.mcp_server_config())?;
    let target = resolve_target(&mcp_ctx, args)?;
    let mut rows = Vec::new();
    let mut gaps = Vec::new();
    let mut truncated = false;
    let mut omitted_rows = 0_usize;
    let mut dropped_kinds = Vec::new();

    let pack_modes = ["planning", "review", "change_impact"];
    for mode in pack_modes {
        let response = context_pack_tool(
            &mcp_ctx,
            ContextPackRequest {
                budget_bytes: args.budget_bytes,
                depth: Some(args.depth),
                limit: Some(args.limit),
                repo: app.repo_filter.clone(),
                mode: mode.to_owned(),
                target: target.clone(),
            },
        )
        .with_context(|| format!("building {mode} QA evidence pack"))?;

        collect_pack_rows(mode, &response, &mut rows, &mut gaps);
        if let Some(meta) = &response.meta {
            truncated |= meta.budget.truncated;
            omitted_rows += meta.budget.omitted_items;
            if let Some(reason) = meta.budget.omission_reason {
                let reason = format!("{reason:?}").to_ascii_lowercase();
                if !dropped_kinds.contains(&reason) {
                    dropped_kinds.push(reason);
                }
            }
            for warning in &meta.warnings {
                push_gap(&mut gaps, mode, "pack_warning", warning, false);
            }
            if meta.budget.truncated {
                push_gap(
                    &mut gaps,
                    mode,
                    "truncated",
                    "The response budget truncated evidence; Braingent must not claim complete coverage.",
                    true,
                );
            }
        }
    }

    if args.base.is_none() || args.head.is_none() {
        push_gap(
            &mut gaps,
            "git_diff",
            "missing_diff_refs",
            "Base and head refs were not supplied; changed-file coverage must come from supplied Braingent context.",
            false,
        );
    }

    let scan_rows = filesystem_evidence(app, ctx, &target, args.scan_limit, &mut gaps)?;
    rows.extend(scan_rows);

    let row_count = rows.len();
    let gap_count = gaps.len();
    let output = QaEvidenceOutput {
        event: "qa_evidence_completed",
        schema_version: QA_EVIDENCE_SCHEMA_VERSION,
        target: target.clone(),
        base_ref: args.base.clone(),
        head_ref: args.head.clone(),
        manifest_summary: QaEvidenceSummary {
            row_count,
            gap_count,
            pack_modes: pack_modes.to_vec(),
            truncated,
            omitted_rows,
            dropped_kinds,
        },
        rows,
        gaps,
    };

    let lines = vec![
        format!(
            "QA evidence [{}] for {}",
            output.schema_version, output.target
        ),
        format!(
            "Rows: {}; gaps: {}; truncated: {}.",
            output.manifest_summary.row_count,
            output.manifest_summary.gap_count,
            output.manifest_summary.truncated
        ),
    ];
    RenderedCommand::success_serialized(&output, lines)
}

fn resolve_target(ctx: &gather_step_mcp::McpContext, args: &QaEvidenceArgs) -> Result<String> {
    if args.route_method.is_some() != args.route_path.is_some() {
        bail!("--route-method and --route-path must be provided together.");
    }

    let mut provided = 0_u8;
    provided += u8::from(args.target.is_some());
    provided += u8::from(args.symbol.is_some());
    provided += u8::from(args.event_target.is_some());
    provided += u8::from(args.route_method.is_some() && args.route_path.is_some());

    if provided != 1 {
        bail!(
            "Provide exactly one target source: positional target, --symbol, --event-target, or --route-method with --route-path."
        );
    }

    if let Some(target) = args.target.as_ref().or(args.symbol.as_ref()) {
        return Ok(target.clone());
    }

    if let Some(target) = args.event_target.as_ref() {
        let response = trace_event_tool(
            ctx,
            TraceEventRequest {
                budget_bytes: None,
                limit: Some(10),
                target: target.clone(),
            },
        )?;
        let Some(match_) = response.data.matches.first() else {
            bail!("No event-like target matched `{target}`.");
        };
        return Ok(match_.target_id.clone());
    }

    let (Some(method), Some(path)) = (args.route_method.as_ref(), args.route_path.as_ref()) else {
        bail!("--route-method and --route-path must be provided together.");
    };
    let response = trace_route_tool(
        ctx,
        TraceRouteRequest {
            budget_bytes: None,
            limit: Some(10),
            method: method.clone(),
            path: path.clone(),
        },
    )?;
    let Some(target_id) = response.data.target_id else {
        bail!("No route target matched {method} {path}.");
    };
    Ok(target_id)
}

fn collect_pack_rows(
    mode: &'static str,
    response: &ContextPackResponse,
    rows: &mut Vec<QaEvidenceRow>,
    gaps: &mut Vec<QaEvidenceGap>,
) {
    for item in &response.data.items {
        let ordinal = rows.len() + 1;
        rows.push(QaEvidenceRow {
            id: format!("GS-{}-{ordinal:03}", mode_label(mode)),
            fact_kind: pack_fact_kind(mode, &item.category, &item.file_path),
            source_resolver: format!("{mode}_pack"),
            confidence: confidence_from_score(item.score),
            citation_key: citation_key(
                mode,
                item.repo.as_str(),
                item.file_path.as_str(),
                item.line_start,
            ),
            repo: Some(item.repo.clone()),
            file_path: Some(item.file_path.clone()),
            line_start: item.line_start,
            symbol_id: Some(item.symbol_id.clone()),
            symbol_kind: Some(item.symbol_kind.clone()),
            symbol_name: Some(item.symbol_name.clone()),
            category: Some(item.category.clone()),
            surface: infer_surface(
                &item.symbol_kind,
                &item.category,
                &item.file_path,
                &item.symbol_name,
            ),
            reason: item.reason.clone(),
        });
    }

    for caller in &response.data.change_impact.cross_repo_callers {
        let ordinal = rows.len() + 1;
        rows.push(QaEvidenceRow {
            id: format!("GS-IMPACT-{ordinal:03}"),
            fact_kind: "cross_repo_caller".to_owned(),
            source_resolver: format!("{mode}_pack"),
            confidence: "high".to_owned(),
            citation_key: citation_key(
                mode,
                caller.repo.as_str(),
                caller.file_path.as_str(),
                caller.line_start,
            ),
            repo: Some(caller.repo.clone()),
            file_path: Some(caller.file_path.clone()),
            line_start: caller.line_start,
            symbol_id: Some(caller.symbol_id.clone()),
            symbol_kind: Some(caller.symbol_kind.clone()),
            symbol_name: Some(caller.symbol_name.clone()),
            category: Some("caller".to_owned()),
            surface: infer_surface(
                &caller.symbol_kind,
                "caller",
                &caller.file_path,
                &caller.symbol_name,
            ),
            reason: "Upstream caller reaches the QA evidence target.".to_owned(),
        });
    }

    for repo in &response.data.change_impact.confirmed_downstream_repos {
        let ordinal = rows.len() + 1;
        rows.push(repo_impact_row(
            ordinal,
            mode,
            repo,
            "confirmed_downstream_repo",
            "high",
        ));
    }
    for repo in &response.data.change_impact.probable_downstream_repos {
        let ordinal = rows.len() + 1;
        rows.push(repo_impact_row(
            ordinal,
            mode,
            repo,
            "probable_downstream_repo",
            "medium",
        ));
    }
    for repo in &response.data.change_impact.unresolved_possible {
        push_gap(
            gaps,
            mode,
            "unresolved_downstream_repo",
            format!("Possible downstream repo `{repo}` was not fully resolved."),
            true,
        );
    }
    if let Some(truncated) = &response.data.change_impact.truncated_repos {
        push_gap(
            gaps,
            mode,
            "fanout_truncated",
            format!(
                "{} downstream repo(s) were truncated by the fan-out cap: {}.",
                truncated.count,
                truncated.names.join(", ")
            ),
            true,
        );
    }

    for gap in &response.data.unresolved_gaps {
        push_gap(gaps, mode, "unresolved_gap", gap, true);
    }
}

fn repo_impact_row(
    ordinal: usize,
    mode: &'static str,
    repo: &str,
    fact_kind: &str,
    confidence: &str,
) -> QaEvidenceRow {
    QaEvidenceRow {
        id: format!("GS-IMPACT-{ordinal:03}"),
        fact_kind: fact_kind.to_owned(),
        source_resolver: format!("{mode}_pack"),
        confidence: confidence.to_owned(),
        citation_key: format!("{mode}:{repo}:repo-impact"),
        repo: Some(repo.to_owned()),
        file_path: None,
        line_start: None,
        symbol_id: None,
        symbol_kind: None,
        symbol_name: None,
        category: Some("downstream".to_owned()),
        surface: "integration".to_owned(),
        reason: "Downstream repository should be considered for manual QA smoke coverage."
            .to_owned(),
    }
}

fn filesystem_evidence(
    app: &AppContext,
    ctx: &StorageContext,
    target: &str,
    limit: usize,
    gaps: &mut Vec<QaEvidenceGap>,
) -> Result<Vec<QaEvidenceRow>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let registry = RegistryStore::open(ctx.registry_path())
        .with_context(|| format!("opening registry at {}", ctx.registry_path().display()))?;
    let mut rows = Vec::new();

    let mut repos: Vec<_> = registry.registry().repos.iter().collect();
    repos.sort_by(|(left_name, left_repo), (right_name, right_repo)| {
        left_name.cmp(right_name).then_with(|| {
            left_repo
                .path
                .display()
                .to_string()
                .cmp(&right_repo.path.display().to_string())
        })
    });

    for (repo_name, registered) in repos {
        if app
            .repo_filter
            .as_ref()
            .is_some_and(|filter| filter != repo_name)
        {
            continue;
        }
        let repo_root = if registered.path.is_absolute() {
            registered.path.clone()
        } else {
            ctx.workspace_root().join(&registered.path)
        };
        for evidence in scan_repo_files(
            repo_name,
            &repo_root,
            target,
            limit.saturating_sub(rows.len()),
            gaps,
        )? {
            if rows.len() >= limit {
                return Ok(rows);
            }
            let fact_kind = if is_test_file(&evidence.file_path) {
                "existing_test_signal"
            } else if feature_flag_key(&evidence.text).is_some() {
                "feature_flag"
            } else {
                continue;
            };
            let confidence = if fact_kind == "feature_flag" {
                if evidence.text.contains(target) {
                    "high"
                } else if has_static_flag_key(&evidence.text) {
                    "medium"
                } else {
                    "unresolved"
                }
            } else {
                "medium"
            };
            let ordinal = rows.len() + 1;
            rows.push(QaEvidenceRow {
                id: format!("GS-SCAN-{ordinal:03}"),
                fact_kind: fact_kind.to_owned(),
                source_resolver: "workspace_scan".to_owned(),
                confidence: confidence.to_owned(),
                citation_key: citation_key(
                    "workspace_scan",
                    evidence.repo.as_str(),
                    evidence.file_path.as_str(),
                    evidence.line_start,
                ),
                repo: Some(evidence.repo),
                file_path: Some(evidence.file_path.clone()),
                line_start: evidence.line_start,
                symbol_id: None,
                symbol_kind: None,
                symbol_name: None,
                category: Some(fact_kind.to_owned()),
                surface: if fact_kind == "feature_flag" {
                    "feature_flag".to_owned()
                } else {
                    "test".to_owned()
                },
                reason: evidence.text.trim().to_owned(),
            });
        }
    }

    Ok(rows)
}

fn scan_repo_files(
    repo: &str,
    root: &Path,
    target: &str,
    limit: usize,
    gaps: &mut Vec<QaEvidenceGap>,
) -> Result<Vec<FileLineEvidence>> {
    let mut evidence = Vec::new();
    scan_dir(repo, root, root, target, limit, &mut evidence, gaps)?;
    Ok(evidence)
}

fn scan_dir(
    repo: &str,
    root: &Path,
    dir: &Path,
    target: &str,
    limit: usize,
    evidence: &mut Vec<FileLineEvidence>,
    gaps: &mut Vec<QaEvidenceGap>,
) -> Result<()> {
    if evidence.len() >= limit || is_ignored_dir(dir) {
        return Ok(());
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) => {
            push_gap(
                gaps,
                "workspace_scan",
                "scan_unreadable_dir",
                format!("Could not read directory `{}`: {error}.", dir.display()),
                true,
            );
            return Ok(());
        }
    };
    let mut entries = entries.collect::<std::result::Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.path());

    for entry in entries {
        if evidence.len() >= limit {
            break;
        }
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            scan_dir(repo, root, &path, target, limit, evidence, gaps)?;
        } else if file_type.is_file() && should_scan_file(&path) {
            scan_file(repo, root, &path, target, limit, evidence, gaps)?;
        } else if should_scan_file(&path) {
            push_gap(
                gaps,
                "workspace_scan",
                "scan_unsupported_file_type",
                format!("Skipped non-regular file `{}`.", path.display()),
                true,
            );
        }
    }
    Ok(())
}

fn scan_file(
    repo: &str,
    root: &Path,
    path: &Path,
    target: &str,
    limit: usize,
    evidence: &mut Vec<FileLineEvidence>,
    gaps: &mut Vec<QaEvidenceGap>,
) -> Result<()> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => {
            push_gap(
                gaps,
                "workspace_scan",
                "scan_unreadable_file",
                format!("Could not read file `{}`: {error}.", path.display()),
                true,
            );
            return Ok(());
        }
    };
    let Ok(relative) = path.strip_prefix(root) else {
        return Ok(());
    };
    let relative = relative.display().to_string();
    let test_file = is_test_file(&relative);
    for (index, line) in text.lines().enumerate() {
        if evidence.len() >= limit {
            break;
        }
        let has_target_test = test_file && line.contains(target);
        let has_flag = is_feature_flag_line(line);
        if has_target_test || has_flag {
            evidence.push(FileLineEvidence {
                repo: repo.to_owned(),
                file_path: relative.clone(),
                line_start: u32::try_from(index + 1).ok(),
                text: line.to_owned(),
            });
        }
    }
    Ok(())
}

fn is_ignored_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git" | "node_modules" | "target" | ".gather-step" | "dist" | "build" | ".next"
            )
        })
}

fn should_scan_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| {
            matches!(
                ext,
                "ts" | "tsx" | "js" | "jsx" | "rs" | "py" | "go" | "java" | "kt" | "cs"
            )
        })
}

fn is_feature_flag_line(line: &str) -> bool {
    [
        "variation(",
        "isEnabled(",
        "getVariant(",
        "useFlag(",
        "@flag(",
    ]
    .iter()
    .any(|pattern| line.contains(pattern))
}

fn feature_flag_key(line: &str) -> Option<&str> {
    if !is_feature_flag_line(line) {
        return None;
    }
    line.split(['"', '\'']).nth(1)
}

fn has_static_flag_key(line: &str) -> bool {
    feature_flag_key(line).is_some_and(|key| !key.trim().is_empty())
}

fn is_test_file(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.contains(".test.")
        || lower.contains(".spec.")
        || lower.contains("_test.")
        || lower.contains("/tests/")
        || lower.contains("\\tests\\")
}

fn pack_fact_kind(mode: &str, category: &str, file_path: &str) -> String {
    if is_test_file(file_path) {
        return "existing_test_signal".to_owned();
    }
    match mode {
        "review" => "changed_behavior_candidate".to_owned(),
        "change_impact" => "impact_candidate".to_owned(),
        "planning" => "planning_context".to_owned(),
        _ => category.to_owned(),
    }
}

fn infer_surface(symbol_kind: &str, category: &str, file_path: &str, symbol_name: &str) -> String {
    let text = format!("{symbol_kind} {category} {file_path} {symbol_name}").to_ascii_lowercase();
    if text.contains("test") || is_test_file(file_path) {
        "test".to_owned()
    } else if text.contains("route")
        || text.contains("controller")
        || text.contains("endpoint")
        || text.contains("api")
    {
        "api".to_owned()
    } else if text.contains("event")
        || text.contains("topic")
        || text.contains("queue")
        || text.contains("consumer")
    {
        "event".to_owned()
    } else if text.contains("dto")
        || text.contains("schema")
        || text.contains("payload")
        || text.contains("contract")
    {
        "contract".to_owned()
    } else if text.contains("component")
        || text.contains("page")
        || text.contains("tsx")
        || text.contains("jsx")
    {
        "ui".to_owned()
    } else {
        "code".to_owned()
    }
}

fn confidence_from_score(score: u16) -> String {
    match score {
        850..=u16::MAX => "high",
        650..=849 => "medium",
        1..=649 => "low",
        0 => "unresolved",
    }
    .to_owned()
}

fn citation_key(mode: &str, repo: &str, file_path: &str, line_start: Option<u32>) -> String {
    match line_start {
        Some(line) => format!("{mode}:{repo}:{file_path}:{line}"),
        None => format!("{mode}:{repo}:{file_path}"),
    }
}

fn mode_label(mode: &str) -> &'static str {
    match mode {
        "planning" => "PLAN",
        "review" => "REVIEW",
        "change_impact" => "IMPACT",
        _ => "PACK",
    }
}

fn push_gap(
    gaps: &mut Vec<QaEvidenceGap>,
    source_resolver: impl Into<String>,
    kind: impl Into<String>,
    message: impl Into<String>,
    blocks_complete_coverage: bool,
) {
    let ordinal = gaps.len() + 1;
    gaps.push(QaEvidenceGap {
        id: format!("GS-GAP-{ordinal:03}"),
        source_resolver: source_resolver.into(),
        kind: kind.into(),
        message: message.into(),
        blocks_complete_coverage,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helper_functions_keep_schema_visible_values_stable() {
        assert!(is_feature_flag_line(
            "const enabled = useFlag('orders-v2');"
        ));
        assert_eq!(
            feature_flag_key("const enabled = useFlag('orders-v2');"),
            Some("orders-v2")
        );
        assert!(has_static_flag_key("variation(\"orders-v2\", false);"));
        assert!(is_test_file("src/OrderList.test.tsx"));
        assert!(is_test_file("src/tests/order_list.rs"));

        assert_eq!(
            pack_fact_kind("planning", "component", "src/OrderList.tsx"),
            "planning_context"
        );
        assert_eq!(
            pack_fact_kind("review", "component", "src/OrderList.test.tsx"),
            "existing_test_signal"
        );
        assert_eq!(
            infer_surface(
                "class",
                "controller",
                "src/orders.controller.ts",
                "OrdersController"
            ),
            "api"
        );
        assert_eq!(
            infer_surface("function", "component", "src/OrderList.tsx", "OrderList"),
            "ui"
        );

        assert_eq!(confidence_from_score(900), "high");
        assert_eq!(confidence_from_score(700), "medium");
        assert_eq!(confidence_from_score(200), "low");
        assert_eq!(confidence_from_score(0), "unresolved");
        assert_eq!(
            citation_key("planning", "frontend", "src/OrderList.tsx", Some(12)),
            "planning:frontend:src/OrderList.tsx:12"
        );
        assert_eq!(
            citation_key("planning", "frontend", "src/OrderList.tsx", None),
            "planning:frontend:src/OrderList.tsx"
        );
        assert_eq!(mode_label("planning"), "PLAN");
        assert_eq!(mode_label("review"), "REVIEW");
        assert_eq!(mode_label("change_impact"), "IMPACT");
        assert_eq!(mode_label("other"), "PACK");
    }
}
