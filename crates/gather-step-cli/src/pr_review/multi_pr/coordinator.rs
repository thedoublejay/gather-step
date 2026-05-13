//! Coordinator for `gather-step pr-review --pr-set`.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    thread,
};

use anyhow::{Context, Result};

use crate::{
    app::AppContext,
    commands::pr_review::{OutputFormat, PrReviewRunArgs, ReviewEngine, SeverityMode, run_inner},
    pr_review::{
        delta_report::GITHUB_COMMENT_LIMIT,
        multi_pr::{
            delta_report::{
                ErroredPrReview, MultiPrDeltaReport, PerPrDeltaReport, PrReviewSetEntryStatus,
            },
            manifest::{PrEntry, PrSetManifest},
        },
    },
};

#[derive(Debug, Clone)]
pub struct PrSetRunArgs {
    pub manifest_path: PathBuf,
    pub set_id: Option<String>,
    pub parallelism: usize,
    pub engine: ReviewEngine,
    pub keep_cache: bool,
    pub cache_root: Option<PathBuf>,
    pub config: Option<PathBuf>,
    pub severity: SeverityMode,
    pub format: OutputFormat,
    pub github_comment_file: Option<PathBuf>,
    pub no_baseline_check: bool,
}

pub fn run_pr_set(app: &AppContext, args: &PrSetRunArgs) -> Result<(String, bool)> {
    let mut manifest = PrSetManifest::from_path(&args.manifest_path).with_context(|| {
        format!(
            "Loading PR-set manifest `{}`.",
            args.manifest_path.display()
        )
    })?;
    if let Some(set_id) = args.set_id.as_ref() {
        manifest.id.clone_from(set_id);
        manifest
            .validate()
            .with_context(|| "Validating the PR-set manifest after applying --set-id.")?;
    }

    let levels = dependency_levels(&manifest);
    let mut completed = Vec::new();
    let mut errors = Vec::new();
    let mut completed_ids = BTreeSet::new();
    let mut blocked_ids = BTreeSet::new();
    let mut threshold_exceeded = false;
    let run_settings = PerPrRunSettings::from_args(args);

    for level in levels {
        let mut runnable = Vec::new();
        for entry in level {
            let blocked_by: Vec<String> = entry
                .depends_on
                .iter()
                .filter(|dependency| blocked_ids.contains(*dependency))
                .cloned()
                .collect();
            if blocked_by.is_empty()
                && entry
                    .depends_on
                    .iter()
                    .all(|dependency| completed_ids.contains(dependency))
            {
                runnable.push(entry);
            } else {
                blocked_ids.insert(entry.id.clone());
                errors.push(skipped_entry(
                    &entry,
                    format!(
                        "Skipped because dependency review did not complete successfully: {}.",
                        blocked_by.join(", ")
                    ),
                ));
            }
        }

        for result in run_level(app, &run_settings, runnable, args.parallelism.max(1)) {
            match result {
                Ok(report) => {
                    threshold_exceeded |= report.threshold_exceeded;
                    completed_ids.insert(report.id.clone());
                    completed.push(report);
                }
                Err(error) => {
                    blocked_ids.insert(error.id.clone());
                    errors.push(*error);
                }
            }
        }
    }

    let report = MultiPrDeltaReport::from_parts(
        &manifest,
        Some(args.manifest_path.clone()),
        completed,
        errors,
        threshold_exceeded,
    );
    let effective_format = if app.json_output {
        OutputFormat::Json
    } else {
        args.format
    };
    let rendered = match effective_format {
        OutputFormat::Json => report.render_json()?,
        OutputFormat::GithubComment => report.render_github_comment(GITHUB_COMMENT_LIMIT),
        OutputFormat::Braingent => report.render_braingent(),
        OutputFormat::Markdown => report.render_markdown(),
    };

    if let Some(path) = args.github_comment_file.as_ref() {
        std::fs::write(path, report.render_github_comment(GITHUB_COMMENT_LIMIT)).with_context(
            || format!("Writing the PR-set GitHub comment to `{}`.", path.display()),
        )?;
    }

    Ok((rendered, report.threshold_exceeded))
}

#[derive(Debug, Clone)]
struct PerPrRunSettings {
    engine: ReviewEngine,
    keep_cache: bool,
    cache_root: Option<PathBuf>,
    config: Option<PathBuf>,
    severity: SeverityMode,
    no_baseline_check: bool,
}

impl PerPrRunSettings {
    fn from_args(args: &PrSetRunArgs) -> Self {
        Self {
            engine: args.engine,
            keep_cache: args.keep_cache,
            cache_root: args.cache_root.clone(),
            config: args.config.clone(),
            severity: args.severity,
            no_baseline_check: args.no_baseline_check,
        }
    }
}

fn run_level(
    app: &AppContext,
    settings: &PerPrRunSettings,
    entries: Vec<PrEntry>,
    parallelism: usize,
) -> Vec<Result<PerPrDeltaReport, Box<ErroredPrReview>>> {
    if entries.is_empty() {
        return Vec::new();
    }
    if parallelism <= 1 || entries.len() == 1 {
        return entries
            .into_iter()
            .map(|entry| run_one(app, settings, entry))
            .collect();
    }

    let mut results = Vec::new();
    for chunk in entries.chunks(parallelism) {
        let chunk_results = thread::scope(|scope| {
            let handles: Vec<_> = chunk
                .iter()
                .map(|entry| {
                    let entry = entry.clone();
                    let app = app.clone();
                    let settings = settings.clone();
                    scope.spawn(move || run_one(&app, &settings, entry))
                })
                .collect();

            handles
                .into_iter()
                .map(|handle| match handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(Box::new(ErroredPrReview {
                        id: "<panic>".to_owned(),
                        repo: "<unknown>".to_owned(),
                        pr: None,
                        base: String::new(),
                        head: String::new(),
                        status: PrReviewSetEntryStatus::Failed,
                        message: "PR-set worker thread panicked.".to_owned(),
                    })),
                })
                .collect::<Vec<_>>()
        });
        results.extend(chunk_results);
    }

    results
}

fn run_one(
    app: &AppContext,
    settings: &PerPrRunSettings,
    entry: PrEntry,
) -> Result<PerPrDeltaReport, Box<ErroredPrReview>> {
    let mut entry_app = app.clone();
    if let Some(repo_path) = resolve_entry_workspace(&app.workspace_path, &entry.repo) {
        entry_app.workspace_path = repo_path;
    }
    let config = settings.config.clone().or_else(|| {
        let parent_config = app.workspace_path.join("gather-step.config.yaml");
        (entry_app.workspace_path != app.workspace_path && parent_config.is_file())
            .then_some(parent_config)
    });

    let review_args = PrReviewRunArgs {
        base: entry.base.clone(),
        head: entry.head.clone(),
        engine: settings.engine,
        keep_cache: settings.keep_cache,
        cache_root: settings.cache_root.clone(),
        config,
        severity: settings.severity,
        format: OutputFormat::Json,
        github_comment_file: None,
        no_baseline_check: settings.no_baseline_check,
    };

    let (rendered, threshold_exceeded) = run_inner(&entry_app, &review_args)
        .map_err(|error| failed_entry(&entry, error.to_string()))?;
    let delta_report = serde_json::from_str(&rendered).map_err(|error| {
        failed_entry(
            &entry,
            format!("Failed to parse child DeltaReport JSON: {error}."),
        )
    })?;

    Ok(PerPrDeltaReport {
        id: entry.id,
        repo: entry.repo,
        pr: entry.pr,
        base: review_args.base,
        head: review_args.head,
        threshold_exceeded,
        delta_report,
    })
}

fn resolve_entry_workspace(parent: &Path, repo: &str) -> Option<PathBuf> {
    let repo_path = Path::new(repo);
    let candidates = if repo_path.is_absolute() {
        vec![repo_path.to_path_buf()]
    } else {
        vec![parent.join(repo_path)]
    };

    candidates.into_iter().find_map(|candidate| {
        let git_dir = candidate.join(".git");
        if git_dir.exists() {
            std::fs::canonicalize(candidate).ok()
        } else {
            None
        }
    })
}

fn failed_entry(entry: &PrEntry, message: String) -> Box<ErroredPrReview> {
    Box::new(ErroredPrReview {
        id: entry.id.clone(),
        repo: entry.repo.clone(),
        pr: entry.pr,
        base: entry.base.clone(),
        head: entry.head.clone(),
        status: PrReviewSetEntryStatus::Failed,
        message,
    })
}

fn skipped_entry(entry: &PrEntry, message: String) -> ErroredPrReview {
    ErroredPrReview {
        id: entry.id.clone(),
        repo: entry.repo.clone(),
        pr: entry.pr,
        base: entry.base.clone(),
        head: entry.head.clone(),
        status: PrReviewSetEntryStatus::Skipped,
        message,
    }
}

fn dependency_levels(manifest: &PrSetManifest) -> Vec<Vec<PrEntry>> {
    let mut entries: BTreeMap<String, PrEntry> = manifest
        .prs
        .iter()
        .cloned()
        .map(|entry| (entry.id.clone(), entry))
        .collect();
    let mut completed = BTreeSet::new();
    let mut levels = Vec::new();

    while !entries.is_empty() {
        let ready_ids: Vec<String> = entries
            .iter()
            .filter(|(_, entry)| {
                entry
                    .depends_on
                    .iter()
                    .all(|dependency| completed.contains(dependency))
            })
            .map(|(id, _)| id.clone())
            .collect();

        if ready_ids.is_empty() {
            break;
        }

        let mut level = Vec::new();
        for id in ready_ids {
            if let Some(entry) = entries.remove(&id) {
                completed.insert(id);
                level.push(entry);
            }
        }
        levels.push(level);
    }

    levels
}

#[cfg(test)]
mod tests {
    use super::{dependency_levels, resolve_entry_workspace};
    use crate::pr_review::multi_pr::manifest::{PrEntry, PrSetManifest};

    fn entry(id: &str, depends_on: Vec<&str>) -> PrEntry {
        PrEntry {
            id: id.to_owned(),
            repo: id.to_owned(),
            base: "main".to_owned(),
            head: format!("feature/{id}"),
            pr: None,
            depends_on: depends_on.into_iter().map(str::to_owned).collect(),
        }
    }

    #[test]
    fn dependency_levels_group_independent_entries() {
        let manifest = PrSetManifest {
            version: 0,
            id: "set".to_owned(),
            title: None,
            prs: vec![
                entry("api", vec![]),
                entry("web", vec!["api"]),
                entry("docs", vec![]),
            ],
        };

        let levels = dependency_levels(&manifest);

        assert_eq!(
            levels[0]
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            vec!["api", "docs"]
        );
        assert_eq!(levels[1][0].id, "web");
    }

    #[test]
    fn resolve_entry_workspace_ignores_non_git_repo_name() {
        let temp = tempfile::tempdir().expect("tempdir");

        assert!(resolve_entry_workspace(temp.path(), "api").is_none());
    }
}
