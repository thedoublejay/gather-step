use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand, ValueEnum};
use gather_step_core::{RegistryStore, WorkspaceRegistry};
use gather_step_output::{
    ClaudeMdOptions, generate_rule_files, render_workspace_summary_agents,
    render_workspace_summary_claude,
};
use gather_step_storage::{GraphStoreDb, MetadataStore, MetadataStoreDb};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;

use crate::app::AppContext;

#[derive(Debug, Args)]
pub struct GenerateCommand {
    #[command(subcommand)]
    pub command: GenerateSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum GenerateSubcommand {
    ClaudeMd(GenerateClaudeMdArgs),
    AgentsMd(GenerateAgentsMdArgs),
    Codeowners(GenerateCodeownersArgs),
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum ClaudeMdTarget {
    /// Generate `.claude/rules/*.md` from the indexed graph.
    #[default]
    Rules,
    /// Generate `CLAUDE.gather.md` from the workspace registry.
    Summary,
}

#[derive(Debug, Args)]
pub struct GenerateClaudeMdArgs {
    #[arg(long, help = "Optional explicit output file or directory")]
    pub output: Option<PathBuf>,
    #[arg(long, help = "Limit graph-backed rule output to one repo")]
    pub repo: Option<String>,
    #[arg(long, value_enum, default_value = "rules")]
    pub target: ClaudeMdTarget,
}

#[derive(Debug, Args)]
pub struct GenerateAgentsMdArgs {
    #[arg(long, help = "Optional explicit output path")]
    pub output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct GenerateCodeownersArgs {
    #[arg(long, help = "Optional explicit output path")]
    pub output: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct GenerateOutput {
    event: &'static str,
    files: Vec<GeneratedFileOutput>,
}

#[derive(Debug, Serialize)]
struct GeneratedFileOutput {
    path: String,
    bytes: usize,
}

pub fn run(app: &AppContext, command: GenerateCommand) -> Result<()> {
    match command.command {
        GenerateSubcommand::ClaudeMd(args) => run_claude_md(app, args),
        GenerateSubcommand::AgentsMd(args) => run_agents_md(app, args),
        GenerateSubcommand::Codeowners(args) => run_codeowners(app, args),
    }
}

fn run_claude_md(app: &AppContext, args: GenerateClaudeMdArgs) -> Result<()> {
    match args.target {
        ClaudeMdTarget::Rules => run_claude_md_rules(app, args),
        ClaudeMdTarget::Summary => run_claude_md_summary(app, args),
    }
}

fn run_claude_md_rules(app: &AppContext, args: GenerateClaudeMdArgs) -> Result<()> {
    let output = app.output();
    let repo_filter = args.repo.or_else(|| app.repo_filter.clone());
    let paths = app.workspace_paths();
    let registry = RegistryStore::open(&paths.registry_path)
        .with_context(|| format!("opening {}", paths.registry_path.display()))?;
    if let Some(repo) = repo_filter.as_deref()
        && registry.registry().repo(repo).is_none()
    {
        bail!("repo `{repo}` is not present in the workspace registry");
    }
    let graph = GraphStoreDb::open(&paths.graph_path)
        .with_context(|| format!("opening {}", paths.graph_path.display()))?;
    let metadata_path = paths.storage_root.join("metadata.sqlite");
    let metadata = MetadataStoreDb::open(&metadata_path)
        .with_context(|| format!("opening {}", metadata_path.display()))?;
    let files = generate_rule_files(
        &graph,
        Some(&metadata),
        registry.registry(),
        &ClaudeMdOptions {
            repo_filter,
            workspace_root: Some(app.workspace_path.clone()),
            ..ClaudeMdOptions::default()
        },
    )?;

    let written = if let Some(output_path) = args.output {
        write_explicit_output(&output_path, &files)?
    } else {
        write_default_outputs(&app.workspace_path, &files)?
    };

    let payload = GenerateOutput {
        event: "generate_claude_md_completed",
        files: written,
    };

    output.emit(&payload)?;
    for file in &payload.files {
        output.line(format!("Wrote {}", file.path));
    }

    Ok(())
}

fn run_claude_md_summary(app: &AppContext, args: GenerateClaudeMdArgs) -> Result<()> {
    if args.repo.is_some() || app.repo_filter.is_some() {
        bail!("--repo is only supported by `generate claude-md --target=rules`");
    }

    let output = app.output();
    let paths = app.workspace_paths();
    let registry = RegistryStore::open(&paths.registry_path)
        .with_context(|| format!("opening {}", paths.registry_path.display()))?;
    let content = render_workspace_summary_claude(registry.registry(), env!("CARGO_PKG_VERSION"));
    let target = args
        .output
        .unwrap_or_else(|| app.workspace_path.join("CLAUDE.gather.md"));
    let written = write_text_output(&target, &content)?;

    let payload = GenerateOutput {
        event: "generate_claude_md_completed",
        files: vec![written],
    };
    output.emit(&payload)?;
    for file in &payload.files {
        output.line(format!("Wrote {}", file.path));
    }
    Ok(())
}

fn run_agents_md(app: &AppContext, args: GenerateAgentsMdArgs) -> Result<()> {
    let output = app.output();
    let paths = app.workspace_paths();
    let registry = RegistryStore::open(&paths.registry_path)
        .with_context(|| format!("opening {}", paths.registry_path.display()))?;
    let content = render_workspace_summary_agents(registry.registry(), env!("CARGO_PKG_VERSION"));
    let target = args
        .output
        .unwrap_or_else(|| app.workspace_path.join("AGENTS.gather.md"));
    let written = write_text_output(&target, &content)?;

    let payload = GenerateOutput {
        event: "generate_agents_md_completed",
        files: vec![written],
    };
    output.emit(&payload)?;
    for file in &payload.files {
        output.line(format!("Wrote {}", file.path));
    }
    Ok(())
}

pub fn run_summary_pair(app: &AppContext) -> Result<()> {
    let output = app.output();
    let paths = app.workspace_paths();
    let metadata_path = paths.storage_root.join("metadata.sqlite");
    let generation_bar = ai_generation_bar(app);

    if paths.graph_path.exists() && metadata_path.exists() {
        run_claude_md_rules(
            app,
            GenerateClaudeMdArgs {
                output: None,
                repo: None,
                target: ClaudeMdTarget::Rules,
            },
        )?;
    } else {
        output.line("warning: skipped .claude/rules/ generation because no workspace index exists");
        output.line(
            "hint: run `gather-step index`, then `gather-step generate claude-md --target rules`",
        );
    }
    if let Some(bar) = &generation_bar {
        bar.inc(1);
        bar.set_message("Generating CLAUDE.gather.md...");
    }

    run_claude_md_summary(
        app,
        GenerateClaudeMdArgs {
            output: None,
            repo: None,
            target: ClaudeMdTarget::Summary,
        },
    )?;
    if let Some(bar) = &generation_bar {
        bar.inc(1);
        bar.set_message("Generating AGENTS.gather.md...");
    }
    run_agents_md(app, GenerateAgentsMdArgs { output: None })?;
    if let Some(bar) = generation_bar {
        bar.inc(1);
        bar.finish_and_clear();
    }
    Ok(())
}

fn ai_generation_bar(app: &AppContext) -> Option<ProgressBar> {
    app.progress_is_visible().then(|| {
        let bar = app.multi_progress.add(ProgressBar::new(3));
        bar.set_style(
            ProgressStyle::with_template(
                " {spinner:.cyan.bold} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len}  {msg}",
            )
            .expect("AI context progress template is valid")
            .progress_chars("█░░")
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        bar.set_message("Generating AI context files...");
        bar.enable_steady_tick(std::time::Duration::from_millis(80));
        bar
    })
}

fn run_codeowners(app: &AppContext, args: GenerateCodeownersArgs) -> Result<()> {
    let output = app.output();
    let paths = app.workspace_paths();
    let registry = RegistryStore::open(&paths.registry_path)
        .with_context(|| format!("opening {}", paths.registry_path.display()))?;
    let metadata_path = paths.storage_root.join("metadata.sqlite");
    let metadata = MetadataStoreDb::open(&metadata_path)
        .with_context(|| format!("opening {}", metadata_path.display()))?;
    let content = render_codeowners(&app.workspace_path, registry.registry(), &metadata)?;

    let target = args
        .output
        .unwrap_or_else(|| app.workspace_path.join("CODEOWNERS"));
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    fs::write(&target, &content).with_context(|| format!("writing {}", target.display()))?;

    let payload = GenerateOutput {
        event: "generate_codeowners_completed",
        files: vec![GeneratedFileOutput {
            path: target.display().to_string(),
            bytes: content.len(),
        }],
    };
    output.emit(&payload)?;
    output.line(format!("Wrote {}", target.display()));
    Ok(())
}

fn write_default_outputs(
    workspace_root: &std::path::Path,
    files: &[gather_step_output::RuleFile],
) -> Result<Vec<GeneratedFileOutput>> {
    let mut written = Vec::with_capacity(files.len());
    for file in files {
        let path = workspace_root.join(&file.relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::write(&path, &file.content).with_context(|| format!("writing {}", path.display()))?;
        written.push(GeneratedFileOutput {
            path: path.display().to_string(),
            bytes: file.content.len(),
        });
    }
    Ok(written)
}

fn write_explicit_output(
    output_path: &std::path::Path,
    files: &[gather_step_output::RuleFile],
) -> Result<Vec<GeneratedFileOutput>> {
    if files.is_empty() {
        return Ok(Vec::new());
    }

    match classify_output_path(output_path, files.len())? {
        ExplicitOutputTarget::File => {
            if let Some(parent) = output_path.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            fs::write(output_path, &files[0].content)
                .with_context(|| format!("writing {}", output_path.display()))?;
            Ok(vec![GeneratedFileOutput {
                path: output_path.display().to_string(),
                bytes: files[0].content.len(),
            }])
        }
        ExplicitOutputTarget::Directory => {
            fs::create_dir_all(output_path)
                .with_context(|| format!("creating {}", output_path.display()))?;
            let mut written = Vec::with_capacity(files.len());
            for file in files {
                let target = output_path.join(
                    std::path::Path::new(&file.relative_path)
                        .file_name()
                        .expect("generated file should have a file name"),
                );
                fs::write(&target, &file.content)
                    .with_context(|| format!("writing {}", target.display()))?;
                written.push(GeneratedFileOutput {
                    path: target.display().to_string(),
                    bytes: file.content.len(),
                });
            }
            Ok(written)
        }
    }
}

fn write_text_output(output_path: &Path, content: &str) -> Result<GeneratedFileOutput> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    fs::write(output_path, content)
        .with_context(|| format!("writing {}", output_path.display()))?;
    Ok(GeneratedFileOutput {
        path: output_path.display().to_string(),
        bytes: content.len(),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExplicitOutputTarget {
    File,
    Directory,
}

fn classify_output_path(
    output_path: &Path,
    generated_files: usize,
) -> Result<ExplicitOutputTarget> {
    if generated_files == 0 {
        return Ok(ExplicitOutputTarget::Directory);
    }

    if output_path.exists() {
        let metadata = fs::metadata(output_path)
            .with_context(|| format!("reading {}", output_path.display()))?;
        if metadata.is_dir() {
            return Ok(ExplicitOutputTarget::Directory);
        }
        if generated_files > 1 {
            bail!(
                "explicit file output requires a single generated file; use a directory path instead"
            );
        }
        return Ok(ExplicitOutputTarget::File);
    }

    if generated_files > 1 {
        if path_looks_like_file(output_path) {
            bail!(
                "explicit file output requires a single generated file; use a directory path instead"
            );
        }
        return Ok(ExplicitOutputTarget::Directory);
    }

    if path_ends_with_separator(output_path) {
        return Ok(ExplicitOutputTarget::Directory);
    }

    Ok(ExplicitOutputTarget::File)
}

fn path_ends_with_separator(path: &Path) -> bool {
    let path = path.to_string_lossy();
    path.ends_with(std::path::MAIN_SEPARATOR) || path.ends_with('/') || path.ends_with('\\')
}

fn path_looks_like_file(path: &Path) -> bool {
    !path_ends_with_separator(path) && path.extension().is_some()
}

fn render_codeowners(
    workspace_root: &Path,
    registry: &WorkspaceRegistry,
    metadata: &impl MetadataStore,
) -> Result<String> {
    let mut lines = vec![
        "# CODEOWNERS generated by gather-step".to_owned(),
        "# Ownership is derived from indexed git analytics.".to_owned(),
        String::new(),
    ];
    let mut entries = BTreeMap::<String, String>::new();

    for (repo_name, repo) in &registry.repos {
        let repo_relative = repo
            .path
            .strip_prefix(workspace_root)
            .unwrap_or(repo.path.as_path())
            .to_string_lossy()
            .replace('\\', "/");
        for analytics in metadata.list_file_analytics_for_repo(repo_name)? {
            let Some(owner) = analytics.top_owner_email else {
                continue;
            };
            if analytics.top_owner_pct <= 0.0 {
                continue;
            }
            entries.insert(
                format!(
                    "/{repo_relative}/{}",
                    analytics.file_path.replace('\\', "/")
                ),
                owner,
            );
        }
    }

    if entries.is_empty() {
        bail!("no ownership data available to generate CODEOWNERS");
    }

    for (path, owner) in entries {
        lines.push(format!("{path} {owner}"));
    }

    Ok(lines.join("\n") + "\n")
}

#[cfg(test)]
mod tests {
    use gather_step_core::{DepthLevel, RegisteredRepo, WorkspaceRegistry};
    use gather_step_storage::{FileAnalytics, MetadataStore, MetadataStoreDb};

    use super::{ExplicitOutputTarget, classify_output_path, render_codeowners};
    use std::{
        collections::BTreeMap,
        env, fs,
        path::{Path, PathBuf},
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> Self {
            let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "gather-step-generate-{name}-{}-{id}",
                process::id()
            ));
            fs::create_dir_all(&path).expect("test dir should exist");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn classify_nonexistent_extensionless_single_output_as_file() {
        let temp = TestDir::new("single-file");
        let target = temp.path().join("CLAUDE");
        assert_eq!(
            classify_output_path(&target, 1).expect("classification should succeed"),
            ExplicitOutputTarget::File
        );
    }

    #[test]
    fn classify_existing_directory_as_directory() {
        let temp = TestDir::new("existing-dir");
        let target = temp.path().join("rules");
        fs::create_dir_all(&target).expect("rules dir should exist");
        assert_eq!(
            classify_output_path(&target, 1).expect("classification should succeed"),
            ExplicitOutputTarget::Directory
        );
    }

    #[test]
    fn reject_multi_file_output_to_existing_file() {
        let temp = TestDir::new("existing-file");
        let target = temp.path().join("output.md");
        fs::write(&target, "existing").expect("output file should exist");
        let error = classify_output_path(&target, 2).expect_err("classification should fail");
        assert!(
            error
                .to_string()
                .contains("explicit file output requires a single generated file")
        );
    }

    #[test]
    fn reject_multi_file_output_to_nonexistent_file_like_path() {
        let temp = TestDir::new("file-like-output");
        let target = temp.path().join("CLAUDE.md");
        let error = classify_output_path(&target, 2).expect_err("classification should fail");
        assert!(
            error
                .to_string()
                .contains("explicit file output requires a single generated file")
        );
    }

    #[test]
    fn classify_nonexistent_extensionless_multi_output_as_directory() {
        let temp = TestDir::new("multi-dir");
        let target = temp.path().join("rules");
        assert_eq!(
            classify_output_path(&target, 2).expect("classification should succeed"),
            ExplicitOutputTarget::Directory
        );
    }

    #[test]
    fn render_codeowners_uses_top_owner_entries_per_file() {
        let temp = TestDir::new("codeowners");
        let metadata = MetadataStoreDb::open(temp.path().join("metadata.sqlite")).expect("open");
        metadata
            .replace_file_analytics_for_repo(
                "backend_standard",
                &[FileAnalytics {
                    repo: "backend_standard".to_owned(),
                    file_path: "src/controller.ts".to_owned(),
                    total_commits: 2,
                    commits_90d: 2,
                    commits_180d: 2,
                    commits_365d: 2,
                    hotspot_score: 1.0,
                    bus_factor: 1,
                    top_owner_email: Some("owner@example.com".to_owned()),
                    top_owner_pct: 0.8,
                    complexity_trend: None,
                    last_modified: 1,
                    computed_at: 1,
                }],
            )
            .expect("analytics");
        let registry = WorkspaceRegistry {
            version: 1,
            repos: BTreeMap::from([(
                "backend_standard".to_owned(),
                RegisteredRepo::new(temp.path().join("apps/backend_standard"), DepthLevel::Full),
            )]),
        };

        let rendered =
            render_codeowners(temp.path(), &registry, &metadata).expect("render should work");
        assert!(rendered.contains("/apps/backend_standard/src/controller.ts owner@example.com"));
    }
}
