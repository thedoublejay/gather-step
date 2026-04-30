use std::{
    collections::BTreeSet,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use console::style;
use gather_step_core::{GatherStepConfig, IndexingConfig, RepoConfig};
use serde::Serialize;

use crate::{
    app::AppContext,
    commands::{generate, index, setup_mcp, watch},
    path_safety,
    path_safety::PathSafetyError,
};

#[expect(
    clippy::struct_excessive_bools,
    reason = "CLI flags are independent switches and clap requires explicit fields"
)]
#[derive(Debug, Args, Default)]
pub struct InitArgs {
    #[arg(
        long,
        help = "Write the config to this path instead of the workspace default"
    )]
    pub config: Option<PathBuf>,
    #[arg(long, help = "Overwrite an existing config file")]
    pub force: bool,
    #[arg(long, help = "Index discovered repos after writing the config")]
    pub index: bool,
    #[arg(long = "no-index", help = "Skip indexing", conflicts_with = "index")]
    pub no_index: bool,
    #[arg(long, help = "Start watch mode after indexing")]
    pub watch: bool,
    #[arg(long = "no-watch", conflicts_with = "watch")]
    pub no_watch: bool,
    #[arg(
        long,
        help = "Generate .claude/rules/, CLAUDE.gather.md, and AGENTS.gather.md"
    )]
    pub generate_ai_files: bool,
    #[arg(long = "no-generate-ai-files", conflicts_with = "generate_ai_files")]
    pub no_generate_ai_files: bool,
    #[arg(long, value_enum)]
    pub setup_mcp: Option<setup_mcp::McpScope>,
}

#[derive(Debug, Serialize)]
struct InitOutput {
    event: &'static str,
    config_path: String,
    repo_count: usize,
    repos: Vec<InitRepoOutput>,
}

#[derive(Debug, Serialize)]
struct InitRepoOutput {
    name: String,
    path: String,
}

#[derive(Clone, Debug)]
pub struct DiscoveredRepo {
    pub name: String,
    pub relative_path: String,
}

pub async fn run(app: &AppContext, args: InitArgs) -> Result<()> {
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| app.workspace_paths().config_path);

    if config_path.exists() && !args.force {
        bail!(
            "config already exists at {}\nhint: pass --force to overwrite",
            config_path.display()
        );
    }

    if app.is_interactive() {
        run_wizard(app, args).await
    } else {
        run_non_interactive(app, args).await
    }
}

async fn run_non_interactive(app: &AppContext, args: InitArgs) -> Result<()> {
    write_default_config(app, &args)?;
    let output = app.output();

    if args.index && !args.no_index {
        index::run(app, init_index_args()).await?;
    }
    if args.generate_ai_files && !args.no_generate_ai_files {
        generate::run_summary_pair(app)?;
    }
    if let Some(scope) = args.setup_mcp {
        setup_mcp::run(app, setup_mcp::SetupMcpArgs { scope })?;
    }
    if args.watch && !args.no_watch {
        output.line(format!(
            "\n  {} Gather Step is ready.",
            style("✓ Setup complete.").green().bold()
        ));
        watch::run(app, watch::WatchArgs::default()).await?;
    }

    Ok(())
}

async fn run_wizard(app: &AppContext, args: InitArgs) -> Result<()> {
    let repos = discover_git_repos(&app.workspace_path)?;

    let output = app.output();
    output.line(format!(
        "\n  {}",
        style("Gather Step workspace setup").bold()
    ));
    output.line(format!(
        "  Found {} git repo(s) in {}",
        style(repos.len()).cyan().bold(),
        style(app.workspace_path.display()).dim()
    ));
    for repo in &repos {
        output.line(format!(
            "    {} {}",
            style(&repo.name).cyan(),
            style(format!("→ {}", repo.relative_path)).dim()
        ));
    }

    let do_index = if args.index {
        true
    } else if args.no_index {
        false
    } else {
        prompt_yes_no("Index these repos now?", true)?
    };
    let do_ai = if args.generate_ai_files {
        true
    } else if args.no_generate_ai_files {
        false
    } else {
        prompt_yes_no(
            "Generate AI context files (.claude/rules/, CLAUDE.gather.md, AGENTS.gather.md)?",
            true,
        )?
    };
    let scope = match args.setup_mcp {
        Some(scope) => Some(scope),
        None => prompt_mcp_scope()?,
    };
    let do_watch = if args.watch {
        true
    } else if args.no_watch {
        false
    } else {
        prompt_yes_no("Watch for changes and re-index automatically?", false)?
    };

    write_default_config_with_repos(app, &args, repos)?;

    if do_index {
        index::run(app, init_index_args()).await?;
    }
    if do_ai {
        generate::run_summary_pair(app)?;
    }
    if let Some(scope) = scope {
        setup_mcp::run(app, setup_mcp::SetupMcpArgs { scope })?;
    }
    output.line(format!(
        "\n  {} gather-step is ready.",
        style("✓ Setup complete.").green().bold()
    ));
    if do_watch {
        watch::run(app, watch::WatchArgs::default()).await?;
    }

    Ok(())
}

fn init_index_args() -> index::IndexArgs {
    index::IndexArgs {
        auto_recover: true,
        ..index::IndexArgs::default()
    }
}

fn write_default_config(app: &AppContext, args: &InitArgs) -> Result<()> {
    let repos = discover_git_repos(&app.workspace_path)?;
    write_default_config_with_repos(app, args, repos)
}

fn write_default_config_with_repos(
    app: &AppContext,
    args: &InitArgs,
    repos: Vec<DiscoveredRepo>,
) -> Result<()> {
    let output = app.output();
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(|| app.workspace_paths().config_path);

    if repos.is_empty() {
        bail!(
            "No git repositories found under {}",
            app.workspace_path.display()
        );
    }

    let config = GatherStepConfig {
        allow_listed_repos: Vec::new(),
        repos: repos
            .iter()
            .map(|repo| RepoConfig {
                name: repo.name.clone(),
                path: repo.relative_path.clone(),
                depth: None,
            })
            .collect(),
        github: None,
        jira: None,
        indexing: IndexingConfig::default(),
    };

    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    fs::write(&config_path, serde_yaml_ng::to_string(&config)?)
        .with_context(|| format!("writing {}", config_path.display()))?;

    let payload = InitOutput {
        event: "init_completed",
        config_path: config_path.display().to_string(),
        repo_count: repos.len(),
        repos: repos
            .into_iter()
            .map(|repo| InitRepoOutput {
                name: repo.name,
                path: repo.relative_path,
            })
            .collect(),
    };

    output.emit(&payload)?;
    output.line(format!("Wrote {}", payload.config_path));
    output.line(format!("Detected {} repo(s)", payload.repo_count));
    for repo in payload.repos {
        output.line(format!("  {} -> {}", repo.name, repo.path));
    }

    Ok(())
}

fn prompt_yes_no(message: &str, default: bool) -> Result<bool> {
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    let mut stdout = io::stdout().lock();
    write!(stdout, "{message} {suffix} ")?;
    stdout.flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    match answer.trim() {
        "y" | "Y" | "yes" | "YES" | "Yes" => Ok(true),
        "n" | "N" | "no" | "NO" | "No" => Ok(false),
        _ => Ok(default),
    }
}

fn prompt_mcp_scope() -> Result<Option<setup_mcp::McpScope>> {
    let mut stdout = io::stdout().lock();
    write!(
        stdout,
        "Register as an MCP server? [local/global/skip] (default: local) "
    )?;
    stdout.flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(match answer.trim() {
        "global" => Some(setup_mcp::McpScope::Global),
        "local" | "" => Some(setup_mcp::McpScope::Local),
        _ => None,
    })
}

/// Thin wrapper around the internal git-repo discovery used by [`run`].
///
/// Exposed so that integration tests can drive the discovery logic with
/// crafted directory layouts (including symlink escapes) without invoking the
/// full `init` command pipeline.
pub fn discover_git_repos_for_test(workspace_root: &Path) -> Result<Vec<DiscoveredRepo>> {
    discover_git_repos(workspace_root)
}

fn discover_git_repos(workspace_root: &Path) -> Result<Vec<DiscoveredRepo>> {
    let mut repos = Vec::new();
    let mut names = BTreeSet::new();
    walk_for_git_repos(workspace_root, workspace_root, &mut repos, &mut names)?;
    let has_root_repo = repos.iter().any(|repo| repo.relative_path == ".");
    let has_nested_repos = repos.iter().any(|repo| repo.relative_path != ".");
    if has_root_repo && has_nested_repos {
        repos.retain(|repo| repo.relative_path != ".");
    }
    repos.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(repos)
}

fn walk_for_git_repos(
    workspace_root: &Path,
    current: &Path,
    repos: &mut Vec<DiscoveredRepo>,
    names: &mut BTreeSet<String>,
) -> Result<()> {
    let git_path = current.join(".git");

    // Use symlink_metadata so we detect symlinks without following them.
    if let Ok(meta) = fs::symlink_metadata(&git_path)
        && meta.file_type().is_symlink()
    {
        // Resolve the symlink target and verify it stays inside the
        // workspace.  If the target escapes, bail with a clear error
        // rather than silently indexing foreign state.
        let target = fs::read_link(&git_path)
            .with_context(|| format!("reading symlink target of {}", git_path.display()))?;
        let canonical_root =
            path_safety::canonical_workspace_root(workspace_root).with_context(|| {
                format!("canonicalizing workspace root {}", workspace_root.display())
            })?;
        // Resolve the target relative to the containing directory.
        let containing = git_path.parent().unwrap_or(workspace_root);
        let resolved = if target.is_absolute() {
            target.clone()
        } else {
            containing.join(&target)
        };
        path_safety::canonicalize_inside_workspace(&resolved, &canonical_root)
            .map_err(|_| PathSafetyError::GitSymlinkEscape {
                link_path: git_path.clone(),
                target: target.clone(),
            })
            .map_err(anyhow::Error::from)?;
    }

    if git_path.exists() {
        repos.push(DiscoveredRepo {
            name: unique_repo_name(workspace_root, current, names),
            relative_path: relative_repo_path(workspace_root, current)?,
        });
        if current != workspace_root {
            return Ok(());
        }
    }

    for entry in fs::read_dir(current).with_context(|| format!("reading {}", current.display()))? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_dir() {
            continue;
        }

        let path = entry.path();
        if should_skip_dir(&path) {
            continue;
        }

        walk_for_git_repos(workspace_root, &path, repos, names)?;
    }

    Ok(())
}

fn should_skip_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };

    matches!(
        name,
        ".git" | ".gather-step" | "node_modules" | "dist" | "target"
    )
}

fn unique_repo_name(
    workspace_root: &Path,
    repo_path: &Path,
    names: &mut BTreeSet<String>,
) -> String {
    let relative = repo_path.strip_prefix(workspace_root).unwrap_or(repo_path);
    let mut candidate = repo_path
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("workspace")
        .to_owned();

    if names.insert(candidate.clone()) {
        return candidate;
    }

    candidate = relative
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>()
        .join("-");
    if candidate.is_empty() {
        "workspace".clone_into(&mut candidate);
    }

    let mut suffix = 2_usize;
    let base = candidate.clone();
    while !names.insert(candidate.clone()) {
        candidate = format!("{base}-{suffix}");
        suffix += 1;
    }

    candidate
}

fn relative_repo_path(workspace_root: &Path, repo_path: &Path) -> Result<String> {
    let relative = repo_path
        .strip_prefix(workspace_root)
        .with_context(|| format!("building relative path for {}", repo_path.display()))?;

    if relative.as_os_str().is_empty() {
        Ok(".".to_owned())
    } else {
        Ok(relative.to_string_lossy().replace('\\', "/"))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::atomic::{AtomicU64, Ordering},
    };

    use pretty_assertions::assert_eq;

    use super::discover_git_repos;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(name: &str) -> Self {
            let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                env::temp_dir().join(format!("gather-step-cli-{name}-{}-{id}", process::id()));
            fs::create_dir_all(&path).expect("temp dir should exist");
            Self { path }
        }

        fn path(&self) -> &PathBuf {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn discovers_nested_git_repos() {
        let temp = TempDir::new("discover");
        fs::create_dir_all(temp.path().join("apps/api/.git")).expect("api repo");
        fs::create_dir_all(temp.path().join("apps/web/.git")).expect("web repo");

        let repos = discover_git_repos(temp.path()).expect("repos should be discovered");
        let pairs = repos
            .into_iter()
            .map(|repo| (repo.name, repo.relative_path))
            .collect::<Vec<_>>();

        assert_eq!(
            pairs,
            vec![
                ("api".to_owned(), "apps/api".to_owned()),
                ("web".to_owned(), "apps/web".to_owned()),
            ]
        );
    }

    #[test]
    fn keeps_root_repo_when_no_nested_repo_exists() {
        let temp = TempDir::new("root-only");
        fs::create_dir_all(temp.path().join(".git")).expect("root repo");

        let repos = discover_git_repos(temp.path()).expect("repos should be discovered");
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].relative_path, ".");
        assert!(!repos[0].name.is_empty());
    }

    #[test]
    fn drops_root_repo_when_nested_repos_are_present() {
        let temp = TempDir::new("root-and-nested");
        fs::create_dir_all(temp.path().join(".git")).expect("root repo");
        fs::create_dir_all(temp.path().join("apps/api/.git")).expect("api repo");
        fs::create_dir_all(temp.path().join("apps/web/.git")).expect("web repo");

        let repos = discover_git_repos(temp.path()).expect("repos should be discovered");
        let pairs = repos
            .into_iter()
            .map(|repo| (repo.name, repo.relative_path))
            .collect::<Vec<_>>();

        assert_eq!(
            pairs,
            vec![
                ("api".to_owned(), "apps/api".to_owned()),
                ("web".to_owned(), "apps/web".to_owned()),
            ]
        );
    }
}
