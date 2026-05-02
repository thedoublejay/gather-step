use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use console::style;
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{self, Event, KeyCode, KeyEventKind},
    execute, queue,
    style::Print,
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
    terminal::{disable_raw_mode, enable_raw_mode},
};
use gather_step_core::{DeploymentConfig, GatherStepConfig, IndexingConfig, RepoConfig};
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
    if app.is_interactive() {
        run_wizard(app, args).await
    } else {
        run_non_interactive(app, args).await
    }
}

async fn run_non_interactive(app: &AppContext, args: InitArgs) -> Result<()> {
    let config_path = init_config_path(app, &args);
    if config_path.exists() && !args.force {
        let repos = load_existing_config_repos(&config_path)?;
        emit_config_summary(app, &config_path, &repos, "Using existing config")?;
    } else {
        write_default_config(app, &args)?;
    }
    let output = app.output();

    if args.index && !args.no_index {
        index::run(app, init_index_args(Some(config_path.clone()))).await?;
    }
    if args.generate_ai_files && !args.no_generate_ai_files {
        generate::run_summary_pair(app)?;
    }
    if let Some(scope) = args.setup_mcp {
        setup_mcp::run(app, setup_mcp::SetupMcpArgs { scope })?;
    }
    if args.watch && !args.no_watch {
        emit_setup_complete(&output);
        watch::run(app, watch::WatchArgs::default()).await?;
    }

    Ok(())
}

async fn run_wizard(app: &AppContext, args: InitArgs) -> Result<()> {
    let repos = discover_git_repos(&app.workspace_path)?;
    let config_path = init_config_path(app, &args);
    let existing_config = if config_path.exists() && !args.force {
        Some(load_existing_config(&config_path)?)
    } else {
        None
    };
    let existing_config_repos = existing_config.as_ref().map(discovered_repos_from_config);

    let output = app.output();
    output.line(format!(
        "\n  {}",
        style("Hi, welcome to Gather Step setup").cyan().bold()
    ));
    output.line(format!(
        "  {}",
        style(
            "Gather Step builds a local code graph so your agent can plan with repo, route, event, and contract context."
        )
        .dim()
    ));
    output.line(format!(
        "  Workspace: {}",
        style(app.workspace_path.display()).dim()
    ));
    if existing_config_repos.is_some() {
        output.line(format!(
            "  {} {}",
            style("Existing config:").yellow().bold(),
            style(config_path.display()).dim()
        ));
    }
    output.line(format!(
        "\n  Found {} {}",
        style(repos.len()).cyan().bold(),
        style(git_repository_count_label(repos.len())).dim(),
    ));
    let selected_repos = prompt_repo_selection(1, &repos, existing_config_repos.as_deref())?;

    let do_index = if args.index {
        true
    } else if args.no_index {
        false
    } else {
        prompt_yes_no(2, "Index the selected repositories now?", true)?
    };
    let do_ai = if args.generate_ai_files {
        true
    } else if args.no_generate_ai_files {
        false
    } else {
        prompt_yes_no(
            3,
            "Generate AI context files now? (.claude/rules/, CLAUDE.gather.md, AGENTS.gather.md)",
            true,
        )?
    };
    let scope = match args.setup_mcp {
        Some(scope) => Some(scope),
        None => prompt_mcp_scope(4)?,
    };
    let do_watch = if args.watch {
        true
    } else if args.no_watch {
        false
    } else {
        prompt_yes_no(
            5,
            "Watch for repository changes and re-index automatically?",
            false,
        )?
    };

    write_default_config_with_repos(app, &args, &selected_repos, existing_config.as_ref())?;

    if do_index {
        index::run(app, init_index_args(Some(config_path.clone()))).await?;
    }
    if do_ai {
        generate::run_summary_pair(app)?;
    }
    if let Some(scope) = scope {
        setup_mcp::run(app, setup_mcp::SetupMcpArgs { scope })?;
    }
    emit_setup_complete(&output);
    if do_watch {
        watch::run(app, watch::WatchArgs::default()).await?;
    }

    Ok(())
}

fn init_index_args(config: Option<PathBuf>) -> index::IndexArgs {
    index::IndexArgs {
        config,
        auto_recover: true,
        ..index::IndexArgs::default()
    }
}

fn init_config_path(app: &AppContext, args: &InitArgs) -> PathBuf {
    args.config
        .clone()
        .unwrap_or_else(|| app.workspace_paths().config_path)
}

fn write_default_config(app: &AppContext, args: &InitArgs) -> Result<()> {
    let repos = discover_git_repos(&app.workspace_path)?;
    write_default_config_with_repos(app, args, &repos, None)
}

fn write_default_config_with_repos(
    app: &AppContext,
    args: &InitArgs,
    repos: &[DiscoveredRepo],
    existing_config: Option<&GatherStepConfig>,
) -> Result<()> {
    let config_path = init_config_path(app, args);

    if repos.is_empty() {
        bail!(
            "No Git repositories found under {}.",
            app.workspace_path.display()
        );
    }

    let configured_repos = materialize_repo_config(repos, existing_config);
    let config = match existing_config {
        Some(existing) => GatherStepConfig {
            allow_listed_repos: retain_allow_listed_repos_for_selected(existing, &configured_repos),
            repos: configured_repos,
            github: existing.github.clone(),
            jira: existing.jira.clone(),
            indexing: existing.indexing.clone(),
            deployment: existing.deployment.clone(),
        },
        None => GatherStepConfig {
            allow_listed_repos: Vec::new(),
            repos: configured_repos,
            github: None,
            jira: None,
            indexing: IndexingConfig::default(),
            deployment: DeploymentConfig::default(),
        },
    };
    let summary_repos = discovered_repos_from_config(&config);

    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    fs::write(&config_path, serde_norway::to_string(&config)?)
        .with_context(|| format!("writing {}", config_path.display()))?;

    emit_config_summary(app, &config_path, &summary_repos, "Wrote config")?;

    Ok(())
}

fn materialize_repo_config(
    repos: &[DiscoveredRepo],
    existing_config: Option<&GatherStepConfig>,
) -> Vec<RepoConfig> {
    let mut existing_by_path = BTreeMap::new();
    let mut existing_by_name = BTreeMap::new();
    if let Some(existing) = existing_config {
        for repo in &existing.repos {
            existing_by_path.insert(repo.path.as_str(), repo);
            existing_by_name.insert(repo.name.as_str(), repo);
        }
    }

    repos
        .iter()
        .map(|repo| {
            if let Some(existing) = existing_by_path.get(repo.relative_path.as_str()) {
                return (*existing).clone();
            }
            if let Some(existing) = existing_by_name.get(repo.name.as_str()) {
                return RepoConfig {
                    name: existing.name.clone(),
                    path: repo.relative_path.clone(),
                    depth: existing.depth,
                };
            }
            RepoConfig {
                name: repo.name.clone(),
                path: repo.relative_path.clone(),
                depth: None,
            }
        })
        .collect()
}

fn retain_allow_listed_repos_for_selected(
    existing_config: &GatherStepConfig,
    selected_repos: &[RepoConfig],
) -> Vec<String> {
    let selected_names = selected_repos
        .iter()
        .map(|repo| repo.name.as_str())
        .collect::<BTreeSet<_>>();
    existing_config
        .allow_listed_repos
        .iter()
        .filter(|repo_name| selected_names.contains(repo_name.as_str()))
        .cloned()
        .collect()
}

fn emit_config_summary(
    app: &AppContext,
    config_path: &Path,
    repos: &[DiscoveredRepo],
    action: &str,
) -> Result<()> {
    let output = app.output();
    let payload = InitOutput {
        event: "init_completed",
        config_path: config_path.display().to_string(),
        repo_count: repos.len(),
        repos: repos
            .iter()
            .map(|repo| InitRepoOutput {
                name: repo.name.clone(),
                path: repo.relative_path.clone(),
            })
            .collect(),
    };

    output.emit(&payload)?;
    output.line(format!(
        "{} {}",
        style(action).green().bold(),
        style(&payload.config_path).dim()
    ));
    output.line(format!(
        "  {} {}",
        style(payload.repo_count).cyan().bold(),
        style(repository_count_label(payload.repo_count)).dim()
    ));

    Ok(())
}

fn emit_setup_complete(output: &crate::app::Output) {
    output.line(format!(
        "\n  {} {}",
        style("✓ Setup complete.").green().bold(),
        style("Gather Step is ready.").green().bold(),
    ));
    output.line(format!(
        "  Start planning with your agent. Example: {}. Docs: {}",
        style("\"Start planning your next task with gather-step\"").cyan(),
        style("https://gatherstep.dev/reference/mcp-tools/").underlined()
    ));
}

fn load_existing_config_repos(config_path: &Path) -> Result<Vec<DiscoveredRepo>> {
    Ok(discovered_repos_from_config(&load_existing_config(
        config_path,
    )?))
}

fn load_existing_config(config_path: &Path) -> Result<GatherStepConfig> {
    GatherStepConfig::from_yaml_file(config_path)
        .with_context(|| format!("loading existing config {}", config_path.display()))
}

fn discovered_repos_from_config(config: &GatherStepConfig) -> Vec<DiscoveredRepo> {
    config
        .repos
        .iter()
        .map(|repo| DiscoveredRepo {
            name: repo.name.clone(),
            relative_path: repo.path.clone(),
        })
        .collect()
}

fn repository_count_label(count: usize) -> &'static str {
    if count == 1 {
        "configured repository"
    } else {
        "configured repositories"
    }
}

fn git_repository_count_label(count: usize) -> &'static str {
    if count == 1 {
        "Git repository"
    } else {
        "Git repositories"
    }
}

fn prompt_yes_no(step: usize, message: &str, default: bool) -> Result<bool> {
    let suffix = if default { "[Y/n]" } else { "[y/N]" };
    let mut stdout = io::stdout().lock();
    write!(
        stdout,
        "{} {} {} ",
        style(format!("{step})")).cyan().bold(),
        style(message).white().bold(),
        style(suffix).yellow().bold()
    )?;
    stdout.flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    match answer.trim() {
        "y" | "Y" | "yes" | "YES" | "Yes" => Ok(true),
        "n" | "N" | "no" | "NO" | "No" => Ok(false),
        _ => Ok(default),
    }
}

fn prompt_mcp_scope(step: usize) -> Result<Option<setup_mcp::McpScope>> {
    let mut stdout = io::stdout().lock();
    write!(
        stdout,
        "{} {} {} ",
        style(format!("{step})")).cyan().bold(),
        style("Register Gather Step as an MCP server?")
            .white()
            .bold(),
        style("[local/global/skip] (default: local)")
            .yellow()
            .bold()
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

fn prompt_repo_selection(
    step: usize,
    repos: &[DiscoveredRepo],
    existing_config_repos: Option<&[DiscoveredRepo]>,
) -> Result<Vec<DiscoveredRepo>> {
    if repos.is_empty() {
        bail!("No Git repositories found under the workspace.");
    }

    let default_names = existing_config_repos.map(|repos| {
        repos
            .iter()
            .map(|repo| repo.name.as_str())
            .collect::<BTreeSet<_>>()
    });
    let default_paths = existing_config_repos.map(|repos| {
        repos
            .iter()
            .map(|repo| repo.relative_path.as_str())
            .collect::<BTreeSet<_>>()
    });
    let mut selected = repos
        .iter()
        .enumerate()
        .filter_map(|(idx, repo)| {
            let selected_by_name = default_names
                .as_ref()
                .is_some_and(|names| names.contains(repo.name.as_str()));
            let selected_by_path = default_paths
                .as_ref()
                .is_some_and(|paths| paths.contains(repo.relative_path.as_str()));
            (existing_config_repos.is_none() || selected_by_name || selected_by_path).then_some(idx)
        })
        .collect::<BTreeSet<_>>();
    if selected.is_empty() {
        selected.extend(0..repos.len());
    }

    if io::stdin().is_terminal() && io::stdout().is_terminal() {
        return prompt_repo_selection_interactive(step, repos, selected);
    }

    prompt_repo_selection_text(step, repos, selected)
}

fn prompt_repo_selection_text(
    step: usize,
    repos: &[DiscoveredRepo],
    mut selected: BTreeSet<usize>,
) -> Result<Vec<DiscoveredRepo>> {
    loop {
        let mut stdout = io::stdout().lock();
        writeln!(
            stdout,
            "\n{} {}",
            style(format!("{step})")).cyan().bold(),
            style("Select repositories to include").white().bold()
        )?;
        writeln!(
            stdout,
            "   {}",
            style("Use numbers or ranges to toggle, `all`, `none`, or press Enter to confirm.")
                .dim()
        )?;
        for (idx, repo) in repos.iter().enumerate() {
            let checked = selected.contains(&idx);
            let marker = if checked {
                style("[x]").green().bold()
            } else {
                style("[ ]").yellow()
            };
            writeln!(
                stdout,
                "   {} {} {}",
                marker,
                style(format!("{}.", idx + 1)).cyan(),
                style(format!("{}  ({})", repo.name, repo.relative_path)).white()
            )?;
        }
        write!(
            stdout,
            "   {} ",
            style("Selection [all/none/1,3/1-3/Enter]:").yellow().bold()
        )?;
        stdout.flush()?;

        let mut answer = String::new();
        io::stdin().read_line(&mut answer)?;
        let answer = answer.trim();
        if answer.is_empty() || answer.eq_ignore_ascii_case("done") {
            break;
        }
        if answer.eq_ignore_ascii_case("all") || answer.eq_ignore_ascii_case("a") {
            selected.clear();
            selected.extend(0..repos.len());
            continue;
        }
        if answer.eq_ignore_ascii_case("none") || answer.eq_ignore_ascii_case("n") {
            selected.clear();
            continue;
        }

        for token in answer.split([',', ' ']).filter(|token| !token.is_empty()) {
            toggle_selection_token(token, repos.len(), &mut selected)?;
        }
    }

    if selected.is_empty() {
        bail!("Select at least one repository before continuing.");
    }

    Ok(selected
        .into_iter()
        .filter_map(|idx| repos.get(idx).cloned())
        .collect())
}

struct RepoPickerTerminalGuard;

impl RepoPickerTerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("enabling raw terminal mode")?;
        execute!(io::stdout(), EnterAlternateScreen, Hide)
            .context("entering repository picker screen")?;
        Ok(Self)
    }
}

impl Drop for RepoPickerTerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
    }
}

fn prompt_repo_selection_interactive(
    step: usize,
    repos: &[DiscoveredRepo],
    mut selected: BTreeSet<usize>,
) -> Result<Vec<DiscoveredRepo>> {
    let _guard = RepoPickerTerminalGuard::enter()?;
    let mut cursor = 0usize;
    let mut scroll = 0usize;
    let mut message = String::new();

    loop {
        draw_repo_picker(step, repos, &selected, cursor, scroll, &message)?;
        let Event::Key(key) = event::read().context("reading repository picker input")? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        message.clear();
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                cursor = cursor.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                cursor = (cursor + 1).min(repos.len().saturating_sub(1));
            }
            KeyCode::PageUp => {
                cursor = cursor.saturating_sub(10);
            }
            KeyCode::PageDown => {
                cursor = (cursor + 10).min(repos.len().saturating_sub(1));
            }
            KeyCode::Home => cursor = 0,
            KeyCode::End => cursor = repos.len().saturating_sub(1),
            KeyCode::Char(' ') => toggle_selection_index(cursor, &mut selected),
            KeyCode::Char('a' | 'A') => {
                selected.clear();
                selected.extend(0..repos.len());
            }
            KeyCode::Char('n' | 'N') => {
                selected.clear();
            }
            KeyCode::Enter => {
                if selected.is_empty() {
                    "Select at least one repository before continuing.".clone_into(&mut message);
                } else {
                    break;
                }
            }
            KeyCode::Esc | KeyCode::Char('q' | 'Q') => {
                bail!("repository selection cancelled");
            }
            _ => {}
        }
        scroll = repo_picker_scroll(cursor, scroll);
    }

    Ok(selected
        .into_iter()
        .filter_map(|idx| repos.get(idx).cloned())
        .collect())
}

fn repo_picker_scroll(cursor: usize, current_scroll: usize) -> usize {
    let (_, terminal_height) = terminal::size().unwrap_or((80, 24));
    let visible_rows = repo_picker_visible_rows(terminal_height);
    if cursor < current_scroll {
        cursor
    } else if cursor >= current_scroll + visible_rows {
        cursor.saturating_sub(visible_rows.saturating_sub(1))
    } else {
        current_scroll
    }
}

fn repo_picker_visible_rows(terminal_height: u16) -> usize {
    usize::from(terminal_height).saturating_sub(7).max(1)
}

fn draw_repo_picker(
    step: usize,
    repos: &[DiscoveredRepo],
    selected: &BTreeSet<usize>,
    cursor: usize,
    scroll: usize,
    message: &str,
) -> Result<()> {
    let (terminal_width, terminal_height) = terminal::size().unwrap_or((80, 24));
    let visible_rows = repo_picker_visible_rows(terminal_height);
    let mut stdout = io::stdout().lock();
    queue!(
        stdout,
        MoveTo(0, 0),
        Clear(ClearType::All),
        Print(format!(
            "{} {}\r\n",
            style(format!("{step})")).cyan().bold(),
            style("Select repositories to include").white().bold()
        )),
        Print(format!(
            "   {}\r\n",
            style("↑/↓ move  Space toggle  Enter confirm  a all  n none  q cancel").dim()
        )),
        Print(format!(
            "   {} {} selected\r\n\r\n",
            style(selected.len()).cyan().bold(),
            style(selection_count_label(selected.len())).dim()
        )),
    )?;

    for (idx, repo) in repos.iter().enumerate().skip(scroll).take(visible_rows) {
        let pointer = if idx == cursor {
            style(">").blue().bold()
        } else {
            style(" ")
        };
        let marker = if selected.contains(&idx) {
            style("[x]").green().bold()
        } else {
            style("[ ]").yellow()
        };
        let label_width = usize::from(terminal_width).saturating_sub(14);
        let label = truncate_chars(
            &format!("{}  ({})", repo.name, repo.relative_path),
            label_width,
        );
        queue!(
            stdout,
            Print(format!(
                "   {} {} {} {}\r\n",
                pointer,
                marker,
                style(format!("{:>2}.", idx + 1)).cyan(),
                style(label).white()
            ))
        )?;
    }

    if !message.is_empty() {
        queue!(
            stdout,
            Print("\r\n"),
            Print(format!("   {}\r\n", style(message).yellow().bold()))
        )?;
    }
    stdout.flush()?;
    Ok(())
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    if max_chars <= 1 {
        return "…".to_owned();
    }
    let mut truncated = value
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn selection_count_label(count: usize) -> &'static str {
    if count == 1 {
        "repository"
    } else {
        "repositories"
    }
}

fn toggle_selection_token(
    token: &str,
    repo_count: usize,
    selected: &mut BTreeSet<usize>,
) -> Result<()> {
    if let Some((start, end)) = token.split_once('-') {
        let start = parse_selection_index(start, repo_count)?;
        let end = parse_selection_index(end, repo_count)?;
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        for idx in start..=end {
            toggle_selection_index(idx, selected);
        }
        return Ok(());
    }

    let idx = parse_selection_index(token, repo_count)?;
    toggle_selection_index(idx, selected);
    Ok(())
}

fn parse_selection_index(token: &str, repo_count: usize) -> Result<usize> {
    let number = token
        .parse::<usize>()
        .with_context(|| format!("invalid repository selection `{token}`"))?;
    if !(1..=repo_count).contains(&number) {
        bail!("repository selection `{number}` is outside 1..={repo_count}");
    }
    Ok(number - 1)
}

fn toggle_selection_index(idx: usize, selected: &mut BTreeSet<usize>) {
    if !selected.remove(&idx) {
        selected.insert(idx);
    }
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

    use gather_step_core::{
        DeploymentConfig, DepthLevel, GatherStepConfig, IndexingConfig, RepoConfig,
    };

    use super::{
        DiscoveredRepo, discover_git_repos, materialize_repo_config,
        retain_allow_listed_repos_for_selected,
    };

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

    #[test]
    fn repo_config_merge_filters_allow_list_to_selected_repos() {
        let existing = GatherStepConfig {
            allow_listed_repos: vec!["api".to_owned(), "web".to_owned()],
            repos: vec![
                RepoConfig {
                    name: "api".to_owned(),
                    path: "apps/api".to_owned(),
                    depth: Some(DepthLevel::Level2),
                },
                RepoConfig {
                    name: "web".to_owned(),
                    path: "apps/web".to_owned(),
                    depth: Some(DepthLevel::Level1),
                },
            ],
            github: None,
            jira: None,
            indexing: IndexingConfig::default(),
            deployment: DeploymentConfig::default(),
        };
        let selected = vec![DiscoveredRepo {
            name: "api".to_owned(),
            relative_path: "apps/api".to_owned(),
        }];
        let merged = materialize_repo_config(&selected, Some(&existing));

        assert_eq!(
            retain_allow_listed_repos_for_selected(&existing, &merged),
            vec!["api".to_owned()]
        );
    }

    #[test]
    fn repo_config_name_match_keeps_discovered_path() {
        let existing = GatherStepConfig {
            allow_listed_repos: Vec::new(),
            repos: vec![RepoConfig {
                name: "api".to_owned(),
                path: "old/api".to_owned(),
                depth: Some(DepthLevel::Level2),
            }],
            github: None,
            jira: None,
            indexing: IndexingConfig::default(),
            deployment: DeploymentConfig::default(),
        };
        let selected = vec![DiscoveredRepo {
            name: "api".to_owned(),
            relative_path: "new/api".to_owned(),
        }];
        let merged = materialize_repo_config(&selected, Some(&existing));

        assert_eq!(
            merged,
            vec![RepoConfig {
                name: "api".to_owned(),
                path: "new/api".to_owned(),
                depth: Some(DepthLevel::Level2),
            }]
        );
    }
}
