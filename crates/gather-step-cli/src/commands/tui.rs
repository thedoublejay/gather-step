use std::{
    collections::VecDeque,
    io::{self, Stdout},
    panic,
    path::PathBuf,
    sync::Once,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use clap::Args;
use crossterm::{
    cursor::{Hide, Show},
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use gather_step_core::RegistryStore;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Row, Table, Wrap},
};

use crate::app::AppContext;

#[derive(Debug, Args, Clone, Copy, Default, PartialEq, Eq)]
pub struct TuiArgs {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Pane {
    Repos,
    Results,
    Detail,
    Events,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum DetailTab {
    Symbols,
    Routes,
    Events,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputMode {
    Normal,
    Filter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RepoRow {
    name: String,
    path: String,
    files: u64,
    symbols: u64,
    indexed: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EventRow {
    label: String,
    message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TuiState {
    workspace: PathBuf,
    selected_pane: Pane,
    selected_repo: usize,
    filter: String,
    input_mode: InputMode,
    tab: DetailTab,
    repos: Vec<RepoRow>,
    events: VecDeque<EventRow>,
    help_open: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TuiAction {
    Quit,
    NextPane,
    ToggleHelp,
    StartFilter,
    ExitOverlay,
    FilterChar(char),
    FilterBackspace,
    Clear,
    MoveDown,
    MoveUp,
    SelectTab(DetailTab),
    OpenDetail,
}

pub fn run(app: &AppContext, args: TuiArgs) -> Result<()> {
    run_with_options(app, args)
}

pub fn run_with_options(app: &AppContext, args: TuiArgs) -> Result<()> {
    if !app.tui_is_available() {
        bail!(
            "TUI requires an interactive terminal\nhint: run `gather-step status`, `gather-step watch`, or pass `--json` for scriptable output"
        );
    }

    let state = TuiState::load(app, args)?;
    run_terminal(state)
}

impl TuiState {
    fn load(app: &AppContext, _args: TuiArgs) -> Result<Self> {
        let paths = app.workspace_paths();
        let registry = RegistryStore::open(&paths.registry_path)
            .with_context(|| format!("opening {}", paths.registry_path.display()))?;
        let repos = registry
            .registry()
            .repos
            .iter()
            .map(|(name, repo)| RepoRow {
                name: name.clone(),
                path: repo.path.display().to_string(),
                files: repo.file_count,
                symbols: repo.symbol_count,
                indexed: repo
                    .last_indexed_at
                    .clone()
                    .unwrap_or_else(|| "not indexed".to_owned()),
            })
            .collect::<Vec<_>>();
        let mut events = VecDeque::new();
        events.push_back(EventRow {
            label: "ready".to_owned(),
            message: "dashboard snapshot loaded".to_owned(),
        });

        Ok(Self {
            workspace: app.workspace_path.clone(),
            selected_pane: Pane::Repos,
            selected_repo: 0,
            filter: String::new(),
            input_mode: InputMode::Normal,
            tab: DetailTab::Symbols,
            repos,
            events,
            help_open: false,
        })
    }

    #[must_use]
    fn visible_repos(&self) -> Vec<(usize, &RepoRow)> {
        if self.filter.is_empty() {
            return self.repos.iter().enumerate().collect();
        }
        let needle = self.filter.as_bytes();
        self.repos
            .iter()
            .enumerate()
            .filter(|(_, repo)| {
                ascii_contains_ignore_case(&repo.name, needle)
                    || ascii_contains_ignore_case(&repo.path, needle)
            })
            .collect()
    }

    fn selected_repo(&self) -> Option<&RepoRow> {
        self.visible_repos()
            .get(self.selected_repo)
            .map(|(_, repo)| *repo)
    }

    fn apply(&mut self, action: TuiAction) -> bool {
        match action {
            TuiAction::Quit => return true,
            TuiAction::NextPane => {
                self.selected_pane = match self.selected_pane {
                    Pane::Repos => Pane::Results,
                    Pane::Results => Pane::Detail,
                    Pane::Detail => Pane::Events,
                    Pane::Events => Pane::Repos,
                };
            }
            TuiAction::ToggleHelp => self.help_open = !self.help_open,
            TuiAction::StartFilter => {
                self.help_open = false;
                self.input_mode = InputMode::Filter;
            }
            TuiAction::ExitOverlay => {
                self.help_open = false;
                self.input_mode = InputMode::Normal;
            }
            TuiAction::FilterChar(ch) => {
                self.filter.push(ch);
                self.selected_repo = 0;
            }
            TuiAction::FilterBackspace => {
                self.filter.pop();
                self.selected_repo = 0;
            }
            TuiAction::Clear => {
                if matches!(self.input_mode, InputMode::Filter) || !self.filter.is_empty() {
                    self.filter.clear();
                    self.input_mode = InputMode::Normal;
                    self.selected_repo = 0;
                } else {
                    self.events.clear();
                }
            }
            TuiAction::MoveDown => {
                let len = self.visible_repos().len();
                if len > 0 {
                    self.selected_repo = (self.selected_repo + 1).min(len - 1);
                }
            }
            TuiAction::MoveUp => {
                self.selected_repo = self.selected_repo.saturating_sub(1);
            }
            TuiAction::SelectTab(tab) => self.tab = tab,
            TuiAction::OpenDetail => {
                self.selected_pane = Pane::Detail;
            }
        }
        false
    }

    #[cfg(test)]
    #[must_use]
    fn snapshot(&self) -> String {
        let repo = self
            .selected_repo()
            .map_or("no repo", |repo| repo.name.as_str());
        format!(
            "workspace={} repo={repo} filter={} tab={:?} help={}",
            self.workspace.display(),
            self.filter,
            self.tab,
            self.help_open,
        )
    }
}

fn run_terminal(mut state: TuiState) -> Result<()> {
    let (mut terminal, _guard) = enter_terminal()?;
    install_terminal_panic_hook();
    loop {
        terminal.draw(|frame| render_state(frame, &state))?;
        if event::poll(Duration::from_millis(200))?
            && let Some(action) = action_from_event(&event::read()?, state.input_mode)
            && state.apply(action)
        {
            break;
        }
    }
    Ok(())
}

fn enter_terminal() -> Result<(Terminal<CrosstermBackend<Stdout>>, TerminalGuard)> {
    enable_raw_mode().context("enabling raw terminal mode")?;
    let guard = TerminalGuard;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Hide).context("entering alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("creating TUI terminal")?;
    terminal.clear().context("clearing TUI terminal")?;
    Ok((terminal, guard))
}

static TERMINAL_PANIC_HOOK: Once = Once::new();

fn install_terminal_panic_hook() {
    TERMINAL_PANIC_HOOK.call_once(|| {
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            restore_terminal();
            previous_hook(panic_info);
        }));
    });
}

fn restore_terminal() {
    let _ = disable_raw_mode();
    let mut stdout = io::stdout();
    let _ = execute!(stdout, Show, LeaveAlternateScreen);
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

fn ascii_contains_ignore_case(haystack: &str, needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn action_from_event(event: &Event, input_mode: InputMode) -> Option<TuiAction> {
    let Event::Key(key) = event else {
        return None;
    };
    if key.kind != KeyEventKind::Press {
        return None;
    }
    action_from_key(*key, input_mode)
}

fn action_from_key(key: KeyEvent, input_mode: InputMode) -> Option<TuiAction> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        return Some(TuiAction::Quit);
    }

    if matches!(input_mode, InputMode::Filter) {
        return action_from_filter_key(key);
    }

    match key.code {
        KeyCode::Char('q') => Some(TuiAction::Quit),
        KeyCode::Char('?') => Some(TuiAction::ToggleHelp),
        KeyCode::Char('/') => Some(TuiAction::StartFilter),
        KeyCode::Esc => Some(TuiAction::ExitOverlay),
        KeyCode::Tab => Some(TuiAction::NextPane),
        KeyCode::Down | KeyCode::Char('j') => Some(TuiAction::MoveDown),
        KeyCode::Up | KeyCode::Char('k') => Some(TuiAction::MoveUp),
        KeyCode::Char('1') => Some(TuiAction::SelectTab(DetailTab::Symbols)),
        KeyCode::Char('2') => Some(TuiAction::SelectTab(DetailTab::Routes)),
        KeyCode::Char('3') => Some(TuiAction::SelectTab(DetailTab::Events)),
        KeyCode::Char('c') => Some(TuiAction::Clear),
        KeyCode::Enter => Some(TuiAction::OpenDetail),
        _ => None,
    }
}

fn action_from_filter_key(key: KeyEvent) -> Option<TuiAction> {
    match key.code {
        KeyCode::Esc | KeyCode::Enter => Some(TuiAction::ExitOverlay),
        KeyCode::Backspace => Some(TuiAction::FilterBackspace),
        KeyCode::Char(ch)
            if !key.modifiers.contains(KeyModifiers::CONTROL)
                && !key.modifiers.contains(KeyModifiers::ALT) =>
        {
            Some(TuiAction::FilterChar(ch))
        }
        _ => None,
    }
}

pub(crate) fn render_state(frame: &mut Frame<'_>, state: &TuiState) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(12),
            Constraint::Length(5),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, chunks[0], state);

    let body_chunks = if area.width >= 100 {
        Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(28),
                Constraint::Percentage(34),
                Constraint::Percentage(38),
            ])
            .split(chunks[1])
    } else {
        Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(35),
                Constraint::Percentage(30),
                Constraint::Percentage(35),
            ])
            .split(chunks[1])
    };
    render_repos(frame, body_chunks[0], state);
    render_results(frame, body_chunks[1], state);
    render_detail(frame, body_chunks[2], state);
    render_events(frame, chunks[2], state);
    render_footer(frame, chunks[3], state);

    if state.help_open {
        render_help(frame, centered_rect(70, 62, area));
    }
}

fn render_header(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let filter = if state.filter.is_empty() {
        "none".to_owned()
    } else {
        state.filter.clone()
    };
    let title = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("gather-step", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw("  workspace: "),
            Span::styled(
                state.workspace.display().to_string(),
                Style::default().fg(Color::Cyan),
            ),
        ]),
        Line::from(format!("index: registry snapshot  filter: {filter}")),
    ])
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(title, area);
}

fn render_repos(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let rows = visible_repo_markers(state)
        .into_iter()
        .map(|(marker, repo)| {
            Row::new(vec![
                marker.to_owned(),
                repo.name.clone(),
                repo.files.to_string(),
                repo.symbols.to_string(),
            ])
        })
        .collect::<Vec<_>>();
    let rows = if rows.is_empty() {
        vec![Row::new(vec![
            " ".to_owned(),
            "No repos indexed".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
        ])]
    } else {
        rows
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Min(12),
            Constraint::Length(7),
            Constraint::Length(9),
        ],
    )
    .header(Row::new(vec!["", "Repo", "Files", "Symbols"]))
    .block(Block::default().title("Repos (/)").borders(Borders::ALL));
    frame.render_widget(table, area);
}

fn visible_repo_markers(state: &TuiState) -> Vec<(&'static str, &RepoRow)> {
    state
        .visible_repos()
        .into_iter()
        .enumerate()
        .map(|(visible_idx, (_, repo))| {
            let marker = if visible_idx == state.selected_repo {
                ">"
            } else {
                " "
            };
            (marker, repo)
        })
        .collect()
}

fn render_results(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let tab = match state.tab {
        DetailTab::Symbols => "Symbols",
        DetailTab::Routes => "Routes",
        DetailTab::Events => "Events",
    };
    let repo = state
        .selected_repo()
        .map_or("workspace", |repo| repo.name.as_str());
    let rows = [
        Row::new(vec![
            "search".to_owned(),
            format!("gather-step search <query> --repo {repo}"),
        ]),
        Row::new(vec![
            "pack".to_owned(),
            format!("gather-step pack <target> --mode planning --repo {repo}"),
        ]),
        Row::new(vec![
            "impact".to_owned(),
            format!("gather-step impact <symbol> --repo {repo}"),
        ]),
    ];
    let table = Table::new(rows, [Constraint::Length(9), Constraint::Min(16)])
        .block(Block::default().title(tab).borders(Borders::ALL));
    frame.render_widget(table, area);
}

fn render_detail(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let lines = if let Some(repo) = state.selected_repo() {
        vec![
            Line::from(vec![
                Span::styled("repo: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(repo.name.clone()),
            ]),
            Line::from(format!("path: {}", repo.path)),
            Line::from(format!("indexed: {}", repo.indexed)),
            Line::from(format!("files: {}  symbols: {}", repo.files, repo.symbols)),
            Line::from("next: gather-step status --repo <repo>"),
        ]
    } else {
        vec![
            Line::from("No indexed repos found."),
            Line::from("next: gather-step init --index"),
        ]
    };
    let detail = Paragraph::new(lines)
        .block(
            Block::default()
                .title("Detail / Preview")
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: true });
    frame.render_widget(detail, area);
}

fn render_events(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let lines = state
        .events
        .iter()
        .rev()
        .take(3)
        .map(|event| {
            Line::from(vec![
                Span::styled(
                    format!("[{}] ", event.label),
                    Style::default().fg(Color::Cyan),
                ),
                Span::raw(event.message.clone()),
            ])
        })
        .collect::<Vec<_>>();
    let events = Paragraph::new(lines)
        .block(Block::default().title("Events").borders(Borders::ALL))
        .wrap(Wrap { trim: true });
    frame.render_widget(events, area);
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let mode = match state.input_mode {
        InputMode::Normal => "normal",
        InputMode::Filter => "filter",
    };
    let footer = Paragraph::new(format!(
        "q quit  ? help  / filter  tab pane  enter detail  c clear  mode: {mode}"
    ));
    frame.render_widget(footer, area);
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let help = Paragraph::new(vec![
        Line::from("gather-step TUI"),
        Line::from(""),
        Line::from("q / Ctrl+C       quit"),
        Line::from("?                toggle help"),
        Line::from("/                filter repos"),
        Line::from("Tab              next pane"),
        Line::from("1 / 2 / 3        symbols, routes, events"),
        Line::from("Enter            open detail"),
        Line::from("c                clear filter or event log"),
        Line::from("Esc / Enter      leave filter mode"),
    ])
    .block(Block::default().title("Help").borders(Borders::ALL));
    frame.render_widget(Clear, area);
    frame.render_widget(help, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;

    fn state() -> TuiState {
        TuiState {
            workspace: PathBuf::from("/workspace"),
            selected_pane: Pane::Repos,
            selected_repo: 0,
            filter: String::new(),
            input_mode: InputMode::Normal,
            tab: DetailTab::Symbols,
            repos: vec![
                RepoRow {
                    name: "api".to_owned(),
                    path: "/workspace/apps/api".to_owned(),
                    files: 12,
                    symbols: 34,
                    indexed: "2026-04-29T00:00:00Z".to_owned(),
                },
                RepoRow {
                    name: "web".to_owned(),
                    path: "/workspace/apps/web".to_owned(),
                    files: 56,
                    symbols: 78,
                    indexed: "not indexed".to_owned(),
                },
            ],
            events: VecDeque::from([EventRow {
                label: "ready".to_owned(),
                message: "dashboard snapshot loaded".to_owned(),
            }]),
            help_open: false,
        }
    }

    #[test]
    fn filter_actions_update_visible_repo_selection() {
        let mut state = state();

        assert_eq!(state.visible_repos().len(), 2);
        assert_eq!(state.selected_repo().expect("repo").name, "api");

        state.apply(TuiAction::FilterChar('b'));

        assert_eq!(state.visible_repos().len(), 1);
        assert_eq!(state.selected_repo().expect("repo").name, "web");
        assert!(state.snapshot().contains("filter=b"));
    }

    #[test]
    fn filtered_repo_selection_marks_visible_position() {
        let mut state = state();
        state.apply(TuiAction::FilterChar('b'));

        let markers = visible_repo_markers(&state)
            .into_iter()
            .map(|(marker, repo)| (marker, repo.name.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(markers, vec![(">", "web")]);
    }

    #[test]
    fn key_actions_cover_primary_bindings() {
        let key = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::NONE);
        assert_eq!(action_from_key(key, InputMode::Normal), None);

        let key = KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE);
        assert_eq!(
            action_from_key(key, InputMode::Normal),
            Some(TuiAction::StartFilter)
        );

        let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(
            action_from_key(key, InputMode::Normal),
            Some(TuiAction::Quit)
        );
    }

    #[test]
    fn filter_mode_treats_printable_keys_as_filter_input() {
        let key = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
        assert_eq!(
            action_from_key(key, InputMode::Filter),
            Some(TuiAction::FilterChar('q'))
        );

        let key = KeyEvent::new(KeyCode::Char('w'), KeyModifiers::NONE);
        assert_eq!(
            action_from_key(key, InputMode::Filter),
            Some(TuiAction::FilterChar('w'))
        );

        let key = KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE);
        assert_eq!(
            action_from_key(key, InputMode::Filter),
            Some(TuiAction::FilterChar('r'))
        );

        let key = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
        assert_eq!(
            action_from_key(key, InputMode::Filter),
            Some(TuiAction::ExitOverlay)
        );

        let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(
            action_from_key(key, InputMode::Filter),
            Some(TuiAction::Quit)
        );
    }

    #[test]
    fn render_handles_desktop_and_narrow_layouts() {
        for (width, height) in [(120, 34), (72, 34)] {
            let backend = TestBackend::new(width, height);
            let mut terminal = Terminal::new(backend).expect("terminal");
            let state = state();

            terminal
                .draw(|frame| render_state(frame, &state))
                .expect("draw should succeed");
        }
    }

    #[test]
    fn render_help_overlay_smoke() {
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("terminal");
        let mut state = state();
        state.help_open = true;

        terminal
            .draw(|frame| render_state(frame, &state))
            .expect("draw should succeed");
    }
}
