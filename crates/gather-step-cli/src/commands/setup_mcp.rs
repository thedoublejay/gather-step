use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use serde::Serialize;
use serde_json::{Map, Value, json};
use toml_edit::{Array, DocumentMut, Item, Table, value};
use tracing::{debug, warn};

use crate::app::AppContext;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpScope {
    Global,
    Local,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpClient {
    Claude,
    Codex,
}

#[derive(Debug, Clone, Copy, Args)]
pub struct SetupMcpArgs {
    /// MCP client to configure.
    #[arg(long, value_enum, default_value = "claude")]
    pub client: McpClient,
    /// Configuration scope. Ignored for Codex, whose config is always global.
    #[arg(long, value_enum, default_value = "local")]
    pub scope: McpScope,
}

#[derive(Debug, Serialize)]
struct SetupMcpOutput {
    event: &'static str,
    client: McpClient,
    scope: McpScope,
    settings_path: String,
    path_resolution: PathResolution,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_path: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum PathResolution {
    Ok,
    NotFound,
}

pub fn run(app: &AppContext, args: SetupMcpArgs) -> Result<()> {
    let settings_path = resolve_settings_path(args.client, args.scope, &app.workspace_path)?;
    let command_path = find_command_on_path("gather-step");
    let path_resolution = if command_path.is_some() {
        PathResolution::Ok
    } else {
        PathResolution::NotFound
    };

    let changed = match args.client {
        McpClient::Claude => write_settings(&settings_path, &app.workspace_path)?,
        McpClient::Codex => write_codex_config(&settings_path, &app.workspace_path)?,
    };

    let payload = SetupMcpOutput {
        event: "setup_mcp_completed",
        client: args.client,
        scope: args.scope,
        settings_path: settings_path.display().to_string(),
        path_resolution,
        command_path: command_path.as_ref().map(|path| path.display().to_string()),
    };
    let output = app.output();
    output.emit(&payload)?;
    if matches!(path_resolution, PathResolution::NotFound) {
        output.line(
            "Warning: `gather-step` was not found on PATH. MCP clients may fail to start the server until their PATH includes the installed binary.",
        );
    }
    if changed {
        output.line(format!("Updated {}", payload.settings_path));
    } else {
        output.line(format!("{} already up to date", payload.settings_path));
    }
    Ok(())
}

/// Best-effort, idempotent auto-registration run at the successful tail of
/// `init`/`index`/`reindex`. Registers the `gather-step` MCP server for both
/// supported clients — Claude (workspace-local `.mcp.json`) and Codex
/// (`~/.codex/config.toml`) — rewriting any stale entry (e.g. the legacy
/// `mcp serve` form) to the canonical `serve` invocation. Skipped entirely when
/// `--no-mcp-setup` / `GATHER_STEP_NO_MCP_SETUP` is set. Failures never abort
/// the host command: they are logged and swallowed so a read-only config or an
/// unwritable home directory cannot break indexing.
pub fn ensure_registration(app: &AppContext) {
    if app.no_mcp_setup {
        debug!("gather-step MCP auto-registration disabled; skipping");
        return;
    }
    let output = app.output();

    let claude_path = app.workspace_path.join(".mcp.json");
    match write_settings(&claude_path, &app.workspace_path) {
        Ok(true) => output.line(format!(
            "Registered gather-step MCP server for Claude in {}",
            claude_path.display()
        )),
        Ok(false) => {}
        Err(error) => warn!(
            %error,
            path = %claude_path.display(),
            "failed to auto-register gather-step MCP server for Claude"
        ),
    }

    if let Some(home) = home_dir() {
        let codex_path = home.join(".codex").join("config.toml");
        match write_codex_config(&codex_path, &app.workspace_path) {
            Ok(true) => output.line(format!(
                "Registered gather-step MCP server for Codex in {}",
                codex_path.display()
            )),
            Ok(false) => {}
            Err(error) => warn!(
                %error,
                path = %codex_path.display(),
                "failed to auto-register gather-step MCP server for Codex"
            ),
        }
    } else {
        warn!("cannot resolve HOME; skipping Codex MCP auto-registration");
    }
}

/// The canonical `gather-step` MCP server entry, shared by the writers and the
/// idempotency check so "what we'd write" and "what we compare against" can
/// never drift.
fn desired_claude_entry(workspace: &str) -> Value {
    json!({
        "command": "gather-step",
        "args": ["--workspace", workspace, "serve"],
    })
}

/// Resolve the config file the chosen client actually reads MCP server
/// definitions from.
///
/// Claude Code does not read `mcpServers` out of `settings.json`: project scope
/// lives in `.mcp.json` at the workspace root and user scope in `~/.claude.json`.
/// Codex reads a single global `~/.codex/config.toml`, so scope does not apply.
fn resolve_settings_path(client: McpClient, scope: McpScope, workspace: &Path) -> Result<PathBuf> {
    match client {
        McpClient::Claude => match scope {
            McpScope::Local => Ok(workspace.join(".mcp.json")),
            McpScope::Global => Ok(home_dir()
                .context("cannot resolve HOME")?
                .join(".claude.json")),
        },
        McpClient::Codex => Ok(home_dir()
            .context("cannot resolve HOME")?
            .join(".codex/config.toml")),
    }
}

/// Merge a workspace-pinned `gather-step` entry into a JSON `mcpServers` map,
/// preserving every other key. Used for Claude's `.mcp.json` and `~/.claude.json`.
///
/// Returns `true` when the file was changed and `false` when the entry already
/// matched the canonical form, so callers can stay quiet on no-op runs.
pub fn write_settings(path: &Path, workspace: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    let mut root = if path.exists() {
        let body =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_str::<Value>(&body)
            .with_context(|| format!("parsing {}", path.display()))?
    } else {
        Value::Object(Map::default())
    };

    let workspace_str = workspace
        .to_str()
        .context("workspace path is not valid UTF-8")?;
    let entry = desired_claude_entry(workspace_str);

    let servers = root
        .as_object_mut()
        .context("settings.json root is not an object")?
        .entry("mcpServers")
        .or_insert_with(|| Value::Object(Map::default()));
    let servers_obj = servers
        .as_object_mut()
        .context("mcpServers is not an object")?;
    if servers_obj.get("gather-step") == Some(&entry) {
        return Ok(false);
    }
    servers_obj.insert("gather-step".to_owned(), entry);

    let serialized = serde_json::to_string_pretty(&root)?;
    std::fs::write(path, format!("{serialized}\n"))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(true)
}

/// Merge a workspace-pinned `gather-step` entry into a Codex `config.toml`,
/// preserving existing servers, other tables, comments, and formatting.
///
/// Returns `true` when the file was changed and `false` when the existing entry
/// already matched the canonical command + args, so repeated auto-runs are
/// no-ops once the config is correct.
pub fn write_codex_config(path: &Path, workspace: &Path) -> Result<bool> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    let mut doc = if path.exists() {
        let body =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        body.parse::<DocumentMut>()
            .with_context(|| format!("parsing {}", path.display()))?
    } else {
        DocumentMut::new()
    };

    let workspace_str = workspace
        .to_str()
        .context("workspace path is not valid UTF-8")?;
    if codex_entry_matches(&doc, workspace_str) {
        return Ok(false);
    }

    let mut args = Array::new();
    args.push("--workspace");
    args.push(workspace_str);
    args.push("serve");

    let mut server = Table::new();
    server.insert("command", value("gather-step"));
    server.insert("args", value(args));

    // Keep `mcp_servers` an implicit table so the entry renders as the
    // idiomatic `[mcp_servers.gather-step]` section rather than an inline table.
    if doc.get("mcp_servers").is_none() {
        let mut servers = Table::new();
        servers.set_implicit(true);
        doc.insert("mcp_servers", Item::Table(servers));
    }
    let servers = doc["mcp_servers"]
        .as_table_mut()
        .context("mcp_servers is not a table")?;
    servers.insert("gather-step", Item::Table(server));

    std::fs::write(path, doc.to_string()).with_context(|| format!("writing {}", path.display()))?;
    Ok(true)
}

/// True when the Codex config already carries the canonical `gather-step` entry:
/// `command = "gather-step"` and `args = ["--workspace", <workspace>, "serve"]`.
/// Any other shape (the legacy `mcp serve` form, an absolute command path, a
/// different workspace) is treated as stale and rewritten.
fn codex_entry_matches(doc: &DocumentMut, workspace: &str) -> bool {
    let Some(entry) = doc
        .get("mcp_servers")
        .and_then(Item::as_table)
        .and_then(|servers| servers.get("gather-step"))
        .and_then(Item::as_table)
    else {
        return false;
    };
    let command_ok = entry
        .get("command")
        .and_then(Item::as_str)
        .is_some_and(|command| command == "gather-step");
    let args_ok = entry
        .get("args")
        .and_then(Item::as_array)
        .map(|args| {
            args.iter()
                .map(toml_edit::Value::as_str)
                .collect::<Option<Vec<_>>>()
        })
        .is_some_and(|args| args.as_deref() == Some(&["--workspace", workspace, "serve"]));
    command_ok && args_ok
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}

fn find_command_on_path(command: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join(command))
        .find(|candidate| candidate.is_file())
}
