use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use serde::Serialize;
use serde_json::{Map, Value, json};
use toml_edit::{Array, DocumentMut, Item, Table, value};

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

    match args.client {
        McpClient::Claude => write_settings(&settings_path, &app.workspace_path)?,
        McpClient::Codex => write_codex_config(&settings_path, &app.workspace_path)?,
    }

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
    output.line(format!("Updated {}", payload.settings_path));
    Ok(())
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
pub fn write_settings(path: &Path, workspace: &Path) -> Result<()> {
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
    let entry = json!({
        "command": "gather-step",
        "args": ["--workspace", workspace_str, "serve"],
    });

    let servers = root
        .as_object_mut()
        .context("settings.json root is not an object")?
        .entry("mcpServers")
        .or_insert_with(|| Value::Object(Map::default()));
    let servers_obj = servers
        .as_object_mut()
        .context("mcpServers is not an object")?;
    servers_obj.insert("gather-step".to_owned(), entry);

    let serialized = serde_json::to_string_pretty(&root)?;
    std::fs::write(path, format!("{serialized}\n"))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Merge a workspace-pinned `gather-step` entry into a Codex `config.toml`,
/// preserving existing servers, other tables, comments, and formatting.
pub fn write_codex_config(path: &Path, workspace: &Path) -> Result<()> {
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
    Ok(())
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
