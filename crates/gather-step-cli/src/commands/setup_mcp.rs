use std::{
    env,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::app::AppContext;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum McpScope {
    Global,
    Local,
}

#[derive(Debug, Clone, Copy, Args)]
pub struct SetupMcpArgs {
    #[arg(long, value_enum, default_value = "local")]
    pub scope: McpScope,
}

#[derive(Debug, Serialize)]
struct SetupMcpOutput {
    event: &'static str,
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
    let settings_path = match args.scope {
        McpScope::Local => app.workspace_path.join(".claude/settings.json"),
        McpScope::Global => home_dir()
            .context("cannot resolve HOME")?
            .join(".claude/settings.json"),
    };
    let command_path = find_command_on_path("gather-step");
    let path_resolution = if command_path.is_some() {
        PathResolution::Ok
    } else {
        PathResolution::NotFound
    };
    write_settings(&settings_path, &app.workspace_path)?;

    let payload = SetupMcpOutput {
        event: "setup_mcp_completed",
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

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME").map(PathBuf::from)
}

fn find_command_on_path(command: &str) -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join(command))
        .find(|candidate| candidate.is_file())
}
