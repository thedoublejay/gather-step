use std::path::{Path, PathBuf};

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
}

pub fn run(app: &AppContext, args: SetupMcpArgs) -> Result<()> {
    let settings_path = match args.scope {
        McpScope::Local => app.workspace_path.join(".claude/settings.json"),
        McpScope::Global => home_dir()
            .context("cannot resolve HOME")?
            .join(".claude/settings.json"),
    };
    let exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("gather-step"));

    write_settings(&settings_path, &app.workspace_path, &exe)?;

    let payload = SetupMcpOutput {
        event: "setup_mcp_completed",
        scope: args.scope,
        settings_path: settings_path.display().to_string(),
    };
    let output = app.output();
    output.emit(&payload)?;
    output.line(format!("Updated {}", payload.settings_path));
    Ok(())
}

pub fn write_settings(path: &Path, workspace: &Path, exe: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }

    let mut root = if path.exists() {
        let body =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_str::<Value>(&body).unwrap_or_else(|_| Value::Object(Map::default()))
    } else {
        Value::Object(Map::default())
    };

    let workspace_str = workspace
        .to_str()
        .context("workspace path is not valid UTF-8")?;
    let exe_str = exe.to_str().context("executable path is not valid UTF-8")?;

    let entry = json!({
        "command": exe_str,
        "args": ["--workspace", workspace_str, "mcp", "serve"],
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
    std::env::var_os("HOME").map(PathBuf::from)
}
