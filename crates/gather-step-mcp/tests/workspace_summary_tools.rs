use std::collections::BTreeSet;

use gather_step_core::WorkspaceRegistry;
use gather_step_mcp::{GatherStepMcpServer, MCP_TOOLS};

#[test]
fn workspace_summary_tool_table_matches_registered_mcp_tools() {
    let body = gather_step_output::render_workspace_summary_claude(
        &WorkspaceRegistry::default(),
        env!("CARGO_PKG_VERSION"),
        MCP_TOOLS,
        &[],
    );

    let documented_tools = body
        .lines()
        .filter_map(documented_tool_name)
        .collect::<BTreeSet<_>>();
    let registered_tools = GatherStepMcpServer::registered_tool_names()
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        documented_tools, registered_tools,
        "generated workspace summaries should document exactly the registered MCP tool names"
    );
}

#[test]
fn mcp_tools_catalog_matches_registered_mcp_tools() {
    let cataloged = MCP_TOOLS
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let registered = GatherStepMcpServer::registered_tool_names()
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        cataloged, registered,
        "MCP_TOOLS catalog must list exactly the tools that the MCP server registers"
    );
}

fn documented_tool_name(line: &str) -> Option<String> {
    let line = line.strip_prefix("| `")?;
    let (tool, _) = line.split_once('`')?;
    // Skip the "| `gather-step <subcommand>` |" rows from the CLI table.
    if tool.starts_with("gather-step ") {
        return None;
    }
    Some(tool.to_owned())
}
