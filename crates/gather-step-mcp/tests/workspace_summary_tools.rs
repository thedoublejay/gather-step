use std::collections::BTreeSet;

use gather_step_core::WorkspaceRegistry;
use gather_step_mcp::GatherStepMcpServer;

#[test]
fn workspace_summary_tool_table_matches_registered_mcp_tools() {
    let body = gather_step_output::render_workspace_summary_claude(
        &WorkspaceRegistry::default(),
        env!("CARGO_PKG_VERSION"),
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

fn documented_tool_name(line: &str) -> Option<String> {
    let line = line.strip_prefix("| `")?;
    let (tool, _) = line.split_once('`')?;
    Some(tool.to_owned())
}
