//! Regression: `batch_query` must route `plan_change` to the typed
//! twelve-section product (`PlanChangeResponse`), not the legacy planning-pack
//! `ContextPackResponse`. The direct MCP route and the batch route must agree.

use std::sync::Arc;

use gather_step_mcp::{
    config::{McpContext, McpServerConfig},
    server::GatherStepMcpServer,
    tools::composite::{BatchQueryOperation, BatchQueryRequest},
};
use gather_step_storage::WorkspaceStores;
use rmcp::handler::server::wrapper::Parameters;

#[tokio::test]
async fn batch_query_plan_change_returns_typed_product() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let stores = Arc::new(WorkspaceStores::open(root).expect("workspace stores should open"));
    let config = McpServerConfig::new(root.join("registry.json"), root.join("graph.redb"));
    let ctx = McpContext::from_workspace_stores(config, stores);
    let server = GatherStepMcpServer::new(ctx);

    let request = BatchQueryRequest {
        ops: vec![BatchQueryOperation {
            tool: "plan_change".to_owned(),
            arguments: serde_json::json!({ "target": "anySymbol" }),
        }],
    };

    let response = server
        .batch_query_tool(Parameters(request))
        .await
        .expect("batch_query should succeed");

    // These keys exist only on the typed PlanChangeResponse, never on the
    // legacy ContextPackResponse — so their presence proves the batch route
    // now matches the direct route.
    let body = serde_json::to_string(&response.0).expect("serialize batch response");
    assert!(
        body.contains("reuse_candidates"),
        "batch plan_change must return the typed product (missing reuse_candidates): {body}"
    );
    assert!(
        body.contains("verification_plan") && body.contains("\"sections\""),
        "batch plan_change must include the contract section manifest: {body}"
    );
    // The newer sections must round-trip through the batch route too: DSO1
    // (display ownership) and B1/B3/WS-16 (the pass-2 + v1-completeness
    // checklists), plus the current contract schema version.
    for section in [
        "display_ownership_checks",
        "pass_two_gap_dimensions",
        "v1_completeness_checklist",
    ] {
        assert!(
            body.contains(section),
            "batch plan_change missing section `{section}`: {body}"
        );
    }
    assert!(
        body.contains("\"schema_version\":3"),
        "batch plan_change must carry the current contract schema version: {body}"
    );
}
