//! Release-safety trust/privacy tests for the MCP surface.

#![forbid(unsafe_code)]

use gather_step_mcp::server::GatherStepMcpServer;

// ---------------------------------------------------------------------------
// Defect 5.1 — duplicate debug_route / debug_event aliases removed
// ---------------------------------------------------------------------------

#[test]
fn mcp_tool_list_no_longer_exposes_debug_route_or_debug_event_aliases() {
    let names = GatherStepMcpServer::registered_tool_names();
    assert!(
        !names.iter().any(|n| n == "debug_route"),
        "debug_route alias must be absent from tool list; found in: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "debug_event"),
        "debug_event alias must be absent from tool list; found in: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "debug_pack"),
        "debug_pack must remain registered; not found in: {names:?}"
    );
}

// ---------------------------------------------------------------------------
// Defect 5.2 — PACK_INFLIGHT registry does not grow unboundedly
// ---------------------------------------------------------------------------

#[test]
fn pack_inflight_registry_does_not_grow_unboundedly_across_generations() {
    use gather_step_mcp::tools::packs::{
        inflight_entry_for_test, pack_inflight_len_for_test, try_drop_inflight_entry_for_test,
    };

    let before = pack_inflight_len_for_test();
    for key in (0..256_usize).map(|i| format!("synthetic-key-{i}")) {
        let entry = inflight_entry_for_test(&key);
        drop(entry);
        try_drop_inflight_entry_for_test(&key);
    }
    let after = pack_inflight_len_for_test();
    assert!(
        after <= before + 4,
        "PACK_INFLIGHT must drop entries once flight completes; before={before} after={after}"
    );
}

// ---------------------------------------------------------------------------
// Defect 5.3 — internal serialization errors classified as Internal not InvalidInput
// ---------------------------------------------------------------------------

#[test]
fn internal_serialization_error_is_not_invalid_input() {
    use gather_step_mcp::error::McpServerError;
    use gather_step_mcp::tools::packs::simulate_internal_deserialize_failure_for_test;

    let err = simulate_internal_deserialize_failure_for_test();
    assert!(
        matches!(err, McpServerError::Internal(_)),
        "internal deserialization errors must be Internal, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Defect 5.4 — MCP outputs contain no absolute workspace paths
// ---------------------------------------------------------------------------

#[test]
fn mcp_outputs_contain_no_absolute_workspace_paths() {
    use gather_step_mcp::output::redact::relativize_to_workspace;

    let tmp = tempfile::tempdir().unwrap();
    let workspace_root = tmp.path();
    let abs_path = workspace_root.join("repoA").join("src").join("main.rs");

    let rendered = relativize_to_workspace(&abs_path, workspace_root);

    let workspace_prefix = workspace_root.to_string_lossy().into_owned();
    assert!(
        !rendered.contains(&workspace_prefix),
        "relativize_to_workspace must strip the workspace prefix; got: {rendered}"
    );
    assert_eq!(rendered, "repoA/src/main.rs");
}

#[test]
fn relativize_to_workspace_returns_sentinel_for_outside_path() {
    use gather_step_mcp::output::redact::relativize_to_workspace;

    let tmp = tempfile::tempdir().unwrap();
    let workspace_root = tmp.path();
    let outside = std::path::Path::new("/etc/passwd");

    let rendered = relativize_to_workspace(outside, workspace_root);
    assert_eq!(rendered, "<outside-workspace>");
}

// ---------------------------------------------------------------------------
// Defect 5.5 — who_owns does not expose raw author emails by default
// ---------------------------------------------------------------------------

#[test]
fn who_owns_does_not_expose_raw_author_email_by_default() {
    use gather_step_git::redact_email;

    let raw_email = "a@example.com";
    let redacted = redact_email(raw_email);

    // Must not contain the raw email domain
    assert!(
        !redacted.contains("@example.com"),
        "redacted id must not contain raw email domain; got: {redacted}"
    );
    // Must end with @redacted suffix
    assert!(
        redacted.ends_with("@redacted"),
        "redacted id must end with @redacted; got: {redacted}"
    );
    // Must be stable (same input → same output)
    assert_eq!(
        redact_email(raw_email),
        redacted,
        "redact_email must be deterministic"
    );
    // Must produce 16 hex chars before @redacted (first 8 bytes of the
    // keyed BLAKE3 hash, hex-encoded).
    let prefix = redacted.trim_end_matches("@redacted");
    assert_eq!(
        prefix.len(),
        16,
        "prefix must be 16 hex chars; got: {prefix:?}"
    );
    assert!(
        prefix.chars().all(|c| c.is_ascii_hexdigit()),
        "prefix must be hex; got: {prefix:?}"
    );
}

// ---------------------------------------------------------------------------
// spawn_blocking: concurrent tool calls do not block each other
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_tool_calls_both_succeed_without_blocking_runtime() {
    use std::sync::Arc;

    use gather_step_mcp::config::{McpContext, McpServerConfig};
    use gather_step_storage::WorkspaceStores;

    // Build a temporary, empty workspace so both tools can be exercised
    // without requiring pre-indexed data.  `list_repos` and
    // `get_graph_schema` return empty payloads for an empty store, which is
    // sufficient to prove the spawn_blocking path completes.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Arc::new(WorkspaceStores::open(root).expect("empty workspace stores should open"));
    let config = McpServerConfig::new(root.join("registry.json"), root.join("graph.redb"));
    let ctx = McpContext::from_workspace_stores(config, stores);
    let server = GatherStepMcpServer::new(ctx);

    // Fire both tool calls concurrently.  If either panics or returns an
    // unexpected join error the test fails.
    let (schema_result, repos_result) =
        tokio::join!(server.get_graph_schema_tool(), server.list_repos_tool());
    assert!(schema_result.is_ok(), "get_graph_schema_tool must succeed");
    assert!(repos_result.is_ok(), "list_repos_tool must succeed");
}

// ---------------------------------------------------------------------------
// Defect 5.6 — cursor MAC verification uses constant-time comparison
// ---------------------------------------------------------------------------

#[test]
fn cursor_mac_verify_uses_constant_time_comparison() {
    // Structural proof: the search.rs source file must reference subtle's ct_eq.
    let src = include_str!("../src/tools/search.rs");
    assert!(
        src.contains("ct_eq") || src.contains("ConstantTimeEq"),
        "cursor MAC must use a constant-time comparator from subtle; check src/tools/search.rs"
    );
}

#[test]
fn cursor_mac_verify_rejects_tampered_mac() {
    use gather_step_mcp::tools::search::verify_cursor_mac_for_test;

    let key = [0xab_u8; 32];
    let payload = b"test-payload";
    let good_mac = blake3::keyed_hash(&key, payload);

    // Correct MAC passes
    assert!(
        verify_cursor_mac_for_test(payload, good_mac.as_bytes(), &key),
        "correct MAC must pass verification"
    );

    // One-bit-flipped MAC must fail
    let mut bad_mac = *good_mac.as_bytes();
    bad_mac[0] ^= 0x01;
    assert!(
        !verify_cursor_mac_for_test(payload, &bad_mac, &key),
        "tampered MAC must fail verification"
    );
}
