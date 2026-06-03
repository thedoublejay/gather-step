//! Canonical catalog of MCP tools exposed by `gather-step mcp serve`.
//!
//! Generated docs (`CLAUDE.gather.md`, `AGENTS.gather.md`) read from this
//! list, so adding or removing a tool needs to be reflected here.

/// `(tool_name, one-line description)` for every tool registered on the
/// MCP server. Order matches the broad categories used in the docs:
/// orientation, search, tracing, contracts, deployment, packs, ops.
pub const MCP_TOOLS: &[(&str, &str)] = &[
    // Orientation
    ("get_graph_schema", "Inspect graph node and edge schema"),
    (
        "get_graph_schema_summary",
        "Get a compact graph schema overview",
    ),
    ("list_repos", "List indexed repos and high-level status"),
    ("brief", "Get a compact orientation response"),
    ("context", "Get bounded context for a target"),
    ("get_overview", "Get a high-level workspace overview"),
    (
        "get_conventions",
        "Summarize detected workspace conventions",
    ),
    // Search & symbol intelligence
    ("search", "Find a symbol, file, or concept"),
    (
        "get_symbol",
        "Get source, location, and graph edges for a symbol",
    ),
    ("get_callers", "Find callers for a known symbol"),
    ("get_callees", "Find callees for a known symbol"),
    ("who_owns", "Find ownership evidence for a file or symbol"),
    ("get_dead_code", "Surface dead-code candidates"),
    // Tracing
    (
        "trace_impact",
        "Find everything affected by changing a symbol or file",
    ),
    (
        "trace_event",
        "Trace a domain event from producer to consumers",
    ),
    (
        "trace_route",
        "Trace an HTTP or RPC route from entry to handler",
    ),
    ("crud_trace", "Trace CRUD-style request paths"),
    ("event_blast_radius", "Inspect downstream event impact"),
    (
        "list_orphan_topics",
        "Find event topics without linked producers or consumers",
    ),
    // Contracts & cross-repo
    ("cross_repo_deps", "Inspect cross-repo dependency edges"),
    (
        "get_shared_type_usage",
        "Find all usages of a shared contract type",
    ),
    ("payload_schema", "Inspect inferred payload schema evidence"),
    ("contract_drift", "Find contract drift signals"),
    (
        "breaking_change_candidates",
        "Surface likely breaking-change risks",
    ),
    (
        "projection_impact",
        "Trace source, projected field, filter, index, and backfill evidence",
    ),
    // Deployment
    ("where_deployed", "Find deployments for an indexed service"),
    (
        "service_env",
        "List env vars consumed by an indexed service",
    ),
    ("env_var_consumers", "Find services that consume an env var"),
    (
        "undeployed_services",
        "Find indexed services without deployment evidence",
    ),
    (
        "deployed_but_no_code",
        "Find deployment nodes without service-code linkage",
    ),
    (
        "shared_infra",
        "Find databases and brokers with service consumers",
    ),
    // Context packs
    ("context_pack", "Get a task-shaped context pack"),
    ("get_context_pack", "Alias for task-shaped context packs"),
    (
        "planning_pack",
        "Context pack for architecture and planning tasks",
    ),
    (
        "plan_change",
        "Typed plan-change product (twelve planning sections)",
    ),
    ("debug_pack", "Context pack for debugging production issues"),
    ("fix_pack", "Context pack scoped for a bug fix"),
    ("fix_surface", "Get a narrower fix-oriented surface"),
    ("review_pack", "Context pack scoped for code review"),
    ("change_impact_pack", "Cross-repo change impact bundle"),
    ("get_change_impact_pack", "Alias for change impact context"),
    (
        "batch_query",
        "Run multiple read-only graph queries together",
    ),
    // PR review
    (
        "pr_review",
        "Build a disposable review index for a PR branch and return the delta report",
    ),
    (
        "pr_review_set",
        "Build coordinated disposable review indexes from a PR-set manifest or GitHub query",
    ),
];
