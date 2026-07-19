//! Canonical server `instructions` payload.
//!
//! The MCP `initialize` result carries an `instructions` string that clients
//! inject into the model's context on connect (see the MCP lifecycle spec).
//! It is the one place where cross-cutting "how to use this server" guidance
//! reaches an agent automatically, with no user action, the moment an upgraded
//! server reconnects. Keep it concise (it is prepended to the system prompt)
//! and describe the *current* capability surface — especially the response
//! signals an agent should act on — rather than a per-version changelog.
//!
//! When a new response signal or tool family is added, document it here and in
//! the relevant tool `description`; those two surfaces (plus the auto-generated
//! output schemas) are what make a feature discoverable on upgrade.

/// The default server instructions delivered to clients on connect.
pub const DEFAULT_INSTRUCTIONS: &str = "\
gather-step exposes a local, deterministic code graph over your indexed \
workspace: symbols, routes, events, queues, deployments, and cross-repo edges. \
Prefer these tools over guessing from file names — they surface cross-repo \
writers and dormant paths that text search misses.

Typical flow: orient (list_repos, get_overview, get_conventions) -> locate \
(search, get_symbol) -> trace (trace_impact, trace_route, trace_event, \
trace_agent, cross_repo_deps, who_consumes) -> verify before acting.

Response signals to act on:
- confidence_band: edges carry a band derived from numeric confidence — \
`extracted` (>=900, source-observed; verify source scope and semantic support), `inferred` (500-899, strong \
heuristic), or `hint` (<500, weak; verify before relying on it). Prefer \
higher-band edges and confirm `hint` edges from source before acting.
- index_stale: query-response metadata includes this list of repos whose index \
lags their current git HEAD. When it is present the result may be incomplete or \
outdated — re-run `gather-step index` and re-query before trusting an empty or \
negative answer.

Results are read-only and derived on demand; nothing you call here mutates the \
workspace or the graph.";

/// The default server instructions as an owned `String`.
#[must_use]
pub fn default_instructions() -> String {
    DEFAULT_INSTRUCTIONS.to_owned()
}

#[cfg(test)]
mod tests {
    use super::DEFAULT_INSTRUCTIONS;

    #[test]
    fn instructions_document_the_actionable_signals() {
        // These two signals are the ones an agent must act on; the instructions
        // are the surface delivered on connect, so guard their presence.
        assert!(DEFAULT_INSTRUCTIONS.contains("confidence_band"));
        assert!(DEFAULT_INSTRUCTIONS.contains("index_stale"));
        assert!(DEFAULT_INSTRUCTIONS.contains("extracted"));
    }
}
