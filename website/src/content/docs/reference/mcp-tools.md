---
title: "gather-step MCP tools reference"
description: "Reference for every MCP tool exposed by gather-step serve, organized by the kinds of questions an AI assistant uses them to answer automatically."
---

The `gather-step serve` command exposes a local stdio MCP server over the indexed workspace graph.

In normal use, engineers do not call these tools manually. An MCP-aware assistant selects them automatically based on the task. This page exists as a reference so the tool surface is explicit, inspectable, and easier to debug.

## Tool Groups

- **Orientation**: understand what is indexed before deeper queries
- **Search and traversal**: find symbols and walk local call relationships
- **Topology and impact**: trace routes, events, deployments, and cross-repo blast radius
- **PR review**: build a disposable review index for a PR branch and return a structured delta report
- **Contracts**: inspect payload shape and producer-consumer drift
- **Context retrieval**: return short summaries, combined context, and task-shaped packs
- **Repo intelligence**: inspect ownership, dead code, conventions, and repo summaries

## Orientation

### `get_graph_schema`

> "What kinds of graph nodes and edges are available in this workspace?"

Used automatically at the start of a new session when the assistant needs a compact view of the indexed graph shape before issuing more specific calls.

### `get_graph_schema_summary`

> "Give me the quick schema summary for this workspace."

Alias of `get_graph_schema`. It exists for client compatibility and returns the same compact graph-shape summary.

### `list_repos`

> "Which repositories are indexed right now, and are they fresh?"

Used automatically when the assistant needs to confirm repo coverage, framework detection, file counts, symbol counts, and freshness before trusting later answers.

## Search and Traversal

### `search`

> "Find `createOrder` in the indexed workspace."

Used automatically to locate symbols, routes, topics, types, or files before deeper analysis. This is often the first step before `get_symbol`, `trace_route`, or a context pack.

### `get_symbol`

> "Show me the stored metadata for this symbol ID."

Used automatically after `search` when the assistant needs the exact symbol record, including repo, file path, source span, and other stored metadata.

### `get_callers`

> "What calls into this function or method?"

Used automatically when the assistant is debugging upstream entry points, understanding who depends on a function, or preparing review context.

### `get_callees`

> "What does this function call downstream?"

Used automatically when the assistant needs the direct delegated work of a function, method, or handler before tracing broader impact.

## Topology and Impact

### `trace_impact`

> "What features, repos, or pages could be affected if I change this symbol?"

Used automatically to estimate cross-repo blast radius through routes, events, queues, topics, shared symbols, and other virtual graph surfaces.

### `trace_event`

> "Who produces and who consumes `order.created`?"

Used automatically when the assistant needs the producer-consumer map for an event-like target across one or more repos.

### `trace_route`

> "Which clients call `POST /orders`, and which handler serves it?"

Used automatically when the assistant needs the route surface for a known HTTP method and path, including callers and handlers attached to the same route node.

### `crud_trace`

> "Show me the end-to-end flow for `POST /orders`, including callers, handlers, and persistence touchpoints."

Used automatically when the assistant needs a fuller request-path trace than `trace_route`, especially for debugging or implementation planning.

### `event_blast_radius`

> "If this event changes, what downstream code is likely affected?"

Used automatically when the assistant needs a transitive downstream walk from an event-like node rather than only the direct producer and consumer list.

### `list_orphan_topics`

> "Which topics or events have only producers or only consumers?"

Used automatically for event-topology audits, dead-path investigation, and integration checks where the assistant needs to surface incomplete or stale async wiring.

### `cross_repo_deps`

> "What other repositories does this repo depend on through shared graph surfaces?"

Used automatically when the assistant needs repo-level dependency structure before a refactor, migration, or deployment-isolation discussion.

### `where_deployed`

> "Where is service `api` deployed?"

Used automatically when the assistant needs concrete deployment evidence for a service-like workload. The request accepts `service`, optional `repo`, and optional `limit` (1-100).

### `service_env`

> "Which env vars does service `worker` read?"

Used automatically before env var changes or deployment-sensitive implementation work. Values from env files are not returned; Gather Step indexes names only.

### `env_var_consumers`

> "Which services consume `DATABASE_URL`?"

Used automatically to find deployment-level env var consumers across indexed Docker, Compose, Kubernetes, Kustomize, explicit Helm chart, GitHub Actions, configured env-file artifacts, and Compose `env_file` references.

### `undeployed_services`

> "Which indexed services have no deployment edge?"

Used automatically when planning needs to distinguish code/service nodes from deployable runtime owners.

### `deployed_but_no_code`

> "Which deployments have no connected service/source evidence?"

Used automatically for deployment-topology audits, especially after service renames, repo splits, or GitOps drift.

### `shared_infra`

> "What shared brokers or databases appear in deployment config?"

Used automatically when the assistant needs runtime-adjacent infrastructure names before planning a change.

### `get_shared_type_usage`

> "Where is this shared type used across the workspace?"

Used automatically when the assistant needs repo and file usage for a shared symbol or DTO before changing that contract.

## PR Review

### `pr_review`

> "Review this PR using gather-step."

Used automatically when the user asks to review a pull request, do a structural PR review, check what a branch changed, or analyze cross-repo impact of a PR. Trigger phrases include "review this PR", "review the PR using gather-step", "do a code review", "what does this PR change", "analyze the impact of branch X".

**Inputs.**

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `base` | string | yes | Base ref (branch name, tag, or full SHA). The PR's target branch — typically `main`. |
| `head` | string | yes | Head ref (branch name, tag, or full SHA). The PR's source branch. |
| `keep_cache` | bool | no | Preserve the review artifact for follow-up `impact`/`trace`/`pack` queries. Default: `false` — the artifact is deleted after the report is returned. |
| `severity` | string | no | One of `warn` (default), `strict`, `pedantic`. `strict` and `pedantic` cause non-zero exit on threshold violations; `warn` always returns the report regardless. |

**Returns.** A JSON `DeltaReport` (`schema_version: 8`) with these top-level sections:

- `metadata` — base/head SHAs, checkout mode, indexed repos, elapsed time, warnings (e.g., baseline-vs-base mismatch).
- `safety` — review storage path, run id, cleanup policy, cache key.
- `changed_files` — list of repo-relative paths changed in `merge_base..head`.
- `evidence` — canonical evidence rows computed from the typed delta surfaces at query time.
- `routes` — added / removed / changed HTTP routes by `(method, canonical_path)`. Carry handler info via `Serves` edges and downstream impact summaries.
- `symbols` — added / removed / changed exported symbols by `(repo, qualified_name)`. Detects `signature_changed` and `visibility_changed` flags. Removed and changed surfaces carry impact summaries.
- `payload_contracts` — field-level diffs (added / removed / type-changed / `optional`-required flips). Removed and changed contracts can carry impact summaries.
- `events` — producer/consumer set diffs across `Topic`, `Queue`, `Subject`, `Stream`, and `Event` virtual nodes.
- `decorators` — added / removed / changed permission, audit, and authorization decorators.
- `contract_alignments` — cross-repo clusters of related payload contracts with confidence scores.
- `removed_surface_risks` — removed routes / symbols / events with surviving consumers, classified by severity (`high` / `medium` / `low`).
- `deployment` — added / removed / changed deployment-topology surfaces (Dockerfiles, Compose services, K8s manifests, env vars, secrets, config maps, brokers, databases, GitHub Actions deploy jobs).
- `suggested_followups` — synthesized `gather-step pack` and `gather-step trace crud` commands for the highest-impact deltas.

**Hard invariants.**

- The workspace's normal `.gather-step/storage` and `.gather-step/registry.json` are never modified.
- Review artifacts live under the OS cache directory by default (`<cache>/gather-step/pr-review/<workspace_hash>/<run_id>/`).
- Baseline index is checked against the resolved `--base` SHA; mismatches surface as a `metadata.warnings` entry rather than a hard error.

**Latency.** First runs take ~30-90 seconds because a fresh review index is built. Cache-hit runs complete in 1-2 seconds when a retained matching artifact exists for the same `(base_sha, head_sha)` pair.

**Cleanup.** Without `keep_cache`, the artifact is removed when the report is returned. With `keep_cache`, the artifact survives until `pr-review clean` is run (or the OS cache root is cleared). The `suggested_followups` field includes commands pre-filled with `--registry` / `--storage` overrides pointing at the kept review index.

**Implementation note.** The MCP tool shells out to the `gather-step` binary's `pr-review` subcommand. The binary must be on PATH or in the same directory as the MCP server.

## Contracts

### `payload_schema`

> "What payload shape does this event or route appear to use?"

Used automatically when the assistant needs the inferred producer and consumer schema for a virtual target so it can reason about fields instead of only symbol names.

### `contract_drift`

> "Are producers and consumers disagreeing on this payload contract?"

Used automatically when the assistant needs mismatches between the producer-side and consumer-side inferred shapes for the same target.

### `projection_impact`

> "If this field changes, which source fields, projected fields, filters, indexes, and backfills need review?"

Used automatically when the assistant needs static field-level evidence for denormalized or persisted projections. The request accepts `target`, optional `repo`, optional `limit` (1-100), and optional `evidence_verbosity` (`summary` or `full`). The tool returns source and projected fields, derivation edges, read/write/filter/index/backfill evidence, evidence-source labels such as `direct_field_access` and `local_alias_field_access`, missing evidence, and risk hints such as `source_field_unreviewed`, `backfill_unproven`, `index_or_search_mapping_unproven`, `frontend_only_focus`, `optional_payload_filter_mismatch`, `deployed_owner_unchecked`, and `deployed_owner_topology_observed`.

### `breaking_change_candidates`

> "If I change this DTO or producer payload, which consumers are at risk?"

Used automatically when the assistant needs a targeted breaking-change view tied to a producer symbol, DTO, or related contract surface.

## Context Retrieval

### `brief`

> "Give me a one-screen summary of what this symbol is and why it matters."

Used automatically for short summaries when the assistant needs lightweight orientation without spending budget on a deeper trace or pack.

### `context`

> "Give me the combined context around this target."

Used automatically when the assistant needs a broader stitched view that combines symbol metadata, traversal, repo context, and impact hints in one call.

### `context_pack`

> "Build a focused context pack for this target."

Used automatically when the assistant wants a bounded task-shaped retrieval and already knows which mode it wants, such as `planning`, `debug`, `fix`, `review`, or `change_impact`.

### `get_context_pack`

> "Return the context pack for this target."

Alias of `context_pack`. It exists for client compatibility and returns the same bounded pack response.

### `planning_pack`

> "I’m about to work on this area. What do I need to understand first?"

Used automatically when the assistant needs planning-oriented context focused on entry points, dependencies, related surfaces, and likely next investigation steps.

### `plan_change`

> "Help me plan this change."

Alias of `planning_pack`. It exists for clients that prefer a more task-shaped name.

### `debug_pack`

> "This behavior is broken. Give me the most relevant debug context."

Used automatically when the assistant needs inbound paths, nearby event surfaces, persistence touchpoints, and other debugging-oriented evidence.

### `fix_pack`

> "I know the issue. What is the smallest safe edit surface?"

Used automatically when the assistant needs a narrower context set for applying a targeted fix without the wider planning surface.

### `fix_surface`

> "Show me the fix surface for this target."

Alias of `fix_pack`. It exists for clients that frame the same retrieval in terms of edit surface rather than pack mode.

### `review_pack`

> "I’m reviewing this area. What should I think about before approving a change?"

Used automatically when the assistant needs review-oriented context such as impacted relationships, conventions, ownership hints, and nearby hotspots.

### `change_impact_pack`

> "What is the full blast radius if this changes?"

Used automatically when the assistant needs the richest impact-oriented context pack, including cross-repo dependents, bridges, and identified gaps.

### `get_change_impact_pack`

> "Return the change-impact pack for this target."

Alias of `change_impact_pack`. It exists for client compatibility and returns the same impact-focused pack.

### `batch_query`

> "Run the search, context, and impact lookups together so I can answer this in one pass."

Used automatically when the assistant wants multiple bounded Gather Step queries in one round trip instead of making several separate tool calls.

## Repo Intelligence

### `who_owns`

> "Who has the strongest history of working in this file or symbol?"

Used automatically when the assistant needs history-based ownership percentages, likely owners, or bus-factor hints for review routing and change risk.

### `get_dead_code`

> "What dead-code candidates exist in this repo?"

Used automatically when the assistant needs graph-reachability-based dead-code candidates while reviewing cleanup work or repo health.

### `get_conventions`

> "What structural conventions does this repo seem to follow?"

Used automatically when the assistant wants repeated patterns discovered from the indexed graph so it can align suggestions with existing code shape.

### `get_overview`

> "Give me the high-level overview of this repo."

Used automatically when the assistant needs a repo summary that combines graph-level shape with git-derived signals and other indexed analytics.

## Notes for Operators

- All tools are read-only against the indexed state.
- Results depend on index freshness. Run `index` again, or keep `watch` running, if the code has changed.
- The assistant selects these tools automatically. The command names here are reference material, not a required manual workflow.
