---
title: "Gather Step CLI Reference"
description: "Complete command and flag reference for the Gather Step CLI. Covers commands, global flags, output modes, and exit codes."
---

The `gather-step` binary is the primary interface for managing workspace indexes and running the local MCP server. Every command reads its defaults from `gather-step.config.yaml` and the workspace-local state directory at `.gather-step/`.

## Global flags

These flags apply to every command. Pass them before the subcommand name.

| Flag | Type | Default | Description |
|---|---|---|---|
| `--workspace <PATH>` | path | `.` (current directory) | Workspace root path. All default config, registry, and storage paths are derived from this value. |
| `--repo <NAME>` | string | — | Restrict the command to one configured repo name. The name must match a repo listed in the config. |
| `-v, --verbose` | count | 0 (warn level) | Increase log verbosity. Pass once for `info`, twice for `debug`, three or more times for `trace`. Overridden by the `GATHER_STEP_LOG` environment variable. |
| `--json` | bool flag | false | Emit newline-delimited JSON to stdout instead of human-readable text. Tracing logs are still written to stderr in JSON format when this flag is set. |
| `--color <auto\|always\|never>` | enum | `auto` | Control ANSI color for command output after CLI parsing. `auto` respects TTY detection, `NO_COLOR`, `FORCE_COLOR`, and `TERM=dumb`; `--json` disables color in stdout payloads. Clap-rendered help and parse errors are emitted before command setup and follow Clap's own terminal color behavior. |
| `--no-banner` | bool flag | false | Suppress the startup banner printed to stderr. The banner is also suppressed when `--json` is active or when stderr is not a TTY. |
| `--no-interactive` | bool flag | false | Disable interactive prompts and use command defaults. Use this for scripts and CI. |

## Command index

- [`init`](#init) — Discover git repos and write an initial config file.
- [`index`](#index) — Build the full workspace code graph, search index, metadata, and context packs.
- [`reindex`](#reindex) — Clear existing state, then run a full index pass.
- [`clean`](#clean) — Delete workspace-local generated state.
- [`compact`](#compact) — Compact generated storage without deleting indexed state.
- [`status`](#status) — Summarize indexed repos, graph shape, and semantic health.
- [`doctor`](#doctor) — Surface broken graph assumptions, dangling edges, and semantic-link problems.
- [`search`](#search) — Search the indexed symbol graph by name.
- [`trace crud`](#trace-crud) — Trace a route-backed CRUD flow end-to-end.
- [`events trace`](#events-trace) — Show producers and consumers for an event-like target.
- [`events blast-radius`](#events-blast-radius) — Trace transitive downstream impact from an event-like target.
- [`events orphans`](#events-orphans) — List event-like targets that have only producers or only consumers.
- [`impact`](#impact) — Summarize which repos are touched by a symbol's cross-repo virtual targets.
- [`projection-impact`](#projection-impact) — Trace static source-to-projection field impact.
- [`deployment-topology`](#deployment-topology) — Query indexed deployment artifacts, env vars, and shared runtime infrastructure.
- [`pack`](#pack) — Return a bounded context pack for a target symbol.
- [`conventions`](#conventions) — Derive repeated structural conventions from the indexed graph.
- [`generate claude-md`](#generate-claude-md) — Generate assistant-facing CLAUDE.md rule files from the index.
- [`generate agents-md`](#generate-agents-md) — Generate a workspace summary for Codex-style `AGENTS.md` workflows.
- [`generate codeowners`](#generate-codeowners) — Generate a CODEOWNERS file from indexed ownership analytics.
- [`watch`](#watch) — Watch for file changes and trigger incremental indexing.
- [`tui`](#tui) — Open the opt-in full-screen workspace dashboard.
- [`setup-mcp`](#setup-mcp) — Register workspace-pinned Claude MCP settings.
- [`serve`](#serve) — Start the stdio MCP server.
- [`pr-review`](#pr-review) — Build an isolated review index for a PR branch and emit a delta report.

## Command details

### `init`

Discovers all git repositories nested under the workspace root and writes or updates `gather-step.config.yaml`. Skips `.git`, `.gather-step`, `node_modules`, `dist`, and `target` directories. Fails if no git repositories are found.

```bash
gather-step [GLOBAL FLAGS] init [--config <PATH>] [--force] \
  [--index | --no-index] [--watch | --no-watch] \
  [--generate-ai-files | --no-generate-ai-files] [--setup-mcp <SCOPE>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config <PATH>` | path | `<workspace>/gather-step.config.yaml` | Write the config to this path instead of the workspace default. |
| `--force` | bool flag | false | Regenerate the config from discovered repos instead of using the existing config as the starting point. |
| `--index` / `--no-index` | bool flag | prompt/default | Index discovered repos after writing the config, or skip indexing. |
| `--watch` / `--no-watch` | bool flag | prompt/default | Start watch mode after setup, or return immediately. |
| `--generate-ai-files` / `--no-generate-ai-files` | bool flag | prompt/default | Generate `.claude/rules/` when an index exists, plus `CLAUDE.gather.md` and `AGENTS.gather.md`. |
| `--setup-mcp <SCOPE>` | enum | prompt/default | Register the MCP server in `local` or `global` Claude settings. |

**Example**

```bash
cd /path/to/workspace
gather-step init

# non-interactive equivalent
gather-step init --index --generate-ai-files --setup-mcp local --no-watch
```

Interactive `init` starts with a numbered repository picker. Repos from an existing config are selected by default, removed repos stay unchecked, and `all` / `none` shortcuts let you quickly select or clear the list. The remaining prompts ask whether to index, generate AI context, register MCP, and start watch mode. Pressing Enter uses the defaults: index = yes, generate AI context = yes, MCP setup = local, watch = no. Non-interactive scripts should pass those flags explicitly. If `--generate-ai-files` runs before an index exists, Gather Step writes the root summaries and prints a warning that `.claude/rules/` generation requires `gather-step index`.

**Output shape (`--json`)** — emits one line:

```json
{"event":"init_completed","config_path":"...","repo_count":3,"repos":[{"name":"backend_standard","path":"apps/backend_standard"}]}
```

**When to use** — after cloning a multi-repo workspace for the first time, or when you want the guided picker to update the repo list in an existing config.

---

### `index`

Builds the complete workspace index: parses source files, constructs the code graph, builds the search projection, records file analytics via git history, and precomputes a set of context packs. Accepts all IndexArgs flags.

```bash
gather-step [GLOBAL FLAGS] index [--config <PATH>] [--registry <PATH>] [--storage <PATH>] \
  [--depth <LEVEL>] [--artifact-path <PATH>] [--release-gate] [--auto-recover] [--watch]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--config <PATH>` | path | `<workspace>/gather-step.config.yaml` | Path to the workspace config file. |
| `--registry <PATH>` | path | `<workspace>/.gather-step/registry.json` | Override the workspace-local registry path. |
| `--storage <PATH>` | path | `<workspace>/.gather-step/storage` | Override the workspace-local storage directory. |
| `--depth <LEVEL>` | enum | config value or `full` | Override the indexing depth for all repos. Accepts `level1`, `level2`, `level3`, or `full`. |
| `--artifact-path <PATH>` | path | — | Write the index JSON payload to this path for release-pipeline archival. |
| `--release-gate` | bool flag | false | Require a clean git worktree and enforce release-gate index summary invariants. |
| `--auto-recover` | bool flag | false | Delete generated index state before rebuilding. Use when state is corrupt or uses an unsupported schema. |
| `--watch` | bool flag | false | Enter watch mode after indexing completes. In interactive human mode, the CLI prompts for this when the flag is omitted. |

**Example**

```bash
gather-step --workspace /path/to/workspace index
gather-step --workspace /path/to/workspace --repo backend_standard index --depth level2
```

**Output shape (`--json`)** — emits one line:

```json
{"event":"index_completed","config_path":"...","registry_path":"...","storage_root":"...","stats":{"total_repos":3,"indexed_repos":3,"total_files":1200,"total_symbols":8400,"total_edges":42000,"cross_repo_edges":120},"timings":{"total_wall_ms":120000,"graph_build_ms":63000,"parser_augment_ms":2500,"pack_precompute_ms":18000,"metadata_persist_ms":20,"search_flush_ms":200,"durable_sync_ms":150},"repos":[...]}
```

The `timings` object splits the index wall time into the main diagnostic
phases. Use these fields when investigating cold-start regressions, slow pack
precompute, or storage durability cost. The summary fields are phase-faithful:
`graph_build_ms` covers graph write commits, `parser_augment_ms` covers repo
parse/augment preparation, `pack_precompute_ms` covers context-pack warming,
and `metadata_persist_ms` covers metadata cache mutation. Cross-repo counting,
search flush, git analytics, durable sync, and pack target discovery are emitted
as their own timing fields.

**When to use** — after `init`, or when repos have changed significantly enough that an incremental `watch` cycle would be slower than a full rebuild.

---

### `reindex`

Clears existing registry and storage state, then runs a full `index` pass. Accepts the same flags as `index`. Use this to recover from a corrupted state or after major structural refactors.

```bash
gather-step [GLOBAL FLAGS] reindex [--config <PATH>] [--registry <PATH>] [--storage <PATH>] \
  [--depth <LEVEL>] [--artifact-path <PATH>] [--release-gate] [--auto-recover] [--watch]
```

Flags are identical to [`index`](#index). The clean step runs unconditionally before indexing begins.

**Example**

```bash
gather-step --workspace /path/to/workspace reindex --depth full
```

**When to use** — when `doctor` reports structural problems that incremental indexing cannot resolve.

---

### `clean`

Deletes the workspace-local registry and storage directory. This is a destructive operation. Without `--yes`, the command prints the paths to be deleted and requires the user to type `clean` to confirm. When `--json` is active, `--yes` is required because there is no interactive prompt.

Path overrides must stay inside the workspace-local `.gather-step/` directory. Attempts to point `--registry` or `--storage` outside that root are rejected.

```bash
gather-step [GLOBAL FLAGS] clean [--registry <PATH>] [--storage <PATH>] [--yes] [--include-review]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--registry <PATH>` | path | `<workspace>/.gather-step/registry.json` | Override the workspace-local registry path. Must stay inside `.gather-step/`. |
| `--storage <PATH>` | path | `<workspace>/.gather-step/storage` | Override the workspace-local storage directory. Must stay inside `.gather-step/`. |
| `--yes`, `-y` | bool flag | false | Skip the interactive confirmation prompt. Required when `--json` is active. |
| `--include-review` | bool flag | false | Also wipe all `pr-review` artifact directories for this workspace (OS cache dir). Without this flag, review artifacts kept with `--keep-cache` are not touched. |

**Example**

```bash
gather-step --workspace /path/to/workspace clean --yes
gather-step --workspace /path/to/workspace clean --yes --include-review
```

**Output shape (`--json`)** — emits one line:

```json
{"event":"clean_completed","registry_path":"...","storage_root":"..."}
```

**When to use** — before a full re-clone, or to free disk space when the workspace is no longer active. Pass `--include-review` to also remove any kept `pr-review` caches.

---

### `compact`

Compacts generated storage in place. This is the command to run when you want
to compress generated state: it can reclaim graph-store pages and checkpoint
the metadata database without deleting the index or re-reading source
repositories.

```bash
gather-step [GLOBAL FLAGS] compact [--storage <PATH>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--storage <PATH>` | path | `<workspace>/.gather-step/storage` | Override the workspace-local storage directory. Must stay inside `.gather-step/`. |

**Example**

```bash
gather-step --workspace /path/to/workspace compact
```

**Output shape (`--json`)** — emits one line:

```json
{"event":"compact_completed","storage_root":"...","graph_path":"...","graph_size_before_bytes":104857600,"graph_size_after_bytes":73400320,"graph_compacted":true,"metadata_compacted":true,"elapsed_ms":420}
```

**When to use** — after large reindexes or heavy incremental churn, when you
want to reclaim generated-state space without a destructive `clean`.

---

### `status`

Reads the registry and opens the storage coordinator to report per-repo freshness, file and symbol counts, graph node counts, unresolved call inputs, and semantic health summaries. Also reports workspace-level graph node and edge kind counts.

```bash
gather-step [GLOBAL FLAGS] status
```

No command-specific flags. Scope to a single repo with the global `--repo` flag.

**Example**

```bash
gather-step --workspace /path/to/workspace status
gather-step --workspace /path/to/workspace --repo backend_standard status --json
```

**Output shape (`--json`)** — emits one line:

```json
{"event":"status_completed","workspace":"...","registry_path":"...","storage_root":"...","repos":[{"repo":"backend_standard","path":"...","path_exists":true,"depth_level":"full","last_indexed_at":"1713200000","registry_file_count":400,"registry_symbol_count":2800,"graph_node_count":2800,"metadata_file_count":400,"unresolved_inputs":12,"frameworks":["nestjs","mongoose"],"semantic_health":{...}}],"graph":{...}}
```

**When to use** — to check whether a workspace is fresh before running analysis commands.

---

### `doctor`

Inspects each registered repo for broken workspace assumptions: missing paths, registry vs. metadata count mismatches, dangling edges, search projection failures, actionable unresolved call inputs, and semantic-link incompleteness. Exits non-zero only when the process itself fails, not when issues are found — the `ok` field in the output indicates health.

```bash
gather-step [GLOBAL FLAGS] doctor
```

No command-specific flags. Scope to a single repo with the global `--repo` flag.

**Example**

```bash
gather-step --workspace /path/to/workspace doctor
gather-step --workspace /path/to/workspace doctor --json
```

**Output shape (`--json`)** — emits one line:

```json
{"event":"doctor_completed","ok":false,"issue_count":2,"repos":[{"repo":"backend_standard","ok":false,"issues":["14 unresolved call input(s) remain"],"unresolved_inputs":14,"dangling_edges":0,"semantic_health":{...}}]}
```

**When to use** — after indexing to verify the graph is internally consistent before relying on MCP tools.

---

### `search`

Runs a prefix and fuzzy name search over the indexed symbol graph and returns ranked hits. Supports filtering by repo (via global `--repo`), node kind, and result count.

```bash
gather-step [GLOBAL FLAGS] search <QUERY> [--limit <N>] [--kind <KIND>]
```

| Argument/Flag | Type | Default | Description |
|---|---|---|---|
| `<QUERY>` | string (positional) | required | Search term. Supports partial names and fuzzy matching. |
| `--limit <N>` | usize | 20 | Maximum number of hits to return. Clamped to 128. |
| `--kind <KIND>` | string | — | Filter by node kind. Accepted values: `file`, `function`, `class`, `type`, `module`, `entity`, `route`, `topic`, `queue`, `subject`, `stream`, `event`, `shared_symbol`, `payload_contract`, `repo`, `convention`, `service`. |

**Example**

```bash
gather-step --workspace /path/to/workspace search createOrder
gather-step --workspace /path/to/workspace --repo backend_standard search OrderService --kind class --limit 5
```

**Output shape (`--json`)** — emits one line:

```json
{"event":"search_completed","query":"createOrder","total_hits":3,"hits":[{"repo":"backend_standard","file_path":"src/orders/orders.service.ts","line":42,"symbol_name":"createOrder","qualified_name":"OrdersService.createOrder","node_kind":"function","exact_match":true,"adjusted_score":1.0}]}
```

**When to use** — to find a `symbol_id` for use in MCP traversal tools, or to verify that a symbol was indexed correctly.

---

### `trace crud`

Traces a route-backed CRUD flow by resolving the route entry point and walking the graph to surface frontend callers, backend handlers, continuation nodes, entities, and database hints. Accepts either a `(method, path)` pair or a direct `symbol_id`. Both cannot be provided at the same time.

```bash
gather-step [GLOBAL FLAGS] trace [--registry <PATH>] [--storage <PATH>] crud --method <METHOD> --path <ROUTE_PATH> [--limit <N>]
gather-step [GLOBAL FLAGS] trace [--registry <PATH>] [--storage <PATH>] crud --symbol-id <SYMBOL_ID> [--limit <N>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--method <METHOD>` | string | — | HTTP method, e.g. `GET`, `POST`. Required when `--path` is provided. |
| `--path <ROUTE_PATH>` | string | — | Route path, e.g. `/orders`. Required when `--method` is provided. |
| `--symbol-id <SYMBOL_ID>` | string | — | Stable hex symbol ID as the trace entry point. Mutually exclusive with `--method`/`--path`. |
| `--limit <N>` | usize | 25 | Maximum matches per result section (callers, handlers, continuation, entities, database hints). |
| `--registry <PATH>` | path | workspace registry | Read symbol registry JSON from this path. Used by `pr-review --keep-cache` follow-up commands. |
| `--storage <PATH>` | path | workspace storage | Read storage artifacts from this directory. Used by `pr-review --keep-cache` follow-up commands. |

**Example**

```bash
gather-step --workspace /path/to/workspace trace crud --method POST --path /orders --limit 10
gather-step --workspace /path/to/workspace trace crud --symbol-id deadbeefdeadbeef
```

**Output shape (`--json`)** — emits one line with `event: "trace_crud_completed"` and fields for `callers`, `handlers`, `continuation`, `entities`, `database_hints`, `method`, `path`, `target_id`, `target_name`, and `truncated`.

**When to use** — when investigating how a specific HTTP endpoint is called and what it touches.

---

### `events trace`

Resolves a topic, queue, subject, stream, or event target by name suffix and shows all producer and consumer symbols attached to it. The `subject` must resolve to exactly one virtual event node; use `--repo` to disambiguate when multiple repos define nodes with similar names.

```bash
gather-step [GLOBAL FLAGS] events trace <SUBJECT> [--limit <N>]
```

| Argument/Flag | Type | Default | Description |
|---|---|---|---|
| `<SUBJECT>` | string (positional) | required | Event, topic, or queue identifier or name suffix. |
| `--limit <N>` | usize | 20 | Maximum matches per section (producers and consumers each). |

**Example**

```bash
gather-step --workspace /path/to/workspace events trace order.created
gather-step --workspace /path/to/workspace --repo backend_standard events trace order.created --limit 5
```

**Output shape (`--json`)** — emits one line with `event: "events_trace_completed"` and fields for `target`, `producers`, `consumers`, and `truncated`.

**When to use** — to map which services emit and which services consume a specific messaging event.

---

### `events blast-radius`

Walks the graph transitively outward from an event-like virtual node and returns all downstream symbols affected by a change to the event. Depth controls how many graph hops to follow.

```bash
gather-step [GLOBAL FLAGS] events blast-radius <SUBJECT> [--limit <N>] [--depth <N>]
```

| Argument/Flag | Type | Default | Description |
|---|---|---|---|
| `<SUBJECT>` | string (positional) | required | Event, topic, or queue identifier or name suffix. |
| `--limit <N>` | usize | 20 | Maximum nodes to return. |
| `--depth <N>` | usize | 2 | Blast-radius traversal depth in graph hops. |

**Example**

```bash
gather-step --workspace /path/to/workspace events blast-radius order.created --depth 3
```

**Output shape (`--json`)** — emits one line with `event: "events_blast_radius_completed"` and `blast_radius` array items with `depth`, `name`, `repo`, `file_path`, `node_kind`, and `cumulative_confidence`.

**When to use** — before modifying a Kafka topic or shared event shape to estimate cross-repo change surface.

---

### `events orphans`

Lists event-like virtual nodes that have producers but no consumers, consumers but no producers, or neither. These represent likely dead event pathways or incomplete integrations.

```bash
gather-step [GLOBAL FLAGS] events orphans [--limit <N>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--limit <N>` | usize | 20 | Maximum orphan targets to return. |

**Example**

```bash
gather-step --workspace /path/to/workspace events orphans --limit 50
```

**Output shape (`--json`)** — emits one line with `event: "events_orphans_completed"` and `orphans` array items with `name`, `kind`, `producers`, `consumers`, `classification`, and `severity`.

**When to use** — during event topology audits to find stale or incomplete message flows.

---

### `impact`

Searches for symbols matching a name, then for each matching symbol follows its outgoing edges to find virtual nodes (routes, topics, shared symbols). For each virtual node it traces which repos are reachable, producing a cross-repo impact summary.

```bash
gather-step [GLOBAL FLAGS] impact [--registry <PATH>] [--storage <PATH>] <SYMBOL> [--limit <N>]
```

| Argument/Flag | Type | Default | Description |
|---|---|---|---|
| `<SYMBOL>` | string (positional) | required | Symbol name to inspect. Used as a search query. |
| `--limit <N>` | usize | 20 | Maximum search candidates to inspect. |
| `--registry <PATH>` | path | workspace registry | Read symbol registry JSON from this path. Used by `pr-review --keep-cache` follow-up commands. |
| `--storage <PATH>` | path | workspace storage | Read storage artifacts from this directory. Used by `pr-review --keep-cache` follow-up commands. |

**Example**

```bash
gather-step --workspace /path/to/workspace impact OrderCreatedDto
```

**Output shape (`--json`)** — emits one line with `event: "impact_completed"` and `matches` array items each containing `source_repo`, `source_file`, `source_symbol`, and a `virtual_targets` list of touched cross-repo surfaces.

**When to use** — to quickly understand the blast radius of modifying a shared DTO or service class.

---

### `projection-impact`

Traces static field-level projection relationships for a target field. The report includes source fields, projected fields, derivation edges, readers, writers, filters, indexes, backfills, missing evidence, and planning risk hints.

```bash
gather-step [GLOBAL FLAGS] projection-impact --target <FIELD> [--limit <N>] \
  [--evidence-verbosity <summary|full>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--target <FIELD>` | string | required | Field or projected field name to inspect. |
| `--limit <N>` | usize | 20 | Maximum field candidates to inspect (1-100). |
| `--evidence-verbosity <summary\|full>` | enum | `full` | Controls whether large evidence lists are capped (`summary`) or returned in full (`full`). |

**Example**

```bash
gather-step --workspace /path/to/workspace --repo backend projection-impact --target subtaskIds --evidence-verbosity full --json
```

**Output shape (`--json`)** — emits one serialized projection-impact report with `target`, `resolved`, `ambiguity`, `candidates`, `source_fields`, `projected_fields`, `derivation_edges`, `readers`, `writers`, `filters`, `indexes`, `backfills`, `risk_hints`, `missing_evidence`, and `confidence`. Text output includes the most likely projection chain, missing evidence, and next checks. When deployment topology evidence exists for the affected repo, projection impact replaces `deployed_owner_unchecked` with `deployed_owner_topology_observed`; otherwise it keeps the warning and adds `deployment_topology` to `missing_evidence`.

JSON/YAML index mapping extraction is intentionally limited to filenames containing `mapping`, `index`, `search`, or `projection`, so ordinary manifests are not parsed as projection maps.

**When to use** — before changing a persisted projection, denormalized field, query filter, search mapping, or backfill-sensitive derived value.

---

### `deployment-topology`

Queries deployment artifacts indexed from Dockerfiles, Docker Compose, Kubernetes manifests, Kustomize files, explicit Helm chart artifacts, GitHub Actions workflows, configured env files, and env files referenced by Compose. Values from env files are not stored; only env var names are indexed.

```bash
gather-step [GLOBAL FLAGS] deployment-topology [--limit <N>] <SUBCOMMAND>
```

| Subcommand | Required flag | Description |
|---|---|---|
| `where-deployed` | `--service <NAME>` | Show deployments connected to a service-like workload. |
| `service-env` | `--service <NAME>` | Show env vars read by a service-like workload. |
| `env-var-consumers` | `--env-var <NAME>` | Show services that read an env var. |
| `undeployed-services` | — | List indexed service nodes with no deployment edge. |
| `deployed-but-no-code` | — | List deployment nodes with no connected service/source evidence. |
| `shared-infra` | — | List indexed brokers and databases observed in deployment config. |

| Flag | Type | Default | Description |
|---|---|---|---|
| `--limit <N>` | usize | 20 | Maximum result count (1-100). |

**Examples**

```bash
gather-step --workspace /path/to/workspace deployment-topology where-deployed --service api
gather-step --workspace /path/to/workspace --repo backend deployment-topology service-env --service worker --json
gather-step --workspace /path/to/workspace deployment-topology env-var-consumers --env-var DATABASE_URL
```

**Output shape (`--json`)** — emits one serialized deployment-topology report with `query`, optional `repo`, `deployments`, `services`, `env_vars`, `shared_infra`, `workflow_jobs`, `edges`, and `missing_evidence`. Text output starts with counts and then lists matched edges plus missing-evidence markers.

**When to use** — before planning deployment-sensitive code changes, checking runtime ownership, reviewing env var changes, or verifying whether projection-impact risk has concrete deployment evidence.

---

### `pack`

Returns a bounded context pack for a target symbol. A pack is a ranked, budget-capped bundle of the most relevant symbols, semantic bridges, suggested next steps, and unresolved gaps for a specific task mode. Context packs are precomputed for the top two symbols per repo during `index`, so pack retrieval is fast.

```bash
gather-step [GLOBAL FLAGS] pack [--registry <PATH>] [--storage <PATH>] <TARGET> [--mode <MODE>] [--limit <N>] [--depth <N>] [--budget-bytes <N>]
```

| Argument/Flag | Type | Default | Description |
|---|---|---|---|
| `<TARGET>` | string (positional) | required | Target symbol name or hex `symbol_id`. |
| `--mode <MODE>` | enum | `planning` | Pack mode. Accepts `planning`, `debug`, `fix`, `review`, `change_impact` (also accepted as `change-impact`). |
| `--limit <N>` | usize | 6 | Maximum ranked items to include in the pack. |
| `--depth <N>` | usize | 2 | Traversal depth for caller and callee context. |
| `--budget-bytes <N>` | usize | — | Optional response byte budget override. When the pack exceeds this limit, items are trimmed from the tail. |
| `--registry <PATH>` | path | workspace registry | Read symbol registry JSON from this path. Used by `pr-review --keep-cache` follow-up commands. |
| `--storage <PATH>` | path | workspace storage | Read storage artifacts from this directory. Used by `pr-review --keep-cache` follow-up commands. |

**Example**

```bash
gather-step --workspace /path/to/workspace pack OrdersService --mode planning
gather-step --workspace /path/to/workspace pack OrdersService --mode debug --depth 3 --limit 8
```

**Output shape (`--json`)** — emits one line with `event: "context_pack_completed"`, top-level `response_schema_version`, `data`, and `meta`. The `data` payload contains `mode`, `target`, `found`, ranked `items`, `semantic_bridges`, `transport_links`, `next_steps`, `unresolved_gaps`, `planning_rescue`, and `change_impact`. The `change_impact` block includes `confirmed_downstream_repos`, `probable_downstream_repos`, `downstream_repos` (backward-compatible alias), and `truncated_repos`. The `meta` block includes `resolution`, `resolved_symbol_id`, `candidate_count`, `completeness`, `budget`, `ambiguity`, `resolution_confidence`, `confidence_model_version`, `winner_margin`, and any warnings.

**When to use** — to hand a bounded, task-shaped context payload to an AI assistant before starting a feature, debugging session, or review.

---

### `conventions`

Derives repeated structural conventions from the indexed graph and registry, such as common naming patterns, module layouts, and decorator usage. Scoped to a single repo with the global `--repo` flag.

```bash
gather-step [GLOBAL FLAGS] conventions
```

No command-specific flags.

**Example**

```bash
gather-step --workspace /path/to/workspace conventions
gather-step --workspace /path/to/workspace --repo backend_standard conventions --json
```

**Output shape (`--json`)** — emits one line with `event: "conventions_completed"` and a `conventions` string array.

**When to use** — to generate an overview of established coding patterns before writing a convention rule file.

---

### `generate claude-md`

Generates Claude Code rule files for the workspace. With `--target=rules`, the command writes multiple graph-backed rule files under `.claude/rules/`. With `--target=summary`, the command writes a registry-only workspace summary to `CLAUDE.gather.md`.

```bash
gather-step [GLOBAL FLAGS] generate claude-md [--output <PATH>] [--repo <NAME>] \
  [--target <rules|summary>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--output <PATH>` | path | Workspace default locations | Explicit output file or directory. `rules` generates multiple files, so pass a directory path such as `./claude-rules/`; file-like paths such as `CLAUDE.md` are rejected. |
| `--repo <NAME>` | string | — | Limit graph-backed rule content to one repo. This still writes the shared rule files plus the repo-specific rule file. Overrides the global `--repo` flag. |
| `--target <rules|summary>` | enum | `rules` | Choose graph-backed rule files or the registry-only `CLAUDE.gather.md` summary. `--repo` is only valid with `rules`. |

**Example**

```bash
gather-step --workspace /path/to/workspace generate claude-md
gather-step --workspace /path/to/workspace generate claude-md --repo backend_standard --output ./claude-rules/
gather-step --workspace /path/to/workspace generate claude-md --target summary --output ./CLAUDE.gather.md
```

**Output shape (`--json`)** — emits one line with `event: "generate_claude_md_completed"` and `files` array of `{path, bytes}`.

**When to use** — after indexing, to produce context files for AI assistants working in each repo.

---

### `generate agents-md`

Generates a registry-only workspace summary for Codex-style agent workflows and writes it to `AGENTS.gather.md` unless `--output` is provided.

```bash
gather-step [GLOBAL FLAGS] generate agents-md [--output <PATH>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--output <PATH>` | path | `<workspace>/AGENTS.gather.md` | Explicit output path. |

**Example**

```bash
gather-step --workspace /path/to/workspace generate agents-md
```

**Output shape (`--json`)** — emits one line with `event: "generate_agents_md_completed"` and `files` array of `{path, bytes}`.

**When to use** — after indexing, to refresh lightweight workspace context for Codex or AGENTS.md-based assistants.

---

### `generate codeowners`

Generates a CODEOWNERS file from file-ownership analytics stored in the metadata database. Each entry maps a file path to the top owner email address. Requires that git history analytics have been indexed.

```bash
gather-step [GLOBAL FLAGS] generate codeowners [--output <PATH>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--output <PATH>` | path | `<workspace>/CODEOWNERS` | Explicit output path. |

**Example**

```bash
gather-step --workspace /path/to/workspace generate codeowners
gather-step --workspace /path/to/workspace generate codeowners --output .github/CODEOWNERS
```

**Output shape (`--json`)** — emits one line with `event: "generate_codeowners_completed"` and `files` array of `{path, bytes}`.

**When to use** — to bootstrap a CODEOWNERS file from actual commit history rather than hand-maintenance.

---

### `watch`

Runs a long-lived file watcher that detects source changes and triggers incremental per-repo indexing. While it is running, `watch` also starts the local workspace daemon so concurrent read-only CLI commands can query the live index. Emits structured watch events (start, complete, overflow, error) as they occur. Shuts down cleanly on `Ctrl+C` and emits a summary status line on exit.

In `--json` mode all events go to stdout as newline-delimited JSON. In human mode all output goes to stderr.

```bash
gather-step [GLOBAL FLAGS] watch [N] [--config <PATH>] [--storage <PATH>] \
  [--poll-interval-ms <N>] [--debounce-ms <N>] \
  [--consecutive-error-limit <N>] [--error-backoff-ms <N>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `N` | positional u64 | unlimited | Stop after this many completed indexing runs. |
| `--config <PATH>` | path | `<workspace>/gather-step.config.yaml` | Path to workspace config. |
| `--storage <PATH>` | path | `<workspace>/.gather-step/storage` | Path to storage root. |
| `--poll-interval-ms <N>` | u64 | 250 | Watch-loop cadence in milliseconds for debounce/backoff processing. On polling backends this is also the file-system poll interval. |
| `--debounce-ms <N>` | u64 | 2000 | Debounce window in milliseconds before triggering an indexing run after the last detected change. |
| `--consecutive-error-limit <N>` | u32 | 5 | Number of consecutive indexing errors before the watcher enters backoff. |
| `--error-backoff-ms <N>` | u64 | 5000 | Backoff duration in milliseconds after reaching the consecutive error limit. |

**Example**

```bash
gather-step --workspace /path/to/workspace watch
gather-step --workspace /path/to/workspace watch 1
gather-step --workspace /path/to/workspace watch --debounce-ms 500 --poll-interval-ms 100
```

Visible terminals show a spinner and labeled status lines. Non-TTY and CI runs keep stable stderr lines such as `watch:start`, `watch:indexing_complete`, and `watch:status`. `--json` emits NDJSON events on stdout and hides progress.

**When to use** — during active development, so AI assistant tools always query a fresh index.

---

### `tui`

Opens the opt-in full-screen workspace dashboard. The dashboard shows the current registry snapshot, copyable next commands, selected repo details, and a compact event log. It does not run the file watcher or mutate index state; use `watch`, `index`, or `reindex` for backend work. It never starts automatically from scripted commands.

```bash
gather-step [GLOBAL FLAGS] tui
```

Primary keys: `q` quit, `?` help, `/` filter, `Tab` next pane, `Enter` detail, `c` clear, `1`/`2`/`3` switch Symbols/Routes/Events. In filter mode, printable keys edit the filter; `Esc` or `Enter` exits filter mode.

The TUI requires stdin, stdout, and stderr to be TTYs. In scripts or CI, use `status`, `watch`, or `--json` instead.

---

### `setup-mcp`

Writes an idempotent `mcpServers.gather-step` block pinned to the current workspace. The generated entry uses the current `gather-step` binary path and launches the stdio MCP server for that workspace.

```bash
gather-step [GLOBAL FLAGS] setup-mcp [--scope <local|global>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--scope <local|global>` | enum | `local` | Write `.claude/settings.json` in the workspace or `~/.claude/settings.json`. |

**Example**

```bash
gather-step --workspace /path/to/workspace setup-mcp --scope local
```

**Output shape (`--json`)** — emits one line with `event: "setup_mcp_completed"`, `scope`, and `settings_path`.

**When to use** — after setup, so Claude can launch the workspace-pinned MCP server automatically.

---

### `serve`

Starts the stdio MCP server backed by an existing workspace index. The server reads from stdin and writes to stdout using the MCP protocol. The process runs until stdin is closed.

```bash
gather-step [GLOBAL FLAGS] serve [--graph <PATH>] [--registry <PATH>] \
  [--config <PATH>] [--max-limit <N>] [--server-name <NAME>] [--watch] \
  [--poll-interval-ms <N>] [--debounce-ms <N>] \
  [--consecutive-error-limit <N>] [--error-backoff-ms <N>] \
  [--trace-tool-calls <PATH>]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--graph <PATH>` | path | `<workspace>/.gather-step/storage/graph.redb` | Path to the graph store file. |
| `--registry <PATH>` | path | `<workspace>/.gather-step/registry.json` | Path to the workspace registry. |
| `--config <PATH>` | path | `<workspace>/gather-step.config.yaml` | Path to workspace config, used by `--watch`. |
| `--max-limit <N>` | usize | 1000 | Per-call result limit cap applied to all MCP tools. |
| `--server-name <NAME>` | string | `"gather-step"` | Server name reported to MCP clients in the `server_info` handshake. |
| `--watch` | bool flag | false | Run the filesystem watcher in the same process so the MCP server stays fresh during development. |
| `--poll-interval-ms <N>` | u64 | 250 | Watch-loop cadence in milliseconds. |
| `--debounce-ms <N>` | u64 | 2000 | Debounce window before triggering an indexing run after detected changes. |
| `--consecutive-error-limit <N>` | u32 | 5 | Consecutive indexing errors before watcher backoff. |
| `--error-backoff-ms <N>` | u64 | 5000 | Backoff duration after the error limit is reached. |
| `--trace-tool-calls <PATH>` | path | — | Append MCP tool-call trace records as JSONL for offline analysis. |

**Example**

```bash
gather-step --workspace /path/to/workspace serve
gather-step --workspace /path/to/workspace serve --watch
gather-step serve --graph .gather-step/storage/graph.redb --registry .gather-step/registry.json
```

**When to use** — to connect an MCP-capable AI assistant such as Claude Code to an indexed workspace. Add `--watch` during active development when you want one process to serve MCP and keep the index fresh.

---

### `pr-review`

Builds an isolated review index for a PR branch and emits a structured delta report. The review index is written to a disposable directory under the OS cache (`<cache>/gather-step/pr-review/<workspace-hash>/<run-id>/`) and deleted on exit unless `--keep-cache` is set.

The report (`schema_version: 7`) populates `metadata`, `safety`, `changed_files`, `suggested_followups`, and all typed delta surfaces (`routes`, `symbols`, `payload_contracts`, `events`, `contract_alignments`, `decorators`, `deployment`). Removed and changed payload contracts can carry downstream impact summaries.

The `deployment` surface captures changes to deployment topology: added, removed, and changed deployment targets, env-var additions and removals with the set of consumers that read each var, secret and config-map membership changes, shared-infra additions/removals, and workflow-job changes. Each deployment delta records the artifact kind inferred from the path (`dockerfile`, `compose`, `kubernetes`, `kustomize`, `helm`, `github_actions`, or `unknown`) plus a `change_reasons` list for file, service, stored image evidence, and env-var bindings.

**Run a review**

```bash
gather-step [GLOBAL FLAGS] pr-review --base <REF> --head <REF> [--engine temp-index] \
  [--severity <MODE>] [--format <FORMAT>] [--keep-cache] [--no-baseline-check]
```

| Flag | Type | Default | Description |
|---|---|---|---|
| `--base <REF>` | string | required | Base ref (branch, tag, SHA, or any git rev). |
| `--head <REF>` | string | required | Head ref (branch, tag, SHA, `HEAD`, etc.). |
| `--engine <ENGINE>` | enum | `temp-index` | Engine to use for the review index. `temp-index` builds a full isolated index. This flag is retained for forward-compatible engine selection; no alternate public engine is currently exposed. |
| `--severity <MODE>` | enum | `warn` | `warn` always exits 0. `strict` exits 2 on High risks or incompatible payload type changes. `pedantic` exits 2 on any Medium+ risk, any payload change, or removed permission decorators. |
| `--format <FORMAT>` | enum | `markdown` | `markdown` emits a human-readable Markdown report. `json` emits compact machine-readable JSON. `github-comment` emits Markdown truncated to GitHub's 65 536-char comment limit. `braingent` emits Markdown with YAML frontmatter for Braingent archival. |
| `--keep-cache` | bool flag | false | Keep the review artifact directory after the run. Cache hits are available when a retained matching artifact already exists. |
| `--github-comment-file <PATH>` | path | — | Write the GitHub-comment-formatted output to this file in addition to stdout. Accepted with any `--format` for scripting convenience. |
| `--no-baseline-check` | bool flag | false | Suppress the warning emitted when the workspace HEAD does not match `--base`. Use in CI environments where the workspace is always checked out at the feature branch. |
| `--json` | bool flag | false | **Deprecated.** Use `--format json` instead. Emits JSON output; equivalent to `--format json`. |
| `--strict` | bool flag | false | **Deprecated.** Use `--severity strict` instead. |

**Clean up review artifacts**

```bash
gather-step [GLOBAL FLAGS] pr-review clean --dry-run
gather-step [GLOBAL FLAGS] pr-review clean --run-id <ID>
gather-step [GLOBAL FLAGS] pr-review clean --base <REF> --head <REF>
gather-step [GLOBAL FLAGS] pr-review clean --older-than <DURATION>
gather-step [GLOBAL FLAGS] pr-review clean --all
```

Exactly one selector must be given. Combine `--dry-run` with any selector to preview without deleting.

| Flag | Type | Description |
|---|---|---|
| `--dry-run` | bool flag | List artifacts that would be deleted; delete nothing. |
| `--run-id <ID>` | string | Delete the artifact directory for one explicit run ID. Removes `InProgress` artifacts when explicitly targeted. |
| `--base <REF>` | string | Delete artifacts whose recorded base ref matches this ref. Must be used together with `--head`. |
| `--head <REF>` | string | Delete artifacts whose recorded head ref matches this ref. Must be used together with `--base`. |
| `--older-than <DURATION>` | string | Delete completed, failed, and quarantined artifacts older than this duration. Format: `<n><unit>` where unit is one of `s`, `m`, `h`, `d`, `w`. Skips `InProgress` artifacts to avoid racing a live indexing run. |
| `--all` | bool flag | Delete every review artifact for this workspace, including `InProgress` ones. |

**Examples**

```bash
# Run a review and print a Markdown report
gather-step --workspace /path/to/workspace pr-review --base main --head feature/my-branch

# Run with JSON output and keep the cache for follow-up queries
gather-step --workspace /path/to/workspace pr-review \
  --base main --head feature/my-branch \
  --json --keep-cache

# List all kept artifacts (dry run)
gather-step --workspace /path/to/workspace pr-review clean --dry-run

# Prune artifacts older than one week
gather-step --workspace /path/to/workspace pr-review clean --older-than 7d
```

**When to use** — to get a structural delta report before reviewing a PR. The `suggested_followups` in the report include `--registry` / `--storage` overrides that point follow-up commands at the kept review index rather than the workspace baseline.

---

## Compatibility aliases

The hidden `mcp serve` subcommand (`gather-step mcp serve`) is an undocumented alias for `gather-step serve`. It accepts identical flags. Prefer the top-level `serve` form in all client configurations.

## Progress output

`gather-step index` (and `reindex`) renders progress on stderr so that stdout stays reserved for structured output when `--json` is set. Progress rendering adapts to the execution environment.

### Human mode on a TTY

When stderr is an interactive terminal and `--json` is not set, the CLI draws two live progress indicators:

- A **workspace bar** at the top showing overall repo progress — `[elapsed] [======>  ] 3/7 repo-name`.
- A **per-repo spinner** below it showing the active phase — `  | parse repo-name [120/120]`.

The per-repo spinner shows numeric progress only for phases where the count is meaningful. File discovery reports a final count only (not a running total), so during that phase the indicator renders as a plain animated spinner with a phase label rather than a filling bar.

After the per-repo loop finishes, a **finalization spinner** narrates the remaining workspace-level work:

- `Flushing search index...` — final search-index commit.
- `Counting cross-repo edges...` — authoritative cross-repo accounting pass.
- `Precomputing N context packs...` — warm-cache pack generation.

The workspace bar stays visually complete during this window and finishes with `Workspace indexing complete.` once the finalization spinner clears. The final summary also includes elapsed time in `HH:MM:SS` and the on-disk index size.

### Non-TTY output (pipes, redirects, files)

When stderr is not an interactive terminal — for example `gather-step index 2>run.log` or `gather-step index | tee out.txt` — all progress bars and spinners are suppressed. Only structured log lines are written to stderr. No ANSI escape sequences or redraw sequences leak into captured logs.

This keeps log files and piped consumers clean without needing to strip terminal escapes.

### CI environments

When the `CI` environment variable is set to any non-empty value, progress rendering is suppressed even if stderr reports as a TTY. This matches the de-facto convention used by GitHub Actions, Buildkite, GitLab CI, and CircleCI, and prevents progress animation frames from accumulating in CI run logs.

To force progress rendering off regardless of TTY detection:

```bash
CI=1 gather-step index
```

To force it on in an automation context that happens to set `CI`, unset the variable for the single invocation:

```bash
env -u CI gather-step index
```

### `--json` mode

When `--json` is set:

- stdout receives exactly one newline-delimited JSON payload per command (the terminal event — `index_completed`, `search_completed`, etc.).
- stderr receives structured JSON-formatted tracing log lines.
- All progress bars and spinners are suppressed unconditionally, regardless of TTY state or `CI`.

This guarantees stdout remains parseable by `jq`, scripts, and downstream tools without filtering.

### Colors and ANSI

Colored output on stderr is also gated on stderr being a TTY. Pipes, redirects, and `CI`-set environments receive plain text with no ANSI color codes. `--json` mode always disables color.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Command completed successfully. |
| non-zero | An error occurred. The error message is printed to stderr. Specific non-zero codes are not currently distinguished beyond success vs. failure. |
