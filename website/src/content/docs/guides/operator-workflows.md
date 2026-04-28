---
title: Operator Workflows
description: Practical CLI workflows for inspecting the local code graph, tracing routes and Kafka event topology, estimating cross-repo blast radius, building context packs, and keeping the index fresh.
---

This page covers the day-to-day command workflows you will use after the
workspace is indexed. Commands are shown using the short form that assumes
`gather-step` is on your `PATH`. Append `--workspace /path/to/workspace` to
every command if you have not set the workspace through an environment variable
or config.

For flag-level reference on any command, run `gather-step <command> --help`.

## Inspect a Workspace

Always start here before running deeper analysis. These commands tell you
whether the index exists, what it contains, and whether it is healthy enough
to trust.

```bash
gather-step status
gather-step doctor
gather-step search <QUERY> --limit 20
```

### `status`

Prints a table of all configured repos with these columns:

| Column | What it shows |
|---|---|
| repo | Logical repo name from `gather-step.config.yaml` |
| files | Number of source files indexed |
| symbols | Number of named symbols extracted |
| nodes | Graph node count for this repo |
| edges | Graph edge count touching this repo |
| unresolved | Call sites that could not be resolved to a target |
| semantic health | Summary of framework extraction quality |

Add `--json` to get machine-readable output for scripting.

### `doctor`

Runs a structured health check against the indexed state. It reports issues
in five categories:

1. **workspace** — missing repo paths, config validation errors
2. **dangling edges** — edges whose target node no longer exists in the graph
3. **unresolved inputs** — call sites with no confident resolution, surfaced
   as actionable items
4. **search projection** — nodes that should be in the search index but are
   absent
5. **semantic-link** — framework-level extraction gaps (for example, a route
   node with no handler edge)

Run `doctor` before trusting benchmark results, pack output, or trace results
on an unfamiliar workspace.

### `search`

Searches the indexed symbol space by name or pattern:

```bash
gather-step search createOrder --limit 10
gather-step search createOrder --kind Function --limit 5
```

The `--kind` flag filters by node kind (for example `Function`, `Class`,
`Route`, `Topic`). Use `search` to locate a symbol's ID before passing it to
`trace crud --symbol-id` or `pack`.

## Debug a Route Flow (CRUD Trace)

Route tracing answers the question: which frontend caller reaches this backend
route, which handler serves it, and what does the request touch downstream?

### By route

```bash
gather-step trace crud --method POST --path /orders
```

### By backend symbol

```bash
gather-step trace crud --symbol-id <SYMBOL_ID>
```

The output contains:

- **frontend callers** — symbols in frontend repos that call this route, with
  evidence labels
- **backend handlers** — the NestJS (or equivalent) handler node that serves
  the route
- **continuation nodes** — services, functions, and methods the handler calls
- **entities** — schema-like nodes reachable from the continuation path
- **persistence hints** — database-adjacent nodes with confidence and traversal
  depth annotations

Evidence labels distinguish how a caller was resolved:

| Label | Meaning |
|---|---|
| `literal` | The path string appears as a literal in the source |
| `imported_constant` | The path was traced through an imported constant |
| `hint` | Heuristic match — treat with lower confidence |

Dynamic endpoints that cannot be safely reduced to a canonical path remain
unresolved rather than being silently mislinked.

## Map Async Topology (Events)

Event commands give you visibility into the Kafka event topology baked into
the code graph. Producers and consumers are modeled as first-class nodes, so
cross-repo event flows become graph traversals rather than text searches.

```bash
gather-step events trace order.created
gather-step events blast-radius order.created --depth 2
gather-step events orphans
```

### `events trace <SUBJECT>`

Follows the event from every producer to every consumer. Output identifies
which repos emit the event, which repos handle it, and the inferred payload
contract on each side. Use this when debugging a missing event or checking
that the consumer set is what you expect.

### `events blast-radius <SUBJECT> --depth <N>`

Expands the graph outward from the event node up to `--depth` hops. Each hop
follows downstream `PropagatesEvent` and `Consumes` edges and records the
accumulated confidence at each level. Use this to understand how many repos
are transitively affected when a topic changes.

### `events orphans`

Lists events that have producers but no consumers, or consumers but no
producers, in the indexed workspace. These are candidates for dead-code review
or missing-handler investigation.

For architectural background on how events are modeled, see
[Concepts: event topology](/concepts/event-topology/).

## Estimate Change Impact

Two commands address change impact at different levels of depth.

### Lightweight cross-repo view

```bash
gather-step impact createOrder
```

`impact` performs a bounded graph traversal from the named symbol and returns
a list of nodes in other repos that are reachable through dependency edges. It
is fast and good for a quick sanity check before a refactor.

### Full context pack with ranked files

```bash
gather-step pack createOrder --mode change_impact
```

`pack --mode change_impact` runs a heavier analysis that returns ranked
relevant files, semantic bridges connecting the target to its consumers, a
list of identified gaps (for example, unresolved edges), and suggested next
steps. Use this when you need to communicate blast radius to a reviewer or
feed it to an AI coding assistant.

## Build Context Packs for AI Assistants

Context packs are the primary surface for preparing task-shaped context. A
pack bundles the graph neighborhood relevant to a target into a bounded,
ranked response rather than a raw graph dump.

### Basic syntax

```bash
gather-step pack <TARGET> --mode <MODE>
```

### Supported modes

| Mode | Best for |
|---|---|
| `planning` | Estimating scope and identifying dependencies before starting work |
| `debug` | Investigating a broken behavior with relevant call paths highlighted |
| `fix` | Focused context for applying a targeted fix |
| `review` | Summarizing what changed and what it touches for review preparation |
| `change_impact` | Blast-radius analysis before a refactor or API change |

### Additional flags

```bash
gather-step pack createOrder --mode planning --limit 50 --depth 3 --budget-bytes 65536
```

| Flag | Effect |
|---|---|
| `--limit <N>` | Maximum number of ranked items to include |
| `--depth <N>` | Maximum traversal depth from the target node |
| `--budget-bytes <N>` | Hard size cap on the response, useful when feeding output to a context window |
| `--repo <NAME>` | Restrict the pack to a single configured repo |

The response includes ranked relevant items, semantic bridge nodes (cross-repo
connectors), next-step suggestions generated from graph structure, and a list
of unresolved gaps. For a deeper explanation of how packs are assembled, see
[Concepts: context packs](/concepts/context-packs/).

## Generate Derived Artifacts

```bash
gather-step generate claude-md
gather-step generate codeowners
```

### `generate claude-md`

Generates `.claude/rules/*.md` files from the live graph state. The output
files summarize system architecture, routes, and events in a format that can
be committed to the repository and loaded by Claude Code as assistant context.
Because the files are derived from the indexed graph rather than maintained by
hand, they stay in sync with the codebase as the graph is refreshed.

The generator applies a byte budget so the output stays within practical
context-window limits.

### `generate codeowners`

Generates a CODEOWNERS-format file derived from ownership signals in the
indexed graph. Use this as a baseline for repository ownership configuration.

## Keep the Index Fresh

### Manual incremental re-index

```bash
gather-step --workspace /path/to/workspace index
```

Re-running `index` on an already-indexed workspace is incremental. It compares
current file hashes against stored state, re-parses only changed files and
their dependents, and reconciles the graph. You do not need to `clean` first.

### Live watch mode

```bash
gather-step --workspace /path/to/workspace watch
```

`watch` starts a file-system watcher that applies incremental indexing
automatically as files change. Operational details:

- **debounce** — events are batched over a short window before triggering
  re-indexing to avoid thrashing on rapid saves.
- **overflow rescan** — if the event queue overflows (burst of many changes at
  once), the watcher schedules a repo-wide incremental pass rather than
  silently missing updates.
- **repo-level backoff** — if a repo produces consecutive indexing errors, it
  is temporarily suppressed rather than retried in a tight loop.
- **clean shutdown** — `Ctrl+C` cleanly stops the watcher, stops the local daemon,
  and emits the final status summary; pending queued changes are not guaranteed
  to be indexed before exit.

Use `watch` during active development sessions when you want CLI and MCP
answers to reflect current code without manual re-indexing.

### Full reindex

```bash
gather-step --workspace /path/to/workspace reindex
```

Deletes and rebuilds the full index in one command. Use this after large-scale
refactors, config changes, or when incremental state has drifted.

### Compact generated state

```bash
gather-step --workspace /path/to/workspace compact
```

Compacts the generated graph and metadata stores without deleting indexed
state. Use it after large reindexes or long watch-mode sessions when you want
to compress `.gather-step/` storage but keep CLI and MCP queries available.

## Clean Local State

```bash
gather-step --workspace /path/to/workspace clean --yes
```

Removes everything under `.gather-step/`. The `--yes` flag is required to skip
the interactive confirmation prompt. When using `--json` output, `--yes` is
also required so that automated pipelines cannot hang on a prompt.

Source repositories are not affected. Only generated index state is removed.

## Release Validation And Benchmarks

The benchmark harness lives in the `gather-step-bench` binary. It is primarily
for release work, not day-to-day operator queries.

```bash
gather-step-bench pr-oracle build-sample --help
gather-step-bench pr-oracle score --help
gather-step-bench release-gate --help
```

The release gate runs a real-workspace index with `gather-step index
--release-gate --artifact-path ...`, then checks high-contract probes,
planning-pack quality, event tracing, change-impact parity, and PR-oracle
scores. Operators pass explicit planning, event, and impact targets so the
gate cannot accidentally reuse one target shape for all checks.

The committed release-gate baseline came from the 2026-04-27 gate:

| Metric | Value |
|---|---:|
| Result | PASS |
| Indexed repos | 26 |
| Total files | 13,604 |
| Total symbols | 171,124 |
| Total edges | 405,384 |
| Cross-repo edges | 88,365 |
| Index wall time | 118.2s |
| PR-oracle median F1 / recall | 1.000 / 1.000 |

## JSON-First Output

Every command supports `--json` for machine-readable output. This is useful
for piping results into other tools, scripting workflows, and feeding output
to an AI assistant as structured data.

Three flags apply broadly across all commands:

| Flag | Effect |
|---|---|
| `--json` | Emit JSON instead of human-formatted tables and text |
| `--no-banner` | Suppress the startup banner (useful in scripted contexts) |
| `-v` / `--verbose` | Increase log verbosity for debugging |

The `--repo <NAME>` flag is also accepted by most commands to scope output to
a single configured repo.

## Next Steps

- [MCP clients](/guides/mcp-clients/) — expose the same graph to an AI coding
  assistant through the stdio MCP server.
- [CLI reference](/reference/cli/) — complete command and flag documentation.
- [Concepts: polyrepo graph](/concepts/polyrepo-graph/) — how cross-repo
  stitching works under the hood.
