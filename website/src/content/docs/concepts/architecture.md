---
title: Architecture
description: How Gather Step turns multiple repositories into a queryable local code graph — crates, storage model, consistency guarantees, and the full indexing pipeline explained.
---

## Pipeline at a Glance

The following is the end-to-end batch indexing flow for a multi-repo workspace:

```text
gather-step.config.yaml
        |
        v
  Workspace Registry
        |
        v
  Repo Traversal (per repo, .gitignore-aware, file classification + hashing)
        |
        v
  Framework Detection (NestJS, React Query, manifest analysis, ...)
        |
        v
  Source Parsing (swc for TypeScript/JavaScript, tree-sitter for Python)
        |
        v
  Framework-Aware Extraction (routes, events, entities, decorators, ...)
        |
        v
  Call Resolution (ImportMap -> SameModule -> Unique -> Suffix -> FuzzyName -> Fallback)
        |
        v
  Payload Contract Inference (producer shapes + consumer shapes)
        |
        v
  File Batch Assembly
        |
        v
  Storage Coordinator
  |         |         |
redb     Tantivy    SQLite
(graph)  (search)  (metadata)
        |
        v
  Cross-Repo Stitching (virtual node attachment)
        |
        v
  Analysis Layer (event topology, blast radius, contract drift, dead code, ...)
        |
        v
  CLI  /  MCP Server  /  Generated Context Files
```

The design point is simple: expensive discovery happens once during indexing. Every later query reads from structured persisted state rather than traversing raw files again.

## Main Crates

The workspace is split into eight purpose-specific crates. Each has a narrow responsibility. Delivery surfaces (`cli`, `mcp`, `output`) stay thin and reuse the same indexed facts.

| Crate | Responsibility |
|---|---|
| `gather-step-core` | Shared contracts: config, workspace registry, node and edge schema, deterministic ID generation, virtual node helpers. The system-wide contract layer. |
| `gather-step-parser` | Repo traversal, tree-sitter parsing, manifest extraction, framework detectors, call resolution strategies, payload contract inference. All extraction is deterministic and file-oriented. |
| `gather-step-storage` | redb graph store, Tantivy search index, SQLite metadata database, the indexing coordinator, incremental logic, file watchers, and multi-store reconciliation. Owns consistency. |
| `gather-step-analysis` | Graph queries and derived analysis: event topology, contract drift, dead code detection, convention detection, cross-repo tracing, repo overview, semantic health. Query-oriented, always downstream of storage. |
| `gather-step-output` | Generated assistant context files and rule markdown, with byte budgeting for context-window practicality. |
| `gather-step-mcp` | Local stdio MCP server configuration, request limits, and tool implementations over the indexed graph. |
| `gather-step-cli` | The end-user command surface: `init`, `index`, `clean`, `search`, `trace`, `events`, `impact`, `status`, `doctor`, `pack`, `conventions`, `generate`, `watch`, `tui`, `serve`. |
| `gather-step-git` | Git history parsing, ownership signals, co-change analytics, and hotspot primitives used by the analysis layer. |

The separation is intentional. Parsing is deterministic and file-oriented. Storage is persistence-oriented. Analysis is query-oriented. The delivery layers are thin facades over the same indexed facts.

## Storage Model

Generated state lives in `WORKSPACE/.gather-step/` and is split across three specialized stores, each chosen for its access pattern.

### Graph Store — redb

`redb` is an embedded key-value store used as the canonical source of truth for graph traversal:

- all node records
- all edge records
- owner-file edge indexes (file-to-node and node-to-file maps)
- lookup tables keyed by repo, node kind, and external ID

Graph traversal queries — "find all consumers of this topic node", "expand edges from this file" — are served entirely from redb.

### Search Store — Tantivy

Tantivy is an embedded full-text search engine. Only search-relevant node kinds are indexed here, which keeps the search corpus compact. The Tantivy store handles:

- symbol name and qualified-name search
- fuzzy and prefix lookups for the `gather-step search` command
- MCP search tool responses

The Tantivy index is derived from the graph. It is not the source of truth; it is a read-optimized projection.

### Metadata Store — SQLite

SQLite stores operational and derived metadata that does not fit well in a graph or full-text store:

- file hash and index state records (used for incremental indexing)
- reverse dependency relationships for affected-set computation
- payload contract records per topic and side
- git analytics
- context pack records
- watcher and runtime state anchors

SQLite is where the system tracks what has been indexed, what has changed, and what the derived analysis has computed.

### Filesystem Layout

```text
WORKSPACE/
  .gather-step/
    registry.json       # workspace and repo metadata
    graph.redb          # graph store
    search/             # Tantivy index
    metadata.sqlite     # metadata database
```

## Consistency Model

Writing to three separate stores atomically is not possible in the general case. The `StorageCoordinator` implements a savepoint pattern that is atomic-enough for local single-developer use:

1. Pre-delete stale file-scoped metadata from the previous index run.
2. Begin a redb write transaction.
3. Create a persistent savepoint.
4. Write graph node and edge batches.
5. Update Tantivy and SQLite projections.
6. On any failure, roll back the graph transaction and clean partial state.

The coordinator writes through all three stores in sequence. If the write fails midway, the system can recover to a consistent prior state on the next run. The design does not pretend the stores are one database; it simply ensures failures leave the system in a recoverable position rather than a split-brain one.

## Query Model

The query surfaces are separated by their consumer and purpose:

- **Operator-oriented surfaces** (`search`, `status`, `doctor`) answer "what is in the graph" questions. They are suitable for interactive inspection.
- **Task-oriented surfaces** (`trace`, `events`, `impact`, `pack`) answer "how does this part of the system behave" questions. They are suitable for task setup, debugging, and review.
- **MCP tools** expose the same graph to AI clients in bounded, structured form. The design assumption is that assistants should query precomputed semantic state, not rediscover it from raw files.

All three surfaces read from the same indexed state. There is no separate query path for MCP vs CLI. The difference is in how the results are framed and sized.

## Concurrency Model

Repo indexing is guarded by per-repo file locks. This means:

- multiple repos can be indexed in parallel without trampling each other's state
- readers can query persisted graph state while a write is in progress on a different repo
- watch mode and batch indexing share the same lock discipline

The practical consequence is that `gather-step serve` can answer MCP queries while `gather-step watch` is updating a repo in the background. The data a reader sees is from the last completed write, not from a partially-written state.

## Incremental Indexing

The incremental flow is:

1. Snapshot current source file paths and manifest hashes.
2. Compare against stored file index states in SQLite.
3. Classify files as added, modified, deleted, or unchanged.
4. Ask SQLite for the reverse dependents of every changed file.
5. Re-index the changed set plus all affected dependents.
6. Purge deleted file state from graph, search, and metadata.
7. Reconcile projections across all three stores.

The key algorithmic choice is `compute_affected_set`. The system does not re-index only changed files because importers and symbol consumers can become stale when a dependency changes. Re-indexing only the directly modified file would leave callers pointing at outdated graph state.

## Watch Mode

The watch runtime layers operational safety on top of the incremental flow:

- repo-scoped filesystem watchers using the `notify` library
- debounce window to coalesce rapid save events
- capped pending file hints per repo to bound memory
- overflow-triggered repo-wide incremental rescan fallback when the hint queue is saturated
- consecutive-error tracking per repo
- repo-level backoff suppression after repeated failures
- runtime watch events plus a final `watch_status` summary on shutdown

If the watcher loses fidelity because too many events arrived at once, it schedules a repo-wide incremental pass with no path hint rather than silently missing updates. If the internal notify queue overflows, that fallback is scheduled for every watched repo.

## How the Pieces Work Together

The system loop in normal operation is:

1. `gather-step init` or manual config creation sets up `gather-step.config.yaml`.
2. `gather-step index` runs the full pipeline: traversal, parsing, extraction, resolution, persistence, cross-repo stitching.
3. The analysis crate reads the stored graph and metadata to compute event topology, contract drift, dead code candidates, and convention findings.
4. The CLI, MCP server, and rule generation expose those views to engineers and assistants.
5. `gather-step watch` (or repeated `index` calls) keeps the graph fresh as files change.

Every query surface — CLI commands, MCP tools, generated context files — is always downstream of the deterministic extraction and persisted state. Nothing is re-derived from raw source at query time.
