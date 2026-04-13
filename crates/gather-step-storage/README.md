# gather-step-storage

Storage backends and write orchestration for Gather Step.

This crate is where extracted graph data becomes durable local state. It
combines:

- `redb` for graph traversal and adjacency lookups
- `Tantivy` for symbol and text search
- `SQLite` for indexing state and relational metadata
- a `StorageCoordinator` that keeps those stores moving together

## What This Crate Provides

- `GraphStoreDb`: canonical node and edge storage
- `TantivySearchStore`: derived full-text and symbol search projection
- `MetadataStoreDb`: file-state, sync cursors, commits, PRs, analytics, and other metadata
- `StorageCoordinator`: repo-batch indexing flow across graph, search, and metadata
- `reconcile_*` helpers for rebuilding relational/search projections from stored graph state

## When To Use It

Use `gather-step-storage` when you need to:

- persist extracted `NodeData` and `EdgeData`
- answer graph queries such as file, repo, type, and adjacency lookups
- index query-worthy nodes into Tantivy
- track incremental indexing state and metadata in SQLite
- run file- or repo-scoped reindex flows through one boundary

If you only need the schema and identity types, use `gather-step-core`.

## Minimal Example

```rust
use gather_step_storage::StorageCoordinator;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = StorageCoordinator::open(".gather-step/storage")?;

    let _graph = storage.graph();
    let _search = storage.search();
    let _metadata = storage.metadata();

    Ok(())
}
```

Most real indexing paths go through `StorageCoordinator::index_repo_batch(...)`
rather than writing the three stores independently.

## Public Surface

### Graph

`GraphStoreDb` stores the graph of record.

Key operations:

- insert and fetch nodes
- insert and delete edges
- outgoing and incoming adjacency queries
- lookups by file, repo, node type, and external id
- bulk file-scoped replacement via `bulk_insert`

### Search

`TantivySearchStore` stores a derived search projection.

Key operations:

- batch index `SearchDocument` values
- search by user query
- delete indexed documents by file or repo
- rebuild search state from graph state through coordinator/reconcile flows

### Metadata

`MetadataStoreDb` stores operational and relational state.

Key operations:

- file hash state for incremental reindex decisions
- commit ingestion and lookup
- repo sync state
- file analytics and dependency projections
- concurrent readers with a dedicated writer connection

### Coordinator

`StorageCoordinator` is the intended integration boundary for normal indexing.

It owns:

- file dirty-checking through metadata
- delete-before-insert graph replacement
- search projection updates
- metadata state updates after successful indexing
- repo-scoped search reconciliation

## Architecture Notes

This crate intentionally uses different engines for different jobs:

- `redb` is optimized for graph traversal and adjacency lookups
- `Tantivy` is optimized for search relevance and fast candidate retrieval
- `SQLite` is optimized for metadata, sync state, and relational queries

The graph is the canonical derived store. Search is a rebuildable projection.
Metadata is operational state that supports incremental indexing and analysis.

## Important Invariants

- Node identity comes from `gather-step-core` and is deterministic.
- Edge ownership is file-scoped: `owner_file` must resolve to a `NodeKind::File`.
- Edge records are stored canonically once, with lightweight adjacency refs.
- Repo and file-path strings are interned in graph storage to reduce duplication.
- Search only indexes node kinds considered query-worthy by the core schema.
- Reads should observe either the state before a batch or the state after it, never a torn graph snapshot.

## Failure Model

This crate does not provide one fully atomic transaction across `redb`,
`Tantivy`, and `SQLite`.

Instead:

- graph writes are committed first
- search is updated as a derived projection
- metadata is updated after successful indexing
- reconcile paths exist to rebuild lagging projections from canonical stored state

That is a deliberate design choice for a local derived-data system.

## Module Map

- `graph_store`: redb-backed graph persistence
- `search_store`: Tantivy-backed search index
- `metadata`: SQLite-backed metadata store
- `reconcile`: repair and projection rebuild helpers
- `lib.rs`: public exports and `StorageCoordinator`

## Testing

The crate is covered by unit tests plus an MVCC integration test.

Typical verification:

```bash
cargo test -p gather-step-storage
```

For workspace-level validation, this crate currently also runs with
`gather-step-core`.

## See Also

- `crates/gather-step-core`: schema, graph types, IDs, config, and registry
- `COMPLETED/`: implementation notes and completion records
- `OVERVIEW/`: plain-language project summaries
