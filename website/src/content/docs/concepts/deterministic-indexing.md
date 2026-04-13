---
title: Deterministic Indexing
description: How Gather Step builds a zero-LLM code graph using tree-sitter parsing, confidence-banded call resolution, and hash-driven incremental updates — fast, private, and reproducible.
---

## The Core Claim

Gather Step builds its code graph without involving a large language model. The graph is a product of deterministic extraction — tree-sitter parsing, framework-aware rules, and a confidence-banded resolution algorithm — not LLM inference.

This is worth stating plainly because several competing tools in this space use LLMs at index time. That approach trades reproducibility and privacy for broader pattern recognition. Gather Step makes the opposite trade: the extraction process is fast, predictable, and entirely local. No code is transmitted anywhere during indexing.

LLMs are the consumers of the Gather Step graph. They are not the producers of it.

## Tree-sitter Parsing

Source files are parsed using tree-sitter, a deterministic parser generator that produces concrete syntax trees from source text.

The parsing layer:

- parses supported source files (TypeScript, JavaScript, Python) into concrete syntax trees
- collects symbol captures for functions, classes, types, decorators, and imports
- runs framework-aware extraction rules over the syntax tree
- preserves source spans (file, line, column) on every extracted node for later tool responses

This is not text search. The extractor works over the AST, which means it understands the structural relationships between a decorator and the method it annotates, between an import declaration and the module it names, and between a class definition and its base class.

## Framework-Aware Extraction

Tree-sitter gives the system the syntax. Framework-aware rules give the system the semantics.

Generic parsers see that `@Controller('/orders')` is a decorator applied to a class. The framework-aware extractor knows that this decorator, in a NestJS context, defines an HTTP route prefix. It projects that knowledge into a `Route` virtual node with the canonical stitch key `__route__METHOD__/orders`.

The same mechanism applies across the extraction surface:

- NestJS `@EventPattern` and `@MessagePattern` decorators are projected to `Topic` and event consumer edges.
- `@CustomEventPattern` variants used in some NestJS-based workspaces are resolved to their underlying topic name.
- Frontend API client calls (`useQuery`, `useMutation`, `fetch` with a recognizable route shape) are projected to `Consumes` edges on the relevant `Route` virtual node.
- Mongoose `@Prop` and schema-like declarations are projected to `Entity` nodes.
- Manifest dependency declarations become `DependsOn` edges from a repo node to versioned `SharedSymbol` nodes.

Framework detection runs before file-level extraction. The detector identifies which frameworks are present in a repo by inspecting manifest files, import patterns, and framework-specific configuration. Detection results feed the extraction rules so the right rules run against the right files.

## Confidence-Banded Call Resolution

Not all call relationships can be resolved with the same certainty. A call to a function imported explicitly by name from a known module is certain. A call to a function that shares a name with three other functions in three different files is ambiguous.

Gather Step handles this through a six-strategy resolution chain. Each strategy is tried in order. The first strategy that can produce a confident enough match wins. Ambiguity penalties reduce the confidence score when multiple targets fit a strategy.

| Strategy | Description |
|---|---|
| `ImportMap` | The call target is explicitly imported in the same file. The import declaration gives the exact module and symbol. Highest base confidence. |
| `SameModule` | The call target is defined in the same file as the caller. No import required. High confidence. |
| `Unique` | The call target name matches exactly one symbol across the entire indexed workspace. Confidence is high but lower than `ImportMap` because the uniqueness assumption could be invalidated by later indexing. |
| `Suffix` | The call target matches one symbol whose fully-qualified name ends with the target name. Used when partial qualification is enough to disambiguate. Medium confidence. |
| `FuzzyName` | The call target matches one or more symbols by approximate name. Used as a fallback when exact and suffix matching fail. Lower confidence; ambiguity penalties apply if multiple symbols match. |
| `Fallback` | The call target cannot be resolved by any other strategy. The edge is recorded with minimal confidence and the fallback marker. Downstream analysis can treat these edges differently. |

The graph stores three pieces of data on every resolved call edge: the edge itself, the name of the resolver strategy that produced it, and the final confidence value. This means analysis downstream has the full picture. A blast radius calculation can, for example, treat `ImportMap` edges as certain and `FuzzyName` edges as approximate, and surface that distinction to the user.

When no strategy produces a satisfactory result, the edge is left unresolved rather than guessed. Unresolved call sites appear in `gather-step doctor` output so engineers know where index coverage is incomplete.

## Payload Contract Inference

Payload contracts are inferred from the code shapes adjacent to producer and consumer declarations.

For a producer:

```text
client.emit('order.created', { orderId: string, amount: number, userId?: string })
```

The extractor captures: topic name `order.created`, side `producer`, fields `orderId` (string, required), `amount` (number, required), `userId` (string, optional), inference kind, and confidence.

For a consumer:

```text
@EventPattern('order.created')
handleOrderCreated(@Payload() payload: { orderId: string; amount: number }) { ... }
```

The extractor captures: topic name `order.created`, side `consumer`, fields `orderId` (string, required), `amount` (number, required), inference kind, and confidence.

Both sides are stored as `PayloadContract` nodes in the graph and as records in the metadata store. The analysis layer later compares the two sides to detect contract drift — see [Event Topology](/concepts/event-topology/) for details.

Confidence is deliberately banded. An inline object literal is more reliably typed than a variable whose type must be inferred through an import chain. The stored confidence reflects this. Downstream tools can show engineers how well-founded a contract drift finding actually is.

## Incremental Indexing

The full batch indexing pipeline is efficient on a cold start, but running it from scratch on every file save would be impractical. Gather Step handles freshness through hash-driven incremental indexing.

The incremental flow:

1. Snapshot the current source file paths and manifest hashes for each configured repo.
2. Compare against the stored file index states in the metadata database.
3. Classify each file as added, modified, deleted, or unchanged.
4. Ask the metadata database for the reverse dependents of every changed file — the files that import or reference the changed file.
5. Re-index the changed set plus all affected dependents.
6. Purge graph, search, and metadata records for deleted files.
7. Reconcile projections across the graph store, search index, and metadata database.

The key algorithmic choice is step 4: the system re-indexes not just changed files but their affected dependents. This is necessary because call resolution and cross-file relationships can become stale when a dependency changes. Re-indexing only the directly modified file would leave callers pointing at outdated edges.

## Watch Mode

Watch mode layers operational safety on top of incremental indexing for live development sessions:

- Repo-scoped filesystem watchers detect file changes as they happen.
- A debounce window coalesces rapid save events from editors that write intermediate states.
- A capped pending-file hint queue bounds memory use during high-activity bursts.
- When the hint queue overflows — more events arrived than the queue can hold — the system schedules a repo-wide incremental pass with no path hint rather than silently missing updates.
- Consecutive errors per repo trigger a backoff that suppresses that repo from watch-triggered re-indexing until the condition clears.

The overflow fallback is deliberate. It is better to do more work than to silently present a stale graph as current.

## Privacy

The zero-LLM design has a direct privacy consequence: no code leaves the machine during indexing.

The graph is built entirely from local operations — filesystem traversal, tree-sitter parsing, deterministic rule application, and storage writes to files in `WORKSPACE/.gather-step/`. There is no network call, no telemetry, and no external service involved in the indexing pipeline itself.

The graph is then consumed by an AI assistant, which may send graph content to an external model API as part of a query. But that consumption is under the control of the assistant and the engineer, not the indexer. Gather Step's role in the privacy picture is simple: it builds the graph locally and keeps it local.
