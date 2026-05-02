---
title: The Polyrepo Graph
description: How Gather Step stitches multiple repositories into one local code graph using virtual semantic nodes, deterministic IDs, and stitch keys — and why that matters for AI retrieval.
---

## Why One Graph

A per-repo index can tell you what exists inside one codebase. It answers:

- what functions are defined in this file
- what symbols does this module export
- where is this class used within this repo

It is weaker at answering the questions that matter most in a multi-repo platform:

- which frontend caller reaches this backend route
- which repos depend on the same shared contract version
- which event is produced in one service and consumed in two others
- which downstream services are likely affected by a change to this type

The cross-repo blast radius problem is not a search problem. It is a graph traversal problem. The answer requires not just finding the relevant code in each repo, but knowing how those pieces connect to each other across repo boundaries.

The polyrepo code graph exists to make those cross-repo relationships first-class, queryable edges rather than implicit knowledge held in senior engineers' heads.

## Node Kinds

The graph uses a typed node model. Every node has a kind, a deterministic ID, a repo attribution (or a marker indicating it is workspace-level), and source span information where applicable.

### Structural Nodes

These represent static code structure as extracted from source files:

| Kind | What it represents |
|---|---|
| `File` | A source file in a configured repo |
| `Function` | A function or method definition |
| `Class` | A class declaration |
| `Type` | A named type alias or interface |
| `Module` | A module-level namespace or barrel |
| `Import` | An import declaration |
| `Decorator` | A decorator application on a class, method, or parameter |

### Domain and Runtime Nodes

These represent framework-level and runtime concepts extracted by framework-aware rules:

| Kind | What it represents |
|---|---|
| `Entity` | A schema or model construct (e.g., a database entity or Mongoose document) |
| `Route` | An HTTP route, modeled as a virtual node shared between handler and callers |
| `Topic` | A message topic (e.g., Kafka topic), shared across producers and consumers |
| `Queue` | A message queue |
| `Subject` | A pub-sub subject (e.g., NATS subject) |
| `Stream` | A message stream |
| `Event` | A named domain event |
| `Service` | A service-level node derived from framework registration |

### Cross-Repo Abstraction Nodes

These are the bridge points that connect separate repositories in the graph:

| Kind | What it represents |
|---|---|
| `SharedSymbol` | A symbol exported by a shared package, keyed by package name, version, and symbol name |
| `PayloadContract` | An inferred payload shape for a topic or queue, per side (producer or consumer) |

### Repository and Workflow Nodes

These support git-aware intelligence and ownership signals:

| Kind | What it represents |
|---|---|
| `Repo` | A configured repository in the workspace |
| `Convention` | A detected coding or architectural convention |
| `Commit` | A git commit |
| `PR` | A pull request |
| `Review` | A review on a PR |
| `Comment` | A comment on a PR or review |
| `Author` | A code author derived from git history |
| `Ticket` | A linked issue or ticket reference |

## Edge Kinds

Edges are also typed. The graph stores edge kind, source node, target node, resolver name, and confidence on every edge.

### Static Code Relationships

| Kind | What it connects |
|---|---|
| `Defines` | File or class defines a function, type, or member |
| `Calls` | Function calls another function |
| `Imports` | Module imports another module or symbol |
| `Exports` | Module exports a symbol |
| `Extends` | Class extends a base class |
| `Implements` | Class implements an interface |
| `References` | Node references another node without a direct call or import |
| `DependsOn` | Manifest-level package dependency |
| `UsesDecorator` | A class or method applies a decorator |

### Runtime and System Relationships

| Kind | What it connects |
|---|---|
| `Publishes` | A function or class publishes to a topic, queue, or event |
| `Consumes` | A function or class consumes from a topic, queue, or event |
| `Triggers` | An event triggers a handler |
| `Serves` | A handler serves a route |
| `PersistsTo` | A handler or service persists to a storage entity |
| `UsesShared` | A node uses a shared symbol from a cross-repo package |

### Analysis and History Relationships

| Kind | What it connects |
|---|---|
| `BreaksIfChanged` | A change to this node would likely break the target |
| `CoChangesWith` | Two nodes are frequently modified together (git-derived) |
| `OwnedBy` | A file or node is attributed to an author |
| `CrossRepoDepends` | A cross-repo dependency edge beyond the shared-symbol mechanism |
| `PropagatesEvent` | An event propagates from one node to a downstream consumer |
| `DriftsFrom` | A payload contract field drifts from its counterpart on the other side |
| `ContractOn` | A payload contract is attached to a specific topic or queue |

## Deterministic IDs

Node IDs are not random UUIDs. They are computed deterministically from the node's semantic identity: its repo, file path, kind, and canonical name.

This matters for three reasons:

1. **Idempotent re-indexing.** Re-running `gather-step index` on an unchanged workspace produces an identical graph. The same node identity is found in the same table slot. No orphan nodes accumulate from repeated runs.

2. **Stable cross-repo attachment.** A virtual node for a route or topic has the same ID every time it is computed, regardless of which repo triggers its creation. When repo A defines a handler and repo B defines a caller, both attach to the same route node because both compute the same stitch key.

3. **Compact downstream analysis.** Analysis functions can cache results keyed by node ID without worrying that a re-index has changed which entity a given ID refers to.

## Virtual Nodes and Stitch Keys

Cross-repo relationships are normalized through virtual nodes. A virtual node is a graph node whose identity is derived from a canonical external name — a route, topic, queue, or shared symbol — rather than from a physical file location.

The stitch key is the canonical qualified name used to compute the virtual node's ID. Different repos producing or consuming the same external surface compute the same stitch key, find the same virtual node (creating it if it does not yet exist), and attach their local nodes to it.

Implemented stitch key formats:

| Surface | Stitch key format | Example |
|---|---|---|
| HTTP route | `__route__METHOD__/path` | `__route__POST__/orders` |
| Kafka / message topic | `__topic__protocol__name` | `__topic__kafka__order.created` |
| Message queue | `__queue__protocol__name` | `__queue__rabbitmq__invoicing` |
| Shared symbol | `__shared__package@version__symbol` | `__shared__contracts@2.1.0__OrderDto` |

Route matching also normalizes HTTP method aliases (for example, `FETCH` is normalized to `GET`) and strips trailing slashes before computing the stitch key, so equivalent route definitions from different codebases converge to the same node.

### What This Enables

Once virtual nodes are in place, cross-repo graph questions become single traversals:

- "Which frontends call `/orders`?" — find the `Route` virtual node for `__route__POST__/orders`, walk incoming `Consumes` edges, collect the `Function` nodes on the caller side.
- "Which services consume `order.created`?" — find the `Topic` virtual node for `__topic__kafka__order.created`, walk incoming `Consumes` edges, collect the handler functions and their owning repos.
- "What is the blast radius of changing `OrderDto` in the contracts package?" — find the `SharedSymbol` virtual node, walk `UsesShared` edges, collect all `File` and `Function` nodes that depend on it, group by repo.

None of those queries require knowledge of which repo each side lives in. The virtual node handles the stitching.

## Why This Wins for AI Retrieval

When an AI assistant asks "what would break if I change this event's payload?", it needs a precomputed graph answer, not another scan of raw source files.

The polyrepo code graph provides:

- **Stable IDs.** An assistant can reference a node ID across sessions without it changing on re-index.
- **Repo-aware search.** Results are attributed to the repo they came from, so the assistant can tell the engineer "this consumer is in `repo_beta`, file `src/handlers/order.handler.ts`."
- **Route and event topology.** The graph already knows which functions produce or consume which topics. No reasoning from text is needed.
- **Bounded task packs.** Instead of returning a raw multi-hop neighborhood, the context pack system slices the graph into a byte-budgeted, mode-specific bundle shaped for the specific task at hand.

The retrieval step has already happened. The assistant can focus on synthesis.

The same graph powers cross-repo PR review. When `gather-step pr-review` builds a disposable review index for a branch, it uses the polyrepo graph to compute surface deltas — added and removed routes, exported symbols, payload contracts, and event wiring — across every repo touched by the diff. See the [CLI reference](/reference/cli/) for the full flag set and the [PR review guide](/guides/pr-review/) for a walkthrough.
