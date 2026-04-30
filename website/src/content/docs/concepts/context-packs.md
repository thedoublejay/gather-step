---
title: Context Packs
description: Task-shaped context packs give AI coding assistants a bounded, relevant slice of the polyrepo code graph shaped for a specific job — planning, debugging, fixing, review, or change impact.
---

## What a Context Pack Is

A context pack is a bounded, task-shaped slice of the polyrepo code graph built around a specific target symbol, file, route, event, or entity.

It is not a raw graph neighborhood. A raw neighborhood expands in all directions until it hits a size limit, returning nodes that may be structurally adjacent but semantically irrelevant to the task at hand. A context pack is the opposite: it starts from the target, selects the nodes and edges that matter for the specific mode, ranks them by relevance, and returns a structured bundle with an explicit byte budget.

The difference matters practically. An AI assistant that receives a well-shaped context pack can start planning or reasoning immediately. An assistant that receives an unfiltered graph dump has to spend tokens filtering before it can start working.

## Pack Modes

Gather Step supports five pack modes. Each is built for a different point in the development workflow.

### `planning`

Use this mode when you are starting a new feature or estimating the scope of a change.

The planning pack returns:

- the graph neighborhood of the target, focused on outbound dependencies and callers
- related route and event surfaces (if the target is near a handler or producer)
- shared symbol dependencies and version information
- cross-repo touchpoints likely affected by a change to the target
- Mongoose migration siblings, filter literals, and a best-effort coverage note
  when the target is a supported migration; see
  [Data-shape verification](/guides/data-shape-verification/)
- next-step suggestions for where to look before writing any code

This is the mode to use when you want to answer "what do I need to understand before I touch this?"

### `debug`

Use this mode when you are tracing a defect or unexpected behavior.

The debug pack returns:

- the target's inbound call graph (what calls this, and from where)
- event consumers and producers connected to the target path
- related entity and persistence touchpoints
- cross-repo callers if the target is a public surface
- unresolved gaps that may indicate missing coverage in the index

This is the mode to use when you want to answer "how did execution reach this point, and where could it have gone wrong?"

### `fix`

Use this mode when you have isolated a defect and need the minimal edit surface.

The fix pack returns:

- the immediate neighborhood of the target at shallow depth
- the most directly affected callers and dependents
- any contract or interface nodes the target must satisfy
- confidence annotations on heuristic edges, so the assistant knows which links are certain vs inferred

This mode intentionally returns less than `planning` or `debug`. The goal is precision: give the assistant only what it needs to make the fix safely.

### `review`

Use this mode when you are reviewing a change or preparing a summary of what a modification touches.

The review pack returns:

- outbound and inbound relationships for the target
- ownership signals from git-derived analytics (who has recently changed this code)
- convention findings relevant to the target's framework context
- cross-repo surfaces the change may affect
- hotspot signals (high-churn code near the target)

This is the mode to use when you want to answer "what should a reviewer think about before approving this change?"

### `change_impact`

Use this mode when you want to understand the blast radius of a proposed change before committing to it.

The change impact pack returns:

- all known dependents of the target, grouped by repo
- event topology connections if the target is a producer or consumer
- shared symbol consumers if the target is part of a shared contract
- confidence-banded edges so the blast radius can be understood at different confidence thresholds
- unresolved gaps that may indicate the impact is larger than the graph currently knows

This is the mode to use when you want to answer "if I change this, what else breaks?"

## What a Pack Returns

Every pack, regardless of mode, includes the following fields:

- **Ranked relevant items.** The most relevant nodes and edges for the mode and target, ordered by relevance score. Each item includes its node kind, repo, file path, and source span.
- **Semantic bridges.** Edges that connect the target to other parts of the graph that would not appear in a simple depth-bounded neighborhood expansion. These are the links that matter for cross-repo reasoning.
- **Next-step suggestions.** Structured hints about where to look or what to verify next, derived from graph signals rather than generated prose.
- **Unresolved gaps.** Items the graph knows are connected to the target but cannot fully resolve — dynamic endpoints, missing index coverage, low-confidence edges. Surfacing gaps explicitly is more useful than silently omitting them.
- **Byte budget.** The pack is sized to fit a practical context window. The system does not return an unbounded expansion; it returns the most relevant content within a configurable size limit.

## CLI Usage

```bash
gather-step pack <TARGET> --mode <mode>
```

`TARGET` can be a symbol name, a file path relative to the workspace root, a route path (e.g., `POST /orders`), or a topic name.

Examples:

```bash
# Planning pack for a function by name
gather-step pack createOrder --mode planning

# Change impact pack for an event topic
gather-step pack order.created --mode change_impact

# Debug pack for a specific file
gather-step pack src/handlers/order.handler.ts --mode debug

# Review pack using a symbol ID from a previous search
gather-step pack --symbol-id <SYMBOL_ID> --mode review
```

For JSON output (suitable for piping or automation):

```bash
gather-step pack createOrder --mode planning --json
```

## MCP Tools

AI assistants connected through MCP can request context packs without the engineer invoking the CLI manually. The following MCP tools are available:

| Tool | What it does |
|---|---|
| `context_pack` | Returns a context pack for a target in the specified mode |
| `planning_pack` | Shorthand for `context_pack` with `mode=planning` |
| `debug_pack` | Shorthand for `context_pack` with `mode=debug` |
| `fix_pack` | Shorthand for `context_pack` with `mode=fix` |
| `review_pack` | Shorthand for `context_pack` with `mode=review` |
| `change_impact_pack` | Shorthand for `context_pack` with `mode=change_impact` |
| `batch_query` | Returns multiple packs or graph queries in a single request, for workflows that need several targets at once |

In a typical AI workflow, the assistant calls `planning_pack` at the start of a feature task, receives the bounded pack, and begins its analysis from structured graph context rather than from file-level search.

## Why Packs Beat Raw Search

Three concrete advantages over returning raw search results to an assistant:

1. **Token efficiency.** A raw search result returns a list of matching nodes. A context pack returns a ranked, mode-filtered slice that excludes the noise. The assistant spends its context budget on the relevant material.

2. **Relevance.** Graph traversal with mode-specific filters surfaces nodes the assistant would not find through keyword search. A semantic bridge between a handler and its downstream event topic, for example, is not findable by searching for the handler's name.

3. **Bounded size.** The byte budget in the pack response gives the assistant a predictable input size. Large workspaces do not produce unbounded dumps that overflow the context window.
