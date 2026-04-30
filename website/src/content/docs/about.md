---
title: About Gather Step
description: What Gather Step does, where it fits in an AI-assisted workflow, and which implemented capabilities it exposes today.
---

Gather Step is a local-first code graph for multi-repo workspaces. It indexes source code once, stores the resulting graph locally, and exposes that graph through a CLI and a stdio MCP server so an AI assistant can retrieve structured context instead of rediscovering it from raw files on every task.

## Where the Name Comes From

In basketball, the gather step is the controlled step between securing the ball and committing to the next move. It is the setup that makes the shot, pass, or drive possible.

That maps directly to how Gather Step is intended to be used in engineering workflows. Before a person or an AI assistant can plan, review, debug, or change code safely, it needs to gather context first: routes, events, contracts, dependencies, and likely impact. Gather Step is named for that preparation step.

## What Problem It Solves

The hard part of many engineering tasks is not the edit. It is the context gathering that happens before the edit:

- finding the route or event surface involved
- locating downstream consumers
- understanding which repos are connected through shared contracts
- estimating blast radius before changing a public surface
- packaging the right evidence for an AI assistant without flooding its context window

Gather Step turns that work into indexed data and bounded queries.

## How It Fits With AI Assistants

Gather Step is not a code generator and it is not a chat memory system.

- A coding assistant handles synthesis, explanation, and code changes.
- Gather Step handles retrieval over the current indexed workspace.

In practice, the assistant launches `gather-step serve` through MCP and calls tools such as `search`, `trace_route`, `trace_event`, `trace_impact`, `projection_impact`, or `planning_pack` automatically when it needs them. The person using the assistant does not need to memorize the tool list for day-to-day work.

## Implemented Capabilities

The documentation site is intentionally limited to implemented behavior. The current feature set includes:

- indexing one or more repos into a workspace-local graph
- symbol search and symbol metadata lookup
- caller and callee traversal
- route tracing for request flows
- event tracing and downstream event blast radius
- cross-repo dependency and impact tracing
- static projection-impact tracing for source fields, derived fields, filters, indexes, and backfills
- shared type usage lookup
- payload schema inspection and contract drift analysis
- context packs for `planning`, `debug`, `fix`, `review`, and `change_impact`
- repo-level overview, ownership, dead-code, and convention analysis
- derived outputs such as assistant rules and ownership files

## Operating Model

Generated state is written to `WORKSPACE/.gather-step/`. Query-time commands and MCP tools read from that persisted state. They do not parse the entire codebase again for every request.

That design gives Gather Step three useful properties:

- repeatable results from the same indexed snapshot
- bounded responses suitable for automation and assistant context windows
- low-latency queries after the initial index exists

## Start Points

- Use [Getting started](/guides/getting-started/) for the shortest setup path.
- Use [Workspace setup](/guides/workspace-setup/) if you need config and indexing details.
- Use [MCP tools reference](/reference/mcp-tools/) if you want to inspect the tool surface your assistant uses automatically.
