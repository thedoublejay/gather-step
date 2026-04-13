---
title: Event Topology
description: How Gather Step models Kafka producers, consumers, payload contracts, and cross-repo blast radius as a queryable graph — and what questions that lets you answer.
---

## What Event Topology Means Here

In an event-driven microservice platform, services communicate through topics, queues, subjects, and streams rather than direct calls. A service that publishes to `order.created` does not know — and should not need to know — which services consume it. That decoupling is the point.

But it creates a documentation and safety problem. When you want to change the payload shape of `order.created`, or understand who is affected by an outage in the publishing service, the answer lives across several codebases with no single place to look it up.

Gather Step models the event-driven topology of your workspace as first-class graph structure. Producers, consumers, topics, payload contracts, and blast radius are not derived from text search. They are extracted from framework-level code patterns during indexing, stored as typed nodes and edges, and queryable through both the CLI and the MCP server.

This is the clearest differentiator in the current Gather Step feature set. No other code intelligence tool in the current market ships producer-consumer tracing as a queryable graph surface.

## How Extraction Works

### Framework-Aware Pattern Recognition

The event topology layer depends on framework-aware extraction, not generic syntax parsing. Generic parsers see decorator syntax. Gather Step maps specific decorator patterns to their semantic meaning.

Current extraction coverage is strongest for NestJS-style microservice patterns:

- `@EventPattern('order.created')` — marks a method as a consumer of the `order.created` topic.
- `@MessagePattern('get-user')` — marks a method as a handler for the `get-user` message pattern.
- `@CustomEventPattern(...)` — a decorator variant used in some NestJS-based codebases that wraps the standard event pattern with project-specific conventions; Gather Step extracts the underlying topic name.
- Client-side Kafka emission patterns — `client.emit('order.created', payload)` and similar programmatic producer calls.

For each recognized pattern, the extractor creates:

- A typed virtual node (`Topic`, `Queue`, `Subject`, `Stream`, or `Event`) keyed by the canonical stitch key.
- A `Publishes` edge from the emitting function to the virtual node.
- A `Consumes` edge from the handler function to the virtual node.

Because the stitch key is deterministic (see [The Polyrepo Graph](/concepts/polyrepo-graph/)), a producer in `repo_alpha` and a consumer in `repo_beta` end up attached to the same virtual topic node. The graph connects them without either repo knowing about the other.

Broader framework coverage — NATS subjects, RabbitMQ queues, other messaging patterns — is driven by rule packs. The extraction mechanism is the same; what changes is the rule that recognizes the pattern.

## Payload Contract Inference

For each producer and consumer recognized, Gather Step infers the payload contract from the code shape adjacent to the emit or handler call:

- **Producer-side**: the shape of the argument passed to the emit call, including inferred field names, types, and optionality.
- **Consumer-side**: the shape of the parameter received by the handler method, including inferred field names, types, and optionality.

Each inferred field carries:

- field name
- inferred type name
- optionality flag
- inference kind (how the field was derived)
- confidence value

These are stored as `PayloadContract` nodes in the graph and as records in the metadata store, keyed by topic and side (producer or consumer).

### Contract Drift Detection

Once both sides have inferred contracts, the analysis layer runs drift detection. It compares the consolidated producer field set against the consolidated consumer field set for the same topic and reports discrepancies:

| Drift kind | What it means |
|---|---|
| Type mismatch | Producer says `string`, consumer expects `number` for the same field name |
| Optionality mismatch | Producer always sends the field, consumer marks it as optional (or vice versa) |
| Producer extra field | Producer includes a field the consumer does not declare |
| Consumer missing field | Consumer expects a field the producer does not include |

Confidence is carried from the underlying inferred fields. For a compared pair, the reported confidence is typically the minimum of the two sides, reflecting the weakest link in the inference chain. A high-confidence type mismatch (both sides are unambiguously typed) is treated differently from a low-confidence one where one side could not be resolved.

The drift results are surfaced through `gather-step events trace`, the MCP `contract_drift` tool, and the `breaking_change_candidates` tool.

## What You Can Ask

### Command Line

The `gather-step events` subcommand family covers the main event topology workflows:

**Trace a topic across all repos:**

```bash
gather-step events trace order.created
```

Returns the list of producer functions (with repo, file, and line), the list of consumer functions (with repo, file, and line), inferred payload fields, and any detected contract drift.

**Assess the blast radius of a topic:**

```bash
gather-step events blast-radius order.created
```

Runs a bounded breadth-first traversal from the topic's virtual node, following `Consumes`, `PropagatesEvent`, and `Triggers` edges downstream. Returns visited nodes grouped by repo, with propagated confidence and traversal depth. Truncates when the configured limit is hit rather than returning an unbounded dump.

**Find orphan topics:**

```bash
gather-step events orphans
```

Returns topics, queues, subjects, and streams that have either no registered producer or no registered consumer in the indexed workspace. These are candidates for dead event flows or under-indexed repos.

### MCP Tools

The same capabilities are exposed through MCP tools for AI assistants:

| Tool | What it does |
|---|---|
| `trace_event` | Returns producers, consumers, and payload contract for a topic |
| `event_blast_radius` | Returns the downstream propagation graph for a topic |
| `list_orphan_topics` | Returns event surfaces with missing producers or consumers |
| `payload_schema` | Returns the inferred payload shape for a topic on one or both sides |
| `contract_drift` | Returns detected drift findings between producer and consumer shapes |
| `breaking_change_candidates` | Returns topics where a change to the payload would likely break one or more consumers |

These tools return structured JSON responses. Assistants can chain them — for example, calling `trace_event` to find the producers, then `contract_drift` to inspect whether a proposed payload change would break existing consumers, then `event_blast_radius` to enumerate every downstream repo that would need a code change.

## A Worked Example

Suppose you are about to add a new required field to the payload of `order.created`. Before making the change, you want to know:

1. Which services publish `order.created` and where exactly in their code.
2. Which services consume it and what payload shape they expect.
3. Whether any consumers already have a contract mismatch with the current producer.
4. How far downstream an event from `order.created` can propagate.

Using the CLI:

```bash
# Step 1 and 2: find producers and consumers
gather-step events trace order.created

# Step 3: inspect contract drift
# (included in the trace output above; also available as a focused MCP query)

# Step 4: blast radius
gather-step events blast-radius order.created
```

The result is a complete, cross-repo picture of the event's reach before a single line of code is changed.

## Honest Scope

The event topology feature is strong on NestJS-style decorator patterns, which is where the extraction rules are most developed. Coverage for other messaging frameworks depends on which rule packs are active in your workspace.

Dynamic topic names — topics computed at runtime rather than declared as string literals in decorators — remain unresolved rather than guessed. The graph represents what can be determined with confidence from static analysis. Dynamic patterns show up as unresolved inputs in `gather-step doctor` output.
