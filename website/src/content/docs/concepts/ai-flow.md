---
title: AI & Agent Flow
description: How Gather Step models LLM calls, agent graphs, prompts, tools, vector indexes, and MCP surfaces as first-class graph structure — and what questions that lets you answer about AI pipelines.
---

## What AI Flow Means Here

Modern codebases increasingly contain AI pipelines: an agent composed of nodes, each node calling an LLM, binding tools the model may invoke, fetching managed prompts, retrieving from a vector index, and exposing or calling MCP tools. Like event topology, this flow is decoupled — a prompt is defined in one place, consumed in another; a tool is bound by an agent node but implemented elsewhere; an MCP tool is exposed by a server and called by a client across a boundary.

Gather Step models that AI flow as first-class graph structure, alongside the routes, events, and contract layers. LLM calls, agent graphs, prompts, tools, vector indexes, and MCP surfaces are extracted from framework-level code patterns during indexing, stored as typed nodes and edges, and walked forward through the MCP `trace_agent` tool. This is the layer introduced in v5.0; it is **complementary** — every pre-v5 node and edge is preserved unchanged.

## How Extraction Works

### Node Kinds

The AI layer adds a set of typed graph nodes, many of them converged virtual nodes (a single `llm_model` or `mcp_tool` node is shared by every call site that targets it, the same way producers and consumers meet at one virtual topic):

| Node kind | What it represents |
|---|---|
| `agent_graph` | An agent or agent graph definition. |
| `llm_model` | A converged model node every `invokes_llm` call site points at. |
| `prompt` | A managed prompt artifact. |
| `ai_contract` | A structured-output contract produced by an LLM call site. |
| `vector_index` | A vector index / collection used for retrieval. |
| `mcp_server` | An MCP server that exposes tools. |
| `mcp_tool` | A converged MCP tool node, shared by the exposer and its callers. |

Finer roles — an LLM call, a tool, an agent, an agent node, an embedder, an MCP client — are carried as `ai_role` **facets** on existing symbol nodes rather than as separate node kinds. This keeps the call graph intact: a function that invokes an LLM is still the same symbol node, now tagged with its AI role.

### Edge Kinds

Relationships between these surfaces are typed edges, so the flow is a traversal rather than a text guess:

| Edge kind | Meaning |
|---|---|
| `defines_agent_node` / `composes_agent` | An agent graph defines its nodes and composes them into a flow. |
| `binds_tool` | An agent or graph node binds a tool the LLM may call. |
| `invokes_llm` | A call site invokes an LLM; the target is the converged `llm_model` node. |
| `produces_ai_contract` | A call site produces a structured-output `ai_contract`. |
| `uses_prompt` / `fetches_prompt_from` | A symbol uses a managed prompt, or fetches it from a prompt source. |
| `calls_mcp_tool` / `exposes_mcp_tool` | An MCP client calls a converged `mcp_tool`; an MCP server exposes it. |

Vector-index retrieval is modeled the same way: a tool or graph node retrieves from a `vector_index`, and a collection is indexed into one.

## What You Can Ask

### MCP Tool

AI flow is surfaced primarily through the MCP server, where an assistant uses it automatically. There is no dedicated CLI subcommand for it — `gather-step trace` covers route-backed CRUD flows; AI flow is walked via MCP:

| Tool | What it does |
|---|---|
| `trace_agent` | Walks an AI agent's forward flow from a target node: the agent graph and its nodes, the LLM calls they make, and the tools, prompts, vector indexes, and MCP tools they reach. |

Because `llm_model` and `mcp_tool` are converged nodes, a single trace connects call sites across repos that target the same model or MCP tool — the cross-repo picture without either side referencing the other.

### PR Review

Changes to AI surfaces show up in `gather-step pr-review` through the `ai_contracts` delta section. It reports added, removed, and changed AI structured-output contracts keyed by source symbol — both schema-field diffs and AI-facet changes such as `provider`, `model`, `temperature`, `inference_kind`, and `schema_format`. A prompt rewrite that silently drops a required output field, or a model/temperature change that alters behaviour, becomes a reviewable delta rather than an invisible one. See the [PR review guide](/guides/pr-review/).

## Honest Scope

AI extraction recognizes common LLM/agent/RAG/MCP patterns from supported frameworks during indexing; coverage depends on which patterns appear as static, statically-resolvable code rather than fully dynamic construction. As with the rest of the graph, surfaces computed entirely at runtime remain unresolved rather than guessed, and show up as unresolved inputs in `gather-step doctor` output. The layer is additive: it never removes or rewrites the route, event, contract, or deployment surfaces it sits beside.

## See Also

The AI layer composes with the rest of the graph: an `invokes_llm` call site is still an ordinary symbol with callers and callees, an `ai_contract` is a payload-shaped surface like any other contract, and an `mcp_tool` is a converged virtual node like a `Topic`. For the broader model, see [The Polyrepo Graph](/concepts/polyrepo-graph/) and [Event Topology](/concepts/event-topology/); for change-safety, see the [PR review guide](/guides/pr-review/).
