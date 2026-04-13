---
title: Memory-Backed Planning
description: Pair Gather Step with a Markdown-first engineering memory system so an AI assistant plans from prior decisions and the current code graph instead of either alone.
---

Gather Step gives an AI assistant the **current code graph**: routes, events,
contracts, owners, and cross-repo edges as they are right now. That answers
*"what does the code look like today?"* but not *"what did we already decide,
and why?"*.

The second question is what an **engineering memory** answers. A good memory
holds prior decisions, ticket history, repo profiles, code review notes, and
reusable learnings in plain Markdown so any agent can grep, query, and cite it.

When the two are paired, planning quality improves measurably:

- The agent reads memory first, so it does not re-derive decisions that already
  exist or contradict prior intent.
- The agent reads Gather Step second, so it grounds the plan in the current
  code, not stale assumptions.
- The agent writes a plan that cites both, separating *fact* from *inference*.

This guide describes the pattern. It does not require any specific memory
tool, but it uses [Braingent Manifesto](https://github.com/thedoublejay/braingent-manifesto)
as the concrete example because it is open source, Markdown-first, and
designed exactly for this loop.

## The Loop

```text
                ┌──────────────────────────┐
                │  Engineering memory      │
                │  (e.g. Braingent)        │
                │  prior decisions, tickets│
                │  reviews, learnings      │
                └─────────────┬────────────┘
                              │ 1. read before planning
                              ▼
                ┌──────────────────────────┐
                │  Gather Step             │
                │  current code graph,     │
                │  routes, events, packs   │
                └─────────────┬────────────┘
                              │ 2. ground in current code
                              ▼
                ┌──────────────────────────┐
                │  LLM / coding agent      │
                │  plan + implementation   │
                └─────────────┬────────────┘
                              │ 3. capture outcome
                              ▼
                ┌──────────────────────────┐
                │  Engineering memory      │
                │  (record the result)     │
                └──────────────────────────┘
```

The shape of the loop matters more than the tools that fill each box. Memory
is read before the agent plans, Gather Step grounds the plan in current code,
and the outcome is written back to memory after meaningful work.

## The Four Steps

### 1. Read memory before planning

The agent searches the memory store for context relevant to the task:

- repo profile and architecture notes,
- prior decisions on this area of code,
- recent tickets and code reviews,
- reusable learnings (failure modes, gotchas).

The output is a focused context pack of memory citations, not a wholesale
dump. Each cited fact links back to the file that holds it.

> **Braingent example.** Braingent stores records as Markdown with
> frontmatter. An agent searches first by structured fields
> (`ticket`, `repo`, `topic`, `status`) using the project's `find.sh`
> helper, then falls back to free-text `rg` over record bodies. It cites the
> specific record paths in the plan.

### 2. Ground the plan in Gather Step

The agent then asks Gather Step for current code context:

```bash
gather-step search <SYMBOL>
gather-step trace crud --method POST --path /<route>
gather-step events trace <SUBJECT>
gather-step pack <TARGET> --mode planning
```

The output answers *what the code looks like now*: which files own a symbol,
which repos consume it, what events propagate from it, and what gaps the graph
still has. Because Gather Step indexes the local workspace, the agent can rely
on these facts without external network calls.

### 3. Write a plan that cites both

A memory-backed plan separates four things:

| Section | Source |
|---|---|
| Known facts | Memory citations + Gather Step output |
| Assumptions | The agent's inferences, marked as such |
| Affected areas | Gather Step impact + pack output |
| Verification | Concrete `→ verify:` checks per step |

Because both inputs are cited, a reviewer can challenge any claim by opening
the cited record or rerunning the cited Gather Step command.

### 4. Capture the outcome

After the work lands, the agent writes a short record back to memory: what
was decided, what changed, what was learned, and what to avoid next time.
This is the only step that grows the memory; without it, the loop
unidirectionally drains context the next agent could have reused.

> **Braingent example.** Braingent's capture policy specifies when a record
> is worth writing (PR opened, decision made, learning surfaced) and provides
> a minimal task-record template. A capture is a small Markdown file with
> frontmatter that the next session can find by structured query.

## Why Both, Not Either

Gather Step alone answers structural questions but cannot tell the agent that
"this consumer is being deprecated next quarter" or "we tried this refactor
in Q2 and rolled it back." Memory alone holds intent but cannot tell the
agent which file currently owns a symbol or which downstream repo consumes
it today. Together they cover both axes:

- **What did we decide and why?** — memory.
- **What does the code look like right now?** — Gather Step.

The plan is better than either input alone, and both inputs stay
local-first and Markdown-friendly.

## Other Memory Tools

Any Markdown-first, queryable memory store works with this pattern. The
contract is small:

- records are plain text (Markdown preferred),
- records carry enough metadata for structured search,
- the memory tool exposes a way for an agent to query and cite specific
  records without loading everything.

[Braingent Manifesto](https://github.com/thedoublejay/braingent-manifesto)
is one open-source reference for what such a system looks like end-to-end,
including capture policy, retrieval helpers, and validation scripts.

## Non-Goals

- Gather Step does not store memory. The local index is for the code graph
  only; memory lives in its own repository or store.
- This pattern does not require any specific memory tool. The loop is the
  product, not the tool.
- Capture is not automatic unless the memory tool itself performs it. If a
  capture step is described, an agent or operator must run it explicitly.

## Next Steps

- [Operator workflows](/guides/operator-workflows/) — the Gather Step CLI
  commands referenced above.
- [Context packs](/concepts/context-packs/) — how the planning pack is
  assembled from the graph.
- [MCP clients](/guides/mcp-clients/) — expose the same graph to an AI
  assistant so the loop runs without manual CLI invocation.
