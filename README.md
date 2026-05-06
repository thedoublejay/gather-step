<p align="center">
  <img src="website/public/gs_logo_transparent.webp" alt="Gather Step logo" width="96" />
</p>

<h1 align="center">Gather Step</h1>

<p align="center">
  <a href="https://github.com/thedoublejay/gather-step/actions/workflows/ci.yml">
    <img src="https://github.com/thedoublejay/gather-step/actions/workflows/ci.yml/badge.svg?branch=main" alt="Build status" />
  </a>
  <a href="https://github.com/thedoublejay/gather-step/releases">
    <img src="https://img.shields.io/github/v/release/thedoublejay/gather-step?display_name=tag" alt="Latest release" />
  </a>
</p>

<p align="center">
  A local-first code graph for multi-repo workspaces and AI coding assistants.
</p>

<p align="center">
  One unified graph across your repos, not a federation of isolated ones.
</p>

<p align="center">
  <a href="https://gatherstep.dev">gatherstep.dev</a>
</p>

<p align="center">
  <a href="#quick-start"><strong>Quick Start</strong></a>
  ·
  <a href="https://gatherstep.dev"><strong>Docs</strong></a>
  ·
  <a href="https://github.com/thedoublejay/gather-step"><strong>GitHub</strong></a>
</p>

Gather Step indexes a workspace of repositories into a local graph, then exposes that graph through a CLI and a local MCP server. It is built for the step before implementation: understanding routes, events, shared contracts, repo boundaries, and likely impact before code changes start.

The graph is precomputed and stored locally. MCP queries read from indexed state instead of re-parsing source files at request time.

> One unified graph across your repos, not a federation of isolated ones.

## Why Gather Step

- Local-first CLI and stdio MCP server
- Multi-repo indexing into `WORKSPACE/.gather-step/`
- Guided startup with no-args onboarding, `init`, `setup-mcp`, and watch handoff
- Route, event, shared-symbol, payload-contract, projection-impact, and deployment-topology graph surfaces
- Context packs for `planning`, `debug`, `fix`, `review`, and `change_impact`
- Evidence-only QA planning manifests for downstream Braingent test-plan workflows
- Workspace health commands such as `status`, `doctor`, and `watch`
- Derived outputs for assistant summaries, rules, and ownership files
- Local release validation with high-contract probes and PR-oracle scoring

## What It Helps With

Use Gather Step when the expensive part of the task is gathering context rather than writing the edit:

- tracing a route from caller to handler
- mapping event producers and consumers across repos
- checking which repos or files are affected by a change
- checking projection, backfill, index, and filter evidence before field changes
- finding shared type usage and contract drift
- preparing bounded context for an AI assistant
- inspecting ownership, conventions, and dead-code candidates

## What Makes It Different

### Native Polyrepo Graph

Gather Step treats a workspace as one system. A Kafka producer in repo A and a consumer in repo B meet at the same virtual topic node, so cross-repo reasoning is a graph traversal instead of a guess.

### Event-Driven Topology

It is designed to surface producer-to-consumer relationships across event-driven systems, including frameworks and patterns commonly used in TypeScript backends and service architectures.

### Task-Shaped Context Packs

Instead of returning a raw neighborhood dump, Gather Step returns bounded packs shaped for the job:

- `planning`
- `debug`
- `fix`
- `review`
- `change_impact`

Each pack ranks the relevant graph surfaces for that task, includes next-step hints, and stays within a practical byte budget for AI workflows.

### Local-First MCP Workflow

Once configured, an MCP-aware client can launch `gather-step serve` locally and call the right tools automatically. No API keys. No account. The graph runs on your machine.

## How It Works

Gather Step builds a local index for the workspace and stores generated state under `.gather-step/`. That state powers:

- direct CLI inspection for operators
- a stdio MCP server for AI clients
- bounded context packs instead of raw graph dumps

Source repositories are never modified.

## Requirements

- Rust `1.94.1` for source builds
- A workspace root containing the repos you want to index. `gather-step init` can generate `gather-step.config.yaml`.

Minimal config:

```yaml
repos:
  - name: backend_standard
    path: repos/backend_standard
  - name: frontend_standard
    path: repos/frontend_standard
indexing:
  workspace_concurrency: 4
```

## Quick Start

Install with Homebrew on macOS:

```bash
brew install thedoublejay/tap/gather-step
```

Or build from source:

```bash
cargo build -p gather-step --release
```

Create a config, build the index, generate AI-facing context files, and register Claude MCP settings:

```bash
gather-step --workspace /path/to/workspace init --index --generate-ai-files --setup-mcp local
```

Generated state is stored under `WORKSPACE/.gather-step/`.

During active development, keep the index fresh:

```bash
gather-step --workspace /path/to/workspace watch
```

Once the server is configured in an MCP-aware client, the assistant launches Gather Step locally and calls the right tools automatically. The CLI remains available for direct inspection when you want it.

If you want the full walkthrough, start with [Getting started](website/src/content/docs/guides/getting-started.md) or the published docs at <https://gatherstep.dev>.

## Typical Questions It Can Answer

- What handles `POST /orders` end to end?
- Who consumes `order.created` across the workspace?
- What breaks if I change `CreateOrderInput`?
- Which repos depend on this shared contract?
- Where is `api` deployed, and which services read `DATABASE_URL`?
- Give me a review-oriented pack for `createOrder`.

## Common Commands

```bash
gather-step --workspace /path/to/workspace status
gather-step --workspace /path/to/workspace doctor
gather-step --workspace /path/to/workspace search createOrder
gather-step --workspace /path/to/workspace trace crud --method POST --path /orders
gather-step --workspace /path/to/workspace events trace order.created
gather-step --workspace /path/to/workspace impact CreateOrderInput
gather-step --workspace /path/to/workspace projection-impact --target subtaskIds
gather-step --workspace /path/to/workspace deployment-topology where-deployed --service api
gather-step --workspace /path/to/workspace pack createOrder --mode planning
gather-step --workspace /path/to/workspace qa-evidence createOrder --base main --head feature/my-branch --json
gather-step --workspace /path/to/workspace conventions
gather-step --workspace /path/to/workspace generate claude-md
gather-step --workspace /path/to/workspace generate claude-md --target summary
gather-step --workspace /path/to/workspace generate agents-md
gather-step --workspace /path/to/workspace generate codeowners
gather-step --workspace /path/to/workspace setup-mcp --scope local
gather-step --workspace /path/to/workspace compact
gather-step --workspace /path/to/workspace watch
gather-step --workspace /path/to/workspace pr-review --base main --head feature/my-branch --json
```

`pr-review` builds an isolated review index for a PR branch and emits a structured delta report covering changed files, safety signals, and suggested follow-up commands. See [CLI reference](website/src/content/docs/reference/cli.md#pr-review) for the full flag list.

`qa-evidence` emits canonical code-evidence metadata for QA planning tools. It combines planning/review/change-impact packs with local feature-flag and existing-test signals, but it does not generate test cases or interpret product requirements.

## Security

Gather Step does **not** redact secrets embedded in indexed source files. The deployment env-file parser stores variable names only, but un-rotated API keys, tokens, or credentials in source files, package metadata, or code comments can enter the graph and surface in MCP tool responses.

`gather-step-output/src/sanitize.rs` is a Markdown-injection escaper — it is not a secret scrubber.

Release smoke tests assert that deployment env values do not enter generated graph/search/context-pack/deployment outputs.
They do not replace repository-level secret scanning for source code.

**Before pointing Gather Step at a repository:**

- Rotate any exposed credentials.
- Use `.gitignore` and repo-level secret-scanning tools (such as `trufflehog` or `git-secrets`) to confirm no secrets are committed.
- Do not index repositories that contain live credentials.

## How It Fits Into AI Workflows

After setup, engineers usually stop invoking most Gather Step commands manually. The normal flow is:

1. Ask a question in Claude Code, Cursor, or another MCP-aware client.
2. The client calls Gather Step tools automatically.
3. The assistant answers from indexed graph context instead of repeated file search.

Typical retrieval flow for a change-impact question:

```text
search
-> get_symbol
-> trace_impact
-> get_shared_type_usage
-> change_impact_pack
-> answer
```

This is the core value proposition: precomputed structural context before the move, not exploratory prompting after it.

Gather Step also pairs well with a Markdown-first engineering memory: let memory provide prior decisions and learnings, then use Gather Step to ground the plan in the current code graph. See [memory-backed planning](website/src/content/docs/guides/memory-backed-planning.md) for the loop.

## Documentation

Published docs: <https://gatherstep.dev>

Getting started:
- [Getting started](website/src/content/docs/guides/getting-started.md)
- [Installation](website/src/content/docs/guides/installation.md)
- [Workspace setup](website/src/content/docs/guides/workspace-setup.md)
- [MCP clients](website/src/content/docs/guides/mcp-clients.md)
- [Operator workflows](website/src/content/docs/guides/operator-workflows.md)
- [Memory-backed planning](website/src/content/docs/guides/memory-backed-planning.md)

Reference:
- [CLI reference](website/src/content/docs/reference/cli.md)
- [Configuration reference](website/src/content/docs/reference/configuration.md)
- [MCP tools reference](website/src/content/docs/reference/mcp-tools.md)
- [Projection impact](website/src/content/docs/reference/cli.md#projection-impact)
- [Deployment topology](website/src/content/docs/reference/cli.md#deployment-topology)

Concepts:
- [Polyrepo graph](website/src/content/docs/concepts/polyrepo-graph.md)
- [Context packs](website/src/content/docs/concepts/context-packs.md)

## Development

For contributor validation:

```bash
just ready
```

That runs formatting, linting, tests, dependency checks, and typo checks.

## License

MIT

Copyright (c) 2026 JJ Adonis
