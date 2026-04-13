---
title: Getting Started with Gather Step
description: Install Gather Step, create a workspace config, build the local index, and connect an MCP-aware AI client.
---

Gather Step is usually not operated manually after setup. You install it, index a workspace, connect an MCP-aware client, and the assistant calls the Gather Step tools automatically when it needs graph context.

## Before You Start

You need two things:

- the `gather-step` binary
- a workspace directory containing the repositories you want to index

If you need install details, use [Installation](/guides/installation/).

## 1. Install the Binary

On macOS:

```bash
brew install thedoublejay/tap/gather-step
```

Or build from source:

```bash
cargo build -p gather-step --release
```

Verify the binary:

```bash
gather-step --version
```

## 2. Create a Workspace Config

Point Gather Step at the directory that contains your repositories:

```bash
gather-step --workspace /path/to/workspace init
```

That writes `/path/to/workspace/gather-step.config.yaml`.

Example:

```yaml
repos:
  - name: backend_standard
    path: repos/backend_standard
  - name: frontend_standard
    path: repos/frontend_standard
  - name: shared_contracts
    path: repos/shared_contracts
indexing:
  workspace_concurrency: 4
```

Use neutral logical names in the config. The `name` field is what appears in CLI output, MCP responses, and repo-scoped filters.

## 3. Build the Index

Run a full index:

```bash
gather-step --workspace /path/to/workspace index
```

This creates `.gather-step/` inside the workspace and stores:

- the workspace registry
- the persisted graph
- the search index
- the metadata database

Source repositories are not modified.

## 4. Check That the Workspace Is Healthy

Before wiring in an AI client, confirm the index is usable:

```bash
gather-step --workspace /path/to/workspace status
gather-step --workspace /path/to/workspace doctor
```

Use `status` to confirm the expected repos were indexed. Use `doctor` to surface missing paths, search projection gaps, dangling edges, or unresolved graph problems.

## 5. Connect an MCP Client

Configure your client to launch the local stdio server:

```json
{
  "mcpServers": {
    "gather-step": {
      "command": "/absolute/path/to/gather-step",
      "args": [
        "--workspace",
        "/path/to/workspace",
        "serve"
      ]
    }
  }
}
```

Gather Step does not need to run as a separate network service. The client starts it as needed.

For client-specific setup, use [MCP clients](/guides/mcp-clients/).

## 6. Ask Normal Questions

After setup, the assistant chooses the right Gather Step tools automatically. For example:

- “What handles `POST /orders` end to end?”
- “Who consumes `order.created`?”
- “What repos are affected if I change this shared type?”
- “Give me a review-oriented context pack for `createOrder`.”

The person using the assistant does not need to invoke `trace_route`, `trace_event`, or `planning_pack` directly unless they want to inspect the graph from the terminal.

## How Claude Code Uses It

Once Gather Step is configured as an MCP server, the flow is:

1. You ask a normal question.
2. Claude Code chooses the Gather Step tools automatically.
3. Claude Code combines those tool results into the answer you read.

Example:

> What features or pages are affected if I change `CreateOrderInput`?

Typical automatic tool flow:

```text
Prompt
  -> search
  -> get_symbol
  -> trace_impact
  -> get_shared_type_usage
  -> change_impact_pack
  -> Answer
```

What each step is doing:

- `search` finds the target symbol in the indexed workspace.
- `get_symbol` confirms the exact match and source location.
- `trace_impact` walks cross-repo graph links to find affected repos and surfaces.
- `get_shared_type_usage` checks where the shared type is used directly.
- `change_impact_pack` returns a bounded impact-focused context bundle for the final answer.

The important part is that this happens automatically. The command names are reference material so the retrieval path is visible, not a manual workflow you are expected to memorize.

## Keep It Fresh

During active development, keep the graph up to date with:

```bash
gather-step --workspace /path/to/workspace watch
```

If you prefer one long-running process for both MCP and live indexing, start
the server with watch mode enabled:

```bash
gather-step --workspace /path/to/workspace serve --watch
```

If you are not using watch mode, rerun:

```bash
gather-step --workspace /path/to/workspace index
```

After large reindexes or long watch sessions, reclaim generated-state space
without deleting the index:

```bash
gather-step --workspace /path/to/workspace compact
```

## Next Steps

- [Workspace setup](/guides/workspace-setup/) for config and indexing depth
- [MCP clients](/guides/mcp-clients/) for Claude Code, Cursor, and generic MCP setup
- [Operator workflows](/guides/operator-workflows/) for direct CLI usage
- [MCP tools reference](/reference/mcp-tools/) for the automatic tool surface
