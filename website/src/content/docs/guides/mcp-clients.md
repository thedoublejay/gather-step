---
title: Connect an MCP Client to Gather Step
description: Configure Claude Code, Cursor, or any MCP-aware AI coding assistant to use Gather Step as a local stdio MCP server for code graph queries across multiple repos.
---

MCP (Model Context Protocol) is the standard that AI coding assistants use to
pull structured context from external tools. Gather Step ships as a stdio MCP
server that you run locally alongside your indexed workspace. Any MCP-aware
client, such as Claude Code, Cursor, or another MCP-aware tool, can be pointed at it with a small
JSON configuration entry. No network service is involved: the server reads
from the same `.gather-step/` state your CLI uses.

## Prerequisites

Before connecting a client:

1. **An indexed workspace:** run `gather-step --workspace /path/to/workspace index`
   if you have not already. The MCP server reads from the indexed state; it
   does not build the index for you.
2. **The built binary** on your `PATH` or at a known absolute path. See
   [Installation](/guides/installation/) for build instructions.

## Fast Path for Claude

For Claude Code, Gather Step can write the workspace-local MCP settings entry:

```bash
gather-step --workspace /path/to/workspace setup-mcp --scope local
```

Use `--scope global` only when you want the same workspace-pinned server entry
in `~/.claude/settings.json`. The command is idempotent and updates the
`mcpServers.gather-step` block without touching other server entries.

## Start the Server (Smoke Test)

Verify the server starts cleanly before wiring up a client:

```bash
gather-step --workspace /path/to/workspace serve
```

If the index exists and the workspace config is valid, the server starts and
waits for MCP requests on stdin/stdout. You will not see output until a client
connects, which is expected behavior for a stdio server.

Useful flags for the `serve` command:

| Flag | Purpose |
|---|---|
| `--max-limit <N>` | Cap the number of results returned per tool call (default `1000`) |
| `--server-name <NAME>` | Override the MCP server name reported to clients (useful for distinguishing multiple workspaces) |
| `--graph <PATH>` | Override the default `graph.redb` path if you store it outside `.gather-step/` |
| `--registry <PATH>` | Override the default `registry.json` path |

For most setups, no flags are needed.

## Configure Claude Code

Claude Code reads MCP server configuration from two locations. Use whichever
fits your workflow:

**Project-scoped:** `.mcp.json` in the project root (checked in with the repo):

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

**User-scoped:** `~/.claude/claude_desktop_config.json` (applies to all
projects for this user):

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

Replace `/absolute/path/to/gather-step` with the real path to the compiled
binary (for example, the output of `which gather-step` once it is on your
`PATH`). Replace `/path/to/workspace` with the directory that contains your
`gather-step.config.yaml`.

After saving the config, restart Claude Code. The `gather-step` server entry
should appear in the MCP server list and show a connected state.

## Configure Cursor

Cursor supports MCP through `~/.cursor/mcp.json` or through the Settings UI
under **MCP Servers**. The JSON format is the same:

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

If you prefer the Settings UI, add a new server entry with the transport set
to `stdio`, the command set to the absolute binary path, and the arguments list
containing `--workspace`, your workspace path, and `serve`.

## Configure Any MCP-Aware Client

The configuration pattern above is the generic stdio MCP stanza. Any client
that supports the stdio transport accepts this shape:

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

The contract: the client launches the binary as a child process and communicates
with it over stdin/stdout using the MCP protocol. No TCP port is opened. No
network traffic leaves the machine. The server process ends when the client
process ends.

## How Claude Code Uses It

After configuration, Claude Code does not wait for you to type Gather Step commands manually. It decides which MCP tools to call based on your question and uses those results to build its reply.

Example:

> What features or pages are affected if I change `CreateOrderInput`?

A typical automatic sequence looks like this:

```text
Prompt
  -> search
  -> get_symbol
  -> trace_impact
  -> get_shared_type_usage
  -> change_impact_pack
  -> Answer
```

This is the reason to expose the MCP tool reference at all: the assistant does the tool selection automatically, but the retrieval path is still visible and debuggable when you want to understand how an answer was assembled.

## What to Call First: A Good First Five-Tool Loop

When a client connects for the first time, call these tools in order to orient
before issuing deeper queries:

**1. `get_graph_schema_summary`**
Returns a compact description of the node and edge kinds in the indexed graph.
Use this to understand what the current workspace contains before writing
queries — especially useful when the graph is unfamiliar.

**2. `list_repos`**
Returns the list of repos registered in the workspace, with per-repo file and
symbol counts. Use this to confirm the index is fresh and that all expected
repos are present. If a repo shows zero files, it likely was not indexed
successfully.

**3. `search`**
Searches the Tantivy search index for matching symbols by name or pattern.
Use this to find the identifier of a symbol before using it in `trace_route`,
`context_pack`, or `impact` calls. Searching is cheaper than a full trace and
helps narrow down the target when the name is ambiguous.

**4. `context_pack`**
Builds a task-shaped context slice for a given target symbol and mode. Modes
include `planning`, `debug`, `fix`, `review`, and `change_impact`. This is the
primary tool for giving an AI assistant a bounded, relevant view of the code
it needs to work with — ranked items, semantic bridges, next-step suggestions,
and identified gaps.

**5. `trace_route` / `trace_event`**
`trace_route` answers which frontend callers reach a given backend route, which
handler serves it, and what persistence hints exist downstream. `trace_event`
follows an async event from producers to consumers across repos. Use these
when the task involves debugging a specific request path or async flow in the
polyrepo code graph.

For a complete list of available MCP tools and their parameters, see the
[MCP tools reference](/reference/mcp-tools/).

## Current Limits

- **stdio transport only.** There is no HTTP or SSE transport. The server
  must run on the same machine as the indexed workspace.
- **Local-first.** Generated graph state does not leave the machine. The server
  has no network connectivity requirements.
- **Bounded results per call.** Tool responses are capped to avoid overwhelming
  context windows. Use `--max-limit` if you need a smaller cap.
- **Pack quality depends on index freshness.** Run `gather-step index` or keep
  `gather-step watch` running to ensure results reflect current code. Stale
  indexes produce stale tool responses.

## Next Steps

- [Operator workflows](/guides/operator-workflows/) — CLI commands for all the
  same capabilities the MCP tools expose.
- [MCP tools reference](/reference/mcp-tools/) — full tool list with parameter
  documentation.
- [Concepts: context packs](/concepts/context-packs/) — how packs are built and
  what each mode emphasizes.
