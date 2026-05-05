---
title: PR Review with Gather Step
description: Run a cross-repo structural review on a branch before merge — routes, symbols, payload contracts, events, deployment topology, and removal risks, local-first.
---

`gather-step pr-review` builds a disposable review index for any two refs and
returns a structured delta report covering every public surface changed across
all affected repos. No code leaves the machine. The workspace's normal
`.gather-step/` state is never modified.

## When to Use It

Use `pr-review` before merging any branch that:

- adds or removes an HTTP route, an exported symbol, or a payload contract
- changes the shape of an event producer or consumer
- modifies deployment config (Dockerfiles, Compose services, K8s manifests,
  env vars)
- touches a shared package used by multiple repos

For smaller, self-contained changes with no cross-repo surface the report is
still useful but usually short. The cost is the indexing time on the first run
(30-90 seconds); cache hits complete in 1-2 seconds when a retained matching
artifact exists.

## Running It Locally

### Basic

```bash
gather-step pr-review --base main --head feat/my-change
```

The command resolves both refs to SHAs, expands the set of affected repos from
the changed files, indexes the head branch into a disposable storage location,
and prints the delta report.

### With JSON output

```bash
gather-step pr-review --base main --head feat/my-change --json
```

The JSON form emits a `DeltaReport` (`schema_version: 7`) suitable for
piping into other tools or reading programmatically.

### Keeping the cache for follow-up queries

```bash
gather-step pr-review --base main --head feat/my-change --keep-cache --json
```

With `--keep-cache`, the review index survives after the report is returned.
The `suggested_followups` field in the report contains ready-to-run commands
pre-filled with `--registry` / `--storage` overrides that point at the kept
review index. Run them as-is to query PR-branch state rather than the workspace
baseline:

```bash
# example from suggested_followups:
gather-step pack createOrder --mode review \
  --registry /path/to/review-registry.json \
  --storage /path/to/review-graph.redb
```

### Severity threshold

```bash
gather-step pr-review --base main --head feat/my-change --severity strict
```

| Value | Behaviour |
|---|---|
| `warn` (default) | Always returns the report; exits 0 |
| `strict` | Non-zero exit when `removed_surface_risks` contains `high` findings |
| `pedantic` | Non-zero exit on any `removed_surface_risks` finding |

Use `strict` or `pedantic` in CI to gate merges on risk findings.

## Reading the Report

The report is divided into sections. Each section is empty when nothing changed
in that surface category.

| Section | What it shows |
|---|---|
| `metadata` | Base/head SHAs, checkout mode, indexed repos, elapsed time, warnings |
| `safety` | Review storage path, run ID, cleanup policy, cache key |
| `changed_files` | Repo-relative paths changed in `merge_base..head` |
| `routes` | Added / removed / changed HTTP routes. Removed routes carry downstream impact summaries. |
| `symbols` | Added / removed / changed exported symbols. Flags `signature_changed` and `visibility_changed`. |
| `payload_contracts` | Field-level diffs: added, removed, type-changed, optional-required flips |
| `events` | Producer/consumer set diffs across `Topic`, `Queue`, `Subject`, `Stream`, and `Event` virtual nodes |
| `decorators` | Permission, audit, and authorization decorator changes |
| `contract_alignments` | Cross-repo clusters of related payload contracts with confidence scores |
| `removed_surface_risks` | Removed routes / symbols / events with surviving consumers, classified `high` / `medium` / `low` |
| `deployment` | Deployment-topology changes: Dockerfiles, Compose services, K8s manifests, env vars, secrets, config maps, brokers, databases, GitHub Actions deploy jobs |
| `suggested_followups` | Ready-to-run `gather-step pack` and `trace crud` commands for the highest-impact deltas |

### Interpreting `removed_surface_risks`

`removed_surface_risks` contains the most actionable findings. Each entry has:

- `kind` — `route`, `symbol`, or `event`
- `surface` — the canonical name of the removed surface
- `severity` — `high`, `medium`, or `low`
- `reason` — a human-readable explanation
- `surviving_consumers` — graph nodes that still reference the removed surface

A `high`-severity removal means at least one consumer of the surface is in a
different repo and has no apparent migration. These entries should be reviewed
before merge.

### Interpreting `deployment`

The `deployment` section shows changes to the infrastructure layer alongside
code changes. Common cases to look for:

- A new service appears in Compose or K8s without a corresponding `routes`
  or `symbols` delta — the service may be wired in deployment config but
  not yet indexed.
- An env var is removed from a Dockerfile while `env_var_consumers` shows
  other services still reading it — potential misconfiguration.
- A `shared_infra` entry (broker, database) appears or disappears — worth
  a cross-team check before merge.

## Cleaning Up Artifacts

Without `--keep-cache`, the review index is deleted when the report is returned.

With `--keep-cache`, use the `clean` subcommand to manage artifacts:

```bash
gather-step pr-review clean --dry-run              # list every kept artifact for this workspace
gather-step pr-review clean --run-id <id>          # delete one run by ID
gather-step pr-review clean --base main --head feat/my-change  # delete by refs
gather-step pr-review clean --older-than 7d        # prune artifacts older than 7 days
gather-step pr-review clean --all                  # wipe all review artifacts for this workspace
```

`clean --older-than` skips `InProgress` artifacts so it cannot race a running
review. When a marker has `last_accessed_at`, pruning uses that timestamp so
cache hits keep useful artifacts fresh; older markers fall back to `created_at`.
`--all` removes everything including `InProgress`.

`gather-step clean --include-review` extends the workspace `clean` command to
also wipe review artifacts. A full `gather-step reindex` automatically wipes
review artifacts since their baseline is invalidated.

## MCP Tool Variant

Any MCP-aware client can invoke the same review through the `pr_review` tool.
Claude Code triggers it automatically when you ask:

> "Review this PR using gather-step."

> "What does this branch change structurally?"

> "Check the cross-repo impact of feat/my-change."

The tool accepts the same parameters as the CLI (`base`, `head`, `keep_cache`,
`severity`) and returns the same `DeltaReport` JSON. It works with any client
that supports the stdio transport. See [Connect an MCP Client](/guides/mcp-clients/)
for setup.

**Clients known to work with `pr_review`:**

- Claude Code (stdio MCP, any version with tool-call support)
- Cursor (MCP stdio transport)
- Any client following the stdio MCP protocol

## See Also

- [Operator workflows — Review a PR](/guides/operator-workflows/#review-a-pr) — concise command reference
- [CLI reference](/reference/cli/) — full flag documentation for `pr-review`
- [MCP tools reference](/reference/mcp-tools/#pr-review) — `pr_review` tool parameters and return shape
- [Concepts: polyrepo graph](/concepts/polyrepo-graph/) — how the graph powers cross-repo surface diffs
