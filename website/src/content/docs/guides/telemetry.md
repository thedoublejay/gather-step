---
title: Local Run Telemetry
description: How gather-step records local run telemetry, what the telemetry database stores, and how to read it with the log command — including the summary view, graph-availability signal, and stale-run repair.
---

gather-step records a small amount of telemetry about its own runs into a local
SQLite database so you can answer "why was that run slow or incomplete?" without
any external service. Telemetry is **local-only**: nothing is sent anywhere, and
error messages are stored as a hash, typed category, and a bounded redacted
excerpt rather than raw command output.

## What is recorded

Each instrumented command run writes one row to a local `telemetry.db` (under
your platform's user data directory). The `log` reader itself is not
instrumented, so it cannot appear in its own results. A row captures:

- the command name, start time, duration, and exit status;
- the CLI version, build profile, binary path, and build SHA when supplied by the build;
- peak resident memory (RSS) on platforms that report it;
- warning and error counts, and whether a recovery event fired;
- a command-specific `result_count` (e.g. how many dependencies or hops a query
  returned) when applicable;
- the **graph availability** observed for the run (see below);
- for failures and traced warnings: a typed category, per-install keyed message
  fingerprint, and truncated redacted excerpt; `log --events` exposes the last
  bounded diagnostic events;
- process identity and heartbeat data for long-running `serve` and `watch`
  commands, so repair can check liveness before declaring a run abandoned.

Workspace IDs use a private per-install key stored beside the database. Reads,
summaries, automatic retention, repair, and the 10,000-row cap are scoped to the
current workspace by default. Use `--all-workspaces` only when intentionally
inspecting or maintaining the installation-wide database.

## Graph availability

Graph availability is the dominant reliability signal for agent workflows: most
"empty" or "slow" runs trace back to the graph not being readable. Each run
records one of:

- `available` — the run completed against a readable graph;
- `locked` — the run lost the advisory lock to another gather-step process;
- `missing` — the workspace or a repo had no usable index yet;
- `corrupt` — graph storage was present but unreadable;
- `not_applicable` — the command does not open the graph;
- `unrecorded` — a legacy run did not capture the signal.

## Reading telemetry with `log`

Show recent runs as rows:

```bash
gather-step log --last 50
gather-step log --since 7d --errors-only
gather-step log --before 30d --status error
gather-step log --command index
gather-step log --category graph_lock_contention
gather-step log --events
gather-step log --events --run <run-id>
```

### Summary view

`--summary` aggregates over the selected window (respecting `--last`, `--since`,
and `--command`) instead of listing rows — the fastest way to see the shape of
recent activity:

```bash
gather-step log --summary --since 30d
```

It reports run counts by status, run counts by graph availability, the number of
abandoned runs, peak RSS, and the slowest commands by observed duration. Add
`--json` for machine-readable output.

### Repairing stale runs

If a run's process is killed (crash, `kill -9`, power loss) before it writes its
finish row, its row stays marked `running`. Rows older than the repair threshold
are checked against their PID/start token and heartbeat before they become
`abandoned`; a measured end time is not invented. To repair on demand:

```bash
gather-step log --repair
```

### Pruning

```bash
gather-step log --clear-before 90d
```

deletes rows older than the given age.

## Disabling or isolating telemetry

Set `GATHER_STEP_TELEMETRY=off` to disable local telemetry. Tests and automation
can set `GATHER_STEP_TELEMETRY_ROOT` to an isolated directory. `gather-step log`
prints the absolute database path so it is always clear which store is being
inspected.
