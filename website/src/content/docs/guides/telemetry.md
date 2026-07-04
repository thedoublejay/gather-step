---
title: Local Run Telemetry
description: How gather-step records local run telemetry, what the telemetry database stores, and how to read it with the log command — including the summary view, graph-availability signal, and stale-run repair.
---

gather-step records a small amount of telemetry about its own runs into a local
SQLite database so you can answer "why was that run slow or incomplete?" without
any external service. Telemetry is **local-only**: nothing is sent anywhere, and
error messages are stored as a hash plus a typed category, never as raw text.

## What is recorded

Each command run writes one row to a local `telemetry.db` (under your platform's
user data directory). A row captures:

- the command name, start time, duration, and exit status;
- the CLI version and build provenance (which build profile produced the binary);
- peak resident memory (RSS) on platforms that report it;
- warning and error counts, and whether a recovery event fired;
- a command-specific `result_count` (e.g. how many dependencies or hops a query
  returned) when applicable;
- the **graph availability** observed for the run (see below);
- for failures: a typed error category and a hashed message — never the raw
  message, path, or identifiers.

Rows are retained for 90 days (and capped in count); older rows are pruned
automatically.

## Graph availability

Graph availability is the dominant reliability signal for agent workflows: most
"empty" or "slow" runs trace back to the graph not being readable. Each run
records one of:

- `available` — the run completed against a readable graph;
- `locked` — the run lost the advisory lock to another gather-step process;
- `not_indexed` — the workspace or a repo had no usable index yet;
- `unknown` — the outcome was unrelated to graph state (availability is not
  claimed rather than guessed).

## Reading telemetry with `log`

Show recent runs as rows:

```bash
gather-step log --last 50
gather-step log --since 7d --errors-only
gather-step log --command index
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
finish row, its row stays marked `running`. Rows older than a fixed threshold are
finalized as `abandoned` automatically the next time the telemetry store is
opened. To finalize them on demand and see how many were rewritten:

```bash
gather-step log --repair
```

### Pruning

```bash
gather-step log --clear-before 90d
```

deletes rows older than the given age.
