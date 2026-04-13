# Benchmark CI Integration

This document specifies how the planning-benchmark harness integrates with CI.

## Runner Requirements

- All benchmark comparisons must be made on a **like-for-like** runner class.
  Do not compare a result produced on `aarch64-apple-darwin` against one
  produced on `x86_64-linux-gnu` — the environment metadata in each result
  file encodes `os` and `arch` and the `compare` command will flag a mismatch.
- Pin the runner profile (e.g. GitHub Actions runner size) and do not change
  it without resetting the baseline result set.

## Partitioned Comparisons

Result directories are partitioned by OS and architecture.  The canonical
layout under `benchmark/results/` is:

```
benchmark/results/
  <datetime>/
    index_pass.json      # produced by `gather-step-bench run`
    ...
```

When comparing two runs, both directories must contain results from the same
`environment.os` + `environment.arch` pair.  The `gather-step-bench compare`
command warns and sets a non-zero exit code when environments differ.

## CI Gate

The GitHub Actions `Benchmark Threshold Gate` job runs a stable, bounded
subset as a blocking PR/push gate:

1. `cargo run -p gather-step-bench -- run benchmark/fixtures/curated-monorepo`
   gates fixture indexing and reports RSS when the platform exposes it.
2. `cargo run -p gather-step-bench -- link-quality --fixture
   benchmark/fixtures/curated-monorepo --tasks benchmark/link-quality` gates
   parser/graph recall, precision, and cross-boundary quality on curated tasks.
3. `cargo run -p gather-step-bench -- planning-oracle --fixture
   tests/fixtures/workspace --scenarios tests/fixtures/oracle` gates the
   search/planning path against latency and oracle-quality thresholds.

Full Criterion benches remain manual because they are noisier and runner-class
sensitive; publish speed comparisons only with same-runner, same-day baselines.

## Threshold Source of Truth

All numeric thresholds live in `benchmark/thresholds.yaml`.  The Rust
`Thresholds::load()` function reads this file at benchmark run time.  Changing
thresholds requires a deliberate edit to that file, not a code change.

## Same-Day Manual Baseline

Speed claims for `gather-step` versus a no-tool workflow are uncheckable when
the no-tool baseline is reused across multiple iterations.  Workspace state
shifts (file count, symbol count, repo set) between runs, so an old
operator-recorded time is comparing snapshot-vs-snapshot of two different
workspaces, not a true A/B.

For every benchmark run that reports a speed delta, the operator must also
record a same-day manual baseline using only platform-neutral tools — `rg`,
file reads, `ast-grep` — with a stopwatch.  Capture in the result artifact:

- `manual_baseline_seconds` — wall-clock time
- `manual_baseline_file_set` — final list of files the operator settled on
- `manual_baseline_evidence_path` — recordings, transcripts, or notes proving
  the run actually happened

When `manual_baseline_seconds` is missing, the speed-delta line in the
benchmark summary should read `unverified — no same-day A-side recorded`
rather than carrying a stale ratio.

Cost: 30–60 minutes of operator time per benchmark run.  This is intentional
overhead — the alternative is publishing speed claims that no one can
reproduce.
