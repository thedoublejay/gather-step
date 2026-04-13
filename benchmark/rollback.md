# Rollback Criteria

This document defines the thresholds that, if breached, constitute a
regression requiring rollback or revert.

> **Rule:** Every threshold listed here must be at least as strict as the
> corresponding PR-fail threshold for the same metric.  A rollback threshold
> that is looser than the CI gate is incoherent — it would block rollback
> for a build that CI already rejected.

All numeric values below are authoritative copies of the values in
`benchmark/thresholds.yaml`.  When `thresholds.yaml` is updated, this
document must be updated to match.

---

## Parsing Correctness

| Metric | Rollback Threshold |
|--------|-------------------|
| Parser fixture pass rate | < 1.00 (100 %) |

Any build that fails to parse a previously-passing fixture must be reverted.

---

## Graph Quality

| Metric | Rollback Threshold |
|--------|-------------------|
| Node recall | < 0.95 |
| Node precision (within expected kinds) | < 0.85 |
| Edge recall | < 0.95 |
| Edge precision (within expected kinds) | < 0.85 |

---

## Link Quality

| Metric | Rollback Threshold |
|--------|-------------------|
| Missed repos per task | > 1 |
| Missed files per task | > 3 |
| False-positive repos per task | > 5 |
| Cross-boundary precision | < 0.85 |

---

## API Latency

| Metric | Rollback Threshold |
|--------|-------------------|
| p50 | > 50 ms |
| p95 | > 300 ms |
| p99 | > 1 000 ms |

Latency thresholds apply to the curated-monorepo corpus on the same runner
class used to establish the baseline.

---

## Memory / RSS

| Metric | Rollback Threshold |
|--------|-------------------|
| RSS growth fraction | > 10 % of baseline |
| RSS absolute peak | > 1 073 741 824 bytes (1 GiB) |

On macOS the RSS capture is not available; this threshold is enforced on
Linux CI only.

---

## Rollback Procedure

1. Identify the last green benchmark run in `benchmark/results/`.
2. Run `cargo run -p gather-step-bench -- compare <last-green> <failing>` to
   confirm the regression.
3. Revert the offending commits and re-run the full benchmark suite.
4. Do not re-open the PR until all thresholds are green on CI.
