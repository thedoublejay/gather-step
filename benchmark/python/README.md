# Python Corpus Benchmarks

This directory defines the neutral alias contract for Python corpus benchmarks.
Committed files must use aliases only. Real private repository names, package
names, local paths, raw outputs, and private benchmark JSON stay local.

Local-only inputs:

- `benchmark/python/private-corpus.local.yaml`
- `benchmark/python/private-results/`

Both paths are gitignored. Use aliases such as `py-private-alpha` and
`py-private-beta` in summaries and comparable output. The checked-in example
manifest documents the fields expected by the private corpus harness without
including real paths.

Neutral fixture check:

```sh
cargo run -p gather-step-bench -- planning-oracle \
  --fixture tests/fixtures/python_planning_workspace \
  --scenarios tests/fixtures/python_planning_workspace/scenarios \
  --thresholds benchmark/python/thresholds.yaml \
  --output-dir /tmp/gather-step-python-oracle
```

Neutral speed/storage check:

```sh
cargo run -p gather-step-bench -- workspace-run \
  tests/fixtures/python_planning_workspace \
  --thresholds benchmark/python/thresholds.yaml \
  --output-dir /tmp/gather-step-python-index
```

The `workspace-run` artifact includes wall-clock index time, graph node/edge
counts, cross-repo edge count, RSS growth when available, and storage byte
breakdowns for graph, metadata, and search files.

The neutral scenario may use a `[python_oracle]` table for Python-specific
repo, bridge, rank, warning, resolution, completeness, and unresolved-gap
assertions. Private corpus manifests should use the same alias-only vocabulary
when comparable summaries are needed.
