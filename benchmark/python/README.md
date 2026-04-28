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
