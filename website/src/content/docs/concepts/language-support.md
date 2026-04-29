---
title: Language Support
description: Which languages Gather Step parses today, what each tier extracts, and how Python and TypeScript reach first-class parity. Includes the private corpus benchmarking convention.
---

Gather Step parses source files into a queryable graph of files, modules, classes, functions, imports, and call sites. Not every language reaches the same depth. This page describes the current parsing tiers, what each one extracts, and how to reason about cross-repo edges when your workspace mixes languages.

## Parsing Tiers

Languages are grouped by how much information the indexer recovers. A higher tier means the graph carries enough structure for cross-repo planning, AI context packs, and impact analysis to work end-to-end. A lower tier still allows file discovery and content hashing but leaves later analysis stages with less to query.

### Tier 1 — First-Class

First-class languages produce a complete graph: file nodes, module nodes, class and function symbols with qualified names, import bindings with resolved targets, and call sites with owner and callee qualified names. Cross-repo identity resolution honors `gather-step.config.yaml` so the same on-disk file produces the same node ID across runs.

| Language | Parser | Resolution | Cross-Repo Edges |
| --- | --- | --- | --- |
| TypeScript | swc | tsconfig path aliases, `package.json` workspaces, `exports` conditions | Yes |
| JavaScript | swc | `package.json` workspaces, `exports` conditions | Yes |
| Python | tree-sitter | Absolute current-package imports for `src/<pkg>` and flat `<pkg>` layouts; sibling-package imports across configured repos; `pyproject.toml [project].name` for standalone repo identity | Yes |

Python is first-class as of v2.1.0. TypeScript and JavaScript have been first-class since v1.0.0.

### Tier 2 — File-Discovery Only

File-discovery languages are indexed for presence and content hashing. The parser walks the source tree, captures the file in the graph, and detects framework or build markers, but does not yet extract symbols, imports, or call sites. They participate in workspace topology but not in symbol-level queries.

| Language | Parser Wired | Symbol Extraction | Notes |
| --- | --- | --- | --- |
| Rust | tree-sitter grammar attached | No | File and crate detection only. |
| Go | tree-sitter grammar attached | No | File detection only. |
| Java | tree-sitter grammar attached | No | File detection only. |

If you index a Rust, Go, or Java repository, you will see the files appear in the graph and contribute to file counts, but cross-file edges and call sites for those languages are not yet emitted. The grammars are in place, so promoting them to Tier 1 is a matter of writing the equivalent of `visit_python` for each.

## What "First-Class" Means In Practice

For a Tier 1 language, you can expect:

- **Stable node IDs.** Node identity is a function of `(repo, file_path, kind, qualified_name)`. Re-indexing the same source produces the same IDs, and the same on-disk file produces the same IDs across configured workspace repos.
- **Cross-repo edges.** When a file in repo A imports a symbol from repo B, the import binding resolves to a file node owned by repo B's configured `name`, not repo A. This is what powers cross-repo impact analysis and planning.
- **Qualified names that survive nesting.** A nested function `def normalize` inside `class TitleHandler`'s `handle` method is named `TitleHandler.handle.normalize`. Same-named helpers in sibling functions remain distinct.
- **Resolved imports.** Each import binding carries a resolved file path when the target is reachable, plus the resolver that produced the answer (`import_map`, `path_alias`, `workspace_package`, `python_sibling`, etc.).

For a Tier 2 language, none of the above apply yet. Files are present; symbols are not.

## Resolution Order

When a file in a configured workspace imports a symbol, the resolver walks the following candidates in order. The first hit wins.

1. **Configured workspace lookup.** If `gather-step.config.yaml` declares a sibling repo whose `path` contains the resolved file, the cross-repo node uses that repo's configured `name`. This is the canonical source of truth.
2. **Ancestor-with-manifest.** Walk up from the resolved file looking for a project marker (`pyproject.toml`, `setup.py`, `setup.cfg`, `package.json`). When the marker is `pyproject.toml`, the repo name comes from `[project].name`; otherwise from the directory basename.
3. **Sibling directory scan.** Search up to six ancestor levels of the *current* repo for sibling directories that look like project roots and contain the resolved file.

Diagnostics are emitted (`tracing::warn!`) at each tier when a step fails — malformed YAML, unreadable directories, missing `pyproject.toml` fields. If you see "import not resolved" symptoms, run with `RUST_LOG=gather_step_parser=warn` to surface the underlying cause.

## Private Corpus Convention

When you measure parser quality or storage performance against repositories you cannot publish, follow the private corpus convention so committed artifacts stay neutral.

- **Neutral fixtures live in the repo.** `tests/fixtures/python_planning_workspace/` is a synthetic Python workspace with safe names like `backend_standard` and `service_a`. CI thresholds run against these.
- **Private corpora live on your machine.** `benchmark/python/private-corpus.local.yaml` and `benchmark/python/private-results/` are gitignored. They reference real repositories by alias only — `py-private-alpha`, `py-private-beta`, and so on.
- **Aliases on every checked-in artifact.** Anything that lands in a release note, threshold update, or shared dashboard refers to the alias. Real names, paths, and raw outputs stay local.

The `benchmark/python/private-corpus.example.yaml` file documents the schema (alias, path, file counts, framework families, indexability status). Copy it to `private-corpus.local.yaml`, fill in your real paths, and run the bench harness. See `benchmark/python/README.md` for the full workflow.

This convention is gather-step's local discipline; if you have seen "internal benchmark suite", "user test corpus" (TypeScript), or "fleet" (Google) in other projects, the underlying idea is the same — your data, not the project's data.

## Promoting A Language

If you want to push Rust, Go, or Java into Tier 1, the work is shaped after `visit_python` in `crates/gather-step-parser/src/tree_sitter.rs`:

1. Add a `visit_<lang>` function that handles the language's class, function, import, and call AST nodes.
2. Wire it into the language match arm in `parse_file_core`.
3. Implement language-specific import resolution (path style, manifest format, sibling-package conventions).
4. Add neutral fixtures under `tests/fixtures/` and at least one regression test asserting cross-file edges resolve.
5. Add a planning oracle scenario under `tests/fixtures/oracle/` if the language unlocks a new analysis use case.

The Python promotion in v2.1.0 is the most recent worked example; the changelog and `python_import_regression.rs` together form a reasonable reference.
