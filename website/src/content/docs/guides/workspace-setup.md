---
title: Workspace Setup and Configuration
description: Configure multi-repo workspace indexing in Gather Step. Covers gather-step.config.yaml, depth scoping, generated state layout, common pitfalls, and the clean/reindex cycle.
---

The workspace config is the single file that tells Gather Step what to index,
how deeply to index it, and how to handle file-level scoping rules. Getting
this right is the prerequisite for accurate cross-repo code graph results.

## Workspace Contract

Gather Step requires a workspace root directory that satisfies two conditions:

1. It contains a `gather-step.config.yaml` file at the top level.
2. Every repo listed in that config is reachable from the config root through a
   relative path that stays inside that root.

Gather Step writes all generated state into `.gather-step/` inside that same
root. Source repositories are never modified by indexing.

## Minimal Config

The minimal working config lists at least one repo:

```yaml
repos:
  - name: backend_standard
    path: repos/backend_standard
```

A typical multi-repo starting point:

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

`name` is the stable logical identifier used in CLI output, MCP tool responses,
and `--repo` scoping flags. `path` is relative to the directory containing
`gather-step.config.yaml`.

## Full Config Reference (at a Glance)

The table below covers the fields most commonly tuned in practice. For the
complete schema with types and validation rules, see
[Configuration reference](/reference/configuration/).

| Field | Type | Default / Required |
|---|---|---|
| `repos` | array | required |
| `repos[].name` | string | required — no path separators |
| `repos[].path` | string | required — relative, inside config root |
| `repos[].depth` | string | `full` |
| `allow_listed_repos` | array of strings | none — index all |
| `github` | object | optional |
| `jira` | object | optional |
| `indexing.exclude` | array of glob strings | none |
| `indexing.language_excludes` | array of strings | none |
| `indexing.include_languages` | array of strings | all supported |
| `indexing.include_dotfiles` | bool | `false` |
| `indexing.min_file_size` | integer (bytes) | none |
| `indexing.max_file_size` | integer (bytes) | none |
| `indexing.workspace_concurrency` | integer | system default |

The `github` and `jira` sections are part of the config schema. The primary
workflows, including local indexing, CLI queries, and MCP, do not require them.

## Bootstrapping with `init`

When starting from scratch, run `init` from the workspace root:

```bash
cd /path/to/workspace
gather-step init
```

In an interactive terminal, pressing Enter accepts the default onboarding path: keep the selected repos, index now, generate AI context files, register local MCP settings, and leave watch mode off.

### Repo picker

`init` walks the workspace directory, discovers directories that contain a
`.git` folder, and opens a checkbox-style repo picker before writing
`gather-step.config.yaml`. It skips directories it should not traverse:

- `.git`
- `.gather-step`
- `node_modules`
- `dist`
- `target`

The picker is fully keyboard-driven:

```text
  ↑/↓ move    Space toggle    Enter confirm    a all    n none    q cancel
  ─────────────────────────────────────────────────────────────────────────
  [✓] backend_api
  [✓] frontend_app
  [ ] docs_site                  ← unchecked: stays out of the config
  [✓] shared_contracts
  [✓] worker_service
  [ ] internal_admin
  ...
  4 of 6 selected
```

Toggle a repo to include or exclude it; press `a` to select every discovered
repo, `n` to deselect everything, and Enter to commit. The selected set is
written straight to `gather-step.config.yaml`:

```yaml
repos:
  - name: backend_api
    path: backend_api
  - name: frontend_app
    path: frontend_app
  - name: shared_contracts
    path: shared_contracts
  - name: worker_service
    path: worker_service
indexing:
  workspace_concurrency: 1
```

Each row in the picker maps one-to-one with a `repos[]` entry: the
checkbox state controls whether the repo is in the config, the directory
name becomes the logical `name`, and the path under the workspace root
becomes `path`. Adjust `depth` for large repos you want to scan
shallowly, add `indexing` scoping rules per repo, or rename `name` to
match a canonical service identifier.

If a config already exists, `init` uses it as the starting point. Existing repos
are preselected, removed repos stay unchecked, and repo-specific settings such
as `name` and `depth` are preserved for selected repos. Use `--force` only when
you intentionally want a fresh generated draft from repository discovery.

### Removing a repo later

Re-running `gather-step init` and unchecking a repo is the supported way
to drop it from the workspace. The next `gather-step index` notices the
config change, unregisters the repo, and purges its graph, search, and
metadata state — so the workspace stays consistent without any manual
cleanup.

For scripts or CI, pass flags explicitly instead of relying on prompts:

```bash
gather-step --workspace /path/to/workspace init \
  --index \
  --generate-ai-files \
  --setup-mcp local \
  --no-watch
```

Use `--no-index`, `--no-generate-ai-files`, or `--no-watch` to make a scripted
setup return immediately after writing the config.

`--generate-ai-files` writes `.claude/rules/` only after an index exists because
the rule files are graph-backed. When you intentionally skip indexing, Gather
Step still writes `CLAUDE.gather.md` and `AGENTS.gather.md`, then prints a
warning with the follow-up command:

```bash
gather-step --workspace /path/to/workspace index
gather-step --workspace /path/to/workspace generate claude-md --target rules
```

## Generated State

After `gather-step index` completes, the workspace looks like this:

```text
/path/to/workspace/
  gather-step.config.yaml
  .gather-step/
    registry.json              — workspace-level repo metadata and index state
    storage/
      graph.redb               — graph nodes and edges (redb store)
      search/                  — Tantivy full-text and symbol search index
      metadata.sqlite          — file hashes, dependencies, payload contracts,
                                 context pack records, watcher state
```

Key properties of this layout:

- **Source repositories are never modified.** Indexing writes only to
  `.gather-step/`.
- **All graph-backed CLI and MCP commands read from this directory.** If it is
  empty or absent, commands like `status`, `doctor`, `trace`, and `serve` have
  nothing to work from. Run `index` first.
- **The graph store, search index, and metadata database are updated together.**
  The storage coordinator maintains consistency across all three; partial writes
  are rolled back on failure.

## Depth and Scoping

### Depth levels

Each repo entry accepts an optional `depth` field that controls how deeply
Gather Step parses the code structure inside that repo:

| Value | What it means |
|---|---|
| `level1` | Shallow — file-level and top-level symbol extraction only |
| `level2` | Module structure and direct call sites |
| `level3` | Cross-file resolution and framework-aware extraction |
| `full` | Complete extraction including payload inference and semantic linking |

The default is `full`. Use a shallower depth for very large repos where you
only need coarse-grained signal, or to reduce indexing time during initial
exploration.

```yaml
repos:
  - name: backend_standard
    path: repos/backend_standard
    depth: full
  - name: large_monorepo
    path: repos/large_monorepo
    depth: level2
```

### Per-command scoping with `--repo`

Most CLI commands accept a `--repo <name>` flag to scope results to one
configured repo. This is useful when:

- the workspace contains many repos and you want focused output
- a target symbol name is ambiguous across repos
- you are tuning config for one repo without re-running the full workspace

```bash
gather-step --workspace /path/to/workspace status --repo backend_standard
gather-step --workspace /path/to/workspace pack createOrder --mode planning --repo backend_standard
```

The `--repo` flag does not affect what is indexed — it only filters CLI output
for that command invocation.

## Common Pitfalls

**Absolute paths in `path` fields** — All `repos[].path` values must be relative
to the config root. An absolute path like `/home/user/projects/myrepo` is
rejected at config load time.

**Paths that escape the config root** — A path like `../sibling_repo` that
resolves outside the config root directory is also rejected. Every repo must be
physically inside or below the workspace root.

**Names with path separators** — Repo `name` values must not contain `/`, `\`,
or `.` path components. Use flat identifiers like `backend_standard`, not
`services/backend`.

**Unknown YAML keys** — The config parser uses strict validation. Unknown field
names are rejected with a descriptive error. If you add a field that is not in
the schema, indexing will not start until it is removed.

**Running graph commands before indexing** — Commands like `trace`, `pack`,
`search`, `doctor`, and `serve` all read from the indexed state in
`.gather-step/`. If the index does not exist yet, run `gather-step index`
first.

## Clean / Compact / Reindex Cycle

### `compact`

Compacts generated storage in place. Use this when the graph and metadata
stores have grown after large reindexes or heavy watch-mode churn, but you do
not want to delete and rebuild the index.

```bash
gather-step --workspace /path/to/workspace compact
```

This is the safe maintenance command for "compress the generated index": it
keeps the registry and indexed graph available while reclaiming storage pages
where possible.

### `clean`

Removes all generated state under `.gather-step/`. Use this when you want to
discard the current index without immediately rebuilding it — for example, to
free disk space, or before handing off a workspace directory.

```bash
gather-step --workspace /path/to/workspace clean --yes
```

The `--yes` flag is required to skip the interactive confirmation prompt.
`--json` output also requires `--yes` so that automation cannot hang on an
interactive prompt.

### `reindex`

Deletes the current index state and then rebuilds it in one step. Equivalent
to `clean --yes` followed immediately by `index`. Use this when:

- a significant amount of code has changed and incremental indexing produced
  stale results
- you want a clean baseline before a benchmark or review session
- you changed the `gather-step.config.yaml` in a way that affects which repos
  are tracked

```bash
gather-step --workspace /path/to/workspace reindex
```

For smaller code changes during normal development, prefer
`gather-step watch` (live incremental updates) or `gather-step index` (manual
incremental re-run) over a full reindex.

### `init --force`

Recreate `gather-step.config.yaml` from scratch on top of an existing
workspace. Use this when the config drifted (manual edits, merge artifacts)
or when you want to rerun repo discovery and overwrite the persisted
selection.

```bash
gather-step --workspace /path/to/workspace init --force
```

`--force` only rewrites the config — it does not touch generated state.
Pair it with `gather-step reindex` when you also want to rebuild the
graph against the regenerated config:

```bash
gather-step --workspace /path/to/workspace init --force
gather-step --workspace /path/to/workspace reindex
```

Repos that disappear from the regenerated config are automatically
unregistered, and their graph, search, and metadata state is purged on
the next index run — so the combined sequence above is a complete
"start over" from any workspace state.

## Next Steps

- [Getting started](/guides/getting-started/) — run the full quickstart if you
  have not indexed yet.
- [Operator workflows](/guides/operator-workflows/) — use the graph once the
  index is ready.
- [Configuration reference](/reference/configuration/) — complete field
  documentation with types and validation rules.
