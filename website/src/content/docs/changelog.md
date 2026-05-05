---
title: "Changelog"
description: "User-visible changes to gather-step, listed by release. Updated manually until a release pipeline is wired up."
---

This changelog lists significant user-visible changes. It is maintained manually until release notes and tagged releases become the automated source of truth.

## Unreleased

No unreleased changes yet.

## v3.1.0

Release status: **draft**.

Indexing-performance, storage, and security release. Replaces the SWC-based TypeScript and JavaScript visitor with an Oxc-driven implementation, lands a series of throughput and footprint compactions, and hardens the watch and config surface against symlink and oversize-file abuse.

### Highlights — TypeScript and JavaScript parser

- Replaced the SWC visitor with an Oxc-driven implementation. Same `ParseState` writes (NodeIds, edges, decorators, call sites, constant strings) as the previous backend so downstream consumers see no behavioural change beyond a function-signature accuracy fix.
- Removed `swc_common`, `swc_ecma_ast`, and `swc_ecma_parser` from the dependency tree (~3.4k lines and a sizeable transitive dependency graph).
- Added an `oxc_test_support` surface that mirrors the helpers test suites previously imported from `swc_test_support` (status checks, full-pipeline symbol probes, raw identifier scans, top-level declaration extraction).
- Function signatures emitted for zero-parameter methods are now precise (`handle()` instead of accidentally swallowing a preceding decorator argument such as `('build')`).

### Highlights — Indexing performance

- Bounded context-pack precompute and pack-target selection by repo count.
- Cached path-alias discovery for the duration of an index run.
- Gated framework augmenters by language so non-TS/JS repos do not pay for them.
- Skipped the size-only filesystem walk on the default index path.
- Avoided cloning traversal source bytes on the hot path.
- Moved git analytics off the writer hot path and bounded its queue depth by repo count.

### Highlights — Storage compactions

- Dropped the redundant search `description` text field; reintroduced `qualified_name` as a dedicated indexed-only field with a lighter tokenizer chain. `SEARCH_INDEX_VERSION` is bumped to `1`.
- Decoded `is_exported` and `lang` from search fast fields instead of stored fields.
- Replaced the `edges_by_kind` projection with counters and compacted edge-metadata tags.
- Truncated `file_index_state.content_hash` to a 128-bit BLAKE3 prefix for the per-file change-detection cache.
- Pruned stale context packs on write and salted cache keys by compatibility.

### Highlights — Security

- Watcher now ignores symlinked event paths.
- `gather-step.local.yaml` and other local config reads are capped at a bounded byte budget.
- `git worktree add` arguments are passed positionally rather than glued into one shell string.
- Deployment topology config rejects symlinked paths.

### Bug fixes

- Search queries split identifier separators (`-`, `_`, `.`, `/`) before parsing so snake-case and slash-bearing repo names tokenize the same way they index.
- Qualified impact queries fall back to the tail segment when the qualified form does not hit the search index.
- Workspace registry counts are refreshed from the final graph at the end of an index run so the registry never drifts behind the graph.
- Incremental classification truncates new content hashes to the stored prefix length before comparing, so the 16-byte hash prefix store does not flag every previously-indexed file as modified.

### Test coverage

- Oxc parser self-validation tests across every TS/JS extraction fixture: status, top-level declared names, and import-binding shape.
- Secret-surface MCP smoke test exercises the redaction surface end-to-end.
- Deployment-topology MCP tools test pins the public response shape.
- Benchmark harness samples resource peaks (max RSS, peak memory footprint, open FDs on Unix).

### Internal architecture

- New `gather-step-deploy` workspace crate. Deployment-artifact parsing (Dockerfile, Compose, Kubernetes, Kustomize, Helm, GitHub Actions, env files) was extracted out of `gather-step-storage` and is now consumed by `gather-step-storage::indexer` and `gather-step-analysis`. No public API change for end users; the `deployment-topology` queries and the PR-review `deployment` delta surface continue to behave the same.
- TypeORM framework parser added (entity decorators, migration `MigrationInterface` `up`/`down` extraction). Powers the existing PR-review `payload_contracts` and migration-edge surfaces for TypeORM repos.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `3.1.0`.

## v3.0.0 (Draft)

Release status: **draft**.

Combined release covering deployment-topology indexing and `gather-step pr-review`, a non-destructive PR analysis command that builds an isolated review index for a PR branch and emits a typed delta report.

### Highlights — Deployment Topology

- Added deployment topology indexing for Dockerfiles, Docker Compose, Kubernetes manifests, Kustomize files, explicit Helm chart artifacts, GitHub Actions deploy jobs, configured env files, and Compose `env_file` references.
- Added graph nodes and edges for deployments, env vars, secrets, config maps, workflow jobs, brokers, and databases.
- Added `gather-step deployment-topology` plus MCP tools for `where_deployed`, `service_env`, `env_var_consumers`, `undeployed_services`, `deployed_but_no_code`, and `shared_infra`.
- Projection impact now replaces `deployed_owner_unchecked` with `deployed_owner_topology_observed` when indexed deployment evidence exists for the affected service or repo.
- Refreshed MCP protocol dependencies by updating `rmcp` and `rmcp-macros` to `1.6.0`.

### Highlights — PR Review Mode

- Added `gather-step pr-review --base <REF> --head <REF>` to build a disposable review index in the OS cache directory and emit a structured `DeltaReport` for human or machine consumption.
- Added `gather-step pr-review clean` with five selectors (`--dry-run`, `--run-id`, `--base/--head`, `--older-than`, `--all`) and an `--include-active` opt-in for pruning the still-resolvable cache. `clean --older-than` skips `InProgress` artifacts so it cannot race a long indexing run.
- Added `--severity {warn, strict, pedantic}` threshold modes. `warn` is the default; `strict` exits with code 2 on any High-severity removed-surface risk or payload type change; `pedantic` extends that to Medium risks and any payload change. Legacy `--strict` flag remains a deprecated alias.
- Added `--format {markdown, json, github-comment, braingent}` plus `--github-comment-file <PATH>` for CI integrations. The GitHub-comment renderer auto-truncates to fit the platform's 65,536-char comment limit. The Braingent renderer emits a YAML-frontmatter Markdown record suitable for archiving in a memory store. Legacy `--json` flag remains a deprecated alias.
- Added `--engine temp-index`. `temp-index` is the default public review engine and builds a full isolated index for the PR head.
- Added `--keep-cache` to preserve the review artifact root for follow-up `trace`, `impact`, `pack`, and `projection-impact` commands. Suggested follow-up commands in the report are pre-filled with `--registry` / `--storage` overrides pointing at the kept index.
- Added `--no-baseline-check` to suppress the workspace-HEAD-vs-`--base` SHA mismatch warning. By default the report's `## Warnings` block surfaces the mismatch so reviewers know the diff may be feature-vs-feature instead of base-vs-head.
- Added `--registry` and `--storage` flags on `trace`, `impact`, `pack`, and other read commands so they can target a kept review artifact root and replay PR-only context.
- Extended `gather-step clean` with `--include-review` to also wipe review artifacts for the workspace.
- Wired a full `gather-step index` reindex to automatically wipe review artifacts (their baseline is invalidated).
- Added a branch-scoped review cache keyed by `(workspace_hash, base_sha, head_sha, config_hash, schema_version, gather_step_version)`. Cache hits skip worktree creation and indexing when a retained matching artifact exists. Marker schema is now v2 (added `cache_key` and `last_accessed_at`); v1 markers remain readable by the cleanup tooling.
- Added `pr_review` MCP tool exposing the same delta report to MCP clients. The tool shells out to the CLI to inherit the workspace-storage safety guard.
- Added a top-level `CLAUDE.md` documenting the agent workflow for "review this PR using gather-step" plus project conventions.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `3.0.0`.

### Deployment Topology

- Env-file values are not stored. Gather Step indexes env var names only.
- `deployment.include`, `deployment.gitops_roots`, and `deployment.env_files` can add non-standard deployment paths to the indexer.
- Deployment topology changes the generated graph schema. Existing `.gather-step` storage should be rebuilt with `gather-step reindex` before relying on deployment-topology output.
- Helm and GitHub Actions detection is intentionally conservative to avoid treating generic `values.yaml`, `chart.yaml`, `helm lint`, or `DEPLOY_*` env references as deployment evidence.
- Incremental indexing now purges stale deployment facts when a previously indexed artifact becomes malformed or stops classifying as deployment data.

### PR Review Mode — hard invariants

- `pr-review` and `pr-review clean` never mutate the workspace's normal `.gather-step/storage` or `.gather-step/registry.json`. Every review run logs the exact baseline storage path, review storage path, run id, and cleanup policy in the report's `safety` metadata block.
- `StorageContext::review_checked` rejects any review path that lives under `<workspace>/.gather-step/`. Workspace-local review artifacts must use a sibling (e.g. `.gather-step-review/`) or the OS cache directory.
- `pr-review clean` refuses to delete any path whose marker file does not match the current workspace hash, and refuses paths overlapping the baseline `storage/` or `registry.json`.

### PR Review Mode — delta report (`schema_version: 7`)

- **Routes**: added / removed / changed by `(method, canonical_path)`. Handler info (repo, file, line, qualified name) attached via `Serves` edges.
- **Symbols**: added / removed / changed exported symbols and shared-symbol stubs by `(repo, qualified_name)`. Reports `signature_changed` and `visibility_changed` flags.
- **Payload contracts**: field-level diffs (added / removed / type-changed / `optional`-required flips) keyed by `(repo, file, target_qualified_name, side)`. Removed and changed contracts can carry downstream impact summaries.
- **Events**: producer and consumer set diffs across `Topic`, `Queue`, `Subject`, `Stream`, and `Event` virtual nodes.
- **Decorators**: added / removed / changed permission, audit, and authorization decorators.
- **Contract alignments**: cross-repo clusters of related payload contracts with high / medium / low confidence, marked when any cluster member is touched by the PR.
- **Removed-surface risks**: removed routes / symbols / events with surviving consumers, classified by severity (`high` for cross-repo, `medium` for same-repo, `low` for unconsumed).
- **Deployment topology**: added / removed / changed deployment targets, env vars, secrets, config maps, shared infrastructure, and GitHub Actions deploy jobs. Changed deployments report file, service, image-evidence, and env-binding change reasons.
- **Impact summaries**: per-removed-and-changed surface, downstream consumer counts grouped by repo and classified as `read_only`, `write_mutate`, `construct_payload`, or `unknown`.
- **Suggested follow-ups**: synthesized `gather-step pack` and `gather-step trace crud` commands targeting the highest-impact deltas. Capped at 10 commands; all carry `--registry` / `--storage` overrides for the kept review index.

### PR Review Mode — implementation notes

- The public review engine remains `temp-index`; internal overlay reads now build a `DiffOverlayStore` from baseline/review graph snapshots and have an active parity fixture against temp-index route deltas.
- Review seed copying attempts filesystem clone / copy-on-write first (`cp -c` on macOS, `cp --reflink=auto` on Linux) and falls back to byte copying. Because the current graph, metadata, and search stores are monolithic files, seed scope remains the full storage tree.
- Decorator extraction stores raw decorator arguments, decorator-line spans, and `UsesDecorator` edges from the decorated class or method to the decorator node.

### Verification Coverage

- Added regression coverage for deployment parser false positives, stale deployment fact purging, service-targeted projection-impact topology matching, shared-infra consumers, topology response mapping, and generated MCP tool summaries.
- 148 `pr_review` library tests covering storage-context safety guards, artifact-root marker schema and cleanup, branch-scoped cache reuse and parity, seed-from-baseline, affected-repo expansion, every delta extractor, severity threshold modes, output renderers (Markdown / JSON / GitHub-comment / Braingent), and the MCP-side wiring.
- 8 git-helpers tests for `resolve_ref`, `resolve_range`, `merge_base`, `changed_files`, and detached-worktree creation / removal.
- 5 worktree-helpers tests confirming target-exists refusal, missing-repo errors, and idempotent removal.
- Stable JSON top-level-key snapshots and Markdown section-header snapshots prevent accidental schema drift.
- Verified with Rust formatting, deploy/analysis/storage/MCP/CLI tests, workspace clippy, website build, and GitHub Actions during release preparation.

## v2.4.0 (2026-05-01)

Release status: **released**.

Setup and indexing usability release for config-respecting onboarding, repo selection, clearer progress copy, watch-count runs, parser-warning cleanup, and docs layout stability.

### Highlights

- Changed `init` to reuse existing `gather-step.config.yaml` files instead of failing or silently regenerating repo lists.
- Added a numbered, checkbox-style repo picker with `all` and `none` shortcuts; repos already present in the config are selected by default.
- Preserved selected repos' existing config metadata such as custom `name`, `depth`, provider settings, and indexing rules.
- Added optional `gather-step watch N` support so watch mode can stop after `N` completed indexing runs.
- Shortened the indexing progress bar, displayed the current repo path above it, and added final elapsed time plus index size.
- Reworded indexing finalization copy to sentence case: `Flushing search index...`, `Counting cross-repo edges...`, and `Precomputing N context packs...`.
- Added start and finish indexing logs with workspace, repo path, duration, and index-size context.
- Skipped SWC for static JSON/YAML mapping files and downgraded ambiguous sibling Python package resolution from warning to debug/no-resolution.
- Added a loader while `generate` writes assistant-facing Markdown files.
- Updated setup-complete copy with a planning prompt example and docs link.
- Fixed the docs content/sidebar overlap on the CLI reference page.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.4.0`.

### Verification Coverage

- Added regression coverage for existing config reuse, `watch N` argument parsing, static mapping parser routing, duplicate Python sibling package ambiguity, and indexing summary formatting.
- Verified with Rust formatting, Cargo check, clippy, targeted CLI/parser tests, and website build during release preparation.

## v2.3.0 (2026-05-01)

Release status: **released**.

Data-shape research carry-forward release for alias-aware field evidence, optional payload filter risk, generated migration probe plans, and broader migration sibling detection.

### Highlights

- Labeled field evidence as `direct_field_access` or `local_alias_field_access` when `projection-impact` / `projection_impact` can explain the origin.
- Followed same-scope TypeScript aliases and object destructuring aliases for typed field-access evidence.
- Promoted optional payload filter mismatch into `projection_impact`, dotted `impact`, MCP `projection_impact`, and planning-pack gap summaries.
- Added generated Mongo `$type` probe plans to migration sibling bands, with copy-paste-safe `db.getCollection(<name>)` commands.
- Extended Mongoose migration detection to imported local model declarations, multiple static collections in one migration, and additional static write methods.
- Added conservative TypeORM migration sibling detection for static `queryRunner.query(...)` SQL table names and static `queryRunner` table-method targets.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.3.0`.
- Refreshed Cargo lock metadata and updated the website dependency set from Astro `6.2.0` to `6.2.1`.

### Data-Shape Research

- Optional payload mismatch stays a static review signal. It adds `optional_payload_filter_mismatch` and `runtime_shape_probe` instead of claiming production data distribution.
- Planning packs now surface optional payload evidence on migration probe plans when an indexed payload contract marks the filtered field optional.
- TypeORM support indexes table siblings only. SQL WHERE-field extraction remains intentionally out of scope, so SQL migrations do not produce Mongo-specific field probe guidance.
- Generated probe plans remain static. Gather Step still does not connect to MongoDB or execute runtime probes.

### Verification Coverage

- Added store-backed planning oracle coverage for field evidence, optional payload contracts, and migration filters.
- Added parser coverage for alias/destructuring field evidence and TypeORM migration table detection.
- Added MCP coverage for optionality mismatch summaries, migration sibling probe plans, response-shape stability, and payload-contract lookup warnings.
- Verified format, clippy, cargo check, targeted parser/analysis/MCP tests, and website build during release preparation.

## v2.2.0 (2026-04-30)

Release status: **released**.

Data-shape awareness release for field-level impact review and Mongo/Mongoose migration planning.

### Highlights

- Added direct TypeScript field reader/writer evidence for typed member access, including nested dotted paths such as `WorkItem.workflow.stepIds`.
- Extended `projection-impact` and `projection_impact` so exact dotted field targets include direct readers, writers, filters, indexes, and backfills in one report.
- Let planning and change-impact packs surface field-impact reminders while preserving the existing context-pack follow-up budget.
- Added Mongoose migration sibling awareness so planning packs can show prior migrations on the same collection, including captured filter literals.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.2.0`.

### Data-Shape Awareness

- Direct field extraction is intentionally scoped to typed local receivers and parameters; dynamic keys, aliases, destructuring, broad `any`/`unknown`, generic containers, and deep optional chains remain unsupported.
- Migration detection is intentionally conservative: files must look like Mongoose-style migration files and expose `up`/`down` behavior before sibling hints are emitted.
- Deployment note: v2.2 changes generated graph/schema state for migration collection edges. Existing `.gather-step` storage should be rebuilt with `gather-step reindex` before relying on v2.2 migration-sibling output.

### Verification Coverage

- Added parser extraction-fidelity coverage for direct field readers/writers and false-positive skips.
- Added analysis and MCP coverage for direct field evidence in projection-impact reports.
- Added CLI routing coverage for dotted field targets through projection-impact.
- Added planning-pack and oracle coverage for migration siblings, pack response shape, and follow-up budget behavior.
- Verified format, clippy, all-features test build, targeted parser/analysis/MCP/CLI tests, and website build during release preparation.

## v2.1.1 (2026-04-30)

Release status: **released**.

Patch release for setup recovery and upgrade UX.

### Highlights

- Fixed `gather-step init --index` so setup-triggered indexing rebuilds generated index state from source repos instead of exiting when old `.gather-step/storage` state is stale or incompatible.
- Improved storage/schema operator messages with sentence-cased, actionable guidance. Graph schema mismatches now point to `gather-step index --auto-recover`.
- Changed recovery progress output to say `Rebuilding generated index state from source repos`.
- Clarified Homebrew upgrade docs to use `brew update` followed by `brew upgrade thedoublejay/tap/gather-step`.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.1.1`.

### Verification Coverage

- Added regression coverage for `init --index` auto-recovering stale generated state.
- Added unit coverage for graph schema mismatch operator guidance.
- Re-ran existing corrupt graph and unsupported metadata schema recovery tests.
- Verified manual smoke indexing with temporary generated state and direct graph/metadata/search store checks.

## v2.1.0 (2026-04-30)

This release polishes the v2 onboarding path, generated AI context, website build pipeline, and dependency graph, promotes Python to first-class parsing parity with TypeScript and JavaScript, and adds static projection-impact tracing.

### Highlights

- Made `gather-step init` the primary setup path in docs and landing copy, with a workspace directory diagram and explicit prompt defaults.
- Updated init output casing to "Gather Step" and made the local MCP default visible in the interactive prompt.
- Kept generated Claude workspace context factual by removing acknowledgement/sign-off instructions while preserving the MCP tool reference table.
- Updated the website workflow to Node 24 and refreshed GitHub Actions used by CI and website builds.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.1.0`.
- Refreshed Cargo dependencies with `cargo update`, including moving `gix` from the yanked `0.82.0` line to `0.83.0`.
- Promoted Python to first-class parsing alongside TypeScript and JavaScript (see [Language Support](/concepts/language-support/)).
- Added projection-impact tracing for derived fields, persisted projections, filters, indexes, and backfills.
- Marked v2.1 release readiness with a fresh release-build benchmark where release-scored Gather Step slices are all High/passing.

### Projection Impact

- Added the `projection-impact --target <FIELD>` CLI command and `projection_impact` MCP tool for static field-level projection tracing, including `evidence_verbosity` controls for summary versus full evidence.
- Added `DataField` graph nodes plus `ReadsField`, `WritesField`, `DerivesFieldFrom`, `FiltersOnField`, `IndexesField`, and `BackfillsField` edges.
- Planning and change-impact packs can now include short projection hints and `projection_impact:*` gap markers while the full evidence stays behind the dedicated projection tool.
- Added oracle, CLI/MCP serialization, integration, and parser extraction-fidelity coverage for projection chains, Mongo-style mappings, JSON/YAML index mappings, and false-positive fixtures.
- Deployment note: v2.1 projection impact changes the generated graph schema. Existing `.gather-step` storage should be rebuilt with `gather-step reindex` before relying on projection-impact output.
- Projection impact intentionally does not infer deployed runtime ownership; verify deployment owners separately when duplicate or transitioning services exist.

### Python Parsing

- Resolved Python `src/<package>/...` and flat `<package>/...` layouts so absolute current-package imports produce stable cross-file edges.
- Linked Python sibling packages across configured workspace repos using the `name` field from `gather-step.config.yaml`, falling back to `pyproject.toml [project].name` and finally the directory basename for standalone repos.
- Added detection-only FastAPI framework pack activation from Python dependency metadata.
- Qualified nested Python functions and methods, including methods inside nested classes, with full owner qualified names such as `Outer.Inner.method`, removing node-ID collisions for same-named helpers.
- Preserved Python class relationships (base classes, implemented interfaces, constructor dependencies) and decorator metadata across nested scopes.
- Added explicit diagnostics (`tracing::warn!`) when `gather-step.config.yaml` cannot be canonicalized or parsed, when configured repo paths fail to canonicalize, or when `read_dir` errors are encountered during sibling-package resolution. Prior behavior silently fell back to the directory-basename heuristic.

### Benchmarking And Tooling

- Added `gather-step-bench workspace-run` to measure wall-clock index time, graph node/edge counts, cross-repo edge count, RSS growth, and storage byte breakdowns (graph, metadata, search, sidecar) for a configured workspace.
- Added a neutral Python planning workspace fixture under `tests/fixtures/python_planning_workspace/` so the planning oracle and storage benchmark have a committed Python target.
- Documented the [private corpus benchmarking convention](/concepts/language-support/#private-corpus-convention) for measuring against repositories that cannot be checked in.
- Renamed `StorageMetrics::metadata_wal_bytes` to `metadata_sidecar_bytes` since the field actually sums the SQLite WAL and SHM files. Deserialization remains backward-compatible with the old bench JSON field name.
- Promoted `HarnessError::Workspace` from a stringified message to a typed `Box<WorkspaceIndexError<RepoIndexerError>>` so `anyhow::downcast` and structured logging can recover the source chain.
- Recorded the fresh 2026-04-30 release-build benchmark summary without checking in local benchmark artifacts.

### Verification Coverage

- Fresh release benchmark from a clean release build at `3f0093e`: curated index High, link quality 3/3 passing, planning oracle 25/25 passing, Python planning 1/1 passing, projection CLI fixture index High, and all projection targets release-scored High.
- Website build and Cloudflare Pages checks.
- Rust CI summary: format, clippy, cargo-deny, cargo-shear, macOS tests, MVCC stress, and MSRV check.
- Added regression tests for configured-repo identity resolution and malformed `gather-step.config.yaml` fallback.

## v2.0.0 (2026-04-28)

CLI onboarding, local MCP setup, release automation, and documentation refresh.

This release builds on `v1.0.0` by making the local-first workflow easier to start, easier to keep fresh, and easier to ship from a tagged release.

### Highlights

- Added a richer no-args startup path: interactive unconfigured workspaces enter setup, configured workspaces show status, and non-interactive shells print help without hanging.
- Completed the `init` wizard for repository discovery, config writing, optional indexing, AI context generation, MCP registration, and watch handoff.
- Added `setup-mcp` for idempotent workspace-local or global Claude settings updates.
- Added AI-facing context generation through graph-backed `.claude/rules/` plus `CLAUDE.gather.md` and `AGENTS.gather.md` summaries.
- Improved operator feedback for startup, `status`, `index`, `reindex`, `watch`, `clean`, and `serve`.
- Bumped the workspace and crate versions to `2.0.0`.
- Added release workflow automation to open Homebrew tap update pull requests.
- Refreshed the website landing page, feature copy, getting-started docs, installation docs, and CLI reference.

### CLI Startup And Status

- Added a no-args command path.
- In an interactive workspace without `gather-step.config.yaml`, no-args mode starts the guided init flow.
- In a configured workspace, no-args mode renders the status summary.
- In non-interactive contexts, no-args mode prints CLI help and returns without prompting.
- Refreshed the banner and startup UX.
- Expanded `status` output with clearer workspace, index, framework, MCP, and semantic-health signals.
- Improved progress reporting for `index`, `reindex`, `watch`, `clean`, and `serve`.

### Init Wizard

- Added end-to-end interactive setup through `gather-step init`.
- Added `init --force` for explicit config overwrite.
- Added `init --index` and `init --no-index`.
- Added `init --watch` and `init --no-watch`.
- Added `init --generate-ai-files` and `init --no-generate-ai-files`.
- Added `init --setup-mcp <local|global>`.
- Added a smooth handoff from setup/indexing into watch mode.
- Kept repository discovery scoped to the init flow and excluded generated or dependency-heavy directories.

### MCP And AI Files

- Added the `setup-mcp` command.
- `setup-mcp --scope local` writes `.claude/settings.json` under the workspace.
- `setup-mcp --scope global` writes `~/.claude/settings.json`.
- MCP settings are updated idempotently without removing unrelated server entries.
- Added `generate claude-md` / `--target rules` for graph-backed Claude Code project rules under `.claude/rules/`.
- Added `generate claude-md --target summary` for `CLAUDE.gather.md`.
- Added `generate agents-md` for Codex-style `AGENTS.gather.md` workflows.
- Reused the same workspace summary renderer in the init wizard and explicit generate commands.

### Release And CI

- Bumped the Cargo workspace, crates, fixture packages, and website package metadata to `2.0.0`.
- Updated the release workflow to open Homebrew tap formula update pull requests after release artifacts are built.
- Kept macOS release artifact smoke tests for `--version`, `--help`, and index/status against an embedded fixture.
- Updated pinned GitHub Actions versions for CI and release support.
- Removed the unused `sharp` website dependency.

### Documentation And Website

- Refreshed the CLI command reference for the current command surface.
- Updated getting-started guidance around the single-command setup path.
- Updated workspace setup docs for init wizard flags and watch handoff.
- Updated MCP client docs with the `setup-mcp` fast path.
- Updated operator workflow docs to describe the committed release-gate baseline.
- Updated landing page feature copy, release stamps, onboarding flow, and install command behavior.
- Made the landing install command copyable with click feedback.

### Verification Coverage

- Added CLI coverage for no-args behavior.
- Added CLI coverage for init behavior, the full wizard path, setup-mcp, and index/watch parsing.
- Added workspace summary output coverage.
- Release preparation test plan includes `cargo test -p gather-step-cli`, `cargo test -p gather-step-output`, `cargo test --workspace`, and `cd website && bun run build`.

## See Also

Binary releases are published at [https://github.com/thedoublejay/gather-step/releases](https://github.com/thedoublejay/gather-step/releases). Each release notes entry describes the user-visible changes for that version.
