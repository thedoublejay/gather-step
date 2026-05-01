---
title: "Changelog"
description: "User-visible changes to gather-step, listed by release. Updated manually until a release pipeline is wired up."
---

This changelog lists significant user-visible changes. It is maintained manually until release notes and tagged releases become the automated source of truth.

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
