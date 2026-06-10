---
title: "Changelog"
description: "User-visible changes to gather-step, listed by release. Updated manually until a release pipeline is wired up."
---

This changelog lists significant user-visible changes. The latest release is shown in full at the top; earlier releases are collapsed under [Earlier releases](#earlier-releases) at the bottom of the page.

## v4.4.4 (2026-06-10)

Release status: **prepared**.

Patch on top of v4.4.3. Four small, independent index- and read-path wins from the audit's performance phase — none of them change what any command returns.

### Changed

- **Conditional metadata `VACUUM`** — every index run (including the one inside `pr-review`) used to finish with an unconditional `VACUUM`: an O(database size) full-file rewrite even when a warm run changed almost nothing. The post-index finalize now checkpoints the WAL as before but vacuums only when the freelist holds ≥ 256 freed pages (~1 MiB). `gather-step compact` still vacuums unconditionally.
- **Read-only freshness probe** — the per-command freshness check opened a full metadata store (writer + 4-reader pool, schema bootstrap, `PRAGMA optimize` on drop) for a read-only peek. It now uses a single `SQLITE_OPEN_READ_ONLY` connection that cannot write to the database or its WAL, falling back to the full open only when read-only access is impossible.
- **Changed files read once, not twice** — on the incremental path a changed file was read once to hash (bytes discarded) and again by the parser. The fd-stable hash read now hands its bytes to the parser, halving I/O on changed files and guaranteeing the parser sees exactly the bytes that were hashed.
- **No full-buffer copy in the watch traversal** — the watch path cloned each file's entire contents before hashing; it now hashes first and moves the buffer, matching its parallel-walk twin.

### Fixed

- **Unreadable mtimes no longer fake freshness** — an mtime read error was recorded as `0`, so a file whose mtime errored at index time and again at check time could be skipped as "unchanged" despite a same-size content edit. Unknown mtimes are now a sentinel that never matches anything — including another unknown — forcing a rehash. All warm-skip speed is kept; the silent-staleness hole is closed.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.4.4`.

## v4.4.3 (2026-06-10)

Release status: **prepared**.

Patch on top of v4.4.2. Fixes the review-artifact resource leak where any `pr-review` run killed before finalizing (Ctrl-C, OOM, the MCP tool's own timeout kill) left a permanent `InProgress` artifact — hundreds of MB of redb/tantivy/SQLite state plus dangling git worktrees — that `clean --older-than` and post-reindex cleanup were forbidden to touch.

### Fixed

- **Writer PID in the review marker** — `review-marker.json` now records the PID of the process that created the artifact. Cleanup distinguishes a live `InProgress` run (still protected) from one whose process is dead (treated like a failed run): `pr-review clean --older-than` and the post-reindex artifact wipe now prune orphaned `InProgress` artifacts instead of protecting them forever. Markers written by older versions carry no PID and keep the always-protected behavior; PID reuse only delays cleanup, never deletes a live run.
- **Graceful-first MCP timeout** — the MCP `pr_review` tool's timeout no longer SIGKILLs its subprocess as the first resort. It sends SIGTERM, waits a 5-second grace window, then falls back to SIGKILL — and a killed run's orphaned artifact is now reapable via the marker PID.
- **Zombie reap on wait failure** — a `try_wait` error in the MCP subprocess supervisor now kills and reaps the child best-effort instead of returning with the process unreaped.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.4.3`.

## v4.4.2 (2026-06-10)

Release status: **prepared**.

Patch on top of v4.4.1. Hardens `pr-review` against silently comparing PRs to stale state — the class of failure where a behind-upstream local base branch or an outdated workspace index produces a clean-looking but wrong delta report.

### Fixed

- **Upstream-divergence warning for `--base`** — when `--base` is a branch name, `pr-review` now compares it against its configured upstream tracking ref (e.g. `origin/main`). If the local branch has diverged, the report warns with both SHAs instead of silently computing a merge-base that attributes unrelated upstream commits to the PR.
- **Baseline index freshness gate** — the baseline side of every delta comes from the persistent workspace index, but its indexed commit was never checked against `--base`. Each registered repo's last-indexed commit is now compared with the SHA the review treats as base; the report warns when the index is stale or has no recorded commit, with the exact `gather-step index` remediation.
- **Dirty-worktree warning** — uncommitted tracked changes in the baseline working tree contaminate the baseline index and can invert deltas; the report now says so explicitly.

All three checks honor `--no-baseline-check` and surface in the report's `metadata.warnings`, which renders before everything else.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.4.2`.

## v4.4.1 (2026-06-10)

Release status: **prepared**.

Patch on top of v4.4.0. Exposes the cross-repo dependency analysis as a first-class CLI subcommand (closing an MCP/CLI surface gap that broke agent wrappers shelling out with the MCP tool name), hardens the daemon proxy against old-daemon/new-CLI version skew, and refreshes Cargo, GitHub Actions, and website dependencies.

### Added

- **`cross-repo-deps` CLI subcommand** — exposes the `cross_repo_deps` analysis (previously MCP-only) on the CLI, with the MCP tool name `cross_repo_deps` kept as a visible alias so wrappers written against the MCP surface invoke it verbatim. Defaults to every configured repo; a positional repo or the global `--repo` flag narrows it, and an unknown repo fails loudly listing the configured repos. Daemon-proxied like the other read commands, and the JSON payload reuses the MCP tool response shape (`dependencies`, `evidence`, per-repo).

### Fixed

- **Daemon version-skew fallback** — a daemon built before a request variant existed answers with a protocol-level `invalid daemon request` failure; the proxy previously emitted that as the command result. It now treats the rejection as "daemon unavailable for this request" and falls back to local execution, preserving the canonical lock-contention error when the older daemon holds the graph lock.

### Changed

- Bumped Cargo dependencies: `chrono 0.4.44 → 0.4.45`, `ignore 0.4.25 → 0.4.26`, `regex 1.12.3 → 1.12.4`, `quick_cache 0.6.22 → 0.6.23`, `ratatui 0.30.0 → 0.30.1`, plus a transitive lockfile refresh.
- Bumped GitHub Actions: `actions/checkout v6.0.2 → v6.0.3`, `crate-ci/typos v1.47.0 → v1.47.2`, `taiki-e/install-action v2.81.2 → v2.81.9` (tag and SHA pins).
- Bumped website dependencies: `@astrojs/starlight 0.39.3 → 0.40.0`, `astro 6.4.4 → 6.4.5`, and the pinned Bun `1.3.12 → 1.3.14`.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.4.1`.

## v4.4.0 (2026-06-05)

Release status: **prepared**.

Indexing-performance release. The warm workspace streaming path now skips parser work for unchanged sources before the storage write layer, parses only changed files plus their reverse dependents, carries deletions through prepared-payload commits so removed files are purged on incremental passes, skips opening unsupported or excluded files during traversal, and bounds the indexing analytics channel by repo count. Index JSON gains per-repo parse counts and pack-target counts, and the release adds internal indexing-profile and graph-verifier harnesses.

### Added

- **Per-repo `files_parsed` and pack-target counts in `index --json`** — each `repos[]` entry reports `files_parsed` (files actually parsed this run) alongside `files` (total indexed), so a warm no-op shows `files_parsed: 0` against the full `files` count. The `timings` object gains `precompute_pack_count`, `hot_pack_target_count`, and `static_pack_target_count`.

### Changed

- **Warm prepared-payload parse skip** — on the workspace streaming path (`prepare_repo_payload` + `commit_repo_payload`), unchanged repos and files are no longer parsed before the storage layer can skip them. Warm passes parse only changed files plus their reverse dependents; cold indexing keeps the full traversal path so it does not regress into a hash-then-parse double read.
- **Classify-before-read traversal** — binary, unsupported, and language-excluded files are now classified before being opened and read, avoiding wasted file opens during traversal.
- **Bounded indexing analytics channel** — the per-repo analytics result channel is now bounded by repo count instead of unbounded, capping peak memory without serializing the pipeline.

### Fixed

- Streaming commits now carry deleted paths through `RepoIndexPayload`, so stale graph, file-index, and search state for removed files is purged on warm incremental passes rather than lingering until a full reindex.
- `collect_selected_repo_files` now counts an include-language mismatch as `skipped_excluded` instead of `skipped_unsupported`, matching the full-traversal walk.

### Tooling

- Added internal `gather-step-bench` harnesses: `profile-index` collects production `index --json` artifacts across cold/warm/change passes, and `verify-graph` checks an indexed fixture against an expected node/edge-kind set (with seeded-failure support for the verifier's own tests).

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.4.0`.

## v4.3.0 (2026-06-04)

Release status: **prepared**.

Planning-, reuse-, and polyrepo-quality release. Fixes retrieval recall so reuse search stops returning empty, ranks reuse candidates using the graph, ships a typed `plan_change` product with a stable section contract, wires polyrepo `pr-review` ref resolution and synthetic worktree indexing into the command path, adds lock-contention disclosure and cleanup hardening, fixes large-workspace indexing hot paths, and refreshes Cargo and website dependencies.

### Added

- **Polyrepo `pr-review` ref resolution** — `--base`/`--head` resolve independently per configured repo (the same ref names in each repo's own history) and changed files are tagged by their owning repo. Repos whose refs do not resolve, or that have no changes in range, are skipped with a recorded note.
- **Multi-repo review worktree** — `pr-review` checks out each changed repo at its head into one synthesized worktree, then indexes it as a single workspace.
- **Stale-index warnings on `context_pack`** — generic `context_pack` queries surface a stale-index warning in `meta.warnings` when the index lags the current git HEAD, matching the existing `plan_change` behaviour.
- **`gather-step doctor` code-quality advisories** — non-gating findings over the indexed graph: dependency cycles (incl. cross-repo, via Tarjan SCC), mock/fixture imports leaking into production modules, and local forks of shared/design-system components that should be reused.
- **Graph-ranked reuse evidence** in planning packs: reuse candidates are ranked by sibling-consumer count, shared/design-system membership, and cross-repo proof strength before truncation, so a blessed shared component ranks above a bespoke fork.
- **Typed `plan_change` product** with a fixed, contract-checked section set. Sections are always present (possibly empty), with an exclusion ledger recording what was dropped so a capped result is never read as exhaustive.
- **Display-ownership planning dimension** (`display_ownership_checks`): every cross-service reference surfaces the question of whether display fields come from the owner service (snapshot/API) rather than a direct cross-service DB lookup.
- **Mongo/Atlas structural safety detectors** with stable rule IDs and confidence: `$lookup` join-key coercion that defeats an index (`GS-MONGO-INDEX-DEFEAT`), bare `$toObjectId` on untrusted input (`GS-MONGO-UNSAFE-COERCION`), unguarded dotted-path `$set` (`GS-MONGO-NULL-PARENT-PATH`), and `dynamic:false` Atlas index↔doc-field drift (`GS-MONGO-ATLAS-INDEX-DRIFT`).
- **Query-time index freshness** (`fresh`/`stale`/`never_indexed`) is now classified against the working tree's HEAD and surfaced per repo in `gather-step status`.
- **Multi-path traversal provenance**: graph traversals now report every distinct path into a node plus `depth_capped`/`truncated` signals when a walk is cut short by depth or fan-out bounds.

### Changed

- **`DeltaReport.schema_version` is now `2`** — the PR-review report gains `changed_files_by_repo`, grouping changed files by their owning repo (paths matching no repo are grouped under `<workspace>`).
- **Multi-word search recall**: a conjunctive query that returns nothing now falls back to a disjunction with a min-should-match floor, so a capability query sharing most of its terms still finds the target symbol. Hits are re-ranked by query-term coverage and expanded through a curated synonym map.
- **Unified `min_confidence` edge filter** across trace/impact/pack traversal, with `None`-confidence edges treated as trusted.
- The `plan_change` contract gate is now **evidentiary** — it asserts schema version, the exact section manifest, and the exclusion ledger, not just section presence.

### Fixed

- `batch_query` now routes `plan_change` requests to the typed product instead of the raw planning pack.
- Read commands no longer silently return empty-but-successful results when the graph store is held by an in-progress index or watch: they use the workspace daemon first, retry through the daemon named by lock metadata if a local open races the holder, and otherwise exit with a distinct, documented code and (under `--json`) a `degraded: graph_locked` disclosure, so a blocked read can never be mistaken for "found nothing".
- Daemon, `watch`, and `serve --watch` shutdown paths now wait for active request/index tasks to release graph handles before removing daemon pid/socket metadata. This prevents stale metadata cleanup from orphaning a still-running graph owner after cancellation, idle clients, accept errors, or shutdown timeouts.
- Review indexing now validates the effective reviewed config's repo roots before indexing, matching the normal `index`/`watch`/`serve` containment checks for missing paths, symlinked repo roots, and paths outside the config root.
- Query-time freshness no longer collapses metadata read failures into `never_indexed`: unreadable metadata is reported as `unknown` for registered repos and logged, while genuinely absent stores remain omitted.
- `pr-review` baseline-check resolver failures are now surfaced in report warnings instead of being debug-only, so the default baseline guard cannot silently become a no-op.
- Polyrepo review cleanup now fails visibly if rollback cannot remove a previously-created child worktree, and `changed_files_by_repo` is derived from the full changed-file set even when the top-level display list is capped.
- Large-workspace indexing no longer replays expensive commit fact extraction before falling back from a stale git-history anchor, and shared-lib barrel resolution no longer recursively runs every framework augmentation while chasing local re-exports.

### Dependencies

- Refreshed the Cargo lockfile to the latest Rust 1.96-compatible versions, including `bitflags 2.11.1 → 2.12.1`, `cc 1.2.62 → 1.2.63`, `inotify 0.11.1 → 0.11.2`, `kqueue 1.1.1 → 1.2.0`, `log 0.4.30 → 0.4.32`, `shlex 1.3.0 → 2.0.1`, `uuid 1.23.1 → 1.23.2`, and `zerocopy 0.8.49 → 0.8.50`.
- Bumped website dependencies: `astro 6.4.2 → 6.4.4` and `@astrojs/starlight 0.39.2 → 0.39.3`, with the Bun lockfile refreshed.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.3.0`.

## v4.2.1 (2026-06-02)

Release status: **prepared**.

Patch on top of v4.2.0. Raises the Rust toolchain to 1.96.0, refreshes the remaining Cargo and GitHub Actions dependencies (including dependencies that the toolchain bump unblocks), and clears a new compiler lint. No user-visible behavior changes.

### Changed

- Raised the Rust toolchain and MSRV `1.94.1 → 1.96.0` (`rust-toolchain.toml`, `rust-version`, and CI/release workflow toolchains).
- Bumped `rusqlite 0.39.0 → 0.40.0`, unblocked by the toolchain bump — `libsqlite3-sys 0.38` requires the `cfg_select!` macro stabilized in Rust 1.95.
- Bumped the exact-pinned Oxc parser stack `0.132.0 → 0.134.0`.
- Bumped further Cargo dependencies: `tree-sitter 0.26.8 → 0.26.9`, `hashbrown 0.17.0 → 0.17.1`, `gix 0.83 → 0.84`, `mimalloc 0.1.50 → 0.1.52`, plus a transitive lockfile refresh.
- Bumped GitHub Actions: `crate-ci/typos v1.45.2 → v1.47.0` and `taiki-e/install-action v2.75.25 → v2.81.2`.

### Fixed

- Replaced a manual `Option::zip` in event-topology scoring to satisfy the new `clippy::manual_option_zip` lint in Rust 1.96. No behavior change.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.2.1`.

## v4.2.0 (2026-05-29)

Release status: **released**.

Minor release on top of v4.1.1. Fixes `setup-mcp` so it writes to the files MCP clients actually read, adds Codex support, and refreshes dependencies. Rolls up the unreleased v4.0.6 and v4.1.1 changes.

### Fixed

- `setup-mcp --scope local` now writes the project-scoped `.mcp.json`, and `--scope global` writes the user-scoped `~/.claude.json`. Previously it wrote to `.claude/settings.json`, which Claude Code does not read for server definitions, so the registered server never appeared in the client.

### Added

- `setup-mcp --client codex` merges a `[mcp_servers.gather-step]` block into `~/.codex/config.toml`, preserving existing servers, other keys, and comments. The default client remains `claude`.

### Docs

- Corrected the MCP clients guide: the Claude user-scoped config path is `~/.claude.json` (not `~/.claude/settings.json`), and the Fast Path section now reflects the `.mcp.json` / `~/.claude.json` / Codex targets.

### Changed

- Refreshed Cargo dependencies to the latest SemVer-compatible versions, including `serde_json 1.0.149 → 1.0.150`, `tokio 1.52.2 → 1.52.3`, `rmcp 1.5.0 → 1.7.0`, `similar 3.1.0 → 3.1.1`, `quick_cache 0.6.21 → 0.6.22`, and `memchr 2.8.0 → 2.8.1`, plus transitive lockfile updates. Intentionally exact-pinned dependencies were left untouched.
- Bumped the website `astro 6.3.5 → 6.4.2` floor and refreshed the website lockfile.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `4.2.0`.

## v4.0.6 (2026-05-20)

Release status: **prepared**.

Patch on top of v4.0.5. Maintenance release that refreshes Cargo and website dependencies, including the exact-pinned Oxc parser stack. No user-visible behavior changes.

### Changed

- Bumped the exact-pinned Oxc parser stack from `0.130.0` to `0.132.0`.
- Refreshed `Cargo.lock` to the latest SemVer-compatible versions for 12 transitive and direct dependencies, including `dashmap 6.1.0 → 6.2.1`, `rmcp 1.6.0 → 1.7.0`, `winnow 1.0.2 → 1.0.3`, `typetag 0.2.21 → 0.2.22`, and `sqlite-wasm-rs 0.5.3 → 0.5.4`.
- Refreshed the website lockfile and pinned floors: `astro 6.3.1 → 6.3.5` and `@astrojs/starlight 0.39.1 → 0.39.2`.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.6`.

## v4.1.1 (2026-05-20)

Release status: **prepared**.

Minor release on top of v4.0.6. Adds coordinated multi-PR review support so related PRs, stacks, and cross-repo feature sets can be reviewed together instead of one branch at a time.

Version numbering note: previously drafted as v4.1.0; renumbered to v4.1.1 because the v4.0.6 dependency-refresh patch shipped first.

### Added

- `gather-step pr-review --pr-set <PATH>` runs a coordinated review from a manifest that lists each PR's repo, base, head, PR number, and dependencies.
- `gather-step pr-review init-set --query <QUERY>` generates a draft PR-set manifest from GitHub search results, and `gather-step pr-review --from-gh <QUERY>` resolves and runs that set in one command.
- PR-set reviews return a `MultiPrDeltaReport` with per-PR `DeltaReport` results, failed/skipped entries, dependency-aware execution status, and cross-PR payload-contract drift.
- `pr_review_set` is now available through MCP for assistant-driven review of related PR sets.

### Changed

- `pr-review` can use a parent workspace `gather-step.config.yaml` while reviewing a child repo. The matching repo entry is rewritten to `path: "."` inside the temporary worktree, so the child repo no longer needs a duplicate committed config.
- Review-set execution supports `--parallelism`, `--set-id`, `--allow-unknown-repos`, `--config`, `--cache-root`, `--keep-cache`, `--severity`, and `--no-baseline-check` at the CLI surface.
- MCP `pr_review` and `pr_review_set` now expose the same config, cache-root, cache-retention, severity, baseline-check, timeout, set-id, parallelism, and GitHub-query controls that automation users need to discover from the tool schema.
- `--cache-root` is now a visible CLI option for `pr-review`, instead of a hidden automation-only flag.

### Docs

- Added PR-set examples for cross-repo sets, stacked PRs in one repo, and divergent-base sets.
- Expanded the PR Review guide, CLI reference, and MCP tools reference with PR-set manifests, GitHub query resolution, child-repo parent-config usage, and MCP input fields.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.1.1`.

## v4.0.5 (2026-05-13)

Release status: **released**.

Patch on top of v4.0.4. Fixes Web PubSub producer extraction so object-form group sends that identify the event under `payload.eventType` connect to event consumers in topology and trace output.

### Fixed

- `pubSubService.sendToGroup({ payload: { eventType: PubSubEventType.X } })` now emits a Web PubSub producer edge for the resolved event `X`.
- Mixed-form `sendToGroup('admins', { eventType: 'notification.created' })` calls now resolve the payload event type instead of treating the group name as the event.
- Object-form `sendToGroup` calls no longer treat unrelated literal metadata, such as `group: 'admins'`, as the event name when no resolvable `payload.eventType` is available.

### Changed

- Bumped the exact-pinned Oxc parser stack from `0.129.0` to `0.130.0`.
- Cargo dependency status was refreshed for the release; `cargo outdated -wR` reports the workspace dependencies are current.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.5`.

## v4.0.4 (2026-05-08)

Release status: **released**.

Patch on top of v4.0.3. Fixes generated and documented MCP setup so clients use the installed `gather-step` command from `PATH` with the public top-level `serve` command.

### Fixed

- `gather-step setup-mcp --scope local` now writes Claude settings with `command: "gather-step"` instead of pinning MCP startup to the absolute path of the current executable.
- Generated MCP args now use `["--workspace", "...", "serve"]`, matching the public CLI surface instead of the hidden `mcp serve` compatibility alias.
- MCP client documentation for Claude Code, Codex CLI, Cursor, and generic stdio MCP clients now shows the same `PATH`-based command shape. The Codex section also calls out that the session must be restarted before `mcp__gather_step` tools appear.
- Added regression coverage for both direct setup command output and the lower-level settings writer so stale `mcp serve` expectations fail in CI.

### Changed

- Refreshed resolvable Cargo lockfile dependencies in the `wasm-bindgen` stack.
- Bumped the website stack to `astro` `^6.3.1` and `@astrojs/starlight` `^0.39.1`.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.4`.

## v4.0.3 (2026-05-07)

Release status: **released**.

Patch on top of v4.0.2. Replaces blanket deployment-artifact warning demotion with structured skip classification, so expected template and dotenv cases stay quiet while real malformed YAML remains visible.

### Fixed

- Templated YAML deployment artifacts using `{{ ... }}` or `{% ... %}` are classified as expected skips when strict YAML parsing fails. These files now emit debug detail plus an aggregate skip counter instead of per-file warning noise.
- Missing Compose `env_file` references are split by convention: `.env` / `.env.*` paths are treated as expected gitignored dotenv skips, while custom names like `prod.env` still warn.
- Deployment indexing now reports a compact aggregate skip summary with counters for templated YAML, missing dotenv files, oversized env files, malformed artifacts, and non-YAML `.github/workflows` siblings.
- Real malformed YAML remains a warning. The parser no longer hides actionable deployment config problems behind a blanket debug demotion.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.3`.

## v4.0.2 (2026-05-07)

Release status: **released**.

Patch on top of v4.0.1. Quiets noisy deployment-artifact parse warnings that fired on every workspace index against external repos.

### Fixed

- `.github/workflows/` is only classified as a GitHub Actions artifact when the file extension is `yaml` or `yml`. `CODEOWNERS`, release `README.md`, and other Markdown docs that happen to live next to workflow files are now skipped instead of being force-fed to a YAML parser.
- `Skipping a malformed deployment artifact during indexing.` is now logged at `debug` rather than `warn`. Helm and Argo CD `{{ ... }}` templates legitimately fail strict YAML parsing, so this is a routine best-effort skip rather than something the user can act on.
- `skipping missing compose env_file` is now logged at `debug` rather than `warn`. `.env` files are routinely gitignored, so the warning fired on every clean checkout for a non-issue.

### Release-wide

- Added a `renovate.json` so dependency updates are proposed on a weekly schedule. Internal `gather-step-*` path deps are excluded; the `tree-sitter` ecosystem and the storage stack (`redb`, `tantivy`, `rusqlite`) are grouped for coherent review.
- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.2`.

## v4.0.1 (2026-05-07)

Release status: **released**.

Patch on top of v4.0.0. Fixes deployment topology evidence for the workspace streaming index path, so GitOps kustomize/compose/workflow artifacts are written when repos are indexed through the CLI workspace pipeline.

### Fixed

- Streaming payload commits now run the same deployment-artifact indexing pass as direct repo indexing. This restores `Service -> Deployment` evidence for `deployment-topology where-deployed` after a normal workspace index.
- Added a regression test that prepares and commits a streaming payload containing an enterprise-like kustomize service and asserts the expected service and deployment nodes are present.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.1`.

## v4.0.0 (2026-05-06)

Release status: **released**.

Builds on v3.5.4 with the v4 QA planning evidence contract. Gather Step now emits factual, canonical code evidence for downstream QA planning while leaving requirement interpretation and test-case generation outside the CLI.

### Added

- New `gather-step qa-evidence` command emits `qa-evidence.v1` JSON with stable evidence IDs, closed evidence kinds/sources, structured citations, manifest summary data, and explicit coverage gaps.
- Canonical evidence metadata is shared across planning/review/change-impact packs, route/event traces, CRUD traces, cross-repo dependency impact, payload schema fields, projection impact, orphan-topic checks, and PR-review delta reports.
- The v4 QA reference fixture covers route evidence, changed UI/API/event surfaces, existing-test signals, dynamic feature-flag gaps, scan truncation gaps, and deterministic CLI evidence IDs.
- Generated AI summary files now include `qa-evidence` in the CLI command catalog, so `CLAUDE.gather.md` and `AGENTS.gather.md` stay in sync with the visible CLI surface.

### Changed

- Public JSON contract baselines are reset to version `1` while there are no known external consumers: MCP `response_schema_version: 1`, PR-review `DeltaReport.schema_version: 1`, and `qa-evidence.v1`.
- Generated search/review cache compatibility is flattened: stale generated state should be rebuilt or cleaned instead of migrated.
- `gather-step generate claude-md --target=rules` now writes graph-backed reference data to `.agent-context/gather-step/{architecture,events,routes,repo-NAME}.md` instead of `.claude/rules/`. Claude Code and Codex pick the data up on demand through an installed skill (`.claude/skills/gather-step-context/SKILL.md`, `.agents/skills/gather-step-context/SKILL.md`) plus a tiny `.claude/rules/gather-step-index.md` pointer, so the ~48 KB architecture file is no longer eagerly loaded into every session. Skill files are skip-if-exists so user edits to skill prose are preserved across re-runs; the data files are always overwritten. Workspaces upgrading from v3 should delete the old `.claude/rules/gather-step-architecture.md`, `gather-step-events.md`, `gather-step-routes.md`, and `gather-step-repo-*.md` files after re-running `gather-step generate claude-md`.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, landing-page release stamps, and website package metadata to `4.0.0`.

## v3.5.4 (2026-05-06)

Release status: **released**.

Patch on top of v3.5.3. Fixes the AI-docs reach problem reported on a 32-repo monorepo: the architecture rule was running out of byte budget mid-table, the master `CLAUDE.gather.md` / `AGENTS.gather.md` files were never picked up by Claude Code or Codex, and the rendered MCP-tool table had drifted out of sync with the live MCP server.

### Fixes

#### Architecture rule fits large workspaces

- `gather-step-architecture.md` now scales its byte budget with workspace size: `architecture_budget(N) = min(24_000 + N * 1_500, 96_000)`. A 32-repo workspace gets ~72 KB instead of the old 16 KB hard cap.
- `## Cross-Repo Dependencies` table compresses to **one row per source repo**, with comma-separated `target (Edge1, Edge2)` entries. Drops dependency rows from O(n²) to O(n) and keeps the repo map fully visible above it.
- Regression test exercises a 32-repo fixture and asserts every repo appears in the rendered map, no truncation marker, output fits within the scaled budget.

#### Master Claude / Codex summaries actually load

- `gather-step init` (and `gather-step generate claude-md --target=summary --install-include` / `gather-step generate agents-md --install-include`) appends a sentinel-fenced managed block to `CLAUDE.md` and `AGENTS.md` at the workspace root. The block reads `@CLAUDE.gather.md` / `@AGENTS.gather.md` so the generated context is auto-loaded by Claude Code and Codex without any manual edit.
- The managed block is bounded by `<!-- gather-step:start -->` / `<!-- gather-step:end -->` so re-runs are idempotent and never disturb user-authored content above or below the fence.
- `--install-include` is guarded so it only runs with the default root summary sidecar. `claude-md --target=rules --install-include` and `--install-include --output <custom-file>` now fail fast instead of silently writing a main-file include that cannot load the generated summary.
- Related error and warning output now uses consistent `Warning:` / `The ... flag ...` grammar for the include flow and destructive-clean confirmation.

#### Restored "use it / cite it / report it" guidance

- `CLAUDE.gather.md` and `AGENTS.gather.md` now carry the `## How to Use Gather Step in Planning` and `## How to Acknowledge Gather Step` sections that were dropped in v3.4. Both files instruct AI tools to reach for `planning_pack`, `cross_repo_deps`, `trace_event`, `trace_route`, and `pr_review` before grep, cite verified findings with file paths, and offer to open <https://github.com/thedoublejay/gather-step/issues> when an indexing result looks wrong.

#### CLI + MCP surface always in sync

- `crates/gather-step-mcp/src/catalog.rs` exports `MCP_TOOLS` as the canonical `(name, description)` table the renderer reads from. A new test (`mcp_tools_catalog_matches_registered_mcp_tools`) compares the catalog against `GatherStepMcpServer::registered_tool_names()` so any new tool added to the server fails CI until the catalog reflects it.
- A matching `CLI_COMMANDS` catalog in `crates/gather-step-cli/src/commands/mod.rs` populates the new `## CLI Commands` section, so the master summary lists every user-visible subcommand (including `pr-review`, `projection-impact`, `deployment-topology`, `pack`, `events`, `conventions`). A unit test compares the catalog to Clap's visible subcommands to catch drift.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `3.5.4`.

## v3.5.3 (2026-05-06)

Release status: **released**.

Patch on top of v3.5.2. Fixes JSON watch-mode automation by adding an explicit readiness event after filesystem watchers are registered, so scripts can wait before touching files and avoid racing startup.

### Fixes

#### CLI / watch mode

- `gather-step watch --json` now emits `{"event":"watch_ready", ...}` after all configured repo watchers are installed.
- `gather-step serve --watch` now emits a matching `watch:ready repos=N` line for embedded watcher sessions.
- Count-limited watch flows (`watch 1`, `watch N`) can now be scripted reliably: wait for `watch_ready`, mutate files, then expect `watch_indexing_complete` and final `watch_status`.
- Added an integration regression that waits for the ready event, edits a fixture file, and verifies the process exits after one indexing run.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, website package metadata, and landing-page version stamps to `3.5.3`.

## v3.5.2 (2026-05-06)

Release status: **released**.

Patch on top of v3.5.1. Fixes the contrast regression introduced when v3.5.1 swapped `.dim()` for `color256(245)` (#8a8a8a — medium gray that disappears on both dark and light terminals), and the docs table layout where wide tables didn't fill the content column and long-cell phrases overflowed.

### Fixes

#### CLI / init UX contrast

- `init` welcome banner, `Workspace:` line, `Existing config:` path, "Found N Git repositories" label, and the `Wrote config <path>` confirmation now print at the terminal's default foreground color instead of `color256(245)`. The path and repo count are visible on every standard light and dark terminal, not just terminals with a specific palette.
- `index` summary numbers (files, symbols, edges, cross-repo, time, index size) now use cyan-bold for the value and default foreground for labels, matching the `✓ Indexed` header. The storage path on the same line is plain default foreground.
- `gather-step --version` banner footer (`v3.5.x · https://gatherstep.dev/`) switched from `color256(245)` to plain cyan so the version stamp and link are readable.
- Repo picker's secondary help text (`↑/↓ move  Space toggle ...`, `Use numbers or ranges to toggle ...`) and `watch` cause/file-count detail text moved to `.dim()` (SGR 2, terminal-relative) so they stay visibly subdued without disappearing.
- `search` per-row `qn` qualified-name annotation moved to `.dim()` for the same reason.

#### Docs / website

- Markdown tables now fill the content column width by default. Previously the `display: block` rule sized them to content and `white-space: nowrap` on non-last cells forced narrow columns to overflow.
- Long phrases in cells (e.g. "required — no path separators", "array of glob strings") wrap inside the cell instead of widening the column or pushing other content out of the row.
- Tighter cell padding and top vertical-alignment so multi-line cells in the workspace-setup config reference table read as a clean grid.
- Inline `<code>` inside cells stays on one line, so identifiers like `indexing.workspace_concurrency` are not broken across lines.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `3.5.2`.

## v3.5.1 (2026-05-06)

Release status: **released**.

Polish pass on top of v3.5.0. No new functionality, no schema changes, no breaking changes. Tightens the init experience that operators see first, scrubs noisy mid-stream warnings, repaints landing-page sections that didn't fill their grid cleanly, restructures the docs so the AI-assistant-driven workflow is first-class, and reshoots the planning benchmark with measured numbers from a real 31-repo workspace.

### Improvements

#### CLI / init UX

- `gather-step --version --long` restores the `Copyright (c) 2026 JJ Adonis` line that earlier releases shipped (clap `long_version`).
- Dropped the RFC 3339 timestamp prefix from interactive log lines so warnings align with the rest of the CLI output. `--json` mode keeps the timer.
- Indexing label trimmed to the repo name only — the workspace root is already shown at the top.
- Detail-text contrast lifted: every `.dim()` call site at the CLI surface (index, init, watch, status, search, storage_report, app footer) moved to `.color256(245)`. Detail text now reads on dark and light terminals.
- Two noisy `tracing::warn` lines demoted to `debug`:
  - NestJS `MessagePattern` skip warnings (fired per-handler when the topic is a constant).
  - `list_orphan_topics` truncation warnings (page truncation is the documented behaviour).

  Operators who need either signal can re-enable via `RUST_LOG=gather_step_parser=debug` / `RUST_LOG=gather_step_analysis=debug`.

#### Landing page

- "What it does": 4 → 3 pillars. The version-tagged "faster indexing" pillar was dropped.
- "What actually makes it different": 7 → 9 features. New cards: Performance, Local-first.
- "From zero to answered": 4 → 6 steps. Added INDEX and PACK between INIT/WATCH and ASK/REVIEW.
- Hero workspace counts updated to v3.5.x measured numbers: 31 repos / 14,296 files / 216,663 symbols / 484,379 edges / 96,787 cross-repo.
- Planning benchmark refreshed end-to-end. Stale "9× faster" hand-curated numbers replaced with measured wall-clock medians on a real 31-repo workspace: `useAuth` 0.79 s → 0.03 s (26×), `CommentCreatedEvent` 1.44 s → 0.03 s (48×), `CreateTaskUseCase` 0.32 s → 0.03 s (11×). Total 28× faster, with explicit methodology in the chart caption.
- Planning oracle panel surfaces the v3.5 25 / 25 PASS, coverage 1.000, p50 3 ms / p95 8 ms / p99 15 ms.
- Every external GitHub link now opens in a new tab with `rel="noopener noreferrer"` and an ARIA label for a11y / SEO.

#### Docs

- Getting Started gained a "How most people use Gather Step" quote block making the AI-assistant-driven workflow first-class. Next Steps promote the CLI reference.
- CLI reference gained the same quote block at the top so users who land directly know they don't need to memorise commands.
- Workspace setup gained an `init --force` subsection plus a richer interactive picker walkthrough showing the keybindings, sample output, and one-to-one mapping between checkbox state and `repos[]` entries.
- Memory-Backed Planning's [Braingent](https://braingent.dev) reference refreshed to mention `braingent_find` / `braingent_get` / `braingent_guide`, capture policy, workflow recipes, and validation scripts.
- Data-Shape Verification: stale "v2.3 adds" wording removed.
- Language-support tables converted to bullet lists so wide cells stop overflowing on narrow screens.
- Changelog: v2.x releases moved into a collapsed `<details>` block under "Earlier releases", with heading levels demoted so the right-side TOC stays focused on the current release.
- Operator workflows: release-gate benchmark table updated to the v3.5.0 baseline (31 repos / 14,296 files / 216,663 symbols / 484,379 edges / 96,787 cross-repo) plus planning-oracle latency percentiles.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `3.5.1`.
- `cargo update` and `bun update` produced no transitive bumps.

## v3.5.0 (2026-05-06)

Release status: **released**.

Combined v3 release covering deployment-topology indexing, `gather-step pr-review` (a non-destructive PR analysis command), the SWC → Oxc TypeScript and JavaScript parser switch, indexing performance and storage compactions, runtime perf experiments (kanal, regex-automata DFA reuse, rkyv adjacency blobs, parking_lot, graph CSR snapshot), schema-strictness hardening, and a security and cleanup pass.

### Major Features

#### PR Review Mode

- Added `gather-step pr-review --base <REF> --head <REF>` to build a disposable review index in the OS cache directory and emit a structured `DeltaReport` for human or machine consumption.
- Added `gather-step pr-review clean` with five selectors (`--dry-run`, `--run-id`, `--base/--head`, `--older-than`, `--all`) and an `--include-active` opt-in for pruning the still-resolvable cache. `clean --older-than` skips `InProgress` artifacts so it cannot race a long indexing run.
- Added `--severity {warn, strict, pedantic}` threshold modes. `warn` is the default; `strict` exits with code 2 on any High-severity removed-surface risk or payload type change; `pedantic` extends that to Medium risks and any payload change.
- Added `--format {markdown, json, github-comment, braingent}` plus `--github-comment-file <PATH>` for CI integrations. The GitHub-comment renderer auto-truncates to fit the platform's 65,536-character comment limit. The [Braingent](https://braingent.dev) renderer emits a YAML-frontmatter Markdown record suitable for archiving in a memory store.
- Added `--engine temp-index` as the default public review engine; builds a full isolated index for the PR head.
- Added `--keep-cache` to preserve the review artifact root for follow-up `trace`, `impact`, `pack`, and `projection-impact` commands. Suggested follow-up commands in the report are pre-filled with `--registry` / `--storage` overrides pointing at the kept index.
- Added `--no-baseline-check` to suppress the workspace-HEAD-vs-`--base` SHA mismatch warning.
- Added `--registry` and `--storage` flags on `trace`, `impact`, `pack`, and other read commands so they can target a kept review artifact root and replay PR-only context.
- Extended `gather-step clean` with `--include-review` to also wipe review artifacts for the workspace.
- A full `gather-step index` reindex automatically wipes review artifacts (their baseline is invalidated).
- Added a branch-scoped review cache keyed by `(workspace_hash, base_sha, head_sha, config_hash, schema_version, gather_step_version)`. Cache hits skip worktree creation and indexing when a retained matching artifact exists.
- Added the `pr_review` MCP tool exposing the same delta report to MCP clients. The tool now ships with a wall-clock timeout, bounded stdout/stderr buffers, and sanitised failure messages so paths and stack traces never leak through MCP traces.
- Added a top-level `CLAUDE.md` documenting the agent workflow for "review this PR using gather-step" plus project conventions.

##### Hard invariants

- `pr-review` and `pr-review clean` never mutate the workspace's normal `.gather-step/storage` or `.gather-step/registry.json`. Every review run logs the exact baseline storage path, review storage path, run id, and cleanup policy in the report's `safety` metadata block.
- `StorageContext::review_checked` rejects any review path that lives under `<workspace>/.gather-step/`. Workspace-local review artifacts must use a sibling (e.g. `.gather-step-review/`) or the OS cache directory.
- `pr-review clean` refuses to delete any path whose marker file does not match the current workspace hash, and refuses paths overlapping the baseline `storage/` or `registry.json`.
- A `ReviewCleanupGuard` runs cleanup on `Drop` under panic, signal, and early-return paths. Worktree-removal failures quarantine the artifact instead of orphaning it. Marker status transitions are enforced by `is_valid_status_transition`, so a `Completed → Quarantined` flip cannot bypass the lifecycle invariants.

##### Delta report (`schema_version: 7`)

- **Routes**: added / removed / changed by `(method, canonical_path)`. Handler info (repo, file, line, qualified name) attached via `Serves` edges.
- **Symbols**: added / removed / changed exported symbols and shared-symbol stubs by `(repo, qualified_name)`. Reports `signature_changed` and `visibility_changed` flags.
- **Payload contracts**: field-level diffs (added / removed / type-changed / `optional`-required flips) keyed by `(repo, file, target_qualified_name, side)`.
- **Events**: producer and consumer set diffs across `Topic`, `Queue`, `Subject`, `Stream`, and `Event` virtual nodes.
- **Decorators**: added / removed / changed permission, audit, and authorization decorators.
- **Contract alignments**: cross-repo clusters of related payload contracts with high / medium / low confidence.
- **Removed-surface risks**: removed routes / symbols / events with surviving consumers, classified by severity.
- **Deployment topology**: added / removed / changed deployment targets, env vars, secrets, config maps, shared infrastructure, and GitHub Actions deploy jobs.
- **Impact summaries**: per-removed-and-changed surface, downstream consumer counts grouped by repo and classified as `read_only`, `write_mutate`, `construct_payload`, or `unknown`.
- **Suggested follow-ups**: synthesized `gather-step pack` and `gather-step trace crud` commands targeting the highest-impact deltas, capped at 10.

#### Deployment Topology

- Added deployment topology indexing for Dockerfiles, Docker Compose, Kubernetes manifests, Kustomize files, Helm chart artifacts, GitHub Actions deploy jobs, configured env files, and Compose `env_file` references.
- Added graph nodes and edges for deployments, env vars, secrets, config maps, workflow jobs, brokers, and databases.
- Added `gather-step deployment-topology` plus MCP tools for `where_deployed`, `service_env`, `env_var_consumers`, `undeployed_services`, `deployed_but_no_code`, and `shared_infra`.
- Projection impact now replaces `deployed_owner_unchecked` with `deployed_owner_topology_observed` when indexed deployment evidence exists.
- Helm and GitHub Actions detection is intentionally conservative to avoid treating generic `values.yaml`, `chart.yaml`, `helm lint`, or `DEPLOY_*` env references as deployment evidence.
- Incremental indexing purges stale deployment facts when a previously indexed artifact becomes malformed or stops classifying as deployment data.
- Removing the last deployment artifact from a repo now purges its prior deployment topology on the next full reindex.
- Env-file values are not stored. Gather Step indexes env var names only.

#### TypeScript and JavaScript Parser (Oxc)

- Replaced the SWC visitor with an Oxc-driven implementation. Same `ParseState` writes (`NodeId`s, edges, decorators, call sites, constant strings) as the previous backend so downstream consumers see no behavioural change beyond a function-signature accuracy fix.
- Removed `swc_common`, `swc_ecma_ast`, and `swc_ecma_parser` from the dependency tree (~3.4k lines and a sizeable transitive dependency graph).
- Added an `oxc_test_support` surface that mirrors the helpers test suites previously imported from `swc_test_support`.
- Function signatures emitted for zero-parameter methods are now precise (`handle()` instead of accidentally swallowing a preceding decorator argument such as `('build')`).

### Improvements

#### Indexing Performance

- Bounded context-pack precompute and pack-target selection by repo count.
- Cached path-alias discovery for the duration of an index run.
- Gated framework augmenters by language so non-TS/JS repos do not pay for them.
- Skipped the size-only filesystem walk on the default index path.
- Avoided cloning traversal source bytes on the hot path.
- Moved git analytics off the writer hot path and bounded its queue depth by repo count.
- Promoted projection and git-classification regexes to module-level lazy statics.
- Avoided repeated dotted-field `format!` allocations in projection-impact matching.
- Replaced `crossbeam-channel` with `kanal` at the workspace-indexing pipeline sites.
- Migrated `std::sync::Mutex` and `std::sync::RwLock` to `parking_lot` where the lock is not held across `.await`, eliminating poisoning paths.
- Migrated the highest-traffic projection regex from the `regex` crate facade to `regex_automata::meta::Regex` and replaced 24 sentinel `source.contains(...)` calls with two `aho-corasick` DFAs built once at startup.
- Added a read-only compressed-sparse-row (CSR) snapshot of graph nodes and edge adjacency for frozen read paths.
- Shipped an experimental rkyv-archived adjacency-blob format with round-trip and bytecheck-validated tests, prerequisite for zero-copy adjacency loads.
- Reference-counted the bulk-mode guard so parallel workspace indexing threads can hold their own guards without prematurely disabling bulk mode.

#### Storage Compactions

- Dropped the redundant search `description` text field; reintroduced `qualified_name` as a dedicated indexed-only field with a lighter tokenizer chain. `SEARCH_INDEX_VERSION` is bumped to `1`.
- Decoded `is_exported` and `lang` from search fast fields instead of stored fields.
- Replaced the `edges_by_kind` projection with counters and compacted edge-metadata tags.
- Truncated `file_index_state.content_hash` to a 128-bit BLAKE3 prefix for the per-file change-detection cache.
- Pruned stale context packs on write and salted cache keys by compatibility.

#### Schema Strictness

- Graph store now requires every existing redb file to carry a stamped schema row. Missing schema tables and missing version rows are rejected with a typed `SchemaVersionMismatch` error so operators can wipe and reindex.
- All three stores (graph, metadata, search) follow the same strict-version policy. No implicit-v0 compatibility shim remains.
- Workspace registry now drops repos that disappear from `gather-step.config.yaml` and the indexer purges their generated graph, search, and metadata state.

#### Security

- Watcher ignores symlinked event paths.
- `gather-step.local.yaml` and other local config reads are capped at a bounded byte budget.
- `git worktree add` arguments are passed positionally rather than glued into one shell string.
- Deployment topology config rejects symlinked paths.
- Path safety rejects symlinked workspace roots in addition to symlinked descendants.
- The MCP `pr_review` tool sanitises its failure surface — exit code is reported, but raw stderr/stdout never echo back into the transcript and are kept for the operator log only.

#### Bug Fixes

- Search queries split identifier separators (`-`, `_`, `.`, `/`) before parsing so snake-case and slash-bearing repo names tokenize the same way they index.
- Qualified impact queries fall back to the tail segment when the qualified form does not hit the search index.
- Workspace registry counts are refreshed from the final graph at the end of an index run so the registry never drifts behind the graph.
- Incremental classification truncates new content hashes to the stored prefix length before comparing, so the 16-byte hash prefix store does not flag every previously-indexed file as modified.

### Cleanup

- Removed the deprecated `pr-review --strict` flag (use `--severity strict`).
- Removed the deprecated per-command `--json` flag on `pr-review` and `pr-review clean` (use the global `--json`).
- Removed the duplicate `get_graph_schema_summary` re-export module.
- Removed the `ChangeImpactSummary.downstream_repos` backward-compat alias; callers now use `confirmed_downstream_repos` and `probable_downstream_repos`.
- Normalised every operator-facing error and warning message to sentence case with a terminating period.

### Internal Architecture

- New `gather-step-deploy` workspace crate. Deployment-artifact parsing was extracted out of `gather-step-storage` and is now consumed by `gather-step-storage::indexer` and `gather-step-analysis`.
- TypeORM framework parser added (entity decorators, migration `MigrationInterface` `up`/`down` extraction). Powers the existing PR-review `payload_contracts` and migration-edge surfaces.
- Refreshed MCP protocol dependencies by updating `rmcp` and `rmcp-macros` to `1.6.0`.
- `BulkModeGuard` is now reference-counted (`AtomicUsize`) so concurrent and nested guards no longer race each other into prematurely disabling bulk mode.

### Verification Coverage

- Added regression coverage for deployment parser false positives, stale deployment fact purging, the full-reindex deployment-purge path, service-targeted projection-impact topology matching, shared-infra consumers, topology response mapping, and generated MCP tool summaries.
- 274 `gather-step-cli` library tests, 162 `gather-step-storage` library tests, 330 `gather-step-parser` library tests, plus integration suites for `cli_commands`, `safety`, `pack_oracle`, and `pack_eval`.
- New tests for: schema-strictness rejection on missing graph schema table or row; full-reindex purges stale deployment artifacts; registry drops repos no longer in config; bulk-mode guard nesting under panic and parallel threads; MCP `pr_review` timeout, bounded buffers, and sanitised errors; `Hash16` blake3-prefix newtype round-trip; rkyv adjacency-blob round-trip and bytecheck-rejection of truncated input.
- Oxc parser self-validation tests across every TS/JS extraction fixture.
- Secret-surface MCP smoke test exercises the redaction surface end-to-end.
- Deployment-topology MCP tools test pins the public response shape.
- Benchmark harness samples resource peaks (max RSS, peak memory footprint, open FDs on Unix).
- 8 git-helpers tests for `resolve_ref`, `resolve_range`, `merge_base`, `changed_files`, and detached-worktree creation / removal.
- Stable JSON top-level-key snapshots and Markdown section-header snapshots prevent accidental schema drift.

### Release-wide

- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `3.5.0`.
- Bumped `oxc_*` to `0.129.0`, `regex-automata` to `0.4.14`, `rkyv` to `0.8.16`, and `@astrojs/starlight` to `0.38.5`.

## Earlier releases

<details>
<summary>v2.x — click to expand</summary>

#### v2.4.0 (2026-05-01)

Release status: **released**.

Setup and indexing usability release for config-respecting onboarding, repo selection, clearer progress copy, watch-count runs, parser-warning cleanup, and docs layout stability.

#### Highlights

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

#### Verification Coverage

- Added regression coverage for existing config reuse, `watch N` argument parsing, static mapping parser routing, duplicate Python sibling package ambiguity, and indexing summary formatting.
- Verified with Rust formatting, Cargo check, clippy, targeted CLI/parser tests, and website build during release preparation.

### v2.3.0 (2026-05-01)

Release status: **released**.

Data-shape research carry-forward release for alias-aware field evidence, optional payload filter risk, generated migration probe plans, and broader migration sibling detection.

#### Highlights

- Labeled field evidence as `direct_field_access` or `local_alias_field_access` when `projection-impact` / `projection_impact` can explain the origin.
- Followed same-scope TypeScript aliases and object destructuring aliases for typed field-access evidence.
- Promoted optional payload filter mismatch into `projection_impact`, dotted `impact`, MCP `projection_impact`, and planning-pack gap summaries.
- Added generated Mongo `$type` probe plans to migration sibling bands, with copy-paste-safe `db.getCollection(<name>)` commands.
- Extended Mongoose migration detection to imported local model declarations, multiple static collections in one migration, and additional static write methods.
- Added conservative TypeORM migration sibling detection for static `queryRunner.query(...)` SQL table names and static `queryRunner` table-method targets.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.3.0`.
- Refreshed Cargo lock metadata and updated the website dependency set from Astro `6.2.0` to `6.2.1`.

#### Data-Shape Research

- Optional payload mismatch stays a static review signal. It adds `optional_payload_filter_mismatch` and `runtime_shape_probe` instead of claiming production data distribution.
- Planning packs now surface optional payload evidence on migration probe plans when an indexed payload contract marks the filtered field optional.
- TypeORM support indexes table siblings only. SQL WHERE-field extraction remains intentionally out of scope, so SQL migrations do not produce Mongo-specific field probe guidance.
- Generated probe plans remain static. Gather Step still does not connect to MongoDB or execute runtime probes.

#### Verification Coverage

- Added store-backed planning oracle coverage for field evidence, optional payload contracts, and migration filters.
- Added parser coverage for alias/destructuring field evidence and TypeORM migration table detection.
- Added MCP coverage for optionality mismatch summaries, migration sibling probe plans, response-shape stability, and payload-contract lookup warnings.
- Verified format, clippy, cargo check, targeted parser/analysis/MCP tests, and website build during release preparation.

### v2.2.0 (2026-04-30)

Release status: **released**.

Data-shape awareness release for field-level impact review and Mongo/Mongoose migration planning.

#### Highlights

- Added direct TypeScript field reader/writer evidence for typed member access, including nested dotted paths such as `WorkItem.workflow.stepIds`.
- Extended `projection-impact` and `projection_impact` so exact dotted field targets include direct readers, writers, filters, indexes, and backfills in one report.
- Let planning and change-impact packs surface field-impact reminders while preserving the existing context-pack follow-up budget.
- Added Mongoose migration sibling awareness so planning packs can show prior migrations on the same collection, including captured filter literals.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.2.0`.

#### Data-Shape Awareness

- Direct field extraction is intentionally scoped to typed local receivers and parameters; dynamic keys, aliases, destructuring, broad `any`/`unknown`, generic containers, and deep optional chains remain unsupported.
- Migration detection is intentionally conservative: files must look like Mongoose-style migration files and expose `up`/`down` behavior before sibling hints are emitted.
- Deployment note: v2.2 changes generated graph/schema state for migration collection edges. Existing `.gather-step` storage should be rebuilt with `gather-step reindex` before relying on v2.2 migration-sibling output.

#### Verification Coverage

- Added parser extraction-fidelity coverage for direct field readers/writers and false-positive skips.
- Added analysis and MCP coverage for direct field evidence in projection-impact reports.
- Added CLI routing coverage for dotted field targets through projection-impact.
- Added planning-pack and oracle coverage for migration siblings, pack response shape, and follow-up budget behavior.
- Verified format, clippy, all-features test build, targeted parser/analysis/MCP/CLI tests, and website build during release preparation.

### v2.1.1 (2026-04-30)

Release status: **released**.

Patch release for setup recovery and upgrade UX.

#### Highlights

- Fixed `gather-step init --index` so setup-triggered indexing rebuilds generated index state from source repos instead of exiting when old `.gather-step/storage` state is stale or incompatible.
- Improved storage/schema operator messages with sentence-cased, actionable guidance. Graph schema mismatches now point to `gather-step index --auto-recover`.
- Changed recovery progress output to say `Rebuilding generated index state from source repos`.
- Clarified Homebrew upgrade docs to use `brew update` followed by `brew upgrade thedoublejay/tap/gather-step`.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.1.1`.

#### Verification Coverage

- Added regression coverage for `init --index` auto-recovering stale generated state.
- Added unit coverage for graph schema mismatch operator guidance.
- Re-ran existing corrupt graph and unsupported metadata schema recovery tests.
- Verified manual smoke indexing with temporary generated state and direct graph/metadata/search store checks.

### v2.1.0 (2026-04-30)

This release polishes the v2 onboarding path, generated AI context, website build pipeline, and dependency graph, promotes Python to first-class parsing parity with TypeScript and JavaScript, and adds static projection-impact tracing.

#### Highlights

- Made `gather-step init` the primary setup path in docs and landing copy, with a workspace directory diagram and explicit prompt defaults.
- Updated init output casing to "Gather Step" and made the local MCP default visible in the interactive prompt.
- Kept generated Claude workspace context factual by removing acknowledgement/sign-off instructions while preserving the MCP tool reference table.
- Updated the website workflow to Node 24 and refreshed GitHub Actions used by CI and website builds.
- Bumped the app, Cargo workspace, internal crate dependency versions, and website package metadata to `2.1.0`.
- Refreshed Cargo dependencies with `cargo update`, including moving `gix` from the yanked `0.82.0` line to `0.83.0`.
- Promoted Python to first-class parsing alongside TypeScript and JavaScript (see [Language Support](/concepts/language-support/)).
- Added projection-impact tracing for derived fields, persisted projections, filters, indexes, and backfills.
- Marked v2.1 release readiness with a fresh release-build benchmark where release-scored Gather Step slices are all High/passing.

#### Projection Impact

- Added the `projection-impact --target <FIELD>` CLI command and `projection_impact` MCP tool for static field-level projection tracing, including `evidence_verbosity` controls for summary versus full evidence.
- Added `DataField` graph nodes plus `ReadsField`, `WritesField`, `DerivesFieldFrom`, `FiltersOnField`, `IndexesField`, and `BackfillsField` edges.
- Planning and change-impact packs can now include short projection hints and `projection_impact:*` gap markers while the full evidence stays behind the dedicated projection tool.
- Added oracle, CLI/MCP serialization, integration, and parser extraction-fidelity coverage for projection chains, Mongo-style mappings, JSON/YAML index mappings, and false-positive fixtures.
- Deployment note: v2.1 projection impact changes the generated graph schema. Existing `.gather-step` storage should be rebuilt with `gather-step reindex` before relying on projection-impact output.
- Projection impact intentionally does not infer deployed runtime ownership; verify deployment owners separately when duplicate or transitioning services exist.

#### Python Parsing

- Resolved Python `src/<package>/...` and flat `<package>/...` layouts so absolute current-package imports produce stable cross-file edges.
- Linked Python sibling packages across configured workspace repos using the `name` field from `gather-step.config.yaml`, falling back to `pyproject.toml [project].name` and finally the directory basename for standalone repos.
- Added detection-only FastAPI framework pack activation from Python dependency metadata.
- Qualified nested Python functions and methods, including methods inside nested classes, with full owner qualified names such as `Outer.Inner.method`, removing node-ID collisions for same-named helpers.
- Preserved Python class relationships (base classes, implemented interfaces, constructor dependencies) and decorator metadata across nested scopes.
- Added explicit diagnostics (`tracing::warn!`) when `gather-step.config.yaml` cannot be canonicalized or parsed, when configured repo paths fail to canonicalize, or when `read_dir` errors are encountered during sibling-package resolution. Prior behavior silently fell back to the directory-basename heuristic.

#### Benchmarking And Tooling

- Added `gather-step-bench workspace-run` to measure wall-clock index time, graph node/edge counts, cross-repo edge count, RSS growth, and storage byte breakdowns (graph, metadata, search, sidecar) for a configured workspace.
- Added a neutral Python planning workspace fixture under `tests/fixtures/python_planning_workspace/` so the planning oracle and storage benchmark have a committed Python target.
- Documented the [external corpus benchmarking convention](/concepts/language-support/#external-corpus-convention) for measuring against repositories that cannot be checked in.
- Renamed `StorageMetrics::metadata_wal_bytes` to `metadata_sidecar_bytes` since the field actually sums the SQLite WAL and SHM files. Deserialization remains backward-compatible with the old bench JSON field name.
- Promoted `HarnessError::Workspace` from a stringified message to a typed `Box<WorkspaceIndexError<RepoIndexerError>>` so `anyhow::downcast` and structured logging can recover the source chain.
- Recorded the fresh 2026-04-30 release-build benchmark summary without checking in local benchmark artifacts.

#### Verification Coverage

- Fresh release benchmark from a clean release build at `3f0093e`: curated index High, link quality 3/3 passing, planning oracle 25/25 passing, Python planning 1/1 passing, projection CLI fixture index High, and all projection targets release-scored High.
- Website build and Cloudflare Pages checks.
- Rust CI summary: format, clippy, cargo-deny, cargo-shear, macOS tests, MVCC stress, and MSRV check.
- Added regression tests for configured-repo identity resolution and malformed `gather-step.config.yaml` fallback.

### v2.0.0 (2026-04-28)

CLI onboarding, local MCP setup, release automation, and documentation refresh.

This release builds on `v1.0.0` by making the local-first workflow easier to start, easier to keep fresh, and easier to ship from a tagged release.

#### Highlights

- Added a richer no-args startup path: interactive unconfigured workspaces enter setup, configured workspaces show status, and non-interactive shells print help without hanging.
- Completed the `init` wizard for repository discovery, config writing, optional indexing, AI context generation, MCP registration, and watch handoff.
- Added `setup-mcp` for idempotent workspace-local or global Claude settings updates.
- Added AI-facing context generation through graph-backed `.claude/rules/` plus `CLAUDE.gather.md` and `AGENTS.gather.md` summaries.
- Improved operator feedback for startup, `status`, `index`, `reindex`, `watch`, `clean`, and `serve`.
- Bumped the workspace and crate versions to `2.0.0`.
- Added release workflow automation to open Homebrew tap update pull requests.
- Refreshed the website landing page, feature copy, getting-started docs, installation docs, and CLI reference.

#### CLI Startup And Status

- Added a no-args command path.
- In an interactive workspace without `gather-step.config.yaml`, no-args mode starts the guided init flow.
- In a configured workspace, no-args mode renders the status summary.
- In non-interactive contexts, no-args mode prints CLI help and returns without prompting.
- Refreshed the banner and startup UX.
- Expanded `status` output with clearer workspace, index, framework, MCP, and semantic-health signals.
- Improved progress reporting for `index`, `reindex`, `watch`, `clean`, and `serve`.

#### Init Wizard

- Added end-to-end interactive setup through `gather-step init`.
- Added `init --force` for explicit config overwrite.
- Added `init --index` and `init --no-index`.
- Added `init --watch` and `init --no-watch`.
- Added `init --generate-ai-files` and `init --no-generate-ai-files`.
- Added `init --setup-mcp <local|global>`.
- Added a smooth handoff from setup/indexing into watch mode.
- Kept repository discovery scoped to the init flow and excluded generated or dependency-heavy directories.

#### MCP And AI Files

- Added the `setup-mcp` command.
- `setup-mcp --scope local` writes `.claude/settings.json` under the workspace.
- `setup-mcp --scope global` writes `~/.claude/settings.json`.
- MCP settings are updated idempotently without removing unrelated server entries.
- Added `generate claude-md` / `--target rules` for graph-backed Claude Code project rules under `.claude/rules/`.
- Added `generate claude-md --target summary` for `CLAUDE.gather.md`.
- Added `generate agents-md` for Codex-style `AGENTS.gather.md` workflows.
- Reused the same workspace summary renderer in the init wizard and explicit generate commands.

#### Release And CI

- Bumped the Cargo workspace, crates, fixture packages, and website package metadata to `2.0.0`.
- Updated the release workflow to open Homebrew tap formula update pull requests after release artifacts are built.
- Kept macOS release artifact smoke tests for `--version`, `--help`, and index/status against an embedded fixture.
- Updated pinned GitHub Actions versions for CI and release support.
- Removed the unused `sharp` website dependency.

#### Documentation And Website

- Refreshed the CLI command reference for the current command surface.
- Updated getting-started guidance around the single-command setup path.
- Updated workspace setup docs for init wizard flags and watch handoff.
- Updated MCP client docs with the `setup-mcp` fast path.
- Updated operator workflow docs to describe the committed release-gate baseline.
- Updated landing page feature copy, release stamps, onboarding flow, and install command behavior.
- Made the landing install command copyable with click feedback.

#### Verification Coverage

- Added CLI coverage for no-args behavior.
- Added CLI coverage for init behavior, the full wizard path, setup-mcp, and index/watch parsing.
- Added workspace summary output coverage.
- Release preparation test plan includes `cargo test -p gather-step-cli`, `cargo test -p gather-step-output`, `cargo test --workspace`, and `cd website && bun run build`.

</details>

## See Also

Binary releases are published at [https://github.com/thedoublejay/gather-step/releases](https://github.com/thedoublejay/gather-step/releases). Each release notes entry describes the user-visible changes for that version.
