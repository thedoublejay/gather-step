# Gather Step Exploration Plan: PR Review Mode

**Status:** Research plan. No implementation started.

## Summary

Gather Step can answer cross-repo questions from the indexed workspace, but today that index usually represents `main`. That means a new PR-only route, DTO, payload type, or gateway mapping is invisible unless the reviewer manually checks out the branch and re-indexes. The proposed feature is a `pr-review` mode that reviews upcoming changes against the current index without mutating that index.

Recommended shape:

- Build an isolated review context from `base` and `head`.
- Index only into review-specific registry and storage roots.
- Compare baseline index surfaces against review index surfaces.
- Report deltas for routes, shared symbols, payload contracts, events, projection-impact evidence, repo wiring, ownership, and suggested follow-up queries.

**ELI5:** Keep the existing Gather Step index as the clean map of the current system. For a PR, make a temporary copy of the world at the branch under review, build a second map there, then compare the two maps and say what changed and who might care.

## Problem Statement

The motivating review found a real gap:

- `gs impact LabelProject` on the current index identified `task` repo consumers of the shared contract.
- Manual follow-up showed `task` only read immutable fields (`displayId`, `status`), so the PR was safe for that repo.
- `gs trace crud --method PATCH --path '/label-projects/:id'` could not find the new route because it existed only on the PR branch.
- `gs impact UpdateLabelProjectInput` correctly returned no impact because the symbol did not exist on `main`.
- Manual review still had to connect equivalent contract shapes across frontend, gateway, and backend: `UpdateLabelProjectPayload`, `UpdateLabelProjectRequestDto`, and gateway `pathMapping`.

The feature should turn that manual branch-aware work into a first-class Gather Step workflow.

## Non-Mutating Index Requirement

Hard invariant:

- `pr-review` must not write to the workspace's normal `.gather-step/storage`.
- `pr-review` must not update the workspace's normal `.gather-step/registry.json`.
- All review indexing must use explicit review storage and registry paths.
- Every read command run as part of review must use the review context, not `app.workspace_paths().storage_root`.

**ELI5:** The normal index is the shared notebook. PR review can write scratch notes, but it must never erase or scribble into the notebook everyone else uses.

## Local Architecture Findings

- Default generated state is derived from `AppContext::workspace_paths()`: config at `gather-step.config.yaml`, registry at `.gather-step/registry.json`, storage at `.gather-step/storage`, and graph at `.gather-step/storage/graph.redb` ([app.rs](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-cli/src/app.rs:166)).
- The `index` command already accepts alternate config, registry, and storage paths through `IndexArgs`, then resolves defaults from the app context ([index.rs](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-cli/src/commands/index.rs:42), [index.rs](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-cli/src/commands/index.rs:254)).
- Workspace indexing already has a storage-root seam: `index_workspace_with_storage` receives a `storage_root` and builds a `StorageWorkspaceIndexDelegate` around `RepoIndexer::open(storage_root, options)` ([workspace_indexer.rs](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-storage/src/workspace_indexer.rs:29), [workspace_indexer.rs](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-storage/src/workspace_indexer.rs:123)).
- Indexing mutates multiple engines (`graph.redb`, Tantivy search, SQLite metadata). The storage README notes these engines are not a single atomic transaction boundary, so review-cache cleanup and corruption handling must be explicit ([README.md](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-storage/README.md:121)).
- The git crate currently focuses on history, ownership, analytics, classification, and intelligence refresh. It does not expose PR diff/worktree orchestration yet ([lib.rs](/Users/jjadonis/Documents/repos/gather-step/crates/gather-step-git/src/lib.rs:3)).
- Normal read commands still reopen default workspace storage. `pr-review` needs a shared `ReviewContext` or storage override so `search`, `impact`, `trace`, `projection-impact`, `pack`, and MCP review tools query the review index when requested.

## Competitor Patterns

Useful external patterns:

- GitHub Copilot code review supports pull request review, automatic review triggers, full-project context gathering, and re-review controls, but GitHub warns it can miss issues and needs human validation. Its strength is PR UX, not deterministic contract-impact reporting. Source: [GitHub Copilot code review](https://docs.github.com/en/copilot/concepts/agents/code-review).
- CodeRabbit emphasizes automatic and incremental PR reviews, with new commits reviewed by focusing on what changed. Its strength is PR comment workflow and avoiding repeated feedback. Source: [CodeRabbit PR review docs](https://docs.coderabbit.ai/overview/pull-request-review).
- Sourcegraph precise code navigation uploads indexes and makes code navigation available on code host diffs. Its useful lesson is commit/index scoped code intelligence, but it is not a Gather Step-style delta report. Source: [Sourcegraph precise code navigation](https://sourcegraph.com/docs/code-navigation/precise-code-navigation).
- SonarQube PR analysis reports issues introduced by the PR itself, defines new code by comparing the PR branch to the target branch, and can decorate PRs. Source: [SonarQube PR analysis](https://docs.sonarsource.com/sonarqube-server/analyzing-source-code/pull-request-analysis/introduction).
- Snyk PR Checks run before/after branch tests and fail only when the new branch has more issues. Source: [Snyk PR checks](https://docs.snyk.io/scan-with-snyk/pull-requests/pull-request-checks).
- GitHub code scanning shows new alerts on changed PR lines as annotations, and keeps branch alerts separate from default-branch status. Source: [GitHub code scanning PR alerts](https://docs.github.com/en/code-security/how-tos/manage-security-alerts/manage-code-scanning-alerts/triaging-code-scanning-alerts-in-pull-requests).
- Semgrep PR comments require diff-aware scanning before posting findings. Source: [Semgrep GitHub PR comments](https://semgrep.dev/docs/semgrep-appsec-platform/github-pr-comments).
- Nx `affected` uses Git history plus a project graph, with explicit `base` and `head`, to compute changed and dependent projects. Source: [Nx affected](https://nx.dev/docs/features/ci-features/affected).

Takeaway for Gather Step:

- AI reviewers are good at comment UX and heuristic feedback.
- Security scanners are good at PR-scoped gates.
- Build graph tools are good at affected project/test selection.
- Gather Step's distinct lane should be deterministic, local-first, cross-repo code graph deltas: routes, events, shared symbols, payload contracts, projection evidence, and repo coupling.

## Recommended Design

Use `git worktree + isolated temp index` for the first implementation.

Command sketch:

```bash
gather-step --workspace /path/to/workspace pr-review --base main --head feature/edit-label-project
gather-step --workspace /path/to/workspace pr-review --base origin/main --head HEAD --json
gather-step --workspace /path/to/workspace pr-review --pr 502 --repo label-review
```

Core flow:

1. Resolve `base` and `head`.
   - Prefer exact SHAs in CI.
   - Locally accept branch names and resolve to SHAs.
   - Optional later: GitHub/GitLab PR lookup.
   - `→ verify: command prints resolved base SHA, head SHA, and checkout mode before indexing`

2. Create an isolated checkout.
   - MVP: detached `git worktree` for each changed repo at `head`.
   - Prefer synthetic merge result when available from the host, because that is closest to what will land.
   - Offer `--checkout=head|merge` once provider support exists.
   - `→ verify: review report includes the exact commit indexed for every repo`

3. Create isolated generated state.
   - Registry path: temp or cache root, never normal `.gather-step/registry.json`.
   - Storage path: temp or cache root, never normal `.gather-step/storage`.
   - Default root: OS temp or cache directory keyed by workspace hash and run id.
   - Later opt-in persistent cache: `.gather-step/reviews/<base-sha>..<head-sha>/`, but still separate from normal storage.
   - `→ verify: snapshot normal registry/storage metadata before and after; no mtime or byte-size change`

4. Index the review context.
   - Reuse `index_workspace_with_storage` and existing parser/storage paths.
   - For MVP, reindex changed repos plus repos required by config if cross-repo edges need full workspace context.
   - Keep cold indexing correct before optimizing.
   - `→ verify: cold review index can answer existing search, impact, trace, and projection-impact queries against PR-only symbols`

5. Run delta extraction.
   - Compare baseline index surfaces to review index surfaces.
   - Produce added, changed, removed, and uncertain buckets.
   - Include file/line evidence and confidence.
   - `→ verify: fixture with new PATCH route reports an added route, controller/usecase symbols, and changed gateway/backend/frontend contract surfaces`

6. Render review report.
   - Human Markdown by default.
   - JSON for CI and MCP.
   - Optional PR comment formatting later.
   - `→ verify: JSON schema snapshot covers route, event, symbol, contract, projection, repo-coupling, and risk sections`

**ELI5:** The simplest reliable version is not a clever overlay. It is a second, disposable index built from the PR branch, then a comparison against the existing index. Once that works, optimize the rebuild.

## Delta Report Shape

The first report should include:

- Review metadata: workspace, base, head, checkout mode, changed repos, indexed repos, cache path, elapsed time.
- New routes/APIs: method, path, controller, request/response DTOs, usecase, downstream calls.
- Changed routes/APIs: same route key, changed handler, changed DTO shape, changed auth/middleware evidence when available.
- Removed routes/APIs: route key removed and known callers/clients still present.
- Shared symbols: added/removed/changed exported symbols and cross-repo references.
- Payload contracts: field-level additions/removals/optional-required changes, plus matching near-equivalent contract names across repos.
- Event surfaces: new producers, new consumers, changed payload shapes, removed topics/queues.
- Projection/data-shape risks: field reads/writes, optional-field evidence, migration/probe hints where available.
- Cross-repo coupling: consumers of touched shared contracts, grouped by read-only, write/mutate, unknown.
- Ownership/hotspot hints: recent owners and high-churn files for changed surfaces.
- Suggested follow-up commands: exact `gather-step impact`, `trace`, `projection-impact`, and `pack --mode review` calls.

## Contract Alignment Heuristics

MVP field-level alignment should not require perfect type identity. It should combine:

- Same route path and method.
- Same gateway mapping target.
- Similar symbol names after stripping verbs/suffixes: `UpdateLabelProjectPayload`, `UpdateLabelProjectRequestDto`, `EditLabelProjectFormData`.
- Same field names and compatible primitive/object shapes.
- Same repo dependency edge or package import edge.
- File-path conventions: `request.dto.ts`, API client payload, frontend form data, gateway path mapping.

Report confidence:

- High: direct route/gateway/backend connection plus matching field set.
- Medium: matching symbol stem plus overlapping fields.
- Low: field overlap only.

`→ verify: REG-13863-style fixture flags frontend-only industry, productCategory, and territory when backend DTO does not accept them`

## Review Context Abstraction

Add a small context layer before adding a public command:

- `StorageContext`: baseline or review registry/storage paths.
- `WorkspaceSource`: normal workspace or review worktree root.
- `ReviewRun`: base/head metadata, changed files, changed repos, indexed repos, cleanup policy.

All read commands used by review should accept a context object instead of directly calling `app.workspace_paths()`.

`→ verify: tests prove search, impact, trace, and projection-impact can run against a supplied storage root`

## Phase A: Read-Path Storage Override

**ELI5:** Before PR review can exist, Gather Step needs a clean way to say "read this other index for this command."

1. Introduce an internal `StorageContext` struct.
   - `→ verify: normal CLI behavior remains unchanged when no override is provided`

2. Refactor read commands to accept explicit registry/storage paths internally.
   - Include `status`, `search`, `impact`, `trace`, `events trace`, `projection-impact`, `pack`, and MCP context construction.
   - `→ verify: focused tests run each command against a fixture storage root outside normal .gather-step/storage`

3. Add guardrails that reject accidental review writes to normal storage.
   - `→ verify: review-mode tests fail if registry/storage path equals app default generated state`

## Phase B: Worktree And Temp Index MVP

**ELI5:** Build the PR's map in a clean temporary checkout, then throw it away unless the user asks to keep it.

1. Add `crates/gather-step-git` helpers for base/head resolution and clean detached worktree creation.
   - `→ verify: branch name and SHA inputs both resolve deterministically in fixture repos`

2. Add `pr-review --base --head --keep-cache --json`.
   - `→ verify: CLI prints resolved SHAs and isolated generated-state paths`

3. Run indexing against review paths.
   - `→ verify: normal workspace .gather-step/storage and .gather-step/registry.json remain unchanged`

4. Clean up interrupted or failed temp runs.
   - `→ verify: simulated index failure removes temp worktree by default and preserves it with the keep-cache flag`

## Phase C: Delta Surfaces

**ELI5:** Once two maps exist, compare named landmarks: routes, symbols, events, and contract shapes.

1. Route/API delta.
   - `→ verify: added PATCH /label-projects/:id appears even when baseline trace crud cannot find it`

2. Shared symbol delta.
   - `→ verify: PR-only symbols appear as added with defining repo/file/line evidence`

3. Payload contract delta.
   - `→ verify: field additions/removals/optional-required changes are summarized by contract`

4. Event delta.
   - `→ verify: new producer/consumer fixtures report topic and repo directionality`

5. Removed surface delta.
   - `→ verify: removed route/symbol with surviving consumers is flagged as high risk`

## Phase D: Cross-Repo Review Intelligence

**ELI5:** The report should not stop at "this changed." It should say "this changed and these other repos may depend on it."

1. Attach `impact` results to added/changed shared symbols and contracts.
   - `→ verify: shared-contract fixture lists downstream repos and read/write classification when available`

2. Add read-only vs mutation classification for consumers.
   - Start with direct field reads/writes and known DTO construction.
   - `→ verify: displayId and status read-only consumption is classified separately from payload construction`

3. Add contract-alignment checks across equivalent frontend/gateway/backend symbols.
   - `→ verify: backend DTO missing frontend form fields is reported as a field-set asymmetry`

4. Add suggested follow-up command generation.
   - `→ verify: report includes executable trace, impact, projection-impact, and pack --mode review commands for top risks`

## Phase E: Cache And Performance

**ELI5:** First make the review correct. Then make repeated reviews fast.

1. Add branch-scoped persistent cache keyed by `base_sha`, `head_sha`, config hash, Gather Step schema version, and parser version.
   - `→ verify: force-push or config change invalidates cache`

2. Seed review index from baseline where safe.
   - `→ verify: cold review and seeded review produce identical delta JSON for fixtures`

3. Reindex affected repos/files conservatively.
   - Include changed files, reverse dependents, package manifest changes, shared contract package changes, route config changes, and gateway mappings.
   - `→ verify: manifest/shared-contract changes expand affected repo set rather than under-reporting`

## Phase F: CI And PR Decoration

**ELI5:** After local review works, make it easy to run in CI and post a short review comment.

1. Add CI-friendly JSON and Markdown output.
   - `→ verify: JSON schema is stable and Markdown has deterministic headings`

2. Add `--github-comment-file` or `--format github-comment`.
   - `→ verify: generated comment stays under GitHub comment limits and links to full artifact`

3. Add optional severity thresholds.
   - Example: fail on removed route with surviving consumers, incompatible contract shape, or unknown high-impact shared symbol.
   - `→ verify: CI exit-code tests cover warn-only and fail modes`

## Open Decisions

- Should default review checkout be `head` or synthetic merge result?
  - Recommendation: synthetic merge when provider/CI exposes it; otherwise `head` with a clear report note.
- Should the first MVP index all configured repos or only changed repos plus known dependents?
  - Recommendation: all configured repos for correctness; optimize later.
- Should persistent review caches live under workspace `.gather-step/reviews` or global cache?
  - Recommendation: temp/global cache by default; workspace-local cache only with explicit flag.
- Should PR review be CLI-only first or MCP-visible immediately?
  - Recommendation: CLI JSON first, then MCP tool once output schema stabilizes.

## Risks

- Accidental fallback to normal storage makes PR-only symbols disappear or mutates the current index.
- Worktree checkout at the wrong SHA creates misleading reports.
- Diff-only extraction misses cross-repo consumers and gives false confidence.
- Partial writes across graph/search/metadata can leave review cache inconsistent.
- Branch cache can go stale after rebase or force-push.
- Contract alignment heuristics can over-match similarly named DTOs unless confidence is explicit.
- Large workspaces may make cold PR review slow enough to need cache before broad use.

## Success Criteria

- `pr-review` finds a route that exists only on the PR branch.
- `pr-review` reports PR-only shared symbols and payload contracts.
- `pr-review` shows downstream cross-repo consumers from the baseline graph and classifies obvious read-only consumption.
- `pr-review` detects frontend/backend/gateway field-set asymmetry for a REG-13863-style fixture.
- Normal `.gather-step/storage` and `.gather-step/registry.json` are unchanged after review.
- The same review run can emit human Markdown and machine JSON.

## Suggested First Slice

Start with Phase A and Phase B only:

1. Internal storage override for read commands.
   - `→ verify: fixture read commands can target a non-default storage root`

2. `pr-review --base --head --json` with detached worktree and temp index.
   - `→ verify: PR-only route is discoverable from the review context while baseline index remains unchanged`

3. Minimal delta report: added routes, added symbols, added payload contracts, changed files, and suggested follow-up commands.
   - `→ verify: REG-13863-style fixture produces the new PATCH route plus review commands for route trace and contract impact`
