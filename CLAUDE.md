# CLAUDE.md — Gather Step

Memory file for Claude / Gather Step agents. Project conventions and the canonical workflows for this codebase.

## Conventions

- **Commit messages:** Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, etc.). Keep the subject under 72 characters.
- **No trailers:** Never add `Co-Authored-By` or `Signed-off-by` to commits.
- **Signing:** Tests must run cleanly with `commit.gpgsign = true` set globally. Fixture helpers that create git commits must disable signing locally (e.g., `git -c commit.gpgsign=false commit ...`) so CI and dev environments that have GPG signing enabled do not break.
- **Lint bar:** `cargo clippy --workspace --all-targets -- -D warnings` must pass clean. No `#[allow(clippy::...)]` without an `#[expect(..., reason = "...")]` that explains why.
- **Formatting:** `cargo fmt --all` before committing. `rustfmt.toml` is the authority.
- **Type-check first:** Run `cargo check --workspace` before submitting any change that touches non-trivial Rust.

## Reviewing a PR with gather-step

When the user says **"review this PR using gather-step"** (or any close variant — "review the PR", "do a structural review with gather-step", etc.):

1. Run `gather-step pr-review --base <base> --head <head> --json` to build the review index and emit a structured delta report. Use `--keep-cache` if the user wants to ask follow-up questions against the review index.
2. **Warn the user before the first run** that it will take a minute or two: a fresh review index is being built from scratch in a disposable Gather Step storage. Subsequent runs against the same SHAs may reuse the cache.
3. Read the report's `metadata`, `safety`, `changed_files`, and `suggested_followups`. The current `schema_version` (defined by `DELTA_REPORT_SCHEMA_VERSION` in `crates/gather-step-cli/src/pr_review/delta_report.rs`) populates `routes`, `symbols`, `payload_contracts`, `events`, `decorators`, `contract_alignments`, `removed_surface_risks`, and `deployment` in addition to the metadata surfaces — none of these are stubs.
4. For deep follow-ups, run the suggested commands as-is. They include `--registry` / `--storage` overrides pointing at the kept review index, so they read the PR-branch state, not the workspace baseline.
5. Only fall back to manual diff inspection if `pr-review` is unavailable or fails. Never silently skip it for a structured review request.

Suggested first-run warning text:

> Building a review index for `<base>..<head>` — this takes a minute or two on the first run because the PR branch is being indexed into a separate disposable Gather Step index. Subsequent runs against the same SHAs may reuse the cache.

## Cleaning up review artifacts

`pr-review` writes a disposable review index under the OS cache directory (`<cache>/gather-step/pr-review/<workspace-hash>/<run-id>/`). Without `--keep-cache`, successful runs delete this directory on exit.

To inspect or remove kept artifacts:

```bash
gather-step pr-review clean --dry-run            # list every kept artifact for this workspace
gather-step pr-review clean --run-id <id>        # delete one explicit run
gather-step pr-review clean --base <ref> --head <ref>   # delete artifacts for a specific PR
gather-step pr-review clean --older-than 7d      # prune stale completed/failed/quarantined caches
gather-step pr-review clean --all                # wipe every review artifact for this workspace
```

`clean --older-than` skips `InProgress` artifacts so it cannot race a long indexing run. `--all` and `--run-id` are explicit and will remove `InProgress` artifacts if the user asks.

`gather-step clean --include-review` extends the workspace `clean` to also wipe review artifacts. A full `gather-step index` reindex automatically wipes review artifacts, since their baseline is invalidated.
