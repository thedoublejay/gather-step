---
title: "Changelog"
description: "User-visible changes to gather-step, listed by release. Updated manually until a release pipeline is wired up."
---

This changelog lists significant user-visible changes. It is maintained manually until release notes and tagged releases become the automated source of truth.

## v2.0.0 (Draft)

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
