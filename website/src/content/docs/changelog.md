---
title: "Changelog"
description: "User-visible changes to gather-step, listed by release. Updated manually until a release pipeline is wired up."
---

This changelog lists significant user-visible changes. It is maintained manually until a release pipeline is wired up to generate it automatically from release notes and tagged releases.

## v2.0.0 (Draft)

First public release of Gather Step.

This release establishes the full local-first product surface: workspace indexing, a multi-command CLI, a stdio MCP server, graph-powered analysis across multiple repositories, task-shaped context retrieval for AI assistants, and an end-user documentation site covering setup, concepts, workflows, and reference material.

### Highlights

- Local-first code graph for multi-repo workspaces.
- One-time indexing with workspace-local persisted state in `.gather-step/`.
- Stdio MCP server for MCP-aware AI clients.
- Cross-repo graph traversal for routes, events, shared types, and impact analysis.
- Contract inspection and producer-consumer drift analysis.
- Task-shaped context packs for planning, debugging, fixing, review, and change impact.
- Repo intelligence features such as ownership, dead-code candidates, and convention discovery.
- Proof-backed planning and impact evidence with confirmed, probable, and advisory downstream repos.
- Release-gate validation with HIGH-contract probes, PR-oracle scoring, and archived release artifacts.
- Operational lifecycle controls for compaction, auto-recovery, schema-versioned storage, and MCP tool tracing.
- Expanded framework extraction across backend services, frontend hooks, routers, gateway proxies, package manifests, and persistence frameworks.
- Initial documentation site with guides, concepts, CLI reference, MCP tools reference, and changelog.

### Workspace Indexing

- Added workspace initialization with automatic repository discovery.
- Added persistent workspace config generation through `gather-step init`.
- Added full indexing through `gather-step index`.
- Added destructive rebuild support through `gather-step reindex`.
- Added workspace-local cleanup through `gather-step clean`.
- Added in-place generated-state compaction through `gather-step compact`.
- Added `index --auto-recover` for rebuilding corrupt or unsupported generated state.
- Added `index --artifact-path` and `index --release-gate` for release-pipeline artifacts and invariants.
- Added support for indexing one or more repositories under a shared workspace root.
- Added workspace registry and storage layout rooted in `.gather-step/`.
- Added graph persistence, metadata storage, and search projections for indexed workspaces.
- Added support for bounded indexing depth configuration and per-run overrides.
- Added low-latency query behavior by reading from persisted indexed state instead of reparsing source on every question.
- Added schema-versioned storage checks, crash-recovery coverage, MVCC consistency coverage, and path/event identity invariants.
- Added filtered search, hook-boundary storage coverage, and incremental reconciliation checks for generated state.

### CLI Surface

- Added the `gather-step` CLI as the primary operator interface.
- Added global workspace, repo, verbosity, JSON, and banner control flags.
- Added `status` to inspect workspace freshness, counts, frameworks, and semantic health.
- Added `doctor` to surface indexing and graph-health issues.
- Added `search` for ranked symbol lookup over the indexed graph.
- Added `trace crud` for route-backed CRUD flow tracing.
- Added `events trace` for producer-consumer mapping on event-like targets.
- Added `events blast-radius` for downstream event impact tracing.
- Added `events orphans` for incomplete async-topology inspection.
- Added `impact` for cross-repo blast-radius summaries.
- Added `pack` for bounded task-oriented retrieval from the CLI.
- Added `conventions` for structural convention extraction.
- Added `generate claude-md` for assistant-facing rule generation.
- Added `generate codeowners` for ownership-file generation from indexed history and analytics.
- Added `compact` for reclaiming generated graph and metadata storage without deleting the index.
- Added `watch` for incremental update workflows.
- Added `serve` to expose the local MCP server, including `serve --watch` for one-process MCP serving plus live incremental indexing.
- Added MCP tool-call tracing with `serve --trace-tool-calls <PATH>`.
- Added structured command error handling for clearer operator-facing failures.
- Added release-pipeline flags and examples for archiveable index artifacts and invariant checks.

### MCP Server And Tooling

- Added a local stdio MCP server started through `gather-step serve`.
- Added orientation tools for schema and repo coverage inspection.
- Added search and traversal tools for symbol lookup, symbol metadata, callers, and callees.
- Added topology and impact tools for routes, CRUD flows, events, event blast radius, orphan topics, cross-repo dependencies, and shared type usage.
- Added contract-oriented tools for payload schema inspection, contract drift checks, and breaking-change candidate surfacing.
- Added context retrieval tools for short summaries, combined context, and task-shaped pack responses.
- Added repo intelligence tools for ownership, dead-code candidates, conventions, and repo overviews.
- Added compatibility aliases for clients that expect alternate tool names for the same retrieval behavior.
- Added read-only MCP behavior against indexed workspace state.
- Added proof-backed planning responses that distinguish direct calls, import bridges, shared-contract consumers, event paths, and co-change advisories.
- Added MCP tool tracing output for release and debugging workflows.

### Cross-Repo Analysis

- Added support for a unified graph across multiple repositories instead of isolated per-repo views.
- Added cross-repo dependency and impact tracing through shared graph surfaces.
- Added shared symbol and shared type usage inspection.
- Added cross-repo blast-radius summaries for symbol changes.
- Added system-level reasoning support for planning changes that span several repositories.
- Added proof construction for structural evidence and advisory evidence so rollout answers can separate confirmed downstream repos from probable ones.
- Added shared-contract analysis for API rollout planning and split downstream evidence.

### Route And CRUD Analysis

- Added route tracing for request-entry and handler mapping.
- Added CRUD flow tracing that connects callers, route handlers, continuation nodes, and persistence hints.
- Added end-to-end request-flow inspection intended for planning and debugging work.
- Added routing parity coverage to keep CLI, analysis, and MCP route evidence aligned.

### Event Topology

- Added event producer-consumer tracing across the indexed workspace.
- Added downstream event blast-radius analysis.
- Added orphan event and topic detection for incomplete async wiring.
- Added support for event-topology questions that are difficult to answer through plain symbol search alone.
- Added canonical event identity joins for producer/consumer rollout evidence.
- Added typed event-emitter extraction coverage and event rollout fixtures.

### Contracts And Schemas

- Added payload schema inspection for indexed route and event surfaces.
- Added producer-consumer contract drift analysis.
- Added breaking-change candidate detection tied to DTOs, producer payloads, and related contract surfaces.
- Added support for answering field-shape questions from indexed structural data rather than only source text matches.
- Added shared-contract rollout analysis for shared API and type changes.

### Framework And Extraction Coverage

- Added frontend hook extraction and strengthened React hook, frontend router, and Storybook extraction.
- Added gateway proxy, Azure, Next.js, Prisma, Drizzle, Mongoose, Tailwind, and workspace-manifest extraction improvements.
- Added path-guard handling and TypeScript/JavaScript traversal fidelity improvements for safer indexing.
- Added extraction fixtures for typed event emitters and framework-specific edge cases.

### Context Packs

- Added bounded context retrieval designed for assistant context windows.
- Added `planning` packs for pre-change orientation.
- Added `debug` packs for investigation-oriented context.
- Added `fix` packs for targeted edit surfaces.
- Added `review` packs for review-time risk and convention checks.
- Added `change_impact` packs for blast-radius and dependent-surface analysis.
- Added shared context-pack APIs across CLI and MCP retrieval flows.
- Added proof-aware anchor selection, virtual hook fallback handling, and ambiguity blocking when evidence is not strong enough.
- Added release-gate pack probes for planning, event, and impact targets.

### Repo Intelligence

- Added ownership inference based on repository history.
- Added dead-code candidate surfacing from graph reachability.
- Added convention extraction from repeated indexed structural patterns.
- Added repo overviews that combine graph shape with git-derived analytics.
- Added support for review-routing and change-risk discussions grounded in indexed evidence.

### Operating Model

- Added repeatable query behavior against a persisted indexed snapshot.
- Added bounded responses suited to assistant context windows and automation.
- Added local-only operation with no requirement to send source code to a remote service.
- Added a workflow where the assistant retrieves graph context on demand instead of rediscovering it from raw files each session.
- Added release validation with `gather-step-bench release-gate`, HIGH-contract probes, explicit planning/event/impact targets, and PR-oracle median F1/recall scoring.
- Latest v1.0 release evidence: the 2026-04-27 release gate passed on 26 repos, 13,604 files, 171,124 symbols, 405,384 edges, and 88,365 cross-repo edges in a 118.2s cold index; PR-oracle median F1 and recall were both 1.000.
- Added release-gate fixture coverage for shaped pass/fail cases, PR-oracle pass/fail cases, and single-repo oracle configuration.
- Added benchmark environment, reliability, tool-trace, PR-oracle, and release-gate modules.
- Replaced timestamped benchmark result snapshots with release-gate fixtures intended for repeatable validation.

### Privacy And Release Hygiene

- Removed stale private-workspace repository names from public docs, tests, and examples.
- Kept example repository names generic across release notes, feature copy, and fixture comments.
- Removed obsolete release helper surface that no longer matches the current release workflow.

### Documentation

- Added initial documentation site built with Astro and Starlight.
- Added getting-started documentation for installation, indexing, health checks, and MCP client setup.
- Added workspace-setup guidance for multi-repo indexing.
- Added concept documentation for polyrepo graphs, event topology, context packs, deterministic indexing, and architecture.
- Added operator workflow documentation for direct CLI usage patterns.
- Added full CLI reference covering the user-visible command surface.
- Added MCP tools reference documenting the tool groups and intended assistant usage.
- Added about and changelog pages for release-oriented project documentation.
- Updated the landing feature section to reflect proof-backed impact, release validation, recovery, tracing, and expanded framework extraction.
- Updated getting-started, workspace setup, operator workflow, and CLI reference docs for compaction, auto-recovery, release gates, `serve --watch`, and `--trace-tool-calls`.

## See Also

Binary releases are published at [https://github.com/thedoublejay/gather-step/releases](https://github.com/thedoublejay/gather-step/releases). Each release notes entry describes the user-visible changes for that version.
