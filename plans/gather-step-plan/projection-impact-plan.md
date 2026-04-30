# Gather Step Implementation Plan: Projection Impact

**Status:** v2.1 released and complete as of 2026-04-30. Follow-on data-shape awareness work continues in `v2.2-data-shape-awareness-plan.md`.

## Summary

Build projection impact as a graph-backed analysis that answers: "If this source field changes, which derived fields, persisted projections, readers, filters, indexes, backfills, and planning risks must be reviewed?"

The first implementation target is static projection tracing. It identifies projection-related runtime surfaces, but it does not infer deployed runtime ownership. Deployed runtime ownership is a separate workstream because it uses a different evidence class: CI workflows, Helm releases, deployment manifests, repo profiles, and advisory notes.

Old Gather Step schema compatibility is explicitly out of scope. If the graph vocabulary changes, bump the schema and require a clean rebuild/reindex.

**ELI5:** Gather Step should stop treating a field as one isolated string. It should notice when `task.subtasks` creates `subtaskIds`, when code filters or indexes that projected field, and when a plan forgot the source field or migration work.

## Goals

- Add a generic projection-impact capability that works beyond any single workspace.
- Make it stronger for particular workspaces through fixtures, repo profiles, and advisory hints without hardcoding customer-specific rules into core logic.
- Keep the MVP scoped to extractable static evidence first.
- Use dedicated CLI/MCP surfaces first, then inject short projection summaries into existing `planning` and `change_impact` packs.
- Add regression oracles that catch the original planning failure shape and the later "which consumer is deployed?" failure shape.

## Non-Goals

- No backward compatibility with old `.gather-step` graph schema versions.
- No full database ERD, live database introspection, or runtime query tracing.
- No new `PackMode` for projection impact in the MVP.
- No customer-specific invariants in core graph, parser, storage, or analysis logic.
- No deployed-runtime ownership inference inside the projection MVP.
- No attempt to solve every hidden-coupling case in the first slice.

## Current Code Anchors

- Graph schema: `crates/gather-step-core/src/schema.rs`
- Shared graph data and ID contracts: `crates/gather-step-core/src/graph.rs`
- Virtual/shared node helpers: `crates/gather-step-core/src/virtual_nodes.rs`
- Parser entrypoints: `crates/gather-step-parser/src/tree_sitter.rs`, `crates/gather-step-parser/src/ts_js_swc.rs`, `crates/gather-step-parser/src/frameworks/`
- Parser payload/shared/event helpers: `crates/gather-step-parser/src/payload.rs`
- Parser fixture harness: `crates/gather-step-parser/tests/extraction_fidelity.rs`
- Storage/indexing: `crates/gather-step-storage/src/indexer.rs`, `crates/gather-step-storage/src/graph_store.rs`, `crates/gather-step-storage/src/schema_version.rs`
- Analysis modules: `crates/gather-step-analysis/src/impact.rs`, `crates/gather-step-analysis/src/query.rs`, `crates/gather-step-analysis/src/proofs.rs`, `crates/gather-step-analysis/src/canonical.rs`, `crates/gather-step-analysis/src/pack_assembly.rs`
- CLI commands: `crates/gather-step-cli/src/commands/impact.rs`, `crates/gather-step-cli/src/commands/pack.rs`, `crates/gather-step-cli/src/commands/reindex.rs`, `crates/gather-step-cli/src/commands/clean.rs`
- Oracle harness: `crates/gather-step-cli/tests/pack_oracle.rs`, `tests/fixtures/oracle/`
- Benchmark oracle contracts: `crates/gather-step-bench/src/planning_oracle.rs`, `crates/gather-step-bench/src/main.rs`
- MCP pack tooling and registration: `crates/gather-step-mcp/src/tools/packs.rs`, `crates/gather-step-mcp/src/server.rs`, `crates/gather-step-mcp/src/lib.rs`
- Benchmark gate: `benchmark/CI.md`

## Subagent Review Incorporated

- QA review: add red/green projection oracles, clean-slate schema rebuild acceptance, negative parser fixtures, strict CLI/MCP JSON snapshots, and a separate deployment-owner oracle.
- Architecture review: split projection tracing from deployed-runtime ownership, keep schema minimal until extractor proof exists, keep workspace-specific knowledge advisory, and prefer a dedicated CLI/MCP tool over a new pack mode.
- Code-map review: schema enum changes must stay aligned with graph data/id contracts, parser virtual-node conventions, both storage indexer parse paths, proof mapping, MCP server registration, and benchmark oracle edge-kind assertions.

## Workstream A: Projection Impact MVP

**Scope:** fixture/oracle contract, minimal graph vocabulary, TypeScript/Nest/Mongo extraction, projection-chain analysis, dedicated CLI/MCP tool, then short summary injection into existing packs.

**ELI5:** This is the part that catches "we changed the projected field but forgot the source field, filters, search index, and backfill."

### A0. Lock Review Contract And Failing Oracles

Implement tests before implementation changes.

1. Add `tests/fixtures/oracle/projection_impact_stale_projection/`.
   - Model a source field such as `Task.subtasks`.
   - Model a projected/persisted field such as `subtaskIds` or `subtasksCount`.
   - Include frontend usage that looks plausible but is not sufficient.
   - Include missing or stale backfill/index evidence.
   - `→ verify: oracle fails before implementation because Gather Step cannot identify source-to-projection impact or the missing runtime surfaces.`

2. Add `tests/fixtures/oracle/projection_impact_fixed_projection/`.
   - Include source field, projection writer, readers, filters, index/search mapping, and backfill/migration evidence.
   - Assert the report includes both source and projected fields.
   - Assert risk hints are lower or absent when required surfaces are present.
   - `→ verify: oracle fails before implementation, then passes after parser, storage, analysis, and CLI/MCP work land.`

3. Extend oracle expectations only as far as needed.
   - Candidate expectation fields: `expected_projection_fields`, `expected_source_fields`, `expected_projection_risks`, `expected_backfill_files`, `expected_index_files`, `forbidden_focus_only_files`.
   - Keep existing oracle fields for `require_top1_canonical`, `expected_structural_repos`, and response-size budgets.
   - → verify: `cargo test -p gather-step --test pack_oracle projection_impact -- --nocapture` reports clear assertion failures rather than ambiguous pack text diffs.

4. Add a benchmark-oracle entry if the fixture is eligible for CI benchmark coverage.
   - `→ verify: benchmark gate still reports bounded response size and no budget regression for existing pack oracles.`

**Review checkpoint R1:** Review fixture realism and expectation names before touching graph schema.

### A1. Minimal Graph Schema And Rebuild Path

Add only the vocabulary required by A0 oracles. Defer richer node kinds until extractor fixtures prove they need to be first-class.

1. Confirm graph serialization and schema-version boundaries before enum changes.
   - Check how `NodeKind`/`EdgeKind` changes flow through `NodeData`, `EdgeData`, `node_id`, `ref_node_id`, graph storage, search storage, and schema summaries.
   - Use `crates/gather-step-storage/src/search_store.rs` schema-version checks as precedent and confirm the graph-store equivalent.
   - `→ verify: reviewer can point to the exact storage/schema guard that will fail old stores and guide clean rebuild.`

2. Add `NodeKind::DataField`.
   - Represents a persisted, projected, payload, or queryable field by stable field path.
   - Example properties: `field_path`, `entity`, `source`, `confidence`, `evidence_kind`.
   - Do not add `Projection`, `DataIndex`, or `Migration` nodes in the first pass. Represent those as existing `File` or `Function` evidence nodes connected to fields.
   - → verify: `NodeKind::all`, string conversions, serialization, and schema summary tests include `DataField`.

3. Add only field-level edge kinds needed for traversal.
   - `ReadsField`: code reads a field.
   - `WritesField`: code writes or persists a field.
   - `DerivesFieldFrom`: one field is derived from another field.
   - `FiltersOnField`: query/search/filter logic depends on a field.
   - `IndexesField`: index/search mapping covers a field.
   - `BackfillsField`: migration/backfill code repairs or populates a field.
   - Avoid `ProjectsAs` initially because it overlaps with `DerivesFieldFrom`.
   - → verify: `EdgeKind::all`, string conversions, serialization, and graph query tests include only these new edges.

4. Bump graph storage schema version.
   - No compatibility migration required.
   - Old schema should fail with rebuild guidance.
   - → verify: `cargo test -p gather-step-storage schema_version` covers unsupported-schema failure messaging.

5. Add clean-slate rebuild acceptance.
   - Start with no `.gather-step`.
   - Run reindex.
   - Confirm registry, graph storage, schema summary, and `list_repos` still work.
   - → verify: `gather-step reindex --json` from an empty state recreates storage and allows projection queries plus existing MCP repo listing.

**Review checkpoint R2:** Review schema diff before parser work. If A0 cannot be represented with this vocabulary, expand schema explicitly and document why.

### A2. Parser Extraction Fixtures

Use small parser fixtures first, then the oracle workspace.

1. Add parser fixtures for source and projected field discovery.
   - TypeScript class/interface properties.
   - Nest/Mongoose schema fields and decorators such as `@Prop()`.
   - Object literal projection assignments such as `subtaskIds: task.subtasks?.map(...)`.
   - Optional chaining and nullish coalescing around source fields.
   - → verify: `cargo test -p gather-step-parser extraction_fidelity projection_fields` proves expected `DataField` nodes and `DerivesFieldFrom` edges are emitted.

2. Add parser fixtures for read/write/query evidence.
   - Mongo operators: `$set`, `$unset`, `$push`, `$pull`, `$addToSet`, `$inc`.
   - Query/filter shapes: `find({ "subtaskIds": ... })`, query builders, aggregation `$match`.
   - Aggregation and projection shapes: `$project`, `$addFields`, `$lookup` when field paths are statically visible.
   - `→ verify: parser tests prove `ReadsField`, `WritesField`, and `FiltersOnField` edges with stable field IDs.`

3. Add parser fixtures for index/search/backfill evidence.
   - Migration/backfill scripts that write or repair a field.
   - Atlas Search or local index mapping files when they are JSON/YAML and statically parseable.
   - `→ verify: parser/storage tests prove `BackfillsField` and `IndexesField` edges connect to `DataField` nodes.`

4. Add negative fixtures for false-positive control.
   - Unrelated `*Count` names that are local variables only.
   - Log messages or translation strings containing dotted field paths.
   - UI-only computed values that are not persisted and do not query storage.
   - Test fixtures or mocks that should not become production projection evidence unless indexed as test evidence.
   - `→ verify: negative fixtures emit no projection edges, or emit low-confidence advisory evidence that analysis can ignore by default.`

5. Keep extractors generic.
   - Domain-like names are allowed in oracle fixtures.
   - Core extraction logic must key off syntax and evidence type, not project or ticket names.
   - `→ verify: fixtures include at least one separate-domain naming example so `subtaskIds` is not a special case.`

6. Keep parser branch behavior consistent.
   - Update both `parse_file_with_packs` and `parse_file_with_context` paths where extraction semantics depend on enabled packs or context.
   - Preserve existing shared-symbol/event virtual-node conventions in `virtual_nodes.rs` and `payload.rs`.
   - `→ verify: cold index, hot reindex, parser-only fixtures, and oracle fixtures emit the same projection evidence for the same source file.`

**Review checkpoint R3:** Review extraction output before storage and analysis rely on it.

### A3. Storage And Indexer Integration

Thread parser-produced field nodes and field edges into the persisted graph without special casing projection logic in storage.

1. Persist `DataField` nodes and new field edges from `ParsedFile`.
   - Use deterministic IDs: repo + normalized entity/field path + evidence source.
   - Preserve `owner_file`, source spans, confidence, and evidence kind.
   - `→ verify: integration test indexes a fixture workspace and can query field nodes and edges from the graph store.`

2. Update schema summary, search policy, and metadata reporting if they enumerate node or edge kinds.
   - `→ verify: schema summary includes field-level vocabulary and no unknown node/edge warnings appear after reindex.`

3. Validate incremental and clean indexing.
   - One-file reindex updates changed field evidence.
   - Clean rebuild produces the same field graph deterministically.
   - `→ verify: existing incremental reindex test still passes, plus a projection-specific clean rebuild test compares expected field edges.`

4. Confirm storage write boundaries.
   - Projection evidence should enter through normal parser output ingestion and bulk insert/reconciliation paths, not a side-channel analysis write.
   - `→ verify: graph-store persistence tests prove projection nodes/edges survive process restart and re-open.`

**Review checkpoint R4:** Review graph output before analysis ranking is implemented.

### A4. Projection Impact Analysis

Add analysis as a dedicated module, likely `crates/gather-step-analysis/src/projection_impact.rs`.

1. Define request and report structs.
   - Request inputs: `target`, optional repo scope, optional max results, optional evidence verbosity.
   - Report sections: `target`, `source_fields`, `projected_fields`, `derivation_edges`, `writers`, `readers`, `filters`, `indexes`, `backfills`, `risk_hints`, `missing_evidence`, `confidence`.
   - `→ verify: unit tests serialize stable reports with empty arrays rather than missing keys.`

2. Resolve target to field candidates.
   - Accept exact field path, symbol-like names, and file/symbol hints when available.
   - Return ambiguity when multiple fields match instead of silently picking one.
   - `→ verify: ambiguous target fixture returns multiple candidates and a `needs_disambiguation` risk.`

3. Traverse projection chains.
   - Follow `DerivesFieldFrom` in both useful directions: source to projection and projection to source.
   - Collect field readers, writers, filters, indexes, and backfills.
   - Bound traversal depth and result count to protect pack budgets.
   - `→ verify: stale and fixed oracle fixtures return expected source/projection chains and bounded output.`

4. Produce planning risk hints.
   - `source_field_unreviewed`
   - `projection_writer_missing`
   - `backfill_unproven`
   - `index_or_search_mapping_unproven`
   - `filter_contract_impacted`
   - `frontend_only_focus`
   - `deployed_owner_unchecked` when deployment evidence is outside the MVP.
   - `→ verify: stale oracle reports required risks; fixed oracle clears risks when evidence exists.`

5. Keep workspace-specific enhancements advisory.
   - Profile/advisory hints may influence wording and suggested next checks.
   - They must not create hard graph facts unless code or config evidence exists.
   - `→ verify: separate-domain fixture still works with no advisory data loaded.`

6. Integrate proof mapping only after the dedicated report is stable.
   - Add projection edge proof labels in `proofs.rs` so pack summaries can explain why a field or file was included.
   - Keep canonicalization helpers consistent with virtual/shared symbol IDs.
   - `→ verify: oracle assertions tied to concrete edge kinds still pass and projection proofs cite the expected new edge kinds.`

**Review checkpoint R5:** Review report shape and risk taxonomy before public CLI/MCP exposure.

### A5. CLI And MCP Surface

Expose projection impact through a dedicated surface. Do not add a new pack mode in the MVP.

1. Add a CLI command.
   - Preferred command: `gather-step projection-impact --target <field-or-symbol> --json`.
   - If existing CLI conventions make it lower churn, place it under `gather-step impact projection`; choose once before implementation and update this plan.
   - `→ verify: CLI JSON golden test covers stable keys, empty arrays, ambiguity, and bounded output.`

2. Add MCP tool `projection_impact`.
   - Inputs mirror the CLI request.
   - Structured content mirrors the analysis report.
   - Use stable null/empty-array conventions.
   - Register the tool in MCP server tool listing and dispatch, not only in `tools/packs.rs`.
   - `→ verify: MCP call_tool("projection_impact") test asserts registration, input validation, and structured-content snapshot.`

3. Keep text output concise.
   - Show target, most likely source/projection chain, missing evidence, and next checks.
   - `→ verify: text output smoke test confirms no panic and useful headings for empty, ambiguous, and successful cases.`

**Review checkpoint R6:** Review CLI/MCP output before injecting any summary into packs.

### A6. Pack Integration

Inject projection impact into existing planning surfaces only after the dedicated tool is stable.

1. Add a short projection summary to `planning` packs when the target resolves to field evidence.
   - Include only high-signal risk hints and key files.
   - Keep detailed evidence behind the dedicated tool.
   - `→ verify: pack oracle asserts projection hints appear for the stale projection fixture and response budget remains within limits.`

2. Add a short projection summary to `change_impact` packs.
   - Prioritize affected source/projection fields, filters, indexes, and backfills.
   - → verify: `change_impact` oracle includes projection evidence without changing unrelated pack rankings.

3. Preserve existing modes.
   - Do not add `ProjectionImpact` to `PackMode` unless a later review proves a dedicated mode is necessary.
   - `→ verify: existing `planning`, `debug`, `fix`, `review`, and `change_impact` tests still pass.`

4. Keep CLI `impact` and `pack change_impact` behavior from diverging silently.
   - If projection evidence is shown in both places, route both through the same analysis/report structs.
   - `→ verify: CLI impact parity checks and pack oracle checks agree on core projection evidence.`

**Review checkpoint R7:** Review final pack behavior against existing pack oracles and benchmark budgets.

### A7. Documentation And Operator Workflow

Document the workflow as an implementation-facing operator guide.

1. Add CLI/MCP examples.
   - Show a source-field query.
   - Show a projected-field query.
   - Show ambiguous target output.
   - `→ verify: docs examples match CLI JSON/text snapshots.`

2. Document schema reset requirements.
   - Explain that old `.gather-step` stores are invalid after this release.
   - Include clean/reindex commands.
   - `→ verify: docs mention no compatibility migration and point to `clean`/`reindex`.`

3. Document workspace-specific strengthening path.
   - Workspace-specific hints come from fixtures, advisory notes, and repo profiles.
   - Core logic remains generic.
   - `→ verify: docs explicitly say workspace-specific hints are advisory unless backed by indexed evidence.`

**Review checkpoint R8:** Review docs with implementation diff before release.

## Workstream B: Deployed Runtime Ownership Guardrail

**Scope:** catch the later failure mode where two plausible repos/apps contain a consumer, but only one is actually deployed for the environment.

This should follow Workstream A or run in parallel only if it does not expand projection MVP schema. It is not required for the first projection-impact CLI/MCP tool.

**ELI5:** Projection impact tells us what code and fields are related. Runtime ownership tells us which of the plausible services is actually the one running in staging or prod.

### B0. Deployment-Owner Oracle

1. Add `tests/fixtures/oracle/deployed_notification_runtime_owner/`.
   - Include two plausible notification consumers.
   - One candidate has consumer code but no deployment evidence.
   - The other candidate has consumer code plus GitHub Actions/Helm deployment evidence.
   - Model the duplicate-consumer failure shape using generic fixture names.
   - `→ verify: oracle fails before implementation because Gather Step cannot prove which candidate is deployed.`

2. Add expectation fields or reuse rollout oracle fields.
   - Prefer existing `require_top1_canonical`, `expected_structural_repos`, and advisory separation fields where they fit.
   - Add deployment-specific expectations only if existing fields cannot express the proof.
   - `→ verify: deployed app satisfies canonical/structural expectations; non-deployed twin remains advisory.`

**Review checkpoint B1:** Review fixture and expected proof before adding deployment extraction.

### B1. Deployment Evidence Extraction

1. Extract deployment facts from GitHub Actions and Helm files.
   - Workflows: service/app name, image/build context, helm release, environment.
   - Helm: chart/release/app labels, values file paths, deployment names.
   - Manual workflow triggers when they deploy the same runtime.
   - `→ verify: parser/storage fixture emits deployment evidence for the deployed candidate and not for the standalone twin.`

2. Keep deployment extraction separate from projection extraction.
   - Use generic service/deployment evidence vocabulary.
   - Do not infer production truth from repo names.
   - `→ verify: duplicate service fixture reports `duplicate_service_candidates` until deployment evidence disambiguates it.`

### B2. Runtime Owner Analysis

1. Add analysis that ranks candidate consumers by deployment proof.
   - Inputs: topic/event/consumer target, repo scope, optional environment.
   - Signals: code consumer, workflow deploy, Helm release, recent substantive commits, profile/advisory hints.
   - Warnings: `repo_name_bias`, `duplicate_service_candidates`, `migration_target_not_live`, `deployment_owner_mismatch`, `deployed_consumer_unproven`.
   - `→ verify: fixture ranks deployed candidate first and flags standalone candidate as advisory/migration-only when evidence supports that.`

2. Integrate with planning packs as a guardrail.
   - Planning packs should ask "which candidate is deployed?" when duplicate consumers exist.
   - `→ verify: planning oracle for the duplicate notification fixture does not stop at the first repo-name match.`

### B3. Advisory/Profile Integration

1. Add advisory lookup support.
   - Advisory notes can raise risk hints or suggest checks.
   - They must not override indexed deployment evidence.
   - `→ verify: fixture passes without advisory data and gets stronger wording with advisory/profile data.`

2. Capture the deployment-owner learning as reusable context if not already captured.
   - Learning: deployed consumers should be distinguished from standalone or migration-target consumers using indexed deployment evidence.
   - `→ verify: advisory record validates and reindexes.`

**Review checkpoint B2:** Review deployment-owner behavior separately from projection-impact release readiness.

## Backlog: Other Hidden-Coupling Cases

These are real planning-risk classes, but they should not be bundled into Workstream A unless a ticket explicitly needs them.

1. Producer/consumer lifecycle coverage.
   - Detect event producers, all consumers, ignored event types, and rollout asymmetry.
   - `→ verify: event rollout oracle catches producer-only or consumer-only plans.`

2. Feature flag, permission, and visibility gates.
   - Detect flags, roles, ABAC/RBAC checks, and UI/API visibility coupling.
   - `→ verify: fixture catches a plan that updates behavior but skips permission/filter gates.`

3. Enumerated contract and allowlist coverage.
   - Detect enum values, string allowlists, validators, DTOs, OpenAPI schemas, and frontend switch/case usage.
   - `→ verify: fixture catches a new enum value that updates backend only.`

4. Audit, observer, webhook, and notification side effects.
   - Detect decorators, subscribers, event emitters, and outbound side-effect paths.
   - `→ verify: fixture catches a model change that skips audit/notification side effects.`

5. Background, scheduler, admin, and automation paths.
   - Detect cron jobs, queue processors, admin scripts, and one-off repair tools.
   - `→ verify: fixture catches user-facing changes that skip background writers.`

6. UI data-grid and filter-state contracts.
   - Detect persisted filters, query params, table column IDs, and export/report mappings.
   - `→ verify: fixture catches backend field rename without UI filter/export update.`

## Implementation Order

1. A0 oracles and expectation contract.
2. A1 minimal graph schema plus clean rebuild acceptance.
3. A2 parser fixtures and extraction.
4. A3 storage/indexer persistence.
5. A4 analysis report and risk taxonomy.
6. A5 dedicated CLI/MCP tool.
7. A6 pack integration.
8. A7 docs and schema-reset workflow.
9. B0-B3 deployed runtime ownership guardrail.
10. Backlog cases only after separate review.

**ELI5:** First make the failing examples concrete, then teach the graph the smallest new facts it needs, then expose one reliable tool, then let planning packs consume a short version.

## Verification Matrix

- Schema: `cargo test -p gather-step-core schema`
- Parser: `cargo test -p gather-step-parser extraction_fidelity projection`
- Storage: `cargo test -p gather-step-storage schema_version projection`
- Analysis: `cargo test -p gather-step-analysis projection_impact`
- CLI integration: `cargo test -p gather-step --test integration_pipeline projection_impact`
- Oracle: `cargo test -p gather-step --test pack_oracle projection_impact`
- MCP: `cargo test -p gather-step-mcp projection_impact`
- Benchmark: follow `benchmark/CI.md` for oracle budget checks.
- Final gate: `just ready` if local time and dependencies allow.

## Done Criteria

- Projection-impact stale and fixed oracles pass.
- Negative parser fixtures prevent obvious false positives.
- Clean-slate reindex works after schema bump.
- Dedicated CLI and MCP projection-impact surfaces return stable structured output.
- Existing pack modes still pass and do not gain a new projection-specific mode.
- Planning/change-impact packs include concise projection hints without budget regressions.
- No customer-specific rule exists in core extraction or analysis logic.
- Deployed runtime ownership is either implemented as Workstream B or clearly left out of the projection MVP with `deployed_owner_unchecked` risk hints.

## Open Review Questions

1. Should the CLI command be top-level `gather-step projection-impact` or nested under `gather-step impact projection`?
   - `→ verify: decide before A5 implementation and update command docs/tests once.`

2. Do `DataIndex` or `Migration` need first-class nodes after A2 fixtures, or are `File`/`Function` evidence nodes plus `IndexesField`/`BackfillsField` edges enough?
   - `→ verify: answer during R2/R3 based on extractor output, not speculation.`

3. Should deployment ownership add new graph vocabulary, or can it reuse `Service`, `Repo`, `PartOf`, `OwnedBy`, and existing event edges with deployment metadata?
   - `→ verify: answer during B1 based on the deployment-owner oracle.`

4. How much advisory profile data should pack integration read automatically?
   - `→ verify: keep MVP functional without advisory profile data; use it only to strengthen warnings and next checks.`
