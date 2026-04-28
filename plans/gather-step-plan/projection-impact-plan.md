# Gather Step Plan: Projection Impact

## Summary

Add a projection/data-model impact capability to Gather Step so planning packs can trace derived data fields across storage, event consumers, API mappers, filters, indexes, migrations, and frontend readers before implementation starts.

This plan is anchored by the REG-14003 failure mode: a UI column displayed `tasksCount`, but the real defect was a backend denormalized projection that counted `workflow.taskIds` only, ignored subtasks, needed a new `workflow.subtaskIds` field, needed event-handler updates, required a source-of-truth backfill, and created DB/search-index rollout risk.

**ELI5:** A normal code graph can show "this UI reads this API." A projection graph must also show "this API field is a stored shortcut, here is what keeps it fresh, here is what existing data needs, and here are the indexes/search mappings that can break if we change it."

## Goals

- Identify field-level projection chains such as `workflow.taskIds -> tasksCount -> datagrid column`.
- Surface write/update paths for projected fields: event handlers, use cases, migrations, and update operators.
- Surface read/query paths: mappers, DTO/API responses, filters, search/index mappings, and frontend consumers.
- Add a task-shaped `projection_impact` capability usable from CLI/MCP and pack orchestration.
- Add oracle coverage for the REG-14003 class of mistakes.

## Non-Goals

- Full ERD generation.
- Runtime database inspection by default.
- General-purpose schema migration generation.
- Guessing business semantics without code, ticket, or memory evidence.
- Supporting every ORM in the first release.

## Phase 1: Oracle And Fixture First

Create a small synthetic workspace fixture that models the REG-14003 pattern without depending on private RegASK code.

**ELI5:** Before teaching Gather Step the new trick, write the exam. The fixture should fail until the graph can see the important projection surfaces.

Steps:

1. Add fixture repos under `tests/fixtures/workspace/` for:
   - `frontend_projection`: reads `row.tasksCount`.
   - `alert_projection`: owns alert entity/workflow, mapper, event handlers, filters, migration, and Atlas/search mapping.
   - `task_projection`: emits generic task lifecycle events with parent/subtask discriminator.
   - `shared_contracts_projection`: owns event body/type definitions.
   → verify: fixture files parse under the existing indexer without new extraction logic.

2. Add an oracle scenario under `tests/fixtures/oracle/projection_impact_reg_14003_like/`.
   Required evidence should include:
   - alert workflow model/entity file.
   - mapper that derives `tasksCount`.
   - parent-only `taskIds` field.
   - missing or new `subtaskIds` field depending on fixture stage.
   - task-created/task-deleted consumer handlers.
   - task event contract with discriminator.
   - backfill migration.
   - datagrid/filter code using the projected field.
   - Atlas/search-index mapping or DB index file.
   → verify: oracle fails before implementation with clear missing expected files/edge kinds.

3. Extend oracle assertions only as needed for projection-specific expectations.
   Candidate additions:
   - `expected_projection_fields`
   - `expected_projection_writers`
   - `expected_projection_readers`
   - `expected_projection_rollout_risks`
   → verify: existing oracle scenarios remain unchanged or require only additive defaults.

## Phase 2: Core Graph Vocabulary

Add enough graph vocabulary to represent projection chains without over-modeling databases.

**ELI5:** Add labels for the things Gather Step needs to connect: field, writer, reader, source of truth, derived output, index, and migration.

Candidate node kinds:

- `DataField`: stable field path such as `Alert.workflow.taskIds` or `workflow.subtaskIds`.
- `Projection`: derived output such as `tasksCount`.
- `DataIndex`: DB/search index or Atlas mapping surface.
- `Migration`: one-shot data repair or schema/data migration.

Candidate edge kinds:

- `ReadsField`
- `WritesField`
- `DerivesFieldFrom`
- `ProjectsAs`
- `FiltersOnField`
- `IndexesField`
- `BackfillsField`
- `UsesSourceOfTruth`

Implementation notes:

- Keep external IDs deterministic and field-path based.
- Reuse existing `File`/symbol nodes instead of creating redundant code-location nodes.
- Treat field-path evidence as best-effort; graph edges should carry metadata for confidence/source pattern.

→ verify: graph serialization, storage, search, schema summary, and existing tests pass with additive variants.

## Phase 3: TypeScript/Nest/Mongo Extraction MVP

Add parser/semantic augmentation for common projection patterns in TypeScript services.

**ELI5:** Scan code for the shapes engineers already write: class fields, `alert.workflow?.taskIds`, Mongo update operators, filters, migrations, and search mappings.

Extraction targets:

- Class/interface/entity fields:
  - `workflow.taskIds`
  - `workflow.subtaskIds`
  - `tasksCount`
- Property reads:
  - optional chaining and member access.
  - string paths like `"workflow.subtaskIds"`.
- Derived assignments:
  - `tasksCount = taskIds.length + subtaskIds.length`
  - mapper object literals returning API rows.
- Mongo-style writes:
  - `$set`, `$addToSet`, `$pull`, `$push`, `$unset`
  - nested/dotted paths.
- Filter/query use:
  - `$or`, `$exists`, `$size`, `Contains`, search-filter builders.
- Migration/backfill markers:
  - files under migration directories.
  - aggregation pipelines.
  - collection names used as source of truth.
- Atlas/search-index mappings:
  - `dynamic: false`
  - explicit field mapping blocks.

Initial heuristics:

- Prefer strong evidence over broad matching.
- Only emit projection edges for nested field paths or fields that cross API boundaries.
- Record unresolved/weak evidence as warnings, not primary facts.

→ verify: parser unit tests cover each extraction pattern with minimal source snippets.

## Phase 4: Projection Impact Analysis

Build an analysis pass that starts from a target field/symbol/file and assembles the projection chain.

**ELI5:** Given `tasksCount`, walk backward to what data creates it and forward to who reads or queries it.

Traversal rules:

- From API/output field, find `ProjectsAs` and `DerivesFieldFrom`.
- From source fields, find writers, event consumers, filters, indexes, and migrations.
- From event consumers, join through existing event topology to producers and payload contracts.
- From queried fields, check for nearby `DataIndex` / search mappings.
- From new/changed fields, check for `Migration` / `BackfillsField`.

Risk hints:

- `missing_backfill`: projected field added or semantics changed without a migration/backfill.
- `missing_index`: field used in high-volume query/filter without known index.
- `missing_search_mapping`: field used in Atlas/search mapping with `dynamic: false` and no mapping.
- `event_discriminator_gap`: consumer handles a generic event but filters out known discriminator values.
- `future_events_only`: event handlers repair future state but existing records remain stale.
- `projection_semantics_gap`: derived name suggests aggregate/total but inputs cover a narrower source.

→ verify: analysis unit tests produce deterministic chains and risk hints for the fixture graph.

## Phase 5: CLI And MCP Surface

Expose projection impact as a first-class workflow without disrupting existing pack modes.

**ELI5:** Make it easy for an AI client to ask the right question: "what does changing this derived field affect?"

Options:

- Add `projection_impact` as a new pack mode.
- Add a dedicated CLI command such as `gather-step projection impact <target>`.
- Add an MCP tool such as `projection_impact` if response shape differs materially from existing packs.

Preferred first cut:

- Add dedicated CLI/MCP tool returning a projection-specific response.
- Feed a short projection summary into `planning_pack` and `change_impact_pack` when relevant.

Response should include:

- `target`
- `projection_fields`
- `source_fields`
- `writers`
- `readers`
- `filters`
- `indexes`
- `migrations`
- `event_links`
- `risks`
- `next_steps`
- `confidence`

→ verify: MCP schema snapshot or response-shape tests cover the new response without bloating existing pack responses.

## Phase 6: REG-14003-Like Planning Guardrail

Add a planning guardrail that detects when a user-facing field is likely a backend projection and nudges agents toward projection impact before planning implementation.

**ELI5:** If the user says "datagrid count is wrong," Gather Step should say "this looks like a derived backend value; inspect the projection chain first."

Guardrail triggers:

- Field names ending in `Count`, `Total`, `Status`, `has*`, or other derived-sounding names.
- Frontend reads a field with no local computation.
- Backend mapper returns same field.
- Field also appears in filters/search/index paths.
- Field is maintained by event handlers or update operators.

→ verify: planning pack for the oracle target includes projection impact next steps and does not over-focus on the frontend reader.

## Phase 7: Documentation And Operator Workflow

Document when to use projection impact and how to interpret risks.

**ELI5:** Give agents and humans a checklist for derived-data bugs: source, projection, existing data, indexes, rollout.

Docs to update:

- README feature list.
- MCP tool reference.
- CLI reference.
- Context-pack/operator workflow docs.
- Memory-backed planning docs, with Braingent as prior-learning source and Gather Step as current-code graph source.

→ verify: docs mention scope boundaries and do not imply runtime DB inspection or automatic migration generation.

## Rollout Strategy

1. Ship hidden/experimental CLI command behind docs-only usage.
   → verify: oracle passes; no existing pack behavior changes.

2. Enable MCP tool after response shape stabilizes.
   → verify: schema tests and budget tests pass.

3. Feed concise projection hints into `planning_pack`.
   → verify: existing planning oracles stay stable; REG-14003-like oracle requires the projection hint.

4. Add more framework packs only after real misses.
   → verify: new framework support adds fixture/oracle coverage before extractor code.

## Test Plan

- Parser unit tests for field-path extraction and Mongo/search patterns.
- Analysis unit tests for projection-chain assembly and risk classification.
- Oracle scenario for REG-14003-like planning.
- MCP response-shape tests for projection impact.
- CLI integration test for `projection impact`.
- Regression pass for existing pack oracles.

Commands:

```bash
cargo test -p gather-step-parser projection
cargo test -p gather-step-analysis projection
cargo test -p gather-step-mcp projection
cargo test -p gather-step-cli --test pack_oracle projection_impact_reg_14003_like
cargo test --workspace
```

## Complexity Estimate

- MVP oracle + graph vocabulary + limited extractor: 1-2 weeks.
- REG-14003-grade Mongo/Nest/Atlas support: 2-4 weeks.
- General framework coverage across ORMs/databases: 1-2 months.

Main complexity is not adding tool plumbing. Main complexity is extracting projection semantics reliably from real service code without noisy false positives.

## Open Questions

- Should `projection_impact` be a pack mode, a dedicated tool, or both?
- Should field-path nodes be global by path or scoped to entity/model?
- How should the tool distinguish DB indexes from search indexes in response shape?
- How much should Braingent learnings influence risk labels versus only planning prose?
- Should the first release support runtime schema snapshots, or keep strictly static-code only?

## Done Criteria

- REG-14003-like oracle fails without projection support and passes after implementation.
- Planning output identifies backend projection ownership before frontend change suggestions.
- Projection impact output surfaces source fields, writers, readers, filters, migrations, and index/search risks.
- Existing route/event/shared-contract packs do not regress.
- Documentation clearly states scope and limitations.
