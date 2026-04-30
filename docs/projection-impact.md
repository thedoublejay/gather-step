# Projection Impact

Projection impact traces static field relationships in the indexed graph. It answers which source fields, projected fields, readers, filters, indexes, and backfills should be reviewed when a field changes.

## CLI

Run the dedicated command against an indexed workspace:

```bash
gather-step --workspace . --repo backend projection-impact --target subtaskIds --evidence-verbosity full --json
```

The JSON response is the same shape used by the MCP tool:

- `source_fields`: fields the target is derived from.
- `projected_fields`: fields derived from the target or matching the target.
- `derivation_edges`: source-to-projection field chains.
- `readers`, `writers`, `filters`, `indexes`, `backfills`: file evidence grouped by field edge kind.
- `risk_hints`: planning checks such as `source_field_unreviewed`, `projection_writer_missing`, `backfill_unproven`, `index_or_search_mapping_unproven`, `filter_contract_impacted`, `frontend_only_focus`, and `deployed_owner_unchecked`.
- `missing_evidence`: empty when required static evidence is present, otherwise names missing graph facts such as `data_field`, `derivation_edge`, `writer`, `backfill`, or `index_or_search_mapping`.

Use the text output for quick terminal inspection:

```bash
gather-step projection-impact --target subtaskIds
```

Example text output for a projected-field query:

```text
projection impact for `subtaskIds`: 1 candidate(s), confidence high
source fields: backend:id, backend:subtasks
projected fields: backend:subtaskIds
projection chain: backend:id -> backend:subtaskIds; backend:subtasks -> backend:subtaskIds
next checks: deployed_owner_unchecked, filter_contract_impacted, source_field_unreviewed
```

Example JSON output for a source-field query:

```json
{
  "target": "subtasks",
  "resolved": true,
  "ambiguity": null,
  "candidates": [{ "repo": "backend", "field_path": "subtasks", "qualified_name": "backend::src/task_projection.ts::subtasks" }],
  "source_fields": [{ "repo": "backend", "field_path": "subtasks", "qualified_name": "backend::src/task_projection.ts::subtasks" }],
  "projected_fields": [{ "repo": "backend", "field_path": "subtaskIds", "qualified_name": "backend::src/task_projection.ts::subtaskIds" }],
  "derivation_edges": [
    {
      "source": { "repo": "backend", "field_path": "subtasks", "qualified_name": "backend::src/task_projection.ts::subtasks" },
      "projected": { "repo": "backend", "field_path": "subtaskIds", "qualified_name": "backend::src/task_projection.ts::subtaskIds" }
    }
  ],
  "readers": [],
  "writers": [{ "repo": "backend", "file_path": "src/task_projection.ts", "field_path": "subtaskIds", "edge_kind": "WritesField", "confidence": 900 }],
  "filters": [{ "repo": "backend", "file_path": "src/task_projection.ts", "field_path": "subtaskIds", "edge_kind": "FiltersOnField", "confidence": 900 }],
  "indexes": [{ "repo": "backend", "file_path": "src/task.index.ts", "field_path": "subtaskIds", "edge_kind": "IndexesField", "confidence": 900 }],
  "backfills": [{ "repo": "backend", "file_path": "migrations/backfill-subtasks.ts", "field_path": "subtaskIds", "edge_kind": "BackfillsField", "confidence": 900 }],
  "risk_hints": ["deployed_owner_unchecked", "filter_contract_impacted", "source_field_unreviewed"],
  "missing_evidence": [],
  "confidence": "high"
}
```

Example JSON output for an ambiguous target:

```json
{
  "target": "status",
  "resolved": true,
  "ambiguity": "multiple_field_candidates",
  "candidates": [
    { "repo": "backend", "field_path": "status", "qualified_name": "backend::src/account.ts::status" },
    { "repo": "backend", "field_path": "status", "qualified_name": "backend::src/billing.ts::status" }
  ],
  "source_fields": [],
  "projected_fields": [],
  "derivation_edges": [],
  "readers": [],
  "writers": [],
  "filters": [],
  "indexes": [],
  "backfills": [],
  "risk_hints": ["needs_disambiguation", "projection_chain_unproven"],
  "missing_evidence": ["derivation_edge"],
  "confidence": "medium"
}
```

## MCP

Use the `projection_impact` tool:

```json
{
  "target": "subtaskIds",
  "repo": "backend",
  "limit": 20,
  "evidence_verbosity": "full"
}
```

Set `evidence_verbosity` to `summary` to cap large evidence lists while keeping source/projected fields, derivation chains, risks, and missing-evidence fields intact.

The tool is read-only. It does not infer which deployed runtime owns a service. When projection evidence exists, planning packs may include a short next step and `projection_impact:*` gap hints, but the full evidence stays behind the dedicated tool.

## Static Mapping Files

JSON/YAML index mapping extraction is intentionally limited to filenames containing `mapping`, `index`, `search`, or `projection`. This lets search/projection config files contribute `IndexesField` evidence without reclassifying ordinary JSON/YAML manifests such as `package.json` or `tsconfig.json`.

## Schema Reset

This feature adds `DataField` nodes and field-level edges:

- `ReadsField`
- `WritesField`
- `DerivesFieldFrom`
- `FiltersOnField`
- `IndexesField`
- `BackfillsField`

Old generated graph/search state should be rebuilt. There is no compatibility migration for pre-projection-impact stores.

```bash
gather-step reindex
```

## Deployment Notes

For v2.1 deployments, treat projection impact as a generated-state schema change. Stop any long-running `watch` process, run `gather-step reindex`, then restart watch mode if needed.

If generated AI context files are committed in the workspace, regenerate them after the rebuild so the MCP tool table includes `projection_impact`.

Projection impact only traces static code and config evidence. It does not decide which service is deployed in production; verify deployed runtime ownership separately when multiple repos can serve the same business capability.

## RegASK Usage

The extractor is generic. RegASK-specific strength should come from fixtures, repo profiles, and Braingent advisory learnings. Core graph facts must come from indexed code/config evidence, not project names or ticket-specific assumptions.

For RegASK planning, use projection impact to check the field chain first, then separately verify deployed runtime ownership when duplicate or transitioning services exist.
