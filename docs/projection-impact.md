# Projection Impact

Projection impact traces static field relationships in the indexed graph. It answers which source fields, projected fields, readers, filters, indexes, and backfills should be reviewed when a field changes.

## CLI

Run the dedicated command against an indexed workspace:

```bash
gather-step --workspace . --repo backend projection-impact --target subtaskIds --json
```

The JSON response is the same shape used by the MCP tool:

- `source_fields`: fields the target is derived from.
- `projected_fields`: fields derived from the target or matching the target.
- `derivation_edges`: source-to-projection field chains.
- `readers`, `writers`, `filters`, `indexes`, `backfills`: file evidence grouped by field edge kind.
- `risk_hints`: planning checks such as `backfill_unproven`, `index_or_search_mapping_unproven`, and `deployed_owner_unchecked`.

Use the text output for quick terminal inspection:

```bash
gather-step projection-impact --target subtaskIds
```

## MCP

Use the `projection_impact` tool:

```json
{
  "target": "subtaskIds",
  "repo": "backend",
  "limit": 20
}
```

The tool is read-only. It does not infer which deployed runtime owns a service. When projection evidence exists, planning packs may include a short next step and `projection_impact:*` gap hints, but the full evidence stays behind the dedicated tool.

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
gather-step clean --storage
gather-step index
```

## RegASK Usage

The extractor is generic. RegASK-specific strength should come from fixtures, repo profiles, and Braingent advisory learnings. Core graph facts must come from indexed code/config evidence, not project names or ticket-specific assumptions.

For RegASK planning, use projection impact to check the field chain first, then separately verify deployed runtime ownership when duplicate or transitioning services exist.
