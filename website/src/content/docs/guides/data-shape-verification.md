---
title: Data-Shape Verification
description: Use planning packs to spot Mongo migration filters that may no longer match the live data shape.
---

Source types and schemas do not prove the current shape of production data. A
Mongo migration can be type-correct and still miss records because the filter
assumes an old field distribution.

Gather Step v2.2 adds two best-effort source-code signals:

- direct field access edges for typed TypeScript receivers, and
- a focused planning-pack reminder for supported Mongoose migrations.

## Field Access Signal

When a TypeScript file reads or writes a direct dotted field path on a typed
receiver, Gather Step records `ReadsField` and `WritesField` edges against a
`DataField` node such as `WorkItem.workflow` or `WorkItem.workflow.stepIds`.

```ts
interface WorkItem {
  workflow?: { stepIds: string[] };
}

export function planWorkItem(item: WorkItem, id: string) {
  if (item.workflow) {
    item.workflow.stepIds.push(id);
  }
}
```

The field-aware surface is available through `projection_impact`, and dotted
CLI impact queries such as `gather-step impact WorkItem.workflow` use the same
field slice when an indexed data field exists. Planning and change-impact packs
also add a field-impact reminder when the target has direct field evidence.

This is deliberately scoped. Aliased reads, destructured rebinds, dynamic
property keys, `any` / `unknown` receivers, generic container types, UI `Props`
types, and optional chaining beyond depth 1 are not tracked in v2.2. Silence
means "not detected", not "safe".

## Migration Sibling Signal

When the pack target is a detected migration, the response includes a
`migration_siblings` band with:

- the collection name,
- sibling migration files on the same collection,
- the `up` symbol id for each sibling,
- the sibling file's latest indexed commit short SHA when git history is
  available,
- serialized `updateMany` filter literals from those siblings,
- a fixed aggregate query reminder for checking runtime field shape.

## Supported Migration Shape

Detection is intentionally conservative in v2.2. A file is treated as a
Mongoose migration only when all of these are true:

- the path contains a `migrations` directory segment,
- the file imports or requires `mongoose`,
- the file exports `up` and `down`,
- the migration touches exactly one collection in `up` through either:
  - `db.collection('<name>').updateMany(...)`, or
  - a same-file `mongoose.model(..., ..., '<name>')` model followed by
    `Model.updateMany(...)`.

The `migration_siblings` response includes a coverage note with the same
best-effort boundary. TypeORM, Knex, Prisma, Atlas migration definitions,
dynamic collection names, imported model resolution, and multi-collection
migrations are not treated as Mongoose migration siblings in v2.2. Silence from
the `migration_siblings` band means "not detected", not "safe".

## Planning Pack Signal

For a detected migration on `work_items`, the planning pack includes a hint in this
form:

```text
Run db.work_items.aggregate([{ $group: { _id: { $type: '$<field>' }, count: { $sum: 1 } } }]) against a representative environment before trusting any filter that scopes by field type. Source code cannot prove schema-to-data drift.
```

Use the sibling filters as prompts, not proof. If an older migration filtered
`{ workflow: { $type: 'object' } }`, check whether records also exist with a
missing `workflow`, `null`, an array, or another shape before reusing the filter.

## Related Docs

- [Context packs](/concepts/context-packs/) explains what the planning pack
  returns.
- [Operator workflows](/guides/operator-workflows/) covers CLI and MCP usage.
