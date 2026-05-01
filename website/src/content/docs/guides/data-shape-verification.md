---
title: Data-Shape Verification
description: Use planning packs to spot Mongo migration filters that may no longer match the live data shape.
---

Source types and schemas do not prove the current shape of production data. A
Mongo migration can be type-correct and still miss records because the filter
assumes an old field distribution.

Gather Step v2.3 adds these best-effort source-code signals:

- direct field access edges for typed TypeScript receivers, including local
  aliases and destructuring rebinds,
- generated Mongo `$type` probe plans for supported migration siblings,
- an optional payload filter-mismatch hint when an optional indexed payload
  field is also used in a migration filter, and
- broader static migration sibling detection for Mongoose and TypeORM.

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

`projection_impact` labels field evidence as `direct_field_access` or
`local_alias_field_access` when the parser can explain that origin.

This is deliberately scoped. Cross-function alias flow, dynamic property keys,
`any` / `unknown` receivers, generic container types, UI `Props` types, and
optional chaining beyond depth 1 are not tracked. Silence means "not detected",
not "safe".

## Migration Sibling Signal

When the pack target is a detected migration, the response includes a
`migration_siblings` band with:

- the collection name,
- sibling migration files on the same collection,
- the `up` symbol id for each sibling,
- the sibling file's latest indexed commit short SHA when git history is
  available,
- serialized Mongo filter literals from those siblings when available,
- generated Mongo `$type` probe plans for fields found in migration filters,
- optional payload evidence when an indexed payload contract marks that field
  optional.

## Supported Migration Shape

Detection is intentionally conservative. A file is treated as a Mongoose
migration when all of these are true:

- the path contains a `migrations` directory segment,
- the file imports or requires `mongoose`,
- the file exports `up` and `down`,
- the migration touches one or more static collections in `up` through
  `db.collection('<name>')` writes or statically resolved
  `mongoose.model(..., ..., '<name>')` model writes. Local imported model
  declarations are supported when the import resolves inside the repo.

TypeORM migration files are indexed when a static `queryRunner.query(...)` SQL
literal or supported `queryRunner` table method exposes the table name. Static
schema-qualified SQL table names are normalized to the table name.

The `migration_siblings` response includes a coverage note with the same
best-effort boundary. Knex, Prisma, Atlas migration definitions, dynamic
collection/table names, TypeORM entity metadata, and SQL WHERE-field extraction
remain conservative gaps. Silence from the `migration_siblings` band means "not
detected", not "safe".

## Planning Pack Signal

For a detected migration on `work_items`, the planning pack includes a hint in this
form:

```text
Run db.getCollection("work_items").aggregate([{ $group: { _id: { $type: '$<field>' }, count: { $sum: 1 } } }]) against a representative environment before trusting any filter that scopes by field type. Source code cannot prove schema-to-data drift.
```

Use the sibling filters as prompts, not proof. If an older migration filtered
`{ workflow: { $type: 'object' } }`, check whether records also exist with a
missing `workflow`, `null`, an array, or another shape before reusing the filter.

When the indexed payload contract says `workflow?` is optional and a migration
filter scopes by `workflow`, planning and change-impact surfaces add
`projection_impact:optional_payload_filter_mismatch` and ask for a runtime shape
probe.

Generated commands use `db.getCollection(<json-escaped name>)`, so collection
names such as `audit-log` and `foo.bar` remain safe to paste into the Mongo
shell. Gather Step prints the command only; it does not open a database
connection.

## Related Docs

- [Context packs](/concepts/context-packs/) explains what the planning pack
  returns.
- [Operator workflows](/guides/operator-workflows/) covers CLI and MCP usage.
