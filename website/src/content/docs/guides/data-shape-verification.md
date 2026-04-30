---
title: Data-Shape Verification
description: Use planning packs to spot Mongo migration filters that may no longer match the live data shape.
---

Source types and schemas do not prove the current shape of production data. A
Mongo migration can be type-correct and still miss records because the filter
assumes an old field distribution.

Gather Step v2.2 adds a focused planning-pack reminder for supported Mongoose
migrations. When the pack target is a detected migration, the response includes
a `migration_siblings` band with:

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
- the migration touches exactly one `db.collection('<name>')` collection.

TypeORM, Knex, Prisma, Atlas migration definitions, and multi-collection
migrations are not treated as Mongoose migration siblings in v2.2. Silence from
the `migration_siblings` band means "not detected", not "safe".

## Planning Pack Signal

For a detected migration on `alerts`, the planning pack includes a hint in this
form:

```text
Run db.alerts.aggregate([{ $group: { _id: { $type: '$<field>' }, count: { $sum: 1 } } }]) against a representative environment before trusting any filter that scopes by field type. Source code cannot prove schema-to-data drift.
```

Use the sibling filters as prompts, not proof. If an older migration filtered
`{ workflow: { $type: 'object' } }`, check whether records also exist with a
missing `workflow`, `null`, an array, or another shape before reusing the filter.

## Related Docs

- [Context packs](/concepts/context-packs/) explains what the planning pack
  returns.
- [Operator workflows](/guides/operator-workflows/) covers CLI and MCP usage.
