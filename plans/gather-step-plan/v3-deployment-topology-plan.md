# Gather Step Implementation Plan: Deployment Topology (v3)

**Status:** Implemented and verified on 2026-05-01 in branch `feat/v3-deployment-topology`.

## Summary

Build deployment topology as a graph-backed evidence layer that answers: "Which code services are actually deployed, how are they deployed, and which environment or shared infrastructure do they depend on?"

This work is intentionally separate from projection impact. Projection impact traces static code/data-shape relationships; deployment topology uses a different evidence class: Dockerfiles, Compose files, Kubernetes manifests, Helm templates/values, GitHub Actions, env files, platform-gitops layouts, repo profiles, and Braingent advisory learnings.

**ELI5:** Projection impact says "this field change touches these code paths." Deployment topology says "this code path is shipped as this service in this environment, with these env vars and shared dependencies." Both are graph facts, but they come from different files and should not be mixed silently.

## Source Plans Reviewed

- `plans/gather-step-plan/projection-impact-plan.md`
  - Deployment owner validation is explicitly out of scope for projection impact.
  - `deployed_owner_unchecked` remains a risk until deployment evidence exists.
- `plans/gather-step-plan/v2.2-data-shape-awareness-plan.md`
  - Keep config and parser output strict, bounded, and compatible with AI pack budgets.
- `plans/gather-step-plan/v2.3-data-shape-research-plan.md`
  - Preserve existing behavior while adding richer signals.
  - Redact sensitive runtime values from durable output.
- Braingent:
  - `topics/topic--ai-agent-memory/records/2026-04-30--learning--use-gather-step-cross-repo-for-plan-refresh-and-what-tooling-gaps-remain.md`
  - `topics/topic--ai-agent-memory/records/2026-04-28--learning--verify-deployed-source-before-editing-same-name-service-repo.md`
  - The key gap is that plan refreshes do not force deployed-source verification. A deployment-topology index should make that check explicit.
- External plan corpus:
  - `/Users/jjadonis/Documents/repos/plans/personal/gather-step-plans/plans/tasks-v2.md`
  - Carry forward the deploy graph scope into v3: Dockerfile, Compose, Kubernetes, Helm, GitHub Actions, env files, and platform-gitops layouts.

## Goals

- Add first-class deployment topology graph vocabulary.
- Add a dedicated `gather-step-deploy` crate for deployment artifact parsing.
- Parse common deployment evidence offline, deterministically, and without shelling out to Docker, Helm, kubectl, or GitHub.
- Store topology graph facts during indexing.
- Add deployment topology analysis surfaces for CLI and MCP.
- Feed deployment-owner evidence back into planning/change-impact surfaces without changing projection-impact semantics.
- Keep secrets and concrete env values out of durable output.

## Non-Goals

- No PR-head deployment indexing or webhook service.
- No live cloud/Kubernetes/GitHub API calls.
- No Helm rendering via `helm template`.
- No auth/RBAC, feature-flag topology, or ticket integration.
- No hardcoding private workspace names into core logic.

## Phase A: Schema And Config

**Goal:** Land the type surface used by deploy parsing, storage, and analysis.

**ELI5:** Before parsing deployment files, the graph needs words for what it will store: service, deployment, env var, secret, config map, workflow job, database, and broker.

1. Add topology node kinds.
   - `Deployment`
   - `EnvVar`
   - `Secret`
   - `ConfigMap`
   - `WorkflowJob`
   - `Broker`
   - `Database`
   - `→ verify: graph schema serialization tests cover the new node kinds`

2. Add topology edge kinds.
   - `DeployedAs`
   - `ReadsEnv`
   - `BackedBy`
   - `BuiltBy`
   - `Triggers`
   - `UsesBroker`
   - `UsesDatabase`
   - `→ verify: graph schema serialization tests cover the new edge kinds`

3. Add deployment config.
   - `deployment.include` for explicit artifact globs.
   - `deployment.gitops_roots` for configured platform-gitops layouts.
   - `deployment.env_files` for opt-in env parsing.
   - Defaults must be cheap and deterministic.
   - `→ verify: config parser accepts deployment config and still rejects unknown fields`

4. Add virtual node helpers.
   - `__deployment__<repo>__<name>`
   - `__env_var__<name>`
   - `__secret__<name>`
   - `__config_map__<name>`
   - `__broker__<kind>__<canonical_host_port_or_name>`
   - `__database__<kind>__<canonical_host_port_or_name>`
   - `→ verify: helper tests normalize separators, ports, and empty parts`

## Phase B: Deploy Parser Crate

**Goal:** Add `gather-step-deploy` as the deployment artifact parser boundary.

**ELI5:** Deployment formats are not source-code languages. Keeping them in their own crate avoids coupling the TypeScript/Python parser to YAML, env, Dockerfile, and Helm heuristics.

1. Scaffold `crates/gather-step-deploy`.
   - Public API returns parsed topology facts, diagnostics, and redacted evidence.
   - `→ verify: crate builds independently through workspace build`

2. Parse Dockerfiles.
   - Extract image aliases, `ENV` names, and exposed ports.
   - Do not store env values.
   - `→ verify: Dockerfile fixture emits deployment/service evidence and redacted env names`

3. Parse Compose files.
   - Extract services, image/build links, env names, `depends_on`, exposed ports, and common DB/broker image families.
   - Support mapping and list `environment` forms.
   - `→ verify: Compose fixture emits service-to-deployment and service-to-infra facts`

4. Parse Kubernetes manifests.
   - Route by `kind` from YAML documents.
   - Support `Deployment`, `StatefulSet`, `DaemonSet`, `Service`, `ConfigMap`, and `Secret`.
   - Read container env names and `envFrom` references.
   - `→ verify: multi-document fixture emits deployments, env vars, config maps, secrets, and stable diagnostics`

5. Parse Helm templates and values heuristically.
   - Use regex/YAML heuristics only.
   - No subprocess rendering.
   - `→ verify: Helm fixture emits lower-confidence facts and diagnostics for template uncertainty`

6. Parse GitHub Actions workflows.
   - Extract workflow jobs, image/build steps, deploy-ish steps, and workflow triggers.
   - `→ verify: workflow fixture emits `WorkflowJob` and `BuiltBy`/`Triggers` facts`

7. Parse env files.
   - Extract variable names only.
   - Redact or drop values.
   - `→ verify: env fixture snapshots contain names but no values`

## Phase C: Indexing Integration

**Goal:** Store deployment topology facts alongside existing graph facts.

**ELI5:** Parsing alone is not useful until the graph can answer questions from the parsed facts.

1. Discover deployment artifacts through config and cheap defaults.
   - Default families: Dockerfile, compose YAML, `.github/workflows/*.yml`, common k8s/helm directories.
   - Config can expand globs and opt in env files outside default naming.
   - `→ verify: fixture workspace indexes only expected deployment files`

2. Convert parser facts into graph nodes and edges.
   - Preserve owner file and source span when available.
   - Set confidence metadata based on structured vs heuristic evidence.
   - `→ verify: integration test reads graph and finds expected topology nodes/edges`

3. Keep incremental indexing bounded.
   - Deployment artifacts should be reparsed only when relevant files change.
   - `→ verify: incremental fixture updates a changed compose file without forcing full source reparse`

## Phase D: Analysis And Query Surfaces

**Goal:** Add read-only topology reports for humans and AI assistants.

**ELI5:** Users should not need to browse YAML manually. They should ask "where is this service deployed?" or "who consumes this env var?" and get compact evidence.

1. Add analysis module.
   - `where_deployed`
   - `service_env`
   - `env_var_consumers`
   - `undeployed_services`
   - `deployed_but_no_code`
   - `shared_infra`
   - `→ verify: analysis unit tests return stable JSON with empty arrays instead of missing keys`

2. Add CLI command.
   - `gather-step deployment-topology ...`
   - JSON mode is stable and redacted.
   - Human mode is concise.
   - `→ verify: CLI integration tests cover JSON and human output`

3. Add MCP tools.
   - Register topology tools with the same budgets/redaction model as existing pack tools.
   - `→ verify: MCP tool list and dispatch tests cover every new tool`

## Phase E: Planning Integration

**Goal:** Use topology evidence to reduce planning blind spots.

**ELI5:** A plan should warn when it touched code but did not verify the deployed service or real runtime owner.

1. Feed topology hints into planning/change-impact packs.
   - Replace generic `deployed_owner_unchecked` warnings with concrete topology evidence when present.
   - Preserve the warning when topology evidence is missing.
   - `→ verify: planning oracle fixture shows concrete deployed owner evidence for a deployed service`

2. Keep workspace-specific Braingent learnings advisory.
   - They can influence suggested checks.
   - They must not create hard deployment graph facts without code/config evidence.
   - `→ verify: neutral fixture works with no Braingent data loaded`

## Phase F: Docs And Release Notes

**Goal:** Make the new topology surface discoverable and safe to operate.

**ELI5:** A deployment topology feature handles sensitive-adjacent files, so docs must be explicit about what is indexed and what is redacted.

1. Update docs.
   - Config examples.
   - Redaction behavior.
   - Supported artifact families.
   - Known heuristic limits.
   - `→ verify: docs build passes`

2. Add release notes.
   - Feature summary.
   - Upgrade notes for schema/index rebuild.
   - `→ verify: release note file exists and names migration/reindex expectations`

## Implementation Notes

- Phase A landed graph vocabulary, virtual-node helpers, and parsed `deployment` config.
- Phase B added `gather-step-deploy` with redacted parsers for Dockerfile, Compose, Kubernetes, Helm-like YAML/templates, GitHub Actions, and env files.
- Phase C indexes deployment artifacts into graph-owned file batches and wires `deployment.include`, `deployment.gitops_roots`, and `deployment.env_files` into index/watch/serve paths.
- Phase D added the `deployment-topology` CLI command and six MCP tools: `where_deployed`, `service_env`, `env_var_consumers`, `undeployed_services`, `deployed_but_no_code`, and `shared_infra`.
- Phase E updates projection-impact planning risk: concrete deployment topology replaces `deployed_owner_unchecked` with `deployed_owner_topology_observed`; missing topology remains explicit evidence debt.
- Phase F updated README, CLI reference, MCP tools reference, configuration reference, and changelog notes with redaction and reindex expectations.

## Sequencing

1. Phase A first because parser and analysis need graph vocabulary.
   - `→ verify: schema/config tests pass`
2. Phase B next because storage integration should consume one stable parser API.
   - `→ verify: deploy crate tests pass`
3. Phase C after parser snapshots are stable.
   - `→ verify: fixture workspace indexing emits topology graph facts`
4. Phase D once graph facts exist.
   - `→ verify: CLI/MCP topology queries pass`
5. Phase E only after the direct query surface is reliable.
   - `→ verify: planning oracle covers concrete topology evidence`
6. Phase F last, after names and JSON shapes settle.
   - `→ verify: docs/release checks pass`
