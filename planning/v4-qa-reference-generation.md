# v4.0 Planning: QA Reference Generation

Status: planning idea
Date: 2026-05-02
Scope: Generate human-readable QA reference Markdown after implementation. This is not automated test generation.

## Summary

V4 should explore a post-implementation workflow where Braingent and Gather Step produce a QA reference pack: a Markdown file of manual test cases, E2E candidates, API checks, edge cases, and integration risks. The output should cover Jira acceptance criteria, changed code behavior, and downstream/upstream impact without assuming the developer implementation is correct.

ELI5: Today the loop helps decide what to build. This proposal adds a QA pass after the build: read the ticket, inspect what changed, trace affected systems, and write a practical checklist a QA engineer or AI agent can use to verify the work.

## Problem

Planning and implementation context are already stronger when Braingent provides durable memory and Gather Step provides the current code graph. The gap is the final testing loop:

- QA often needs manual test cases after the implementation is already done.
- Developers naturally test the path they intended to change, which can miss requirement gaps, negative paths, and integration blast radius.
- Jira acceptance criteria, code diffs, existing tests, API routes, UI pages, events, and cross-repo consumers are fragmented.
- The desired first output is Markdown that humans can review, not generated test code.

## Target Flow

1. Collect ticket intent.
   - Inputs: Jira issue, acceptance criteria, linked tickets, product notes, attachments where available.
   - Braingent adds prior decisions, known failure modes, QA notes, and ticket history.
   - → verify: every acceptance criterion is extracted into a numbered requirement row or marked ambiguous.

2. Collect implementation evidence.
   - Inputs: git diff, changed files, changed tests, related commits, local branch against base.
   - Gather Step maps touched symbols, routes, pages, events, contracts, projections, field usage, and known callers/consumers.
   - → verify: every changed entry point has at least one risk note or an explicit "no user-visible behavior expected" note.

3. Generate the QA reference.
   - Output: one Markdown file with a traceability matrix and test cases.
   - Cases include happy path, negative path, boundary/data variations, permission/role checks, migration/backfill risk, API contract checks, UI page checks, event/async checks, and regression checks.
   - → verify: every test case cites a source: AC, code diff, Gather Step edge, Braingent record, or explicit inference.

4. Run a QA critic pass.
   - A separate critic prompt challenges the first draft for developer bias, missing AC coverage, duplicated cases, weak expected results, and untestable wording.
   - The critic should prefer requirements and observable behavior over implementation assumptions.
   - → verify: final output includes a "Gaps And Assumptions" section for anything not provable from the ticket or code graph.

5. Capture useful outcomes.
   - When the QA pack finds a reusable pattern or missed risk, Braingent can capture the learning after the task closes.
   - → verify: durable capture only stores high-signal learnings, never secrets or private customer data.

ELI5: The generator should behave like a skeptical QA partner. It reads what the feature promised, checks what the code actually touched, then writes tests that prove the promise and protect connected systems.

## Proposed Markdown Output

The first version should write a single file, for example:

```text
qa-reference/<ticket-or-branch>-test-cases.md
```

Suggested sections:

- Summary: ticket, branch, base, generated date, source inputs.
- Coverage Map: AC-to-test-case matrix.
- Impact Map: pages, APIs, events, contracts, data fields, repos, and integrations affected.
- Test Cases: structured manual cases with IDs, priority, type, source evidence, preconditions, steps, expected result, data variations, and automation candidate flag.
- E2E Candidates: high-value browser/user journeys worth automating later.
- API/Contract Cases: request/response, auth, validation, schema, and downstream consumer checks.
- Regression Set: compact cases protecting old behavior that the diff might disturb.
- Gaps And Assumptions: unclear AC, missing test data, unavailable envs, unindexed repos, or manual confirmations needed.
- Source Links: Jira issue, Braingent records, Gather Step pack targets, commits, files, and external references.

Example case shape:

```markdown
### TC-004: Reject unauthorized approval update

- Type: Manual API
- Priority: High
- Source: AC-2, `PATCH /approvals/:id`, downstream approval status consumer
- Preconditions: User exists without approval admin permission; approval item exists in pending state.
- Steps:
  1. Authenticate as the non-admin user.
  2. Send a status update request for the pending approval.
  3. Refresh the approval detail page or query the detail endpoint.
- Expected Result: Request is rejected, status remains pending, and no approval event is emitted.
- Data Variations: expired token, valid token wrong role, valid role wrong tenant.
- Automation Candidate: Yes, API integration.
```

## V4 MVP Shape

Phase 1: Markdown generator.

ELI5: Build the smallest useful loop first: gather evidence, ask the model to produce structured Markdown, and make gaps explicit.

- Add a derived artifact command conceptually similar to existing generated artifacts:
  - `gather-step generate qa-reference --ticket <KEY-or-url> --base <ref> --head <ref> --out <path>`
  - MCP equivalent later: `qa_reference_pack`.
- Reuse existing pack modes first:
  - `planning` for intended scope.
  - `review` for what changed.
  - `change_impact` for integration edges.
- Keep output deterministic where possible:
  - stable case IDs,
  - stable section order,
  - evidence labels,
  - explicit "inference" labels.
- → verify: run the command against a fixture ticket/diff and assert the Markdown contains AC coverage, impact coverage, and gaps.

Phase 2: Coverage heuristics.

ELI5: Add test-design rules so the output is not just a prettier summary of the developer's diff.

- Requirement coverage: each AC maps to at least one case.
- Change coverage: each user-visible changed route/page/event/contract maps to at least one case.
- Risk coverage: generate negative, boundary, permission, tenancy, concurrency, migration, and rollback cases when evidence suggests them.
- Pairwise coverage: extract dimensions such as role, status, tenant, feature flag, device, locale, and data shape, then generate a compact matrix.
- → verify: fixture expected output includes at least one negative, one boundary, and one integration case when the fixture contains those dimensions.

Phase 3: Integration-aware expansion.

ELI5: If one API or page changes, QA should see who else might break, not only the changed file.

- Use Gather Step to expand affected APIs, UI pages, events, field readers/writers, projections, and cross-repo consumers.
- Mark cases by test level: manual UI, manual API, E2E candidate, contract check, exploratory charter.
- Add "integration smoke" cases for downstream systems when the graph shows consumers.
- → verify: a fixture with cross-repo consumers produces at least one downstream verification case.

Phase 4: Future web app.

ELI5: The first output is Markdown. A web app can come later once the data shape is stable.

- Parse generated Markdown/frontmatter into a browsable test matrix.
- Filter by AC, risk, page, API, priority, test type, and source evidence.
- Let QA mark pass/fail/blocked and export back to Markdown or test management tools.
- → verify: no web UI starts until the Markdown schema survives real QA use.

## External Landscape

No exact open-source project found that combines Jira AC, durable engineering memory, code graph impact, and manual QA Markdown generation. Existing tools cover useful slices:

| Project | What Exists | Useful Lesson For Gather Step |
|---|---|---|
| [plaintest](https://plaintest.readthedocs.io/) | Markdown test cases stored with code, pytest links, metadata, and reports. | Strong fit for repo-native Markdown structure and future traceability between manual docs and automated tests. |
| [TestPlanIt](https://www.testplanit.com/) | Open-source test management with AI test case generation, Markdown import, Jira integration, and later automation export. | Good future web-app inspiration, but heavier than the v4 Markdown-first goal. |
| [Rhesis](https://github.com/rhesis-ai/rhesis) | Open-source GenAI testing platform that generates scenarios from requirements and connected context sources such as Jira, GitHub, Confluence, and MCP. | Confirms the value of requirement plus knowledge-source test generation, especially critic/adversarial patterns. Mostly GenAI-app focused. |
| [Qodo Cover](https://github.com/qodo-ai/qodo-cover) | AI unit-test generation that runs tests and validates coverage improvement. Repository notes it is no longer maintained as of 2025-06-15. | Useful validation pattern: generate candidates, execute/filter, and keep only useful output. Not a manual QA Markdown tool. |
| [GitHub Next TestPilot](https://github.com/githubnext/testpilot) | Archived LLM unit-test generator for JavaScript/TypeScript npm packages. | Useful research pattern for prompt skeletons, examples mined from docs, and parse-to-runnable-test loops. Not a dependency target. |
| [Schemathesis](https://schemathesis.io/) | Open-source API testing from OpenAPI/GraphQL schemas with schema-aware fuzzing, boundary values, response validation, and reproducible failures. | Useful source of API negative/boundary case ideas and future optional integration when an OpenAPI schema exists. |
| [EvoMaster](https://github.com/WebFuzzing/EvoMaster) | Open-source AI-driven system-level test generation/fuzzing for REST, GraphQL, gRPC, and related APIs, producing regression suites. | Useful for API/system test inspiration, especially generated regression suites from observed behavior. Too execution-heavy for MVP. |
| [allpairspy](https://pypi.org/project/allpairspy/) and [pairwise testing](https://www.pairwise.org/) | Open-source pairwise/n-wise combination generation. | Useful algorithm for compact scenario matrices across roles, statuses, feature flags, data shapes, and devices. |
| [gherkin crate](https://docs.rs/gherkin/latest/gherkin/) / Cucumber ecosystem | Parsers and BDD scenario conventions. | Optional output style for teams that prefer Given/When/Then, but v4 should not force BDD. |

## Product Decisions To Consider

- Prefer `generate qa-reference` over a new pack mode for MVP because the user-facing output is a derived Markdown artifact, not only context.
- Add an MCP tool after the CLI shape stabilizes so agents can request the same artifact without shelling out.
- Keep the generator evidence-bound: every claim needs source evidence or an inference label.
- Support ticket input as URL/key/text. Jira integration should be optional because not every team uses Jira.
- Do not write automated tests in v4 MVP. Label automation candidates only.
- Let QA edit the Markdown. Regeneration should preserve stable IDs where possible to avoid churn.
- Treat missing AC, missing env, missing data, and unindexed repo edges as first-class output, not failures to hide.

## Risks

- Hallucinated coverage: the model may invent requirements or expected results. Mitigation: source labels and a gaps section.
- Developer bias: generated cases may mirror the implementation instead of challenging it. Mitigation: separate QA critic pass and requirement-first prompting.
- Huge diffs: output may become too broad. Mitigation: risk ranking, max case budgets, and "exploratory charter" grouping.
- Weak Jira input: tickets may lack AC. Mitigation: generate clarification questions and assumption-marked cases.
- Sensitive data: QA examples may leak customer details. Mitigation: synthetic data defaults and Braingent no-secret policy.
- False confidence: Markdown cases are guidance, not proof. Mitigation: include "manual reference only" and explicit unverified gaps.

## Acceptance Criteria For The V4 Feature

- Given a ticket with AC and a completed branch diff, the command generates one Markdown QA reference file.
- Every AC appears in the coverage matrix with at least one linked test case or a clear "needs clarification" note.
- Every changed API/page/event/contract identified by Gather Step appears in the impact map.
- Integration-impact cases are generated when Gather Step finds downstream consumers, upstream callers, event subscribers, or shared contract readers.
- The output separates fact from inference and lists gaps.
- The output includes manual QA cases and E2E/API automation candidates, but does not create executable tests.
- The output is stable enough for review: deterministic section order, stable IDs, and minimal noisy regeneration.

## Open Questions

- Should the first command live under `generate qa-reference`, `pack --mode qa_reference`, or both?
- Should the output schema use only Markdown headings/lists, or Markdown plus frontmatter per case for future parsing?
- How should Jira issue retrieval work in local-only/offline mode?
- What should be the default case budget for large PRs?
- Should Braingent capture approved QA packs, or only durable learnings discovered from them?
- Should generated cases support optional Gherkin blocks, or stay in manual test-case form by default?

## Suggested First Fixture

Create a small fixture with:

- one Jira-like Markdown ticket containing 3 AC,
- one changed API route,
- one changed UI page,
- one downstream event subscriber,
- one existing regression test file,
- one ambiguous AC.

Expected output should prove:

- AC traceability,
- changed surface traceability,
- one negative case,
- one boundary/data variation case,
- one downstream integration case,
- one explicit clarification item.

