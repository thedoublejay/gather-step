# QA Reference Template

This template is for Braingent-owned QA reference output generated from ticket text, Braingent memory, git diff evidence, and Gather Step graph evidence. It is manual guidance, not proof and not executable test code.

## Frontmatter

```yaml
---
title: "<ticket-or-branch> QA Reference"
status: draft
ticket: "<KEY-or-url-or-local-file>"
branch:
base_ref:
head_ref:
generated_at:
workflow_owner: Braingent
evidence_owner: Gather Step
sources:
  requirements: []
  braingent_records: []
  gather_step_evidence: []
  commits: []
  files: []
manual_reference_only: true
---
```

## Summary

- Goal:
- Scope:
- Base / Head:
- Source Inputs:
- Highest Risks:
- Gaps:

## Coverage Map

| Requirement | Status | Linked Cases | Sources | Gap |
| --- | --- | --- | --- | --- |
| AC-1 | covered | TC-001 | ticket, GS-EVID-... | none |
| AC-2 | needs clarification | none | ticket | ambiguous expected result |

## Impact Map

| Surface | Evidence Metadata | Impact | Linked Cases | Gap |
| --- | --- | --- | --- | --- |
| API route | kind: route_definition | changed validation path | TC-002, TC-003 | none |
| Event topic | kind: event_consumer | downstream subscriber reads changed payload | TC-004 | test data missing |

## Test Cases

### TC-001: <observable behavior>

- Type: Manual UI | Manual API | E2E Candidate | Contract Check | Exploratory Charter
- Priority: High | Medium | Low
- Sources: AC-1, GS-EVID-..., GS-GAP-...
- Fact / Inference: Fact | Inference
- Preconditions:
- Steps:
  1. <step>
  2. <step>
  3. <step>
- Expected Result:
- Data Variations:
- Automation Candidate: Yes | No
- Gaps:

## E2E Candidates

| Candidate | Why It Matters | Sources | Suggested Owner | Gap |
| --- | --- | --- | --- | --- |

## API And Contract Cases

| Case | Contract Surface | Positive / Negative / Boundary | Sources | Gap |
| --- | --- | --- | --- | --- |

## Regression Set

| Case | Prior Behavior Protected | Sources | Gap |
| --- | --- | --- | --- |

## Gaps And Assumptions

- Missing requirement detail:
- Missing environment or data:
- Missing indexed repo or graph edge:
- Inference that QA must confirm:

## Critic Checklist

- Every AC is covered or marked ambiguous.
- Every changed API/page/event/contract has a case or an explicit no-user-visible-impact note.
- At least one negative case exists when permissions, validation, state, tenancy, or auth appear in the evidence.
- At least one boundary/data variation exists when numeric, date, enum, pagination, locale, or schema evidence appears.
- Downstream systems have smoke cases when Gather Step finds consumers or shared contract readers.
- Expected results are observable by QA and not just implementation descriptions.
- No customer secrets, tokens, private repo names, or sensitive data appear in examples.

## Source Links

- Ticket:
- Braingent records:
- Gather Step pack targets:
- Git commits:
- Changed files:
- External references:
