# Expected QA Reference Shape

This fixture proves the v4 MVP loop closes without requiring Gather Step to generate the final Markdown.

## Coverage Map

| AC | Required Evidence | Expected QA Output |
| --- | --- | --- |
| AC-1 | `OrderListPage.tsx`, `orders-list-v2` feature flag | Manual UI case for enabled and disabled flag states. |
| AC-2 | `OrdersController.listOrders`, `status`, `limit` | API case for valid filters, negative case for invalid status, boundary case for `limit`. |
| AC-3 | `order.list.refreshed`, downstream consumer | Integration smoke case proving the subscriber receives the refresh event. |
| Ambiguous | Cancelled-order default behavior is unspecified | Clarification item instead of an invented expected result. |

## Required Case Types

- Happy path: order list v2 shows the status column when the flag is enabled.
- Negative: invalid `status` is rejected by `GET /orders`.
- Boundary/data variation: `limit` accepts a normal value and handles an edge value.
- Downstream integration: `order.list.refreshed` reaches the notification subscriber.
- Regression: existing `OrderListPage.test.tsx` is cited as nearby regression evidence.

## Expected Source Labels

- `GS-PLAN-*` for intended implementation scope.
- `GS-REVIEW-*` for changed behavior and nearby tests.
- `GS-IMPACT-*` for downstream event subscriber evidence.
- `GS-GAP-*` for the ambiguous cancelled-order behavior.

