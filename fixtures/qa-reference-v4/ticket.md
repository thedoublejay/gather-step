# QA Reference Fixture Ticket

## Summary

Add the order list v2 experience behind a feature flag and publish an order-list refresh event when the API returns updated data.

## Acceptance Criteria

1. When `orders-list-v2` is enabled, the order list page shows the new status column.
2. The `GET /orders` API accepts `status` and `limit` filters and rejects invalid status values.
3. After the API returns updated order data, downstream subscribers receive an `order.list.refreshed` event.

## Ambiguous Requirement

- The ticket does not define whether cancelled orders should appear when no `status` filter is provided.

