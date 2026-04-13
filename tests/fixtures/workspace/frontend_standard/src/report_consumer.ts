// Consumer side for the canonical-event-identity join.
// `@CustomEventPattern` is mapped to `NodeKind::Event` while the
// paired producer in `backend_standard/src/report_producer.ts` uses
// `sendMessage` (mapped to `NodeKind::Topic`). Without the Topic↔Event
// sibling hop in `trace_event` / `event_adjacent_targets`, these two sides
// end up on different `ref_node_id(kind, qn)` nodes and never join.

import { CustomEventPattern } from '@nestjs/microservices';

export class ReportConsumer {
  @CustomEventPattern('reports.queue')
  handleReportQueued() {
    return null;
  }
}
