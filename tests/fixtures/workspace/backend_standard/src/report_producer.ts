// Exercises the canonical-event-identity join: `sendMessage` is mapped to
// `NodeKind::Topic` by the NestJS parser while the consumer in
// `frontend_standard/src/report_consumer.ts` uses `@CustomEventPattern`
// which is mapped to `NodeKind::Event`. Same topic name, different
// `ref_node_id(kind, qn)` ids — the sibling-hop logic in `trace_event` /
// `event_adjacent_targets` is what lets the two sides join.

import { Controller } from '@nestjs/common';

import { Messaging } from './topics';

@Controller()
export class ReportProducer {
  constructor(private readonly bus: EventBusClient) {}

  async emitReportQueued() {
    this.bus.sendMessage(Messaging.kafka.reports.queue);
  }
}
