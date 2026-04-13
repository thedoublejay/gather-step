import { Controller } from '@nestjs/common';
import { MessagePattern } from '@nestjs/microservices';

import { Messaging } from './topics';

@Controller()
export class OrderPublisher {
  constructor(private readonly bus: EventBusClient) {}

  @MessagePattern([Messaging.kafka.orders.sync])
  async handleSync(payload: { orderId: string }) {
    return payload;
  }

  async publish() {
    this.bus.send('order.sync', {
      orderId: 'order-123'
    });
    this.bus.emit('order.created', {
      orderId: 'order-123',
      email: 'ops@example.com',
      status: 'active'
    });
  }
}
