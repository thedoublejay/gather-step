import type { CreateOrderInput, OrderRecord } from '@workspace/shared-contracts';

import { OrderRepository } from './order.repository';

export class OrderService {
  constructor(private readonly orderRepository: OrderRepository) {}

  async persistOrder(payload: CreateOrderInput): Promise<OrderRecord> {
    return this.orderRepository.storeOrderRecord(payload);
  }
}
