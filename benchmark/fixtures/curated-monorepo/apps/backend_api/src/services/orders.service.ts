import type { CreateOrderInput, OrderRecord } from '@shared/contracts';

export class OrdersService {
  async createOrder(input: CreateOrderInput): Promise<OrderRecord> {
    return {
      id: 'order-1',
      productId: input.productId,
      quantity: input.quantity,
      customerId: input.customerId,
      status: 'pending',
      createdAt: new Date().toISOString(),
    };
  }

  async listOrders(): Promise<OrderRecord[]> {
    return [];
  }
}
