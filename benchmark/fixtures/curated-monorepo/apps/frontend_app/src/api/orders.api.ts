import type { CreateOrderInput, OrderRecord } from '@shared/contracts';

const BASE_URL = '/api/orders';

export async function listOrders(): Promise<OrderRecord[]> {
  const response = await fetch(BASE_URL);
  return response.json() as Promise<OrderRecord[]>;
}

export async function createOrder(input: CreateOrderInput): Promise<OrderRecord> {
  const response = await fetch(BASE_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(input),
  });
  return response.json() as Promise<OrderRecord>;
}
