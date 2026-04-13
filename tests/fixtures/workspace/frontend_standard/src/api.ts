import {
  SharedAuditRecord,
  type CreateOrderInput,
  type OrderRecord,
} from '@workspace/shared-contracts';

const config = {
  apiPath: {
    gw: {
      orders: {
        create: '/orders'
      }
    }
  }
};

export async function loadOrders(apiClient: any): Promise<OrderRecord[]> {
  return apiClient.get('/orders');
}

export async function loadAuditRecords(
  apiClient: any
): Promise<SharedAuditRecord[]> {
  return apiClient.get('/orders/audit');
}

export async function createOrder(
  apiClient: any,
  payload: CreateOrderInput
): Promise<OrderRecord> {
  return apiClient.post(config.apiPath.gw.orders.create, payload);
}
