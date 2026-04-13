import { loadOrders } from './api';

export async function useAuthentication(apiClient: any): Promise<unknown> {
  return loadOrders(apiClient);
}
