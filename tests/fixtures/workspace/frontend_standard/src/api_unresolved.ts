const config = {
  apiPath: {
    gw: {
      orders: {
        pending: "gw/orders/pending"
      }
    }
  }
};

export async function submitPendingOrder(apiClient: any, payload: unknown) {
  return apiClient.config.apiPath.gw.orders.pending.post(payload ?? config.apiPath.gw.orders.pending);
}
