type AccountProjectionSource = {
  lineItems?: Array<{ amount: number }>;
  orders?: Array<{ id: string }>;
  status?: 'trial' | 'paid';
};

export function buildOptionalProjection(account: AccountProjectionSource) {
  return {
    lineItemTotal: account.lineItems?.reduce((total, item) => total + item.amount, 0) ?? 0,
    orderIds: account.orders?.map((order) => order.id) ?? [],
    accountStatus: account.status ?? 'unknown',
  };
}
