export const projectionContractExample = {
  invoiceItemTotal: account.invoiceItems.reduce((total, item) => total + item.amount, 0),
  orderIds: order.orders.map((order) => order.id),
};
