export async function refreshInvoiceProjection(collection: any) {
  await collection.updateMany(
    { invoiceItemTotal: { $exists: false } },
    { $inc: { invoiceItemTotal: 1 } }
  ); // backfill

  return collection.aggregate([
    { $lookup: { from: 'invoice_items', localField: 'invoiceId', foreignField: 'invoiceId', as: 'invoiceItems' } },
    { $addFields: { invoiceItemTotal: account.invoiceItems.reduce((total, item) => total + item.amount, 0), orderIds: order.orders.map((order) => order.id) } },
    { $match: { invoiceItemTotal: { $gte: 0 } } },
  ]);
}

export async function ensureInvoiceSearchIndex(collection: any) {
  await collection.createIndex({ invoiceItemTotal: 1, orderIds: 1 });
}
