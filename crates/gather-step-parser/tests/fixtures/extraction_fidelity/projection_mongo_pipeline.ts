export async function refreshInvoiceProjection(collection: any) {
  await collection.updateMany(
    { invoiceItemTotal: { $exists: false } },
    {
      $set: {
        invoiceItemTotal: account.invoiceItems?.reduce((total, item) => total + item.amount, 0) ?? 0,
      },
      $unset: { archivedOrderIds: '' },
      $inc: { invoiceItemTotal: 1 },
      $push: { orderIds: order.orderId },
      $pull: { orderIds: obsoleteOrderId },
      $addToSet: { tagIds: tag.id },
    }
  ); // backfill

  return collection.aggregate([
    { $lookup: { from: 'invoice_items', localField: 'invoiceId', foreignField: 'invoiceId', as: 'invoiceItems' } },
    {
      $addFields: {
        invoiceItemTotal: account.invoiceItems?.reduce((total, item) => total + item.amount, 0) ?? 0,
        orderIds: order.orders?.map((order) => order.id) ?? [],
      },
    },
    { $match: { invoiceItemTotal: { $gte: 0 }, orderIds: { $exists: true } } },
  ]);
}

export async function ensureInvoiceSearchIndex(collection: any) {
  await collection.createIndex({ invoiceItemTotal: 1, orderIds: 1 });
  return collection.find({ orderIds: orderId, invoiceItemTotal: { $gte: 0 } });
}
