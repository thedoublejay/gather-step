type InvoiceItem = {
  amount: number;
};

type BillingAccountDocument = {
  invoiceItems: InvoiceItem[];
  status: 'open' | 'closed';
};

export function buildBillingProjection(account: BillingAccountDocument) {
  return {
    billingStatus: account.status,
    invoiceItemTotal: account.invoiceItems.reduce((total, item) => total + item.amount, 0),
  };
}

export async function ensureBillingProjectionIndex(collection: any) {
  await collection.createIndex({ invoiceItemTotal: 1 });
}

export async function backfillInvoiceItemTotal(collection: any) {
  await collection.updateMany({ invoiceItemTotal: { $exists: false } }, { $set: { invoiceItemTotal: 0 } }); // backfill
}

export async function readBillingProjection(collection: any) {
  return collection
    .find({ invoiceItemTotal: { $gte: 0 } })
    .project({ invoiceItemTotal: 1, billingStatus: 1 })
    .toArray();
}
