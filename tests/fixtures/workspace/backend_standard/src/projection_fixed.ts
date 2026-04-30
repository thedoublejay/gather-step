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
    invoiceItemTotal: account.invoiceItems,
  };
}

export const billingProjectionIndexMapping = { invoiceItemTotal: 1 };

export const billingProjectionFilterByTotal = { invoiceItemTotal: { $gte: 0 } };

export function backfillInvoiceItemTotal() {
  return { $set: { invoiceItemTotal: 0 } }; // backfill
}

export const billingProjectionReadModel = { invoiceItemTotal: 1, billingStatus: 1 };
