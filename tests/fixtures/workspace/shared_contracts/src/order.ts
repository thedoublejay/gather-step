export type OrderRecord = {
  orderId: string;
  email: string;
  status: 'active' | 'resolved';
};

export type CreateOrderInput = {
  email: string;
};

export class SharedAuditRecord {
  auditId = '';
  status = 'active';
}
