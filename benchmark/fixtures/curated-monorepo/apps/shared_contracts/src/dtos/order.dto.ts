export interface CreateOrderInput {
  productId: string;
  quantity: number;
  customerId: string;
}

export interface OrderRecord {
  id: string;
  productId: string;
  quantity: number;
  customerId: string;
  status: 'pending' | 'confirmed' | 'shipped';
  createdAt: string;
}
