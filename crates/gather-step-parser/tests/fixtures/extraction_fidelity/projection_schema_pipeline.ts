import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

type Customer = {
  id: string;
};

@Schema({ collection: 'customer_accounts' })
export class CustomerAccountEntity {
  @Prop()
  customers!: Customer[];

  @Prop()
  status!: 'trial' | 'paid';
}

export interface CustomerAccountProjection {
  customerIds: string[];
  customerStatus: string;
}

export function toCustomerAccountProjection(
  account: CustomerAccountEntity
): CustomerAccountProjection {
  return {
    customerIds: account.customers.map((customer) => customer.id),
    customerStatus: account.status,
  };
}

export const CustomerAccountSchema = SchemaFactory.createForClass(CustomerAccountEntity);
