import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';

@Schema({ collection: 'orders' })
export class OrderRecordEntity {
  @Prop()
  email!: string;

  @Prop()
  status!: 'active' | 'resolved';
}

export const OrderRecordSchema = SchemaFactory.createForClass(OrderRecordEntity);
