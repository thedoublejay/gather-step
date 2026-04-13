import { InjectModel } from '@nestjs/mongoose';
import type { Model } from 'mongoose';
import type { CreateOrderInput, OrderRecord } from '@workspace/shared-contracts';

import { OrderRecordEntity } from './order.entity';

export class OrderRepository {
  constructor(
    @InjectModel(OrderRecordEntity.name)
    private readonly orderModel: Model<OrderRecordEntity>
  ) {}

  async storeOrderRecord(payload: CreateOrderInput): Promise<OrderRecord> {
    await this.orderModel.create({
      email: payload.email,
      status: 'active'
    });

    const record = await this.orderModel.findOne({ email: payload.email });
    return {
      orderId: payload.email,
      email: record?.email ?? payload.email,
      status: record?.status ?? 'active'
    };
  }
}
