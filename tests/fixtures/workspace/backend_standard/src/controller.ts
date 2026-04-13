import { Body, Controller, Get, Post } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';
import {
  SharedAuditRecord,
  type CreateOrderInput,
  type OrderRecord,
} from '@workspace/shared-contracts';

import { OrderService } from './order.service';
import { Messaging } from './topics';

const Routes = { orders: { list: 'orders' } };

@Controller({ path: Routes.orders.list })
export class ServiceAController {
  constructor(private readonly orderService: OrderService) {}

  @Get()
  listOrders(): OrderRecord[] {
    return [];
  }

  @Get('audit')
  listAuditRecords(): SharedAuditRecord[] {
    return [];
  }

  @Post()
  async createOrder(@Body() payload: CreateOrderInput): Promise<OrderRecord> {
    return this.orderService.persistOrder(payload);
  }

  @EventPattern(Messaging.kafka.orders.created)
  handleOrderCreated(data: OrderRecord) {
    return data.status;
  }
}
