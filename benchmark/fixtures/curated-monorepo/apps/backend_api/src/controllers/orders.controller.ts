import { Body, Controller, Get, Post } from '@nestjs/common';
import type { CreateOrderInput, OrderRecord } from '@shared/contracts';

import { OrdersService } from '../services/orders.service';

@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Get()
  listOrders(): Promise<OrderRecord[]> {
    return this.ordersService.listOrders();
  }

  @Post()
  createOrder(@Body() input: CreateOrderInput): Promise<OrderRecord> {
    return this.ordersService.createOrder(input);
  }
}
