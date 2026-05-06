import { Controller, Get, Query } from '@nestjs/common';

@Controller('orders')
export class OrdersController {
  @Get()
  listOrders(@Query('status') status?: string, @Query('limit') limit?: string) {
    if (status && !['pending', 'shipped', 'cancelled'].includes(status)) {
      throw new Error('Invalid status');
    }

    return {
      orders: [],
      limit: Number(limit ?? 25),
    };
  }
}

