import { Controller, Get } from '@nestjs/common';
import { EventPattern } from '@nestjs/microservices';

@Controller('orders')
export class OrdersController {
  @Get()
  list(): string {
    return 'orders';
  }

  @EventPattern('order.created')
  handleOrderCreated(): void {
    // consume the order.created kafka event
  }
}
