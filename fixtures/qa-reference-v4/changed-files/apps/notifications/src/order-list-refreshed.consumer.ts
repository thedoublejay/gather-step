import { MessagePattern } from '@nestjs/microservices';

export class OrderListRefreshedConsumer {
  @MessagePattern('order.list.refreshed')
  handleOrderListRefreshed(payload: { orderIds: string[] }) {
    return payload.orderIds.length;
  }
}

