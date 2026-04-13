import { EventPattern } from '@nestjs/microservices';

type OrderFeedDto = {
  orderId: number;
  email?: string;
  status: string;
};

export class OrderFeedConsumer {
  @EventPattern('order.created')
  handleOrderCreated(data: OrderFeedDto) {
    return data.status;
  }
}
