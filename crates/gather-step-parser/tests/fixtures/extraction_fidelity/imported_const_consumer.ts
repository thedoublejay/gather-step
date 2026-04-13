import { CustomEventPattern } from '@nestjs/microservices';
import { ORDER_CREATED, USER_UPDATED } from './imported_const_event';

export class OrderConsumer {
  @CustomEventPattern(ORDER_CREATED)
  handleOrder(data: unknown) {
    return data;
  }

  @CustomEventPattern(USER_UPDATED)
  handleUser(data: unknown) {
    return data;
  }
}
