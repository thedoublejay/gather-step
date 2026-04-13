import { CustomEventPattern } from '@nestjs/microservices';

export class CustomHandler {
  @CustomEventPattern('order.created')
  handle() {
    return true;
  }
}
