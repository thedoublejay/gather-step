import { EventPattern } from '@nestjs/microservices';

export class MultiPatternHandler {
  @EventPattern(['user.created', 'user.updated'])
  handle() {
    return true;
  }
}
