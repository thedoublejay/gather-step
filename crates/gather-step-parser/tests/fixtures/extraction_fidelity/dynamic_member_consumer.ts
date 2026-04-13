import { MessagePattern } from '@nestjs/microservices';

// Dynamic topic — not resolvable, should emit nothing
const dynamicSuffix = Math.random() > 0.5 ? 'a' : 'b';

export class DynamicConsumer {
  @MessagePattern(`dynamic.${dynamicSuffix}`)
  handleDynamic(data: unknown) {
    return data;
  }
}
