import { MessagePattern } from '@nestjs/microservices';
import { EventType } from './event_topics_enum';

// Dispatcher consumer: listens on broad topic, switches on eventType inside body
export class PdfConsumer {
  @MessagePattern('pdf.generation')
  async handle(data: unknown) {
    const event = data as { eventType: string };
    switch (event.eventType) {
      case EventType.PdfGenerationCompleted:
        break;
      default:
        break;
    }
  }
}
