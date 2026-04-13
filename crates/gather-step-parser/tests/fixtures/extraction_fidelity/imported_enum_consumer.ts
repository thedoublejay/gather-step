import { MessagePattern } from '@nestjs/microservices';
import { EventTopic } from './event_topics_enum';

export class NotificationConsumer {
  @MessagePattern(EventTopic.NotificationEvents)
  handleNotification(data: unknown) {
    return data;
  }

  @MessagePattern(EventTopic.PdfGeneration)
  handlePdf(data: unknown) {
    return data;
  }
}
