import { EventType } from './event_topics_enum';

// Producer: emits a broad topic with a payload carrying a static eventType.
// The extractor should produce both the broad Event node (pdf.generation)
// and the fine-grained Event node (pdf.generation.completed).
export class PdfService {
  constructor(private readonly client: any) {}

  async processCompleted(orderId: string) {
    await this.client.emit('pdf.generation', {
      eventType: EventType.PdfGenerationCompleted,
      orderId,
    });
  }
}
