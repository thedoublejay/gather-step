// No messaging decorator — eventType switch must not produce Event nodes
export class ProcessingService {
  process(event: { eventType: string }) {
    switch (event.eventType) {
      case 'pdf.generation.completed':
        break;
      default:
        break;
    }
  }
}
