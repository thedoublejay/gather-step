import { Process, Processor } from '@nestjs/bull';

@Processor()
export class WorkerProcessor {
  @Process('build')
  handle() {
    return true;
  }
}
