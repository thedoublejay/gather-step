import { Controller } from '@nestjs/common';

/**
 * @Get('fake')
 * @EventPattern('fake.event')
 */
@Controller('docs')
export class DocController {
  list() {
    return [];
  }
}
