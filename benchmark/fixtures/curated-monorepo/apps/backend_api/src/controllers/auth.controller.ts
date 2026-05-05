import { Controller, Post, Body } from '@nestjs/common';

@Controller('auth')
export class AuthController {
  @Post('refresh')
  async renewSession(@Body('renewalCode') renewalCode: string): Promise<{ sessionHandle: string }> {
    // Minimal stub for benchmark fixture purposes.
    return { sessionHandle: `renewed-${renewalCode}` };
  }
}
