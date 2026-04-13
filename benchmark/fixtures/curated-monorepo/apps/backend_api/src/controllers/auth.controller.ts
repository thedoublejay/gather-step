import { Controller, Post, Body } from '@nestjs/common';

@Controller('auth')
export class AuthController {
  @Post('refresh')
  async refreshToken(@Body('refreshToken') refreshToken: string): Promise<{ accessToken: string }> {
    // Minimal stub for benchmark fixture purposes.
    return { accessToken: `refreshed-${refreshToken}` };
  }
}
