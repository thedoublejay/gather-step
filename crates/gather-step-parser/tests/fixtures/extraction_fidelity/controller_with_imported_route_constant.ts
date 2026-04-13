import { Controller, Get } from '@nestjs/common';
import { RouteConstants } from './route_constants';

@Controller()
export class AccountController {
  @Get(RouteConstants.v2.accounts.details)
  details() {
    return {};
  }
}
