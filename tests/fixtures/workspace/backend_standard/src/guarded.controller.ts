// Cross-repo `@UseGuards` consumer used by the
// `guard_canonical_primary` oracle scenario. Imports `UserAuthGuard` from
// `@workspace/shared-contracts`, which is resolved by the parser as a
// shared-lib import (binding.resolved_path = None), so the parser branch
// in `add_guard_edges` emits a virtual `SharedSymbol` guard target
// keyed by `__guard__@workspace/shared-contracts__UserAuthGuard` plus a
// `UsesGuardFrom` edge from this controller method to it.

import { Controller, Get, UseGuards } from '@nestjs/common';
import { UserAuthGuard } from '@workspace/shared-contracts';

@Controller('guarded')
export class GuardedController {
  @Get()
  @UseGuards(UserAuthGuard)
  listGuarded() {
    return [];
  }
}
