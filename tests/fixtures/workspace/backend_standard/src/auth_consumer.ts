// Cross-repo consumer of `useAuthentication` used to validate the widened
// planning traversal for cross-repo callers. The import comes
// from a shared-lib package (`@workspace/` prefix), so the parser emits a
// virtual `SharedSymbol` for the binding rather than resolving to the
// canonical declaration in `frontend_standard`. The consumer's `Calls`
// edge lands on that virtual peer — `get_callers` on the canonical
// declaration misses it by default, but the planning-mode upstream
// widening finds peers by name and picks up this repo as a cross-repo
// caller.

import { useAuthentication } from '@workspace/shared-contracts';

export async function checkAuthentication() {
  return useAuthentication();
}
