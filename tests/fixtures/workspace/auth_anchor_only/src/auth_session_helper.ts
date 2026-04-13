import { OrderRecord } from '@workspace/shared-contracts';

/**
 * Internal auth-session utility.
 *
 * This module uses a shared contract type for its return shape, which creates
 * a cross-repo virtual-node connection from this repo to any other repo that
 * also imports OrderRecord.  Crucially, no other repo actually calls or
 * imports authSessionHelper itself — there are no structural cross-repo
 * proof edges for this symbol.
 */
export function authSessionHelper(): OrderRecord {
  return { orderId: 'local-only', email: '', status: 'active' };
}
