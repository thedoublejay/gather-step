import { useAuthentication } from './auth_api';

export async function loadSession(apiClient: any): Promise<unknown> {
  return useAuthentication(apiClient);
}
