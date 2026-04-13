const AUTH_URL = '/api/auth/refresh';

export interface AuthSession {
  token: string;
  expiresAt: number;
}

export async function refreshAuthToken(): Promise<AuthSession> {
  const response = await fetch(AUTH_URL, { method: 'POST' });
  return response.json() as Promise<AuthSession>;
}

export function useAuthentication() {
  return { refreshAuthToken };
}
