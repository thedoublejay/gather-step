const AUTH_URL = '/api/auth/refresh';

export interface AuthSession {
  sessionHandle: string;
  expiresAt: number;
}

export async function renewAuthSession(): Promise<AuthSession> {
  const response = await fetch(AUTH_URL, { method: 'POST' });
  return response.json() as Promise<AuthSession>;
}

export function useAuthentication() {
  return { renewAuthSession };
}
