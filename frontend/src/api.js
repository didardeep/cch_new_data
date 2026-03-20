// Dev: CRA proxy in package.json forwards /api/* → localhost:5500 automatically.
// Production: set REACT_APP_API_URL=https://your-backend.com in your .env
export const API_BASE = process.env.REACT_APP_API_URL || '';

export function getToken() {
  return localStorage.getItem('token');
}

export function setToken(token) {
  localStorage.setItem('token', token);
}

export function clearToken() {
  localStorage.removeItem('token');
  localStorage.removeItem('user');
}

export async function apiCall(endpoint, options = {}) {
  const token = getToken();
  const headers = { 'Content-Type': 'application/json', ...options.headers };
  if (token) headers['Authorization'] = `Bearer ${token}`;

  const resp = await fetch(`${API_BASE}${endpoint}`, { ...options, headers });

  if (resp.status === 401 && !endpoint.startsWith('/api/auth/')) {
    clearToken();
    return Promise.reject(new Error('unauthorized'));
  }

  // Parse JSON — if the server returned a non-JSON body (e.g. HTML 500 page)
  // surface a clear error instead of a cryptic SyntaxError.
  let data;
  try {
    data = await resp.json();
  } catch {
    throw new Error(`Server error (${resp.status})`);
  }

  // For non-2xx responses, throw so callers can catch and show the message.
  if (!resp.ok) {
    throw new Error(data?.error || `Request failed (${resp.status})`);
  }

  return data;
}

export async function apiGet(endpoint) {
  return apiCall(endpoint, { method: 'GET' });
}

export async function apiPost(endpoint, body) {
  return apiCall(endpoint, { method: 'POST', body: JSON.stringify(body) });
}

export async function apiPut(endpoint, body) {
  return apiCall(endpoint, { method: 'PUT', body: JSON.stringify(body) });
}

export async function apiDelete(endpoint) {
  return apiCall(endpoint, { method: 'DELETE' });
}
