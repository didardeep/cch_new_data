import { createContext, useContext, useState, useEffect } from 'react';
import { apiGet, setToken, clearToken, getToken } from './api';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const storedUser = (() => {
    try {
      const raw = localStorage.getItem('user');
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  })();
  const [user, setUser] = useState(storedUser);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const token = getToken();
    if (token) {
      apiGet('/api/auth/me')
        .then(data => {
          if (data?.user) {
            setUser(data.user);
            localStorage.setItem('user', JSON.stringify(data.user));
          } else {
            clearToken();
            setUser(null);
          }
        })
        .catch(() => {
          // Network error (backend temporarily down) — keep stored credentials.
          // Only a server-confirmed auth failure (data without user) clears them.
        })
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  const login = (token, userData) => {
    setToken(token);
    setUser(userData);
    try { localStorage.setItem('user', JSON.stringify(userData)); } catch {}
  };

  const logout = () => {
    clearToken();
    setUser(null);
  };

  const updateUser = (userData) => {
    setUser(userData);
    try { localStorage.setItem('user', JSON.stringify(userData)); } catch {}
  };

  return (
    <AuthContext.Provider value={{ user, loading, login, logout, updateUser }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
