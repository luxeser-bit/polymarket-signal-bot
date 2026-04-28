import { useCallback, useEffect, useState } from 'react';

export const API_BASE = (import.meta.env.VITE_API_URL || 'http://localhost:8000').replace(/\/$/, '');

export function apiUrl(path) {
  if (/^https?:\/\//i.test(path)) return path;
  return `${API_BASE}${path.startsWith('/') ? path : `/${path}`}`;
}

export async function getJson(path, options = {}) {
  const response = await fetch(apiUrl(path), {
    headers: { Accept: 'application/json' },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

export async function postJson(path, body) {
  const response = await fetch(apiUrl(path), {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

export function useApiResource(path, { interval = 0, initialData = null } = {}) {
  const [data, setData] = useState(initialData);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const payload = await getJson(path);
      setData(payload);
      setError('');
      return payload;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }, [path]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const payload = await getJson(path);
        if (!cancelled) {
          setData(payload);
          setError('');
        }
      } catch (err) {
        if (!cancelled) setError(err.message || String(err));
      }
    };
    load();
    if (!interval) return () => {
      cancelled = true;
    };
    const id = window.setInterval(load, interval);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [interval, path]);

  return { data, error, loading, refresh, setData };
}
