import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { API_BASE } from './useApi';

function websocketUrl(path) {
  const base = API_BASE.replace(/^http/i, 'ws');
  return `${base}${path.startsWith('/') ? path : `/${path}`}`;
}

export function useWebSocket(path, { onMessage } = {}) {
  const [state, setState] = useState('connecting');
  const [lastMessage, setLastMessage] = useState(null);
  const [error, setError] = useState('');
  const socketRef = useRef(null);
  const retryRef = useRef(0);
  const timerRef = useRef(null);
  const onMessageRef = useRef(onMessage);
  const url = useMemo(() => websocketUrl(path), [path]);

  useEffect(() => {
    onMessageRef.current = onMessage;
  }, [onMessage]);

  const connect = useCallback(() => {
    window.clearTimeout(timerRef.current);
    setState('connecting');
    const socket = new WebSocket(url);
    socketRef.current = socket;

    socket.onopen = () => {
      retryRef.current = 0;
      setState('open');
      setError('');
    };

    socket.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        setLastMessage(payload);
        onMessageRef.current?.(payload);
      } catch (err) {
        setError(err.message || String(err));
      }
    };

    socket.onerror = () => {
      setError('websocket error');
    };

    socket.onclose = () => {
      setState('closed');
      const retry = Math.min(15000, 500 * 2 ** retryRef.current);
      retryRef.current += 1;
      timerRef.current = window.setTimeout(connect, retry);
    };
  }, [url]);

  useEffect(() => {
    connect();
    return () => {
      window.clearTimeout(timerRef.current);
      if (socketRef.current) {
        socketRef.current.onclose = null;
        socketRef.current.close();
      }
    };
  }, [connect]);

  return {
    url,
    state,
    connected: state === 'open',
    lastMessage,
    error,
  };
}
