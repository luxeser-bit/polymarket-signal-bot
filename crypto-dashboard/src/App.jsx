import { useCallback, useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';
import DashboardLayout from './components/DashboardLayout';
import { getJson } from './hooks/useApi';
import { useInterval } from './hooks/useInterval';
import { useWebSocket } from './hooks/useWebSocket';

const MAX_EQUITY_POINTS = 180;

export default function App() {
  const [live, setLive] = useState(null);
  const [systemStatus, setSystemStatus] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [indexerProgress, setIndexerProgress] = useState(null);
  const [wallets, setWallets] = useState(null);
  const [positions, setPositions] = useState(null);
  const [paperStatus, setPaperStatus] = useState(null);
  const [trainingStatus, setTrainingStatus] = useState(null);
  const [equityHistory, setEquityHistory] = useState([]);
  const [apiErrors, setApiErrors] = useState([]);

  const socket = useWebSocket('/ws/live', {
    onMessage: (payload) => {
      setLive(payload);
      if (payload?.balance !== undefined) {
        setEquityHistory((history) => {
          const label = new Date((payload.time || Date.now() / 1000) * 1000).toLocaleTimeString([], {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
          });
          const next = [
            ...history,
            {
              time: payload.time || Date.now() / 1000,
              label,
              balance: Number(payload.balance || 0),
            },
          ];
          return next.slice(-MAX_EQUITY_POINTS);
        });
      }
    },
  });

  const rememberError = useCallback((message) => {
    setApiErrors((items) => [message, ...items.filter((item) => item !== message)].slice(0, 3));
  }, []);

  const refreshSystem = useCallback(async () => {
    try {
      const payload = await getJson('/system/status');
      setSystemStatus(payload);
      return payload;
    } catch (err) {
      const message = `System status unavailable: ${err.message || err}`;
      rememberError(message);
      throw err;
    }
  }, [rememberError]);

  const refreshPaper = useCallback(async () => {
    try {
      const payload = await getJson('/api/paper/status');
      setPaperStatus(payload);
      return payload;
    } catch (err) {
      const message = `Paper status unavailable: ${err.message || err}`;
      rememberError(message);
      throw err;
    }
  }, [rememberError]);

  const refreshTraining = useCallback(async () => {
    try {
      const payload = await getJson('/api/training/status');
      setTrainingStatus(payload);
      return payload;
    } catch (err) {
      const message = `Training status unavailable: ${err.message || err}`;
      rememberError(message);
      throw err;
    }
  }, [rememberError]);

  const refreshData = useCallback(async () => {
    try {
      const [metricsPayload, walletsPayload, positionsPayload] = await Promise.all([
        getJson('/api/metrics'),
        getJson('/api/wallets'),
        getJson('/api/positions'),
      ]);
      setMetrics(metricsPayload);
      setWallets(walletsPayload);
      setPositions(positionsPayload);
      setApiErrors([]);
    } catch (err) {
      const message = `API unavailable: ${err.message || err}`;
      rememberError(message);
    }
  }, [rememberError]);

  const refreshIndexerProgress = useCallback(async () => {
    try {
      const payload = await getJson('/api/indexer/progress');
      setIndexerProgress(payload);
      return payload;
    } catch (err) {
      const message = `Indexer progress unavailable: ${err.message || err}`;
      rememberError(message);
      throw err;
    }
  }, [rememberError]);

  useEffect(() => {
    refreshSystem().catch(() => {});
    refreshPaper().catch(() => {});
    refreshTraining().catch(() => {});
    refreshIndexerProgress().catch(() => {});
    refreshData().catch(() => {});
  }, [refreshData, refreshIndexerProgress, refreshPaper, refreshSystem, refreshTraining]);

  useInterval(() => refreshSystem().catch(() => {}), 4000);
  useInterval(() => refreshPaper().catch(() => {}), 3000);
  useInterval(() => refreshTraining().catch(() => {}), 3000);
  useInterval(() => refreshIndexerProgress().catch(() => {}), 5000);
  useInterval(() => refreshData().catch(() => {}), 3000);

  useEffect(() => {
    if (socket.error) toast.error(socket.error, { id: 'ws-error' });
  }, [socket.error]);

  const mergedMetrics = useMemo(() => {
    if (!live) return metrics;
    return {
      ...(metrics || {}),
      raw_events: live.raw_events ?? metrics?.raw_events,
      last_block: live.last_block ?? metrics?.last_block,
      blocks_per_second: live.indexer_speed ?? metrics?.blocks_per_second,
    };
  }, [live, metrics]);

  const mergedSystemStatus = useMemo(() => {
    if (!live?.components) return systemStatus;
    const baseComponents = systemStatus?.components || {};
    const components = { ...baseComponents };
    Object.entries(live.components).forEach(([key, value]) => {
      components[key] = {
        ...(baseComponents[key] || {}),
        ...value,
        status: value?.running ? 'running' : 'stopped',
      };
    });
    return {
      ...(systemStatus || {}),
      components,
    };
  }, [live?.components, systemStatus]);

  return (
    <DashboardLayout
      live={live}
      socket={socket}
      systemStatus={mergedSystemStatus}
      onRefreshSystem={refreshSystem}
      metrics={mergedMetrics}
      indexerProgress={indexerProgress}
      wallets={wallets}
      positions={positions}
      paperStatus={paperStatus}
      onRefreshPaper={refreshPaper}
      trainingStatus={trainingStatus}
      onRefreshTraining={refreshTraining}
      onRefreshData={refreshData}
      equityHistory={equityHistory}
      apiErrors={apiErrors}
    />
  );
}
