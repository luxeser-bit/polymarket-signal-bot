import { useMemo, useState } from 'react';
import toast from 'react-hot-toast';
import { FiPlay, FiRefreshCw, FiSquare } from 'react-icons/fi';
import { postJson } from '../hooks/useApi';
import { secondsToDuration } from '../utils/format';

const ORDER = ['indexer', 'monitor', 'live_paper'];

export default function SystemControl({ status, onRefresh }) {
  const [loading, setLoading] = useState(false);
  const components = status?.components || {};
  const allRunning = useMemo(
    () => ORDER.every((key) => components[key]?.running),
    [components],
  );

  async function toggleSystem() {
    setLoading(true);
    try {
      await postJson(allRunning ? '/system/stop' : '/system/start');
      toast.success(allRunning ? 'System stopped' : 'System started');
      await onRefresh?.();
    } catch (err) {
      toast.error(`System command failed: ${err.message || err}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <p className="panel-title">System control</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">Process stack</h2>
        </div>
        <button className="icon-button px-3" onClick={onRefresh} title="Refresh status">
          <FiRefreshCw aria-hidden="true" />
        </button>
      </div>

      <button
        className={`icon-button mb-4 w-full ${allRunning ? 'danger-button' : ''}`}
        onClick={toggleSystem}
        disabled={loading}
      >
        {allRunning ? <FiSquare aria-hidden="true" /> : <FiPlay aria-hidden="true" />}
        {loading ? 'Working' : allRunning ? 'Stop All' : 'Start All'}
      </button>

      <div className="space-y-2">
        {ORDER.map((key) => {
          const item = components[key] || {};
          return (
            <div
              key={key}
              className="flex items-center justify-between rounded-lg border border-slate-700/70 bg-slate-950/40 px-3 py-2"
            >
              <div className="flex items-center gap-2">
                <span
                  className={`h-2.5 w-2.5 rounded-full ${item.running ? 'bg-good shadow-[0_0_12px_rgba(34,197,94,0.65)]' : 'bg-slate-500'}`}
                />
                <span className="text-sm font-medium text-slate-200">{item.name || key}</span>
              </div>
              <div className="text-right text-xs text-slate-500">
                <div>{item.running ? `PID ${item.pid}` : 'stopped'}</div>
                {item.uptime_seconds ? <div>{secondsToDuration(item.uptime_seconds)}</div> : null}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
