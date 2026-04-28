import { useState } from 'react';
import toast from 'react-hot-toast';
import { FiPlay, FiSquare } from 'react-icons/fi';
import { postJson } from '../hooks/useApi';
import { money, signedMoney } from '../utils/format';

export default function PaperTradingControl({ status, live, onRefresh }) {
  const [dryRun, setDryRun] = useState(status?.dry_run ?? true);
  const [loading, setLoading] = useState(false);
  const running = Boolean(status?.running);
  const balance = Number(live?.balance ?? status?.balance ?? 0);
  const pnl = Number(status?.pnl ?? 0);
  const dayPnl = Number(status?.daily_pnl ?? 0);
  const openPositions = Number(live?.open_positions ?? status?.open_positions ?? 0);

  async function togglePaper() {
    setLoading(true);
    try {
      await postJson(running ? '/api/paper/stop' : '/api/paper/start', running ? undefined : { dry_run: dryRun });
      toast.success(running ? 'Paper trading stopped' : 'Paper trading started');
      await onRefresh?.();
    } catch (err) {
      toast.error(`Paper command failed: ${err.message || err}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4">
        <p className="panel-title">Paper trading</p>
        <h2 className="mt-1 text-lg font-semibold text-slate-50">{running ? 'Running' : 'Stopped'}</h2>
      </div>

      <div className="mb-4 grid grid-cols-3 gap-2">
        <Metric label="Balance" value={money(balance, 2)} />
        <Metric label="P&L" value={signedMoney(pnl, 2)} tone={pnl >= 0 ? 'good' : 'bad'} />
        <Metric label="Open" value={openPositions} />
      </div>

      <label className="mb-3 flex items-center justify-between rounded-lg border border-slate-700/70 bg-slate-950/40 px-3 py-2 text-sm text-slate-300">
        <span>Dry-run mode</span>
        <input
          type="checkbox"
          checked={dryRun}
          disabled={running}
          onChange={(event) => setDryRun(event.target.checked)}
          className="h-4 w-4 accent-cyan-400"
        />
      </label>

      <button
        className={`icon-button w-full ${running ? 'danger-button' : ''}`}
        onClick={togglePaper}
        disabled={loading}
      >
        {running ? <FiSquare aria-hidden="true" /> : <FiPlay aria-hidden="true" />}
        {loading ? 'Working' : running ? 'Stop Paper Trading' : 'Start Paper Trading'}
      </button>

      <div className="mt-3 text-xs text-slate-500">24h {signedMoney(dayPnl, 2)}</div>
    </div>
  );
}

function Metric({ label, value, tone }) {
  const color = tone === 'good' ? 'text-emerald-300' : tone === 'bad' ? 'text-red-300' : 'text-slate-50';
  return (
    <div className="rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
      <div className="text-[10px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`mt-1 text-lg font-semibold ${color}`}>{value}</div>
    </div>
  );
}
