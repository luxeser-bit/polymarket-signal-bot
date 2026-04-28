import { useState } from 'react';
import toast from 'react-hot-toast';
import { FiPlay, FiSquare, FiUsers } from 'react-icons/fi';
import { postJson } from '../hooks/useApi';
import { money, numberFull, secondsToDuration, shortAddress, timestampLabel } from '../utils/format';

const COHORTS = [
  ['STABLE', 'text-emerald-300'],
  ['CANDIDATE', 'text-cyan-300'],
  ['WATCH', 'text-amber-300'],
  ['NOISE', 'text-slate-500'],
];

export default function WalletCohorts({ data, training, onRefreshTraining, onRefreshWallets }) {
  const [loading, setLoading] = useState(false);
  const [sampleMode, setSampleMode] = useState(false);
  const counts = data?.counts || {};
  const topWallets = Array.isArray(data?.top_wallets) ? data.top_wallets : [];
  const lastRun = training?.last_run || data?.last_training || null;
  const trainingRunning = Boolean(training?.running);
  const exitExamples =
    training?.exit_examples?.count ?? data?.exit_examples?.count ?? lastRun?.exit_examples ?? 0;

  async function trainModel() {
    setLoading(true);
    try {
      if (trainingRunning) {
        await postJson('/api/training/stop');
        toast.success('Model training stopped');
      } else {
        await postJson('/api/training/start', {
          test: sampleMode,
          limit: sampleMode ? 10000 : undefined,
          force: true,
        });
        toast.success(sampleMode ? 'Sample training started' : 'Model training started');
      }
      await onRefreshTraining?.();
      await onRefreshWallets?.();
    } catch (err) {
      toast.error(`Training failed: ${err.message || err}`);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4 flex items-start justify-between">
        <div>
          <p className="panel-title">Wallet cohorts</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">
            {numberFull(data?.scored_wallets || 0)} scored wallets
          </h2>
        </div>
        <div className="flex items-center gap-2">
          <button
            className="icon-button px-3 py-1.5 text-xs"
            onClick={trainModel}
            disabled={loading}
            title="Run scoring, cohorts and exit model training"
          >
            {trainingRunning ? <FiSquare aria-hidden="true" /> : <FiPlay aria-hidden="true" />}
            {loading ? 'Working' : trainingRunning ? 'Stop Training' : 'Train Model'}
          </button>
          <FiUsers className="text-cyan-300" size={22} aria-hidden="true" />
        </div>
      </div>

      <div className="mb-3 grid gap-2 border border-slate-700/70 bg-slate-950/40 p-3 text-xs uppercase text-slate-400 lg:grid-cols-[1fr_auto]">
        <div>
          <span className="text-cyanLive">Source</span> raw_transactions -&gt; scored_wallets -&gt; wallet_cohorts
          <div className="mt-1">
            <span className={trainingRunning ? 'text-good' : 'text-slate-500'}>
              {trainingRunning ? `Running ${secondsToDuration(training?.uptime_seconds || 0)}` : 'Idle'}
            </span>
            {lastRun?.updated_at ? (
              <span> - last {timestampLabel(lastRun.updated_at)} - {numberFull(lastRun.raw_transactions || 0)} rows</span>
            ) : (
              <span> - no training run yet</span>
            )}
            <span> - exit examples {numberFull(exitExamples)}</span>
          </div>
        </div>
        <label className="flex items-center gap-2 text-slate-400">
          <input
            type="checkbox"
            checked={sampleMode}
            disabled={trainingRunning}
            onChange={(event) => setSampleMode(event.target.checked)}
            className="h-4 w-4 accent-cyan-400"
          />
          Sample 10k
        </label>
      </div>

      <div className="grid grid-cols-4 gap-2">
        {COHORTS.map(([name, color]) => (
          <div key={name} className="rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
            <div className={`text-lg font-semibold ${color}`}>{numberFull(counts[name] || 0)}</div>
            <div className="mt-1 text-[10px] font-semibold uppercase tracking-wide text-slate-500">{name}</div>
          </div>
        ))}
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-2">
        {COHORTS.map(([name, color]) => {
          const rows = topWallets
            .filter((wallet) => (wallet.status || 'NOISE') === name)
            .slice(0, 5);
          return (
            <div key={name} className="min-h-[136px] rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
              <div className={`mb-2 text-xs font-semibold uppercase tracking-wide ${color}`}>{name}</div>
              {rows.length ? (
                <div className="space-y-1.5">
                  {rows.map((wallet) => (
                    <div key={wallet.wallet} className="grid grid-cols-[1fr_auto_auto] gap-2 text-xs">
                      <span className="truncate text-slate-300">{shortAddress(wallet.wallet)}</span>
                      <span className={Number(wallet.pnl || 0) >= 0 ? 'text-emerald-300' : 'text-red-300'}>
                        {money(wallet.pnl || 0, 0)}
                      </span>
                      <span className="text-slate-500">{Number(wallet.sharpe || 0).toFixed(2)}</span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="py-7 text-center text-xs text-slate-600">empty</div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
