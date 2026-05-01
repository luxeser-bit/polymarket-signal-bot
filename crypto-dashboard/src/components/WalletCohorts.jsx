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
const TARGET_WALLETS = 14000;

export default function WalletCohorts({ data, metrics, training, onRefreshTraining, onRefreshWallets }) {
  const [loading, setLoading] = useState(false);
  const [sampleMode, setSampleMode] = useState(false);
  const counts = data?.counts || {};
  const scoredRows = Array.isArray(data?.scored_wallet_rows)
    ? data.scored_wallet_rows
    : Array.isArray(data?.top_wallets)
      ? data.top_wallets
      : [];
  const topWallets = scoredRows;
  const cohortWallets = data?.cohort_wallets && typeof data.cohort_wallets === 'object'
    ? data.cohort_wallets
    : {};
  const lastRun = training?.last_run || data?.last_training || null;
  const trainingRunning = Boolean(training?.running);
  const exitExamples =
    training?.exit_examples?.count ?? data?.exit_examples?.count ?? lastRun?.exit_examples ?? 0;
  const exitRows = Array.isArray(data?.exit_examples?.examples) ? data.exit_examples.examples : [];
  const modelMetrics = data?.model_metrics || {};
  const scoredWallets = Number(data?.scored_wallets || 0);
  const currentRawRows = Number(metrics?.raw_events || data?.raw_transactions || 0);
  const trainedRawRows = Number(lastRun?.raw_transactions || 0);
  const newRowsSinceTraining = Math.max(0, currentRawRows - trainedRawRows);
  const smartFlowWallets = Number(counts.STABLE || 0) + Number(counts.CANDIDATE || 0);
  const walletProgress = Math.min(1, smartFlowWallets / TARGET_WALLETS);
  const walletProgressPct = walletProgress * 100;
  const walletsRemaining = Math.max(0, TARGET_WALLETS - smartFlowWallets);

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
      onRefreshTraining?.().catch(() => {});
      onRefreshWallets?.().catch(() => {});
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
            {numberFull(scoredWallets)} scored wallets
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

      <div className="mb-3 border border-cyan-500/60 bg-cyan-950/20 p-3">
        <div className="mb-2 flex items-center justify-between gap-3 text-xs uppercase">
          <span className="font-semibold text-cyanLive">Wallet target // market movement map</span>
          <span className="text-slate-400">
            {numberFull(smartFlowWallets)} / {numberFull(TARGET_WALLETS)}
          </span>
        </div>
        <div className="h-2 overflow-hidden bg-slate-950">
          <div
            className="h-full bg-cyanLive shadow-[0_0_14px_rgba(34,211,238,0.65)] transition-all duration-300"
            style={{ width: `${Math.max(0.6, walletProgressPct)}%` }}
          />
        </div>
        <div className="mt-2 grid gap-2 text-[10px] font-semibold uppercase tracking-wide text-slate-500 sm:grid-cols-3">
          <span>Coverage {walletProgressPct.toFixed(2)}%</span>
          <span>Remaining {numberFull(walletsRemaining)}</span>
          <span>Stable + candidate wallets</span>
        </div>
      </div>

      <div className="mb-3 grid gap-2 border border-slate-700/70 bg-slate-950/40 p-3 text-xs uppercase text-slate-400 lg:grid-cols-[1fr_auto]">
        <div>
          <span className="text-cyanLive">Source</span> raw_transactions -&gt; scored_wallets -&gt; wallet_cohorts
          <div className="mt-1">
            <span className={trainingRunning ? 'text-good' : 'text-slate-500'}>
              {trainingRunning ? `Running ${secondsToDuration(training?.uptime_seconds || 0)}` : 'Idle'}
            </span>
            {currentRawRows ? (
              <span> - current {numberFull(currentRawRows)} rows</span>
            ) : null}
            {lastRun?.updated_at ? (
              <span> - trained {timestampLabel(lastRun.updated_at)} on {numberFull(trainedRawRows)} rows</span>
            ) : (
              <span> - no training run yet</span>
            )}
            {newRowsSinceTraining > 0 ? (
              <span> - +{numberFull(newRowsSinceTraining)} new</span>
            ) : null}
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

      <div className="mb-3 grid gap-2 sm:grid-cols-4">
        <MetricTile label="median hold" value={secondsToDuration(modelMetrics.median_hold_time || 0)} />
        <MetricTile label="mae" value={modelMetrics.mae == null ? '-' : secondsToDuration(modelMetrics.mae)} />
        <MetricTile label="r2" value={modelMetrics.r2 == null ? '-' : Number(modelMetrics.r2 || 0).toFixed(3)} />
        <MetricTile label="model" value={modelLabel(modelMetrics.model_type, modelMetrics.fallback_reason)} />
      </div>

      <div className="grid grid-cols-4 gap-2">
        {COHORTS.map(([name, color]) => (
          <div key={name} className="rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
            <div className={`text-lg font-semibold ${color}`}>{numberFull(counts[name] || 0)}</div>
            <div className="mt-1 text-[10px] font-semibold uppercase tracking-wide text-slate-500">{name}</div>
          </div>
        ))}
      </div>

      <div className="mt-4 rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
        <div className="mb-2 flex items-center justify-between gap-3">
          <div className="text-xs font-semibold uppercase tracking-wide text-cyanLive">Scored // top sharpe</div>
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
            {numberFull(scoredRows.length)} / 50
          </div>
        </div>
        <div className="max-h-56 overflow-auto pr-1">
          {scoredRows.length ? (
            <table className="w-full text-left text-xs">
              <thead className="sticky top-0 bg-slate-950 text-[10px] uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="py-1 pr-2">wallet</th>
                  <th className="py-1 pr-2">cohort</th>
                  <th className="py-1 pr-2 text-right">sharpe</th>
                  <th className="py-1 pr-2 text-right">win</th>
                  <th className="py-1 text-right">pnl</th>
                </tr>
              </thead>
              <tbody>
                {scoredRows.map((wallet) => (
                  <tr key={`${wallet.user_address || wallet.wallet}-${wallet.cohort || wallet.status}`} className="border-t border-slate-800/80">
                    <td className="py-1.5 pr-2 text-slate-300">{shortAddress(wallet.user_address || wallet.wallet)}</td>
                    <td className="py-1.5 pr-2 text-cyan-300">{wallet.cohort || wallet.status || '-'}</td>
                    <td className="py-1.5 pr-2 text-right text-slate-100">{Number(wallet.sharpe || 0).toFixed(2)}</td>
                    <td className="py-1.5 pr-2 text-right text-slate-400">{(Number(wallet.win_rate || 0) * 100).toFixed(1)}%</td>
                    <td className={Number(wallet.pnl || 0) >= 0 ? 'py-1.5 text-right text-emerald-300' : 'py-1.5 text-right text-red-300'}>
                      {money(wallet.pnl || 0, 0)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="py-6 text-center text-xs text-slate-600">empty</div>
          )}
        </div>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-2">
        {COHORTS.map(([name, color]) => {
          const sourceRows = Array.isArray(cohortWallets[name])
            ? cohortWallets[name]
            : topWallets.filter((wallet) => (wallet.cohort || wallet.status || 'NOISE') === name);
          const rows = sourceRows.slice(0, 50);
          return (
            <div key={name} className="min-h-[136px] rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
              <div className="mb-2 flex items-center justify-between gap-3">
                <div className={`text-xs font-semibold uppercase tracking-wide ${color}`}>{name}</div>
                <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
                  {numberFull(rows.length)} / {numberFull(counts[name] || sourceRows.length)}
                </div>
              </div>
              {rows.length ? (
                <div className="max-h-44 space-y-1.5 overflow-auto pr-1">
                  {rows.map((wallet) => (
                    <div key={wallet.user_address || wallet.wallet} className="grid grid-cols-[1fr_auto_auto] gap-2 text-xs">
                      <span className="truncate text-slate-300">{shortAddress(wallet.user_address || wallet.wallet)}</span>
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

      <div className="mt-4 rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
        <div className="mb-2 flex items-center justify-between gap-3">
          <div className="text-xs font-semibold uppercase tracking-wide text-cyanLive">Exit examples</div>
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">{numberFull(exitRows.length)}</div>
        </div>
        {exitRows.length ? (
          <div className="overflow-auto">
            <table className="w-full text-left text-xs">
              <thead className="text-[10px] uppercase tracking-wide text-slate-500">
                <tr>
                  <th className="py-1 pr-2">whale</th>
                  <th className="py-1 pr-2">market</th>
                  <th className="py-1 pr-2">entry</th>
                  <th className="py-1 pr-2">exit</th>
                  <th className="py-1 pr-2 text-right">pnl</th>
                  <th className="py-1 text-right">predicted</th>
                </tr>
              </thead>
              <tbody>
                {exitRows.slice(0, 10).map((example, index) => (
                  <tr key={`${example.whale_address}-${example.market_id}-${example.entry_time}-${index}`} className="border-t border-slate-800/80">
                    <td className="py-1.5 pr-2 text-slate-300">{shortAddress(example.whale_address)}</td>
                    <td className="py-1.5 pr-2 text-slate-400">{shortAddress(example.market_id)}</td>
                    <td className="py-1.5 pr-2 text-slate-400">{timestampLabel(example.entry_time)}</td>
                    <td className="py-1.5 pr-2 text-slate-400">{timestampLabel(example.exit_time)}</td>
                    <td className={Number(example.pnl_percent || 0) >= 0 ? 'py-1.5 pr-2 text-right text-emerald-300' : 'py-1.5 pr-2 text-right text-red-300'}>
                      {Number(example.pnl_percent || 0).toFixed(2)}%
                    </td>
                    <td className="py-1.5 text-right text-cyan-300">{secondsToDuration(example.predicted_time || 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="py-6 text-center text-xs text-slate-600">empty</div>
        )}
      </div>
    </div>
  );
}

function MetricTile({ label, value }) {
  return (
    <div className="rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
      <div className="text-sm font-semibold text-slate-100">{value}</div>
      <div className="mt-1 text-[10px] font-semibold uppercase tracking-wide text-slate-500">{label}</div>
    </div>
  );
}

function modelLabel(modelType, fallbackReason) {
  const raw = String(modelType || '').toLowerCase();
  if (raw.includes('ridge')) return 'Ridge';
  if (raw.includes('dummy')) return 'Dummy';
  if (fallbackReason) return 'Dummy';
  return '-';
}
