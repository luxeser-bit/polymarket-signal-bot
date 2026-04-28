import { FiUsers } from 'react-icons/fi';
import { money, numberFull, shortAddress } from '../utils/format';

const COHORTS = [
  ['STABLE', 'text-emerald-300'],
  ['CANDIDATE', 'text-cyan-300'],
  ['WATCH', 'text-amber-300'],
  ['NOISE', 'text-slate-500'],
];

export default function WalletCohorts({ data }) {
  const counts = data?.counts || {};
  const topWallets = Array.isArray(data?.top_wallets) ? data.top_wallets : [];

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4 flex items-start justify-between">
        <div>
          <p className="panel-title">Wallet cohorts</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">
            {numberFull(data?.scored_wallets || 0)} scored wallets
          </h2>
        </div>
        <FiUsers className="text-cyan-300" size={22} aria-hidden="true" />
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
