import { useMemo } from 'react';
import { FiList } from 'react-icons/fi';
import { money, numberFull, secondsToDuration } from '../utils/format';

export default function PositionsTable({ data }) {
  const rows = useMemo(() => {
    const positions = Array.isArray(data?.open_positions) ? data.open_positions : [];
    return positions
      .map((position) => normalizePosition(position))
      .sort((a, b) => b.pnl - a.pnl);
  }, [data]);

  return (
    <div className="panel-card p-4">
      <div className="mb-4 flex items-start justify-between">
        <div>
          <p className="panel-title">Open positions</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">{rows.length} active markets</h2>
        </div>
        <FiList className="text-cyan-300" size={22} aria-hidden="true" />
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-sm">
          <thead>
            <tr className="table-head">
              <th className="py-3 pr-4">Market</th>
              <th className="py-3 pr-4">Side</th>
              <th className="py-3 pr-4 text-right">Size</th>
              <th className="py-3 pr-4 text-right">Entry</th>
              <th className="py-3 pr-4 text-right">Current</th>
              <th className="py-3 pr-4 text-right">P&amp;L</th>
              <th className="py-3 pr-4 text-right">P&amp;L %</th>
              <th className="py-3 text-right">Time open</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id} className="border-b border-slate-800">
                <td className="max-w-[420px] truncate border-b border-slate-800 py-3 pr-4 text-slate-200">
                  {row.market}
                </td>
                <td className="border-b border-slate-800 py-3 pr-4">
                  <span className="rounded-full bg-cyan-400/10 px-2 py-1 text-xs font-semibold text-cyan-200">
                    {row.side}
                  </span>
                </td>
                <td className="border-b border-slate-800 py-3 pr-4 text-right text-slate-300">{money(row.size, 2)}</td>
                <td className="border-b border-slate-800 py-3 pr-4 text-right text-slate-300">{row.entry.toFixed(4)}</td>
                <td className="border-b border-slate-800 py-3 pr-4 text-right text-slate-300">{row.current.toFixed(4)}</td>
                <td className={`border-b border-slate-800 py-3 pr-4 text-right font-semibold ${row.pnl >= 0 ? 'text-emerald-300' : 'text-red-300'}`}>
                  {money(row.pnl, 2)}
                </td>
                <td className={`border-b border-slate-800 py-3 pr-4 text-right ${row.pnlPct >= 0 ? 'text-emerald-300' : 'text-red-300'}`}>
                  {numberFull(row.pnlPct, 2)}%
                </td>
                <td className="border-b border-slate-800 py-3 text-right text-slate-400">{secondsToDuration(row.openSeconds)}</td>
              </tr>
            ))}
            {!rows.length ? (
              <tr>
                <td colSpan="8" className="py-8 text-center text-slate-600">No open positions</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function normalizePosition(position) {
  const entry = Number(position.entry_price ?? position.entryPrice ?? 0);
  const current = Number(position.current_price ?? position.currentPrice ?? entry);
  const size = Number(position.size ?? position.size_usdc ?? position.sizeUsdc ?? 0);
  const pnl = Number(position.pnl ?? position.unrealizedPnl ?? (current && entry ? (current / entry - 1) * size : 0));
  return {
    id: position.id || position.position_id || position.positionId || `${position.market_id}-${position.opened_at}`,
    market: position.market || position.title || position.market_id || position.asset || '-',
    side: position.side || 'BUY',
    size,
    entry,
    current,
    pnl,
    pnlPct: size ? (pnl / size) * 100 : 0,
    openSeconds: Math.max(0, Date.now() / 1000 - Number(position.opened_at || position.openedAt || 0)),
  };
}
