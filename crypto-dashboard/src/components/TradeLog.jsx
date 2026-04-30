import { useMemo, useState } from 'react';
import { FiFilter, FiList } from 'react-icons/fi';
import { money, numberFull, shortAddress } from '../utils/format';

export default function TradeLog({ data }) {
  const [dateFilter, setDateFilter] = useState('');
  const [marketFilter, setMarketFilter] = useState('');
  const rows = Array.isArray(data?.rows) ? data.rows : [];
  const filteredRows = useMemo(
    () => rows.filter((row) => matchesDate(row, dateFilter) && matchesMarket(row, marketFilter)),
    [rows, dateFilter, marketFilter],
  );

  return (
    <div className="panel-card p-4">
      <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="panel-title">Trade log</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">
            {numberFull(filteredRows.length)} actions
          </h2>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <div className="flex items-center gap-2 border border-slate-700/70 bg-slate-950/40 px-3 py-2">
            <FiFilter aria-hidden="true" />
            <input
              type="date"
              value={dateFilter}
              onChange={(event) => setDateFilter(event.target.value)}
              className="bg-transparent text-cyanLive outline-none"
              aria-label="Filter by date"
            />
          </div>
          <input
            type="search"
            value={marketFilter}
            onChange={(event) => setMarketFilter(event.target.value)}
            placeholder="market"
            className="min-w-[220px] border border-slate-700/70 bg-slate-950/40 px-3 py-2 text-cyanLive outline-none placeholder:text-slate-600"
            aria-label="Filter by market"
          />
          <FiList className="text-cyan-300" size={22} aria-hidden="true" />
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-sm">
          <thead>
            <tr className="table-head">
              <th className="py-3 pr-4">Time</th>
              <th className="py-3 pr-4">Market</th>
              <th className="py-3 pr-4">Action</th>
              <th className="py-3 pr-4 text-right">Price</th>
              <th className="py-3 pr-4 text-right">Size</th>
              <th className="py-3 pr-4">Reason</th>
              <th className="py-3 text-right">P&amp;L</th>
            </tr>
          </thead>
          <tbody>
            {filteredRows.map((row) => (
              <tr key={row.id} className={rowClass(row)}>
                <td className="border-b border-slate-800 py-3 pr-4 text-slate-500">{dateTimeLabel(row.timestamp)}</td>
                <td className="max-w-[520px] border-b border-slate-800 py-3 pr-4">
                  <div className="truncate text-slate-200" title={row.market || row.asset || row.condition_id}>
                    {row.market || shortAddress(row.asset || row.condition_id)}
                  </div>
                  <div className="text-[10px] text-slate-600">{shortAddress(row.wallet || row.asset || row.condition_id)}</div>
                </td>
                <td className="border-b border-slate-800 py-3 pr-4">
                  <ActionPill action={row.action} />
                </td>
                <td className="border-b border-slate-800 py-3 pr-4 text-right text-slate-300">
                  {Number(row.price || 0) > 0 ? Number(row.price || 0).toFixed(4) : '-'}
                </td>
                <td className="border-b border-slate-800 py-3 pr-4 text-right text-slate-300">{money(row.size || 0, 2)}</td>
                <td className="max-w-[360px] truncate border-b border-slate-800 py-3 pr-4 text-slate-400" title={row.reason || ''}>
                  {row.reason || '-'}
                </td>
                <td className={Number(row.pnl || 0) >= 0 ? 'border-b border-slate-800 py-3 text-right font-semibold text-emerald-300' : 'border-b border-slate-800 py-3 text-right font-semibold text-red-300'}>
                  {money(row.pnl || 0, 2)}
                </td>
              </tr>
            ))}
            {!filteredRows.length ? (
              <tr>
                <td colSpan="7" className="py-8 text-center text-slate-600">No trade log rows</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function ActionPill({ action }) {
  const value = String(action || '').toUpperCase();
  const tone = value === 'SELL'
    ? 'text-amber-300'
    : value === 'REJECT'
      ? 'text-red-300'
      : 'text-cyan-300';
  return (
    <span className={`border border-slate-700/70 bg-slate-950/40 px-2 py-1 text-xs font-semibold ${tone}`}>
      {value || '-'}
    </span>
  );
}

function rowClass(row) {
  const pnl = Number(row.pnl || 0);
  if (pnl > 0) return 'bg-emerald-400/[0.035]';
  if (pnl < 0) return 'bg-red-400/[0.035]';
  return '';
}

function matchesMarket(row, filter) {
  const needle = String(filter || '').trim().toLowerCase();
  if (!needle) return true;
  return [row.market, row.asset, row.condition_id, row.wallet]
    .map((value) => String(value || '').toLowerCase())
    .some((value) => value.includes(needle));
}

function matchesDate(row, filter) {
  if (!filter) return true;
  const timestamp = Number(row.timestamp || 0);
  if (!timestamp) return false;
  const rowDate = new Date(timestamp * 1000).toISOString().slice(0, 10);
  return rowDate === filter;
}

function dateTimeLabel(value) {
  const timestamp = Number(value || 0);
  if (!timestamp) return '-';
  return new Date(timestamp * 1000).toLocaleString([], {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}
