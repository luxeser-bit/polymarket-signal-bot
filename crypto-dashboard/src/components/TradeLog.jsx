import { useMemo, useState } from 'react';
import { FiFilter, FiList, FiSearch } from 'react-icons/fi';
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
    <div className="panel-card p-0">
      <div className="border-b border-slate-700/70 px-3 py-2">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="panel-title">Trade log // execution tape</p>
            <h2 className="mt-1 text-lg font-semibold uppercase text-cyanLive">
              {numberFull(filteredRows.length)} actions
            </h2>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <label className="flex h-9 items-center gap-2 border border-slate-700/70 bg-slate-950/40 px-3 uppercase text-slate-400">
              <FiFilter aria-hidden="true" />
              <span>Date</span>
              <input
                type="date"
                value={dateFilter}
                onChange={(event) => setDateFilter(event.target.value)}
                className="w-[135px] bg-transparent text-cyanLive outline-none"
                aria-label="Filter by date"
              />
            </label>
            <label className="flex h-9 min-w-[260px] items-center gap-2 border border-slate-700/70 bg-slate-950/40 px-3 uppercase text-slate-400">
              <FiSearch aria-hidden="true" />
              <span>Market</span>
              <input
                type="search"
                value={marketFilter}
                onChange={(event) => setMarketFilter(event.target.value)}
                placeholder="filter"
                className="min-w-0 flex-1 bg-transparent text-cyanLive outline-none placeholder:text-slate-600"
                aria-label="Filter by market"
              />
            </label>
            <FiList className="text-cyanLive" size={22} aria-hidden="true" />
          </div>
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2 text-[11px] uppercase text-slate-400 md:grid-cols-4">
          <TapeStat label="source" value="paper_state + consensus" />
          <TapeStat label="shown" value={`${numberFull(filteredRows.length)} / ${numberFull(rows.length)}`} />
          <TapeStat label="wins" value={numberFull(filteredRows.filter((row) => Number(row.pnl || 0) > 0).length)} />
          <TapeStat label="losses" value={numberFull(filteredRows.filter((row) => Number(row.pnl || 0) < 0).length)} />
        </div>
        {data?.error ? (
          <div className="mt-3 border border-red-400/40 bg-red-500/10 px-3 py-2 text-xs uppercase text-red-200">
            {data.error}
          </div>
        ) : null}
      </div>

      <div className="max-h-[420px] overflow-auto px-3 py-2">
        <table className="min-w-[1080px] w-full border-separate border-spacing-0 text-xs uppercase">
          <thead>
            <tr className="table-head text-[10px]">
              <th className="w-[84px] py-2 pr-3">Time</th>
              <th className="w-[78px] py-2 pr-3">Action</th>
              <th className="py-2 pr-3">Market flow</th>
              <th className="w-[110px] py-2 pr-3 text-right">Price</th>
              <th className="w-[120px] py-2 pr-3 text-right">Size</th>
              <th className="w-[210px] py-2 pr-3">Reason</th>
              <th className="w-[110px] py-2 text-right">P&amp;L</th>
            </tr>
          </thead>
          <tbody>
            {filteredRows.map((row) => (
              <tr key={row.id} className={`trade-tape-row ${rowClass(row)}`}>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-slate-400">{timeLabel(row.timestamp)}</td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <span className={`font-bold ${actionTone(row.action)}`}>{actionLabel(row.action)}</span>
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <div className="flex min-w-0 items-center gap-2">
                    <span className="text-slate-500">{marketPrefix(row)}</span>
                    <span className="truncate font-semibold text-slate-100" title={row.market || row.asset || row.condition_id}>
                      {row.market || shortAddress(row.asset || row.condition_id)}
                    </span>
                    <span className="shrink-0 text-slate-500">{shortAddress(row.wallet || row.asset || row.condition_id)}</span>
                  </div>
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-cyanLive">
                  {Number(row.price || 0) > 0 ? Number(row.price || 0).toFixed(4) : '-'}
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-slate-100">{money(row.size || 0, 2)}</td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <span className="block max-w-[210px] truncate text-slate-400" title={row.reason || ''}>
                    {reasonLabel(row.reason)}
                  </span>
                </td>
                <td className={`border-b border-slate-800/90 py-1.5 text-right font-bold ${pnlTone(row.pnl)}`}>
                  {signedMoney(row.pnl)}
                </td>
              </tr>
            ))}
            {!filteredRows.length ? (
              <tr>
                <td colSpan="7" className="py-8 text-center text-slate-600">NO TRADE LOG ROWS</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TapeStat({ label, value }) {
  return (
    <div className="border border-slate-700/70 bg-slate-950/30 px-3 py-2">
      <span className="text-slate-500">{label}</span>
      <b className="ml-2 text-cyanLive">{value}</b>
    </div>
  );
}

function rowClass(row) {
  const pnl = Number(row.pnl || 0);
  if (String(row.action || '').toUpperCase() === 'REJECT') return 'bg-red-400/[0.025]';
  if (pnl > 0) return 'bg-emerald-400/[0.025]';
  if (pnl < 0) return 'bg-red-400/[0.025]';
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

function timeLabel(value) {
  const timestamp = Number(value || 0);
  if (!timestamp) return '-';
  return new Date(timestamp * 1000).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

function actionLabel(action) {
  const value = String(action || '').toUpperCase();
  if (value.includes('REJECT')) return 'REJECT';
  if (value.includes('SELL') || value.includes('CLOSE') || value.includes('EXIT')) return 'EXIT';
  if (value.includes('BUY') || value.includes('OPEN')) return 'ENTER';
  return value || '-';
}

function actionTone(action) {
  const value = actionLabel(action);
  if (value === 'EXIT') return 'text-amber-300';
  if (value === 'REJECT') return 'text-red-300';
  return 'text-cyanLive';
}

function pnlTone(value) {
  const pnl = Number(value || 0);
  if (pnl > 0) return 'text-good';
  if (pnl < 0) return 'text-red-300';
  return 'text-cyanLive';
}

function signedMoney(value) {
  const pnl = Number(value || 0);
  const sign = pnl > 0 ? '+' : '';
  return `${sign}${money(pnl, 2)}`;
}

function reasonLabel(reason) {
  const value = String(reason || '').trim();
  if (!value) return 'manual / system';
  return value.replaceAll('_', ' ');
}

function marketPrefix(row) {
  const action = actionLabel(row.action);
  if (action === 'REJECT') return 'NO';
  if (action === 'EXIT') return 'OUT';
  return 'FLOW';
}
