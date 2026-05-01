import { useMemo } from 'react';
import { FiList } from 'react-icons/fi';
import { money, numberFull, secondsToDuration, shortAddress } from '../utils/format';

export default function PositionsTable({ data, onSelectMarket, selectedMarketId = '' }) {
  const rows = useMemo(() => {
    const positions = Array.isArray(data?.open_positions) ? data.open_positions : [];
    return positions
      .map((position) => normalizePosition(position))
      .sort((a, b) => b.pnl - a.pnl);
  }, [data]);
  const maxHoldRows = rows.filter((row) => row.holdStatus === 'MAX HOLD').length;
  const staleRows = rows.filter((row) => row.holdStatus === 'STALE PRICE').length;

  return (
    <div className="panel-card p-0">
      <div className="border-b border-slate-700/70 px-3 py-2">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="panel-title">Open positions // active exposure tape</p>
            <h2 className="mt-1 text-lg font-semibold uppercase text-cyanLive">{rows.length} active markets</h2>
          </div>
          <FiList className="text-cyanLive" size={22} aria-hidden="true" />
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2 text-[11px] uppercase text-slate-400 md:grid-cols-5">
          <TapeStat label="source" value={data?.source || 'paper'} />
          <TapeStat label="total" value={numberFull(data?.total_positions || rows.length)} />
          <TapeStat label="open cost" value={money(rows.reduce((sum, row) => sum + row.size, 0), 2)} />
          <TapeStat label="max hold" value={numberFull(maxHoldRows)} tone={maxHoldRows ? 'text-red-300' : 'text-cyanLive'} />
          <TapeStat label="stale price" value={numberFull(staleRows)} tone={staleRows ? 'text-amber-300' : 'text-cyanLive'} />
        </div>
      </div>

      <div className="max-h-[420px] overflow-auto px-3 py-2">
        <table className="min-w-[1120px] w-full border-separate border-spacing-0 text-xs uppercase">
          <thead>
            <tr className="table-head text-[10px]">
              <th className="py-2 pr-3">Market flow</th>
              <th className="w-[82px] py-2 pr-3">Side</th>
              <th className="w-[110px] py-2 pr-3 text-right">Size</th>
              <th className="w-[92px] py-2 pr-3 text-right">Entry</th>
              <th className="w-[92px] py-2 pr-3 text-right">Mark</th>
              <th className="w-[112px] py-2 pr-3 text-right">P&amp;L</th>
              <th className="w-[86px] py-2 pr-3 text-right">P&amp;L %</th>
              <th className="w-[116px] py-2 pr-3 text-right">Open</th>
              <th className="w-[126px] py-2 text-right">Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.id}
                className={`trade-tape-row cursor-pointer ${selectedMarketId && selectedMarketId === (row.asset || row.marketId) ? 'bg-cyanLive/[0.08]' : rowClass(row)}`}
                onClick={() => onSelectMarket?.(row)}
                title="Load order book depth"
              >
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <div className="flex min-w-0 items-center gap-2">
                    <span className="text-slate-500">{row.outcome || 'POS'}</span>
                    <span className="truncate font-semibold text-slate-100" title={row.market}>
                      {row.market}
                    </span>
                    <span className="shrink-0 text-slate-500">{shortAddress(row.wallet || row.asset || row.marketId)}</span>
                  </div>
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 font-bold text-cyanLive">{row.side}</td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-slate-100">{money(row.size, 2)}</td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-slate-400">{row.entry.toFixed(4)}</td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-cyanLive" title={row.priceSource}>
                  {row.current.toFixed(4)}
                </td>
                <td className={`border-b border-slate-800/90 py-1.5 pr-3 text-right font-bold ${pnlTone(row.pnl)}`}>
                  {signedMoney(row.pnl)}
                </td>
                <td className={`border-b border-slate-800/90 py-1.5 pr-3 text-right ${pnlTone(row.pnlPct)}`}>
                  {numberFull(row.pnlPct, 2)}%
                </td>
                <td className={`border-b border-slate-800/90 py-1.5 pr-3 text-right ${holdTone(row.holdStatus)}`}>
                  {secondsToDuration(row.openSeconds)}
                </td>
                <td className={`border-b border-slate-800/90 py-1.5 text-right font-bold ${holdTone(row.holdStatus)}`}>
                  {row.holdStatus}
                </td>
              </tr>
            ))}
            {!rows.length ? (
              <tr>
                <td colSpan="9" className="py-8 text-center text-slate-600">NO OPEN POSITIONS</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TapeStat({ label, value, tone = 'text-cyanLive' }) {
  return (
    <div className="border border-slate-700/70 bg-slate-950/30 px-3 py-2">
      <span className="text-slate-500">{label}</span>
      <b className={`ml-2 ${tone}`}>{value}</b>
    </div>
  );
}

function normalizePosition(position) {
  const entry = Number(position.entry_price ?? position.entryPrice ?? 0);
  const current = Number(position.current_price ?? position.currentPrice ?? entry);
  const size = Number(position.size ?? position.size_usdc ?? position.sizeUsdc ?? 0);
  const pnl = Number(position.pnl ?? position.unrealizedPnl ?? (current && entry ? (current / entry - 1) * size : 0));
  const openSeconds = Number(position.open_seconds ?? position.openSeconds ?? Math.max(0, Date.now() / 1000 - Number(position.opened_at || position.openedAt || 0)));
  return {
    id: position.id || position.position_id || position.positionId || `${position.market_id}-${position.opened_at}`,
    marketId: position.market_id || position.condition_id || position.asset || '',
    asset: position.asset || '',
    wallet: position.wallet || '',
    outcome: String(position.outcome || '').toUpperCase(),
    market: position.market || position.title || position.market_id || position.asset || '-',
    side: String(position.side || 'BUY').toUpperCase(),
    size,
    entry,
    current,
    pnl,
    pnlPct: size ? (pnl / size) * 100 : 0,
    openSeconds,
    holdStatus: String(position.hold_status || position.holdStatus || 'LIVE').toUpperCase(),
    priceSource: position.price_source || position.priceSource || '',
  };
}

function rowClass(row) {
  if (row.holdStatus === 'MAX HOLD') return 'bg-red-400/[0.035]';
  if (row.holdStatus === 'STALE PRICE') return 'bg-amber-300/[0.035]';
  if (row.pnl > 0) return 'bg-emerald-400/[0.025]';
  if (row.pnl < 0) return 'bg-red-400/[0.025]';
  return '';
}

function pnlTone(value) {
  const amount = Number(value || 0);
  if (amount > 0) return 'text-good';
  if (amount < 0) return 'text-red-300';
  return 'text-cyanLive';
}

function holdTone(status) {
  if (status === 'MAX HOLD') return 'text-red-300';
  if (status === 'STALE PRICE') return 'text-amber-300';
  return 'text-cyanLive';
}

function signedMoney(value) {
  const amount = Number(value || 0);
  const sign = amount > 0 ? '+' : '';
  return `${sign}${money(amount, 2)}`;
}
