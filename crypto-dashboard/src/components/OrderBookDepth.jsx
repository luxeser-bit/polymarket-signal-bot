import { useCallback, useEffect, useMemo, useState } from 'react';
import { FiDatabase } from 'react-icons/fi';
import { getJson } from '../hooks/useApi';
import { useInterval } from '../hooks/useInterval';
import { money, numberFull, shortAddress } from '../utils/format';

export default function OrderBookDepth({ market, embedded = false }) {
  const marketKey = market?.asset || market?.marketId || market?.market_id || market?.id || '';
  const [book, setBook] = useState(null);
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    if (!marketKey) {
      setBook(null);
      return null;
    }
    try {
      const payload = await getJson(`/api/orderbook/${encodeURIComponent(marketKey)}`);
      setBook(payload);
      setError(payload?.error || '');
      return payload;
    } catch (err) {
      setError(err.message || String(err));
      throw err;
    }
  }, [marketKey]);

  useEffect(() => {
    refresh().catch(() => {});
  }, [refresh]);

  useInterval(() => refresh().catch(() => {}), marketKey ? 10000 : null);

  const bids = Array.isArray(book?.bids) ? book.bids.slice(0, 12) : [];
  const asks = Array.isArray(book?.asks) ? book.asks.slice(0, 12) : [];
  const rows = useMemo(() => depthRows(bids, asks), [bids, asks]);
  const maxNotional = Math.max(1, ...rows.flatMap((row) => [row.bid?.notional || 0, row.ask?.notional || 0]));
  const totalDepth = Number(book?.total_bid_depth || 0) + Number(book?.total_ask_depth || 0);

  const shellClass = embedded
    ? 'border border-slate-700/70 bg-slate-950/30 p-0'
    : 'panel-card p-0';

  return (
    <div className={shellClass}>
      <div className="border-b border-slate-700/70 px-3 py-2">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="panel-title">Order book depth // CLOB</p>
            <h2 className="mt-1 truncate text-lg font-semibold uppercase text-cyanLive" title={book?.title || market?.market || marketKey}>
              {book?.title || market?.market || shortAddress(marketKey) || 'Select market'}
            </h2>
          </div>
          <FiDatabase className="text-cyanLive" size={22} aria-hidden="true" />
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2 text-[11px] uppercase text-slate-400 md:grid-cols-5">
          <DepthStat label="spread" value={book ? numberFull(Number(book.spread || 0), 4) : '-'} />
          <DepthStat label="mid" value={book ? numberFull(Number(book.mid_price || 0), 4) : '-'} />
          <DepthStat label="bid depth" value={money(book?.total_bid_depth || 0, 0)} />
          <DepthStat label="ask depth" value={money(book?.total_ask_depth || 0, 0)} />
          <DepthStat
            label="liquidity"
            value={book?.low_liquidity ? 'LOW' : totalDepth > 0 ? 'OK' : '-'}
            tone={book?.low_liquidity ? 'text-red-300' : 'text-cyanLive'}
          />
        </div>
      </div>

      <div className="px-3 py-2">
        {!marketKey ? (
          <div className="py-12 text-center text-xs uppercase text-slate-600">CLICK A POSITION TO LOAD ORDER BOOK</div>
        ) : error && !book?.bids?.length && !book?.asks?.length ? (
          <div className="py-12 text-center text-xs uppercase text-red-300">{error}</div>
        ) : (
          <>
            <div className="mb-2 grid grid-cols-[1fr_80px_1fr] gap-3 text-[10px] font-bold uppercase text-slate-500">
              <span>Bids</span>
              <span className="text-center">Price</span>
              <span className="text-right">Asks</span>
            </div>
            <div className="max-h-[340px] overflow-hidden">
              {rows.map((row, index) => (
                <div key={`${row.bid?.price || '-'}-${row.ask?.price || '-'}-${index}`} className="grid grid-cols-[1fr_80px_1fr] items-center gap-3 border-b border-slate-800/80 py-1 text-xs">
                  <DepthSide level={row.bid} maxNotional={maxNotional} side="bid" />
                  <div className="text-center text-slate-500">
                    <div>{row.bid ? numberFull(row.bid.price, 3) : '-'}</div>
                    <div>{row.ask ? numberFull(row.ask.price, 3) : '-'}</div>
                  </div>
                  <DepthSide level={row.ask} maxNotional={maxNotional} side="ask" />
                </div>
              ))}
              {!rows.length ? (
                <div className="py-12 text-center text-xs uppercase text-slate-600">NO DEPTH LEVELS</div>
              ) : null}
            </div>
          </>
        )}
        <div className="mt-2 flex flex-wrap justify-between gap-2 text-[10px] uppercase text-slate-500">
          <span>Source <b className="text-cyanLive">{book?.source || '-'}</b></span>
          <span>Asset <b className="text-cyanLive">{shortAddress(book?.asset || marketKey)}</b></span>
          {book?.warning ? <span className="text-amber-300">{book.warning}</span> : null}
        </div>
      </div>
    </div>
  );
}

function DepthStat({ label, value, tone = 'text-cyanLive' }) {
  return (
    <div className="border border-slate-700/70 bg-slate-950/30 px-3 py-2">
      <span className="text-slate-500">{label}</span>
      <b className={`ml-2 ${tone}`}>{value}</b>
    </div>
  );
}

function DepthSide({ level, maxNotional, side }) {
  const width = level ? Math.max(2, Math.min(100, (Number(level.notional || 0) / maxNotional) * 100)) : 0;
  const isBid = side === 'bid';
  return (
    <div className={`relative h-7 overflow-hidden border border-slate-800/80 bg-slate-950/40 ${isBid ? 'text-right' : 'text-left'}`}>
      {level ? (
        <div
          className={`absolute top-0 h-full ${isBid ? 'right-0 bg-cyanLive/30' : 'left-0 bg-amber-300/25'}`}
          style={{ width: `${width}%` }}
        />
      ) : null}
      <div className={`relative z-10 flex h-full items-center gap-2 px-2 ${isBid ? 'justify-end' : 'justify-start'}`}>
        <span className={isBid ? 'text-cyanLive' : 'text-amber-300'}>{level ? numberFull(level.size, 0) : '-'}</span>
        <span className="text-slate-500">{level ? money(level.notional, 0) : '-'}</span>
      </div>
    </div>
  );
}

function depthRows(bids, asks) {
  const length = Math.max(bids.length, asks.length);
  return Array.from({ length }, (_, index) => ({
    bid: bids[index],
    ask: asks[index],
  }));
}
