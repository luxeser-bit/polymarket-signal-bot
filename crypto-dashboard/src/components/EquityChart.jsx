import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { FiTrendingUp } from 'react-icons/fi';
import { money, signedMoney, timestampLabel } from '../utils/format';
import { getJson } from '../hooks/useApi';
import { useInterval } from '../hooks/useInterval';

const TIMEFRAMES = [
  [5 * 60, '5m'],
  [30 * 60, '30m'],
  [60 * 60, '1h'],
  [12 * 3600, '12h'],
  [24 * 3600, '24h'],
  [48 * 3600, '48h'],
];

export default function EquityChart({ history, live }) {
  const [timeframe, setTimeframe] = useState(5 * 60);
  const [equity, setEquity] = useState(null);

  const refreshEquity = useCallback(async () => {
    const payload = await getJson(`/api/equity?timeframe_seconds=${timeframe}&max_points=420`);
    setEquity(payload);
    return payload;
  }, [timeframe]);

  useEffect(() => {
    refreshEquity().catch(() => {});
  }, [refreshEquity]);

  useInterval(() => refreshEquity().catch(() => {}), 5000);

  const points = useMemo(() => {
    const source = Array.isArray(equity?.points) && equity.points.length ? equity.points : history || [];
    if (live?.balance === undefined || live?.time === undefined) return source;
    const livePoint = {
      time: Number(live.time),
      label: liveLabel(live.time, timeframe),
      balance: Number(live.balance || 0),
      pnl: Number(live.pnl || 0),
    };
    const previous = source.at(-1);
    if (!previous || Number(livePoint.time) > Number(previous.time || 0)) {
      return [...source, livePoint];
    }
    return source.map((point, index) => (index === source.length - 1 ? { ...point, ...livePoint } : point));
  }, [equity?.points, history, live?.balance, live?.pnl, live?.time, timeframe]);

  const timeframeLabel = TIMEFRAMES.find(([seconds]) => seconds === timeframe)?.[1] || '5m';
  const current = Number(live?.balance ?? points.at(-1)?.balance ?? 0);
  const first = Number(points[0]?.balance ?? current);
  const delta = current - first;
  const bars = points.map((point, index, items) => ({
    ...point,
    pnl: index === 0 ? 0 : point.balance - items[index - 1].balance,
  }));

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="panel-title">Equity curve</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">{money(current, 2)}</h2>
          <div className={delta >= 0 ? 'text-sm text-emerald-300' : 'text-sm text-red-300'}>
            {signedMoney(delta, 2)}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex flex-wrap border border-slate-700/70 bg-slate-950/40">
            {TIMEFRAMES.map(([seconds, label]) => (
              <button
                key={seconds}
                type="button"
                className={
                  seconds === timeframe
                    ? 'border-r border-slate-700/70 bg-cyanLive/20 px-3 py-1.5 text-xs font-bold text-cyanLive'
                    : 'border-r border-slate-700/70 px-3 py-1.5 text-xs font-bold text-slate-500 hover:text-cyanLive'
                }
                onClick={() => setTimeframe(seconds)}
              >
                {label}
              </button>
            ))}
          </div>
          <FiTrendingUp className="text-cyan-300" size={22} aria-hidden="true" />
        </div>
      </div>

      <div className="grid min-h-[300px] gap-4">
        <div className="h-[300px] rounded-lg border border-slate-700/70 bg-slate-950/30 p-3">
          <div className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
            Balance // {timeframeLabel}
          </div>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={points}>
                <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                <XAxis dataKey="label" stroke="#64748b" tick={{ fontSize: 11 }} />
                <YAxis stroke="#64748b" tick={{ fontSize: 11 }} width={64} />
                <Tooltip
                  contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 8 }}
                  labelStyle={{ color: '#e2e8f0' }}
                  formatter={(value) => [money(value, 2), 'Balance']}
                />
                <Line type="monotone" dataKey="balance" stroke="#22d3ee" strokeWidth={3} dot={false} isAnimationActive />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="h-[300px] rounded-lg border border-slate-700/70 bg-slate-950/30 p-3">
          <div className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-slate-500">
            P&amp;L // {timeframeLabel}
          </div>
          <div className="h-[260px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={bars}>
                <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
                <XAxis dataKey="label" stroke="#64748b" tick={{ fontSize: 11 }} />
                <YAxis stroke="#64748b" tick={{ fontSize: 11 }} width={48} />
                <Tooltip
                  contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 8 }}
                  formatter={(value) => [money(value, 2), 'P&L']}
                />
                <Bar dataKey="pnl" fill="#22d3ee" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
      <div className="mt-3 text-xs text-slate-500">
        Last update {timestampLabel(equity?.updated_at || live?.time)}
      </div>
    </div>
  );
}

function liveLabel(value, timeframe) {
  const timestamp = Number(value || 0);
  if (!timestamp) return '';
  const date = new Date(timestamp * 1000);
  if (timeframe <= 3600) {
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
  return date.toLocaleString([], { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}
