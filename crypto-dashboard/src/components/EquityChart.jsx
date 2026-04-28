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

export default function EquityChart({ history, live }) {
  const points = history || [];
  const current = Number(live?.balance ?? points.at(-1)?.balance ?? 0);
  const first = Number(points[0]?.balance ?? current);
  const delta = current - first;
  const bars = points.slice(-18).map((point, index, items) => ({
    ...point,
    pnl: index === 0 ? 0 : point.balance - items[index - 1].balance,
  }));

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4 flex items-start justify-between">
        <div>
          <p className="panel-title">Equity curve</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">{money(current, 2)}</h2>
          <div className={delta >= 0 ? 'text-sm text-emerald-300' : 'text-sm text-red-300'}>
            {signedMoney(delta, 2)}
          </div>
        </div>
        <FiTrendingUp className="text-cyan-300" size={22} aria-hidden="true" />
      </div>

      <div className="grid min-h-[300px] gap-4">
        <div className="h-[300px] rounded-lg border border-slate-700/70 bg-slate-950/30 p-3">
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
        <div className="h-[300px] rounded-lg border border-slate-700/70 bg-slate-950/30 p-3">
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
      <div className="mt-3 text-xs text-slate-500">
        Last update {timestampLabel(live?.time)}
      </div>
    </div>
  );
}
