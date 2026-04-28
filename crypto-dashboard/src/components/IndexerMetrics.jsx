import { FiDatabase } from 'react-icons/fi';
import { clamp, numberCompact, numberFull, percent } from '../utils/format';

const TARGET = 86000000;

export default function IndexerMetrics({ metrics, live }) {
  const rawEvents = Number(live?.raw_events ?? metrics?.raw_events ?? 0);
  const lastBlock = Number(live?.last_block ?? metrics?.last_block ?? 0);
  const speed = Number(live?.indexer_speed ?? metrics?.blocks_per_second ?? 0);
  const progress = clamp(metrics?.progress ?? rawEvents / TARGET);

  return (
    <div className="panel-card h-full p-4">
      <div className="mb-4 flex items-start justify-between">
        <div>
          <p className="panel-title">Indexer metrics</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">Polygon history</h2>
        </div>
        <FiDatabase className="text-cyan-300" size={22} aria-hidden="true" />
      </div>

      <div className="grid grid-cols-2 gap-3">
        <Metric label="Raw events" value={numberFull(rawEvents)} />
        <Metric label="Last block" value={numberFull(lastBlock)} />
        <Metric label="Speed" value={`${speed.toFixed(2)} blk/s`} />
        <Metric label="Target" value={numberCompact(TARGET, 1)} />
      </div>

      <div className="mt-5">
        <div className="mb-2 flex items-center justify-between text-xs text-slate-400">
          <span>{numberFull(rawEvents)} / {numberFull(TARGET)}</span>
          <span>{percent(progress, 4)}</span>
        </div>
        <div className="h-3 overflow-hidden rounded-full bg-slate-950">
          <div
            className="h-full rounded-full bg-gradient-to-r from-cyan-500 to-emerald-400 transition-all duration-300"
            style={{ width: `${Math.max(0.5, progress * 100)}%` }}
          />
        </div>
      </div>
    </div>
  );
}

function Metric({ label, value }) {
  return (
    <div className="rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className="metric-value mt-1">{value}</div>
    </div>
  );
}
