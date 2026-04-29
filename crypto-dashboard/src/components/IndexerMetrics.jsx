import { FiDatabase } from 'react-icons/fi';
import { clamp, numberCompact, numberFull, percent } from '../utils/format';

const TARGET = 86000000;

export default function IndexerMetrics({ metrics, live, progress: indexerProgress }) {
  const rawEvents = Number(live?.raw_events ?? metrics?.raw_events ?? 0);
  const lastBlock = Number(indexerProgress?.last_block ?? live?.last_block ?? metrics?.last_block ?? 0);
  const speed = Number(indexerProgress?.speed_blocks_per_second ?? live?.indexer_speed ?? metrics?.blocks_per_second ?? 0);
  const progress = clamp(metrics?.progress ?? rawEvents / TARGET);
  const lastBlockDate = formatBlockDate(indexerProgress?.last_block_date);
  const eta = formatEta(indexerProgress?.estimated_completion_seconds);

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

      <div className="mt-3 grid gap-2 border border-slate-700/70 bg-slate-950/40 p-3 text-xs uppercase text-slate-400">
        <div className="flex items-center justify-between gap-3">
          <span>Last block date</span>
          <span className="text-right font-semibold text-cyanLive">{lastBlockDate}</span>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span>ETA</span>
          <span className="text-right font-semibold text-cyanLive">{eta}</span>
        </div>
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

function formatBlockDate(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  const hh = String(date.getUTCHours()).padStart(2, '0');
  const mi = String(date.getUTCMinutes()).padStart(2, '0');
  const ss = String(date.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} UTC`;
}

function formatEta(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return '-';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return `≈ ${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `≈ ${hours}h ${minutes}m`;
  return `≈ ${minutes}m`;
}

function Metric({ label, value }) {
  return (
    <div className="rounded-lg border border-slate-700/70 bg-slate-950/40 p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className="metric-value mt-1">{value}</div>
    </div>
  );
}
