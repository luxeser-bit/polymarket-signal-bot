import { Toaster } from 'react-hot-toast';
import { FiActivity } from 'react-icons/fi';
import LiveWebSocket from './LiveWebSocket';
import SystemControl from './SystemControl';
import IndexerMetrics from './IndexerMetrics';
import WalletCohorts from './WalletCohorts';
import PositionsTable from './PositionsTable';
import EquityChart from './EquityChart';
import PaperTradingControl from './PaperTradingControl';
import { numberFull } from '../utils/format';

export default function DashboardLayout({
  live,
  socket,
  systemStatus,
  onRefreshSystem,
  metrics,
  wallets,
  positions,
  paperStatus,
  onRefreshPaper,
  trainingStatus,
  onRefreshTraining,
  onRefreshData,
  equityHistory,
  apiErrors,
}) {
  const componentStatus = systemStatus?.components || {};
  const monitorRunning = Boolean(componentStatus.monitor?.running);
  const componentUptimes = Object.values(componentStatus)
    .filter((component) => component?.running)
    .map((component) => Number(component?.uptime_seconds || 0));
  const systemUptimeSeconds = Math.max(0, ...componentUptimes);
  const runwayDay = Math.floor(systemUptimeSeconds / 86400);
  const runwayProgress = (systemUptimeSeconds % 86400) / 86400;
  const walletsCount = Number(
    wallets?.scored_wallets
      || Object.values(wallets?.counts || {}).reduce((total, value) => total + Number(value || 0), 0),
  );
  const tradesCount = Number(metrics?.raw_events || live?.raw_events || 0);
  const runwayProgressWidth = systemUptimeSeconds > 0 ? Math.max(0.6, runwayProgress * 100) : 0;

  return (
    <div className="terminal-screen min-h-screen px-1 py-4 lg:px-2">
      <Toaster position="top-right" toastOptions={{ style: { background: '#041216', color: '#8af7ff', border: '1px solid rgba(39,158,174,.6)' } }} />
      <header className="terminal-topbar mb-2 flex flex-col gap-3 border px-4 py-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex items-center gap-3">
          <div className="terminal-led flex h-4 w-4 items-center justify-center text-cyan-300">
            <FiActivity size={22} aria-hidden="true" />
          </div>
          <div>
            <h1 className="text-base font-bold uppercase text-cyanLive">POLYMARKET 200 SEED</h1>
            <p className="text-[11px] uppercase text-slate-500">FastAPI control room // polygon signal engine</p>
          </div>
        </div>
        <div className="terminal-meta flex flex-wrap items-center gap-x-5 gap-y-1 text-xs uppercase">
          <span>ENGINE <b>poly_signal</b></span>
          <span>MONITOR <b className={monitorRunning ? 'text-good' : 'text-slate-500'}>{monitorRunning ? 'SCANNING' : 'STOPPED'}</b></span>
          <span>WALLETS <b>{numberFull(walletsCount)}</b></span>
          <span>TRADES <b>{numberFull(tradesCount)}</b></span>
          <span className={socket.connected ? 'text-good' : 'text-bad'}>{socket.connected ? 'LIVE' : 'OFFLINE'}</span>
        </div>
        <LiveWebSocket connected={socket.connected} error={socket.error} />
      </header>

      <div className="terminal-strip mb-3 flex items-center gap-4 border px-3 py-2 text-xs uppercase">
        <span>RUNWAY <b>DAY {runwayDay}</b></span>
        <div className="terminal-progress h-2 flex-1">
          <div style={{ width: `${runwayProgressWidth}%` }} />
        </div>
        <span className={socket.connected ? 'text-good' : 'text-slate-500'}>{socket.connected ? 'LIVE SCAN' : 'WAITING'}</span>
      </div>

      {apiErrors.length ? (
        <div className="mb-4 rounded-lg border border-red-400/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
          {apiErrors[0]}
        </div>
      ) : null}

      <main className="grid grid-cols-12 gap-2">
        <section className="col-span-12 xl:col-span-4">
          <SystemControl status={systemStatus} onRefresh={onRefreshSystem} />
        </section>
        <section className="col-span-12 xl:col-span-4">
          <IndexerMetrics metrics={metrics} live={live} />
        </section>
        <section className="col-span-12 xl:col-span-4">
          <PaperTradingControl status={paperStatus} live={live} onRefresh={onRefreshPaper} />
        </section>

        <section className="col-span-12 2xl:col-span-7">
          <EquityChart history={equityHistory} live={live} />
        </section>
        <section className="col-span-12 2xl:col-span-5">
          <WalletCohorts
            data={wallets}
            training={trainingStatus}
            onRefreshTraining={onRefreshTraining}
            onRefreshWallets={onRefreshData}
          />
        </section>

        <section className="col-span-12">
          <PositionsTable data={positions} />
        </section>
      </main>
    </div>
  );
}
