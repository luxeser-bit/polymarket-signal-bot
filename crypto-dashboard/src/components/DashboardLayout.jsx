import { Toaster } from 'react-hot-toast';
import { FiActivity } from 'react-icons/fi';
import LiveWebSocket from './LiveWebSocket';
import SystemControl from './SystemControl';
import IndexerMetrics from './IndexerMetrics';
import WalletCohorts from './WalletCohorts';
import PositionsTable from './PositionsTable';
import EquityChart from './EquityChart';
import PaperTradingControl from './PaperTradingControl';

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
  equityHistory,
  apiErrors,
}) {
  return (
    <div className="min-h-screen px-4 py-4 lg:px-6">
      <Toaster position="top-right" toastOptions={{ style: { background: '#1e293b', color: '#e2e8f0' } }} />
      <header className="mb-4 flex flex-col gap-3 rounded-xl border border-slate-700 bg-slate-950/70 px-4 py-3 shadow-glow lg:flex-row lg:items-center lg:justify-between">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-cyan-400/10 text-cyan-300">
            <FiActivity size={22} aria-hidden="true" />
          </div>
          <div>
            <h1 className="text-xl font-semibold text-slate-50">Polymarket Signal Bot</h1>
            <p className="text-xs text-slate-500">FastAPI control room</p>
          </div>
        </div>
        <LiveWebSocket connected={socket.connected} error={socket.error} />
      </header>

      {apiErrors.length ? (
        <div className="mb-4 rounded-lg border border-red-400/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
          {apiErrors[0]}
        </div>
      ) : null}

      <main className="grid grid-cols-12 gap-4">
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
          <WalletCohorts data={wallets} />
        </section>

        <section className="col-span-12">
          <PositionsTable data={positions} />
        </section>
      </main>
    </div>
  );
}
