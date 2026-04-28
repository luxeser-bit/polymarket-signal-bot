import { FiWifi, FiWifiOff } from 'react-icons/fi';

export default function LiveWebSocket({ connected, error }) {
  return (
    <div className="flex items-center gap-2 rounded-full border border-slate-700 bg-slate-900/70 px-3 py-1.5 text-xs">
      {connected ? (
        <FiWifi className="text-good" aria-hidden="true" />
      ) : (
        <FiWifiOff className="text-bad" aria-hidden="true" />
      )}
      <span className={connected ? 'text-emerald-300' : 'text-red-300'}>
        {connected ? 'Live socket' : 'Socket offline'}
      </span>
      {error ? <span className="hidden text-slate-500 lg:inline">{error}</span> : null}
    </div>
  );
}
