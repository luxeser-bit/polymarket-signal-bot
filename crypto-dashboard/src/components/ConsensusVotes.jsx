import { FiCheckCircle, FiShield, FiXCircle } from 'react-icons/fi';
import { money, numberFull, shortAddress, timestampLabel } from '../utils/format';

export default function ConsensusVotes({ data }) {
  const votes = Array.isArray(data?.votes) ? data.votes : [];
  const counts = data?.counts || {};
  const executed = Number(counts.EXECUTE || 0);
  const rejected = Number(counts.REJECT || 0);

  return (
    <div className="panel-card p-4">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <p className="panel-title">Agent consensus votes</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-50">
            {numberFull(executed)} executed / {numberFull(rejected)} rejected
          </h2>
        </div>
        <FiShield className="text-cyan-300" size={22} aria-hidden="true" />
      </div>

      <div className="grid gap-2 md:grid-cols-3">
        <MetricTile label="total meetings" value={numberFull(data?.total || 0)} />
        <MetricTile label="execute" value={numberFull(executed)} tone="text-good" />
        <MetricTile label="reject" value={numberFull(rejected)} tone="text-bad" />
      </div>

      <div className="mt-4 max-h-72 overflow-auto pr-1">
        {votes.length ? (
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-slate-950 text-[10px] uppercase tracking-wide text-slate-500">
              <tr>
                <th className="py-1 pr-2">time</th>
                <th className="py-1 pr-2">market</th>
                <th className="py-1 pr-2">strategist</th>
                <th className="py-1 pr-2">skeptic</th>
                <th className="py-1 pr-2 text-right">conf</th>
                <th className="py-1 pr-2 text-right">size</th>
                <th className="py-1 text-right">decision</th>
              </tr>
            </thead>
            <tbody>
              {votes.map((vote) => (
                <tr key={vote.signal_id} className="border-t border-slate-800/80">
                  <td className="py-1.5 pr-2 text-slate-500">{timestampLabel(vote.decided_at)}</td>
                  <td className="py-1.5 pr-2">
                    <div className="max-w-[340px] truncate text-slate-300" title={vote.title || vote.asset}>
                      {vote.title || shortAddress(vote.asset)}
                    </div>
                    <div className="text-[10px] text-slate-600">{shortAddress(vote.wallet)}</div>
                  </td>
                  <td className="py-1.5 pr-2">
                    <VotePill verdict={vote.strategist_verdict} reason={vote.strategist_reason} />
                  </td>
                  <td className="py-1.5 pr-2">
                    <VotePill verdict={vote.skeptic_verdict} reason={vote.skeptic_reason} />
                  </td>
                  <td className="py-1.5 pr-2 text-right text-cyan-300">
                    {(Number(vote.signal_confidence || 0) * 100).toFixed(0)}%
                  </td>
                  <td className="py-1.5 pr-2 text-right text-slate-400">{money(vote.size_usdc || 0, 0)}</td>
                  <td className="py-1.5 text-right">
                    <DecisionPill decision={vote.final_decision} reason={vote.reason} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="py-8 text-center text-xs text-slate-600">no consensus meetings yet</div>
        )}
      </div>
    </div>
  );
}

function MetricTile({ label, value, tone = 'text-slate-100' }) {
  return (
    <div className="border border-slate-700/70 bg-slate-950/40 p-3">
      <div className={`text-lg font-semibold ${tone}`}>{value}</div>
      <div className="mt-1 text-[10px] font-semibold uppercase tracking-wide text-slate-500">{label}</div>
    </div>
  );
}

function VotePill({ verdict, reason }) {
  const agree = String(verdict || '').toLowerCase() === 'agree';
  return (
    <span
      className={agree ? 'inline-flex items-center gap-1 text-good' : 'inline-flex items-center gap-1 text-bad'}
      title={reason || ''}
    >
      {agree ? <FiCheckCircle aria-hidden="true" /> : <FiXCircle aria-hidden="true" />}
      {agree ? 'agree' : 'disagree'}
    </span>
  );
}

function DecisionPill({ decision, reason }) {
  const execute = String(decision || '').toUpperCase() === 'EXECUTE';
  return (
    <span className={execute ? 'font-semibold text-good' : 'font-semibold text-bad'} title={reason || ''}>
      {execute ? 'EXECUTED' : 'REJECTED'}
    </span>
  );
}
