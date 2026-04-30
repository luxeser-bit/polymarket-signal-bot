import { FiShield } from 'react-icons/fi';
import { money, numberFull, shortAddress } from '../utils/format';

export default function ConsensusVotes({ data }) {
  const votes = Array.isArray(data?.votes) ? data.votes : [];
  const counts = data?.counts || {};
  const executed = Number(counts.EXECUTE || 0);
  const rejected = Number(counts.REJECT || 0);
  const agreeBoth = votes.filter((vote) => isAgree(vote.strategist_verdict) && isAgree(vote.skeptic_verdict)).length;
  const splitVotes = votes.filter((vote) => isAgree(vote.strategist_verdict) !== isAgree(vote.skeptic_verdict)).length;

  return (
    <div className="panel-card p-0">
      <div className="border-b border-slate-700/70 px-3 py-2">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="panel-title">Agent consensus votes // signal board</p>
            <h2 className="mt-1 text-lg font-semibold uppercase text-cyanLive">
              {numberFull(executed)} executed / {numberFull(rejected)} rejected
            </h2>
          </div>
          <FiShield className="text-cyanLive" size={22} aria-hidden="true" />
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2 text-[11px] uppercase text-slate-400 md:grid-cols-5">
          <TapeStat label="meetings" value={numberFull(data?.total || votes.length)} />
          <TapeStat label="execute" value={numberFull(executed)} tone="text-good" />
          <TapeStat label="reject" value={numberFull(rejected)} tone="text-red-300" />
          <TapeStat label="both agree" value={numberFull(agreeBoth)} />
          <TapeStat label="split vote" value={numberFull(splitVotes)} tone={splitVotes ? 'text-amber-300' : 'text-cyanLive'} />
        </div>
      </div>

      <div className="max-h-[360px] overflow-auto px-3 py-2">
        <table className="min-w-[1100px] w-full border-separate border-spacing-0 text-xs uppercase">
          <thead>
            <tr className="table-head text-[10px]">
              <th className="w-[84px] py-2 pr-3">Time</th>
              <th className="py-2 pr-3">Signal board</th>
              <th className="w-[150px] py-2 pr-3">Strategist</th>
              <th className="w-[170px] py-2 pr-3">Skeptic</th>
              <th className="w-[90px] py-2 pr-3 text-right">Conf</th>
              <th className="w-[110px] py-2 pr-3 text-right">Size</th>
              <th className="w-[132px] py-2 text-right">Decision</th>
            </tr>
          </thead>
          <tbody>
            {votes.map((vote) => (
              <tr key={vote.signal_id} className={`trade-tape-row ${rowClass(vote)}`}>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-slate-400">{timeLabel(vote.decided_at)}</td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <div className="flex min-w-0 items-center gap-2">
                    <span className="text-slate-500">{String(vote.outcome || vote.action || 'FLOW').toUpperCase()}</span>
                    <span className="truncate font-semibold text-slate-100" title={vote.title || vote.asset}>
                      {vote.title || shortAddress(vote.asset)}
                    </span>
                    <span className="shrink-0 text-slate-500">{shortAddress(vote.wallet)}</span>
                  </div>
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <VoteCell verdict={vote.strategist_verdict} confidence={vote.strategist_confidence} reason={vote.strategist_reason} />
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3">
                  <VoteCell verdict={vote.skeptic_verdict} confidence={vote.skeptic_confidence} reason={vote.skeptic_reason} />
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-cyanLive">
                  {percent(vote.signal_confidence)}
                </td>
                <td className="border-b border-slate-800/90 py-1.5 pr-3 text-right text-slate-100">
                  {money(vote.size_usdc || 0, 2)}
                </td>
                <td className={`border-b border-slate-800/90 py-1.5 text-right font-bold ${decisionTone(vote.final_decision)}`} title={vote.reason || ''}>
                  {decisionLabel(vote.final_decision)}
                </td>
              </tr>
            ))}
            {!votes.length ? (
              <tr>
                <td colSpan="7" className="py-8 text-center text-slate-600">NO CONSENSUS MEETINGS YET</td>
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

function VoteCell({ verdict, confidence, reason }) {
  const agree = isAgree(verdict);
  return (
    <div className="flex min-w-0 items-center gap-2" title={reason || ''}>
      <span className={`font-bold ${agree ? 'text-good' : 'text-red-300'}`}>{agree ? 'AGREE' : 'DENY'}</span>
      <span className="text-slate-500">{percent(confidence)}</span>
    </div>
  );
}

function rowClass(vote) {
  if (String(vote.final_decision || '').toUpperCase() === 'EXECUTE') return 'bg-emerald-400/[0.025]';
  return 'bg-red-400/[0.025]';
}

function isAgree(verdict) {
  return String(verdict || '').toLowerCase() === 'agree';
}

function decisionLabel(decision) {
  return String(decision || '').toUpperCase() === 'EXECUTE' ? 'EXECUTED' : 'REJECTED';
}

function decisionTone(decision) {
  return String(decision || '').toUpperCase() === 'EXECUTE' ? 'text-good' : 'text-red-300';
}

function percent(value) {
  return `${(Number(value || 0) * 100).toFixed(0)}%`;
}

function timeLabel(value) {
  const timestamp = Number(value || 0);
  if (!timestamp) return '-';
  return new Date(timestamp * 1000).toLocaleTimeString([], {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}
