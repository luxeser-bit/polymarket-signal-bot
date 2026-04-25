const els = {
  appName: document.getElementById("appName"),
  monitorStatus: document.getElementById("monitorStatus"),
  walletCount: document.getElementById("walletCount"),
  tradeCount: document.getElementById("tradeCount"),
  runway: document.getElementById("runway"),
  progressFill: document.getElementById("progressFill"),
  modeStatus: document.getElementById("modeStatus"),
  metrics: document.getElementById("metrics"),
  scannerFeed: document.getElementById("scannerFeed"),
  consensusFeed: document.getElementById("consensusFeed"),
  positionsFeed: document.getElementById("positionsFeed"),
  bankroll: document.getElementById("bankroll"),
  curveCanvas: document.getElementById("curveCanvas"),
  whaleFeed: document.getElementById("whaleFeed"),
  riskFeed: document.getElementById("riskFeed"),
  tradeLog: document.getElementById("tradeLog"),
  depthBook: document.getElementById("depthBook"),
  exitFeed: document.getElementById("exitFeed"),
  alertFeed: document.getElementById("alertFeed"),
  reviewFeed: document.getElementById("reviewFeed"),
  scaleFeed: document.getElementById("scaleFeed"),
  notice: document.getElementById("notice"),
  clock: document.getElementById("clock"),
  demoBtn: document.getElementById("demoBtn"),
  scanBtn: document.getElementById("scanBtn")
};

let lastState = null;

async function loadState() {
  const res = await fetch("/api/state", { cache: "no-store" });
  if (!res.ok) throw new Error(`state ${res.status}`);
  const state = await res.json();
  render(state);
}

async function postAction(path) {
  setNotice("RUNNING");
  const res = await fetch(path, { method: "POST" });
  if (!res.ok) throw new Error(`${path} ${res.status}`);
  const state = await res.json();
  render(state);
  setNotice(state.notice || "DONE");
}

function render(state) {
  lastState = state;
  els.appName.textContent = state.meta.name;
  els.monitorStatus.textContent = String(state.meta.monitorStatus || "offline").toUpperCase();
  els.walletCount.textContent = state.stats.wallets;
  els.tradeCount.textContent = state.stats.trades;
  els.modeStatus.textContent = state.meta.status;
  els.runway.textContent = state.stats.trades ? `DAY ${Math.max(1, Math.ceil(state.stats.trades / 25))}` : "DAY 1";
  els.progressFill.style.width = `${Math.max(8, Math.min(96, state.stats.avgWalletScore * 100))}%`;
  els.bankroll.textContent = money(state.stats.bankroll);

  renderMetrics(state);
  renderScanner(state.scanner);
  renderConsensus(state.consensus);
  renderPositions(state.positions);
  renderWhales(state.whales);
  renderRisk(state.risk);
  renderTradeLog(state.tradeLog);
  renderDepth(state.orderBook);
  renderExits(state.exitTriggers);
  renderAlerts(state.alerts);
  renderReviews(state.reviews);
  renderScale(state);
  drawCurve(state.bankrollCurve);
  tickClock();
  if (state.meta.monitorSummary) {
    setNotice(state.meta.monitorSummary);
  } else if (state.meta.marketFlowSummary) {
    setNotice(state.meta.marketFlowSummary);
  } else if (state.meta.bulkSummary) {
    setNotice(state.meta.bulkSummary);
  } else if (state.meta.analyticsSummary) {
    setNotice(state.meta.analyticsSummary);
  }
}

function renderMetrics(state) {
  const metrics = [
    ["BANKROLL", money(state.stats.bankroll)],
    ["WALLETS", intfmt(state.stats.wallets)],
    ["WIN RATE", pct(state.stats.winRate)],
    ["OPEN POS", intfmt(state.stats.openPositions)],
    ["OPEN COST", money(state.stats.openCost)],
    ["UNREAL", money(state.stats.unrealizedPnl)],
    ["SIGNALS", intfmt(state.stats.signals)],
    ["DECISIONS", intfmt(state.stats.paperEvents)],
    ["FEATURES", intfmt(state.stats.decisionFeatures || 0)],
    ["STREAM", intfmt(state.stats.streamEvents || 0)]
  ];
  els.metrics.innerHTML = metrics.map(([label, value]) => `
    <div class="metric">
      <strong>${escapeHtml(value)}</strong>
      <span>${escapeHtml(label)}</span>
    </div>
  `).join("");
}

function renderScanner(rows) {
  renderRows(els.scannerFeed, rows, row => `
    <div class="row">
      <span class="tag ${tagClass(row.status)}">${escapeHtml(row.status)}</span>
      <span class="market">${escapeHtml(row.category)} ${escapeHtml(row.market)}</span>
      <span class="value">${money(row.notional)} @ ${Number(row.price).toFixed(3)}</span>
    </div>
  `);
}

function renderConsensus(rows) {
  renderRows(els.consensusFeed, rows, row => `
    <div class="row">
      <span class="tag">${escapeHtml(row.action)}</span>
      <span class="market">${escapeHtml(row.category)} ${escapeHtml(row.outcome)} ${escapeHtml(row.market)}</span>
      <span class="value">${Number(row.confidence * 100).toFixed(0)}%</span>
    </div>
  `);
}

function renderPositions(rows) {
  renderRows(els.positionsFeed, rows, row => `
    <div class="row">
      <span class="market">${escapeHtml(row.category)} ${escapeHtml(row.outcome)} / ${escapeHtml(row.market)}</span>
      <span class="value">${money(row.unrealizedPnl)}</span>
      <span class="muted">${pct(row.unrealizedPct)}</span>
      <div class="posbar"><span style="width:${Math.round(row.progress * 100)}%"></span></div>
    </div>
  `);
}

function renderWhales(rows) {
  renderRows(els.whaleFeed, rows, row => `
    <div class="row">
      <span class="tag">${escapeHtml(row.wallet)}</span>
      <span class="market">stable ${Number(row.stability || 0).toFixed(3)} score ${Number(row.score).toFixed(3)} rep ${Number(row.repeat).toFixed(2)} dd ${Number(row.drawdown).toFixed(2)}</span>
      <span class="value">${escapeHtml(row.state)}</span>
    </div>
  `);
}

function renderRisk(rows) {
  renderRows(els.riskFeed, rows, row => `
    <div class="row">
      <span class="tag ${tagClass(row.state)}">${escapeHtml(row.state)}</span>
      <span class="market">${escapeHtml(row.label)}</span>
      <span class="value">${escapeHtml(row.value)}</span>
    </div>
  `);
}

function renderTradeLog(rows) {
  renderRows(els.tradeLog, rows, row => `
    <div class="row">
      <span class="tag ${row.side === "SELL" ? "warn" : ""}">${escapeHtml(row.side)}</span>
      <span class="market">${escapeHtml(row.market)}</span>
      <span class="value">${money(row.notional)}</span>
    </div>
  `);
}

function renderDepth(rows) {
  if (!rows || !rows.length) {
    els.depthBook.innerHTML = `<div class="empty">NO DEPTH PROXY</div>`;
    return;
  }
  els.depthBook.innerHTML = rows.map(row => `
    <div class="depth-row">
      <span class="value">${Number(row.price).toFixed(3)}</span>
      <span class="bookbar bid" style="width:${Math.round(row.bidDepth * 100)}%"></span>
      <span class="bookbar ask" style="width:${Math.round(row.askDepth * 100)}%"></span>
    </div>
  `).join("");
}

function renderExits(rows) {
  renderRows(els.exitFeed, rows, row => `
    <div class="row">
      <span class="tag ${tagClass(row.state)}">${escapeHtml(row.state)}</span>
      <span class="market">${escapeHtml(row.rule)} ${escapeHtml(row.market)}</span>
      <span class="value">${Number(row.price).toFixed(3)}</span>
    </div>
  `);
}

function renderAlerts(rows) {
  renderRows(els.alertFeed, rows, row => `
    <div class="row">
      <span class="tag ${tagClass(row.status)}">${escapeHtml(row.status)}</span>
      <span class="market">${escapeHtml(row.destination)} ${escapeHtml(row.message)}</span>
      <span class="value">${row.error ? "ERR" : "OK"}</span>
    </div>
  `);
}

function renderReviews(rows) {
  renderRows(els.reviewFeed, rows, row => `
    <div class="row">
      <span class="tag warn">P${String(row.priority || 50).padStart(2, "0")}</span>
      <span class="market">${escapeHtml(row.action)} ${escapeHtml(row.outcome)} ${escapeHtml(row.market)}</span>
      <span class="value">${escapeHtml(row.cohort || row.status)} ${Number(row.confidence * 100).toFixed(0)}%</span>
    </div>
  `);
}

function renderScale(state) {
  const tradesPct = state.stats.targetTrades
    ? state.stats.trades / state.stats.targetTrades
    : 0;
  const watchlistPct = state.stats.targetWallets
    ? state.stats.wallets / state.stats.targetWallets
    : 0;
  const syncedWalletsPct = state.stats.targetWallets
    ? state.stats.bulkWalletsSeen / state.stats.targetWallets
    : 0;
  const rows = [
    { status: "TRADES", label: `${intfmt(state.stats.trades)} local / ${intfmt(state.stats.targetTrades)}`, pct: tradesPct },
    { status: "WATCHLIST", label: `${intfmt(state.stats.wallets)} / ${intfmt(state.stats.targetWallets)}`, pct: watchlistPct },
    { status: "SYNCED", label: `${intfmt(state.stats.bulkWalletsSeen)} checkpointed wallets`, pct: syncedWalletsPct },
    { status: "MARKET FLOW", label: state.meta.marketFlowSummary || "not run yet", pct: state.meta.marketFlowSummary ? 1 : 0 },
    { status: "PAGES", label: `${intfmt(state.stats.bulkPagesSynced)} synced`, pct: Math.min(1, state.stats.bulkPagesSynced / 1000) },
    { status: "DUCKDB", label: state.stats.duckdbExists ? `${formatBytes(state.stats.duckdbSizeBytes)} ${state.stats.duckdbPath}` : "snapshot not built", pct: state.stats.duckdbExists ? 1 : 0 },
    { status: "ANALYTICS", label: `${String(state.meta.analyticsStatus || "idle").toUpperCase()} ${state.meta.analyticsSummary || state.meta.analyticsError || ""}`, pct: state.meta.analyticsStatus === "ready" ? 1 : 0 },
    { status: "POLICY", label: state.meta.signalPolicyActive || state.meta.policySummary || `recommended=${state.meta.policyRecommended}`, pct: state.meta.policySummary ? 1 : 0 },
    { status: "JOURNAL", label: state.meta.journalSummary || "no decisions logged yet", pct: state.stats.paperEvents ? 1 : 0 },
    { status: "BOOK HIST", label: state.meta.bookHistorySummary || "no historical order books yet", pct: state.stats.bookHistorySnapshots ? 1 : 0 },
    { status: "LEARNING", label: state.meta.learningSummary || "no outcome rows yet", pct: state.learning && state.learning.length ? 1 : 0 },
    { status: "FEATURES", label: state.meta.featuresSummary || "not built yet", pct: state.stats.decisionFeatures ? 1 : 0 },
    { status: "STREAM", label: state.meta.streamLastSummary || state.meta.streamSummary || "not listening", pct: state.stats.streamEvents ? 1 : 0 }
  ];
  els.scaleFeed.innerHTML = rows.map(row => `
    <div class="row">
      <span class="tag">${escapeHtml(row.status)}</span>
      <span class="market">${escapeHtml(row.label)}</span>
      <span class="value">${(row.pct * 100).toFixed(4)}%</span>
      <div class="posbar"><span style="width:${Math.max(1, Math.round(row.pct * 100))}%"></span></div>
    </div>
  `).join("");
}

function formatBytes(value) {
  let size = Number(value || 0);
  const units = ["B", "KB", "MB", "GB", "TB"];
  for (const unit of units) {
    if (size < 1024 || unit === units[units.length - 1]) {
      return `${size.toFixed(1)} ${unit}`;
    }
    size /= 1024;
  }
  return "0 B";
}

function renderRows(target, rows, renderer) {
  if (!rows || !rows.length) {
    target.innerHTML = `<div class="empty">NO DATA</div>`;
    return;
  }
  target.innerHTML = rows.map(renderer).join("");
}

function drawCurve(points) {
  const canvas = els.curveCanvas;
  const ctx = canvas.getContext("2d");
  const rect = canvas.getBoundingClientRect();
  const scale = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * scale));
  canvas.height = Math.max(1, Math.floor(rect.height * scale));
  ctx.scale(scale, scale);
  const w = rect.width;
  const h = rect.height;
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "rgba(2, 9, 5, 0.6)";
  ctx.fillRect(0, 0, w, h);

  ctx.strokeStyle = "rgba(39, 158, 174, 0.28)";
  ctx.lineWidth = 1;
  for (let y = 34; y < h; y += 42) {
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(w, y);
    ctx.stroke();
  }

  if (!points || points.length < 2) return;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = Math.max(1, max - min);
  const pad = 26;
  const xy = points.map((p, i) => [
    (i / (points.length - 1)) * w,
    h - pad - ((p - min) / range) * (h - pad * 2)
  ]);

  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, "rgba(53, 230, 242, 0.36)");
  grad.addColorStop(1, "rgba(53, 230, 242, 0.02)");
  ctx.beginPath();
  ctx.moveTo(xy[0][0], h);
  xy.forEach(([x, y]) => ctx.lineTo(x, y));
  ctx.lineTo(xy[xy.length - 1][0], h);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.beginPath();
  xy.forEach(([x, y], i) => {
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#35e6f2";
  ctx.lineWidth = 3;
  ctx.shadowColor = "#35e6f2";
  ctx.shadowBlur = 14;
  ctx.stroke();
  ctx.shadowBlur = 0;
}

function tickClock() {
  const now = new Date();
  els.clock.textContent = now.toLocaleTimeString([], { hour12: false });
}

function setNotice(value) {
  els.notice.textContent = value;
}

function tagClass(value) {
  if (["KILL", "HIGH", "SELL", "EXIT", "LOCKED"].includes(value)) return "bad";
  if (["WATCH", "CHECK", "ARMED", "TAKE", "TRIM", "TIME", "STALE"].includes(value)) return "warn";
  return "";
}

function money(value) {
  const n = Number(value || 0);
  const sign = n < 0 ? "-" : "";
  return `${sign}$${Math.abs(n).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function pct(value) {
  return `${(Number(value || 0) * 100).toFixed(1)}%`;
}

function intfmt(value) {
  return Number(value || 0).toLocaleString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

els.demoBtn.addEventListener("click", () => postAction("/api/demo").catch(err => setNotice(err.message)));
els.scanBtn.addEventListener("click", () => postAction("/api/scan").catch(err => setNotice(err.message)));
window.addEventListener("resize", () => lastState && drawCurve(lastState.bankrollCurve));
setInterval(() => loadState().catch(err => setNotice(err.message)), 6000);
setInterval(tickClock, 1000);
loadState().catch(err => setNotice(err.message));
