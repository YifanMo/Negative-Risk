const els = {
  form: document.getElementById("historyForm"),
  state: document.getElementById("historyState"),
  hours: document.getElementById("hoursInput"),
  fidelity: document.getElementById("fidelityInput"),
  investment: document.getElementById("investmentInput"),
  slippage: document.getElementById("slippageInput"),
  source: document.getElementById("sourceInput"),
  oppCount: document.getElementById("oppCount"),
  tradeCount: document.getElementById("tradeCount"),
  totalPnl: document.getElementById("totalPnl"),
  endingEquity: document.getElementById("endingEquity"),
  sourceLabel: document.getElementById("sourceLabel"),
  coverageWindow: document.getElementById("coverageWindow"),
  coverageEvents: document.getElementById("coverageEvents"),
  coverageNote: document.getElementById("coverageNote"),
  generatedAt: document.getElementById("generatedAt"),
  curveMeta: document.getElementById("curveMeta"),
  equityChart: document.getElementById("equityChart"),
  tradeRows: document.getElementById("tradeRows"),
  topEventRows: document.getElementById("topEventRows"),
};

let replayRequestSeq = 0;

els.form.addEventListener("submit", (event) => {
  event.preventDefault();
  runReplay(true);
});

els.source.addEventListener("change", () => {
  replayRequestSeq += 1;
  syncSourceControls();
  setState("Idle", true);
});

async function runReplay(forceRefresh = false) {
  const requestSeq = ++replayRequestSeq;
  const source = els.source.value || "book-snapshot";
  const request = {
    fidelity: els.fidelity.value || "5",
    investment_usd: els.investment.value || "1000",
    slippage_pct: els.slippage.value || "0.5",
    source,
    refresh: String(forceRefresh),
  };
  if (source !== "pmxt-archive") {
    request.hours = els.hours.value || "24";
  }
  const params = new URLSearchParams(request);
  setState("Loading", true);
  try {
    const res = await fetch(`/api/history/replay?${params.toString()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const payload = await res.json();
    if (requestSeq !== replayRequestSeq) return;
    render(payload);
    setState("Ready", true);
  } catch (err) {
    if (requestSeq !== replayRequestSeq) return;
    setState("Error", false);
    els.equityChart.innerHTML = `<p class="empty">Replay failed: ${escapeHtml(err.message)}</p>`;
  }
}

function render(payload) {
  const summary = payload.summary || {};
  const coverage = payload.coverage || {};
  els.oppCount.textContent = summary.opportunity_count ?? 0;
  els.tradeCount.textContent = summary.simulated_trade_count ?? 0;
  els.totalPnl.textContent = money(summary.total_pnl_usd ?? 0);
  els.endingEquity.textContent = money(summary.ending_equity ?? 0);
  const sourceMode = payload.source?.mode || coverage.source_mode || payload.params?.source || "book-snapshot";
  const sourceName =
    sourceMode === "price-proxy"
      ? "Price proxy"
      : sourceMode === "pmxt-archive"
        ? "PMXT archive order book"
        : "Local book snapshots";
  const dataCount =
    coverage.archive_row_count ??
    coverage.snapshot_count ??
    coverage.price_point_count ??
    coverage.bucketed_snapshot_count ??
    0;
  const dataLabel =
    sourceMode === "price-proxy"
      ? "price points"
      : sourceMode === "pmxt-archive"
        ? "archive rows"
        : "snapshots";
  els.sourceLabel.textContent = sourceName;
  els.coverageWindow.textContent = coverageWindowText(coverage);
  els.coverageEvents.textContent = `${coverage.event_count ?? 0} events / ${dataCount} ${dataLabel}`;
  els.generatedAt.textContent = formatTime(payload.generated_at);
  els.coverageNote.textContent = coverageNoteText(coverage, sourceMode);
  els.coverageNote.classList.toggle(
    "warning",
    (sourceMode === "book-snapshot" && coverage.is_complete_requested === false) ||
      (sourceMode === "pmxt-archive" &&
        ((coverage.local_file_count ?? 0) === 0 || Number(coverage.file_gap_hours ?? 0) > 0))
  );
  const windowLabel =
    sourceMode === "pmxt-archive"
      ? `${num(coverage.covered_hours ?? payload.params?.hours ?? 0, 2)}h local parquet range`
      : `${num(payload.params?.hours ?? 24, 0)}h`;
  els.curveMeta.textContent = `${sourceName} · ${windowLabel} · ${num(payload.params?.fidelity_minutes ?? 5, 0)}m · slippage ${num(payload.params?.slippage_pct ?? 0, 2)}%`;
  renderEquityCurve(payload.equity_curve || []);
  renderTradeDetails(payload.trades || []);
  renderTopEvents(payload.top_events || []);
}

function syncSourceControls() {
  const isPmxt = els.source.value === "pmxt-archive";
  els.hours.disabled = isPmxt;
  els.hours.closest("label")?.classList.toggle("disabled", isPmxt);
  if (isPmxt) {
    els.coverageNote.textContent = "PMXT mode reads local hourly Parquet files from data/pmxt_cache. Lookback hours is ignored.";
    els.coverageNote.classList.remove("warning");
  }
}

function renderEquityCurve(curve) {
  if (curve.length < 2) {
    els.equityChart.innerHTML = `<p class="empty">No positive simulated trades after slippage and threshold.</p>`;
    return;
  }
  const width = 720;
  const height = 240;
  const values = curve.map((point) => Number(point.equity)).filter(Number.isFinite);
  const minY = Math.min(...values);
  const maxY = Math.max(...values);
  const span = Math.max(1e-9, maxY - minY);
  const points = curve
    .map((point, idx) => {
      const x = (idx / Math.max(1, curve.length - 1)) * width;
      const y = height - ((Number(point.equity) - minY) / span) * height;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const last = curve[curve.length - 1];
  els.equityChart.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Simulated equity curve">
      <polyline points="${points}" fill="none" stroke="currentColor" stroke-width="3" />
    </svg>
    <div class="chart-caption">
      <span>Min ${money(minY)}</span>
      <strong>Final ${money(last.equity)}</strong>
      <span>Max ${money(maxY)}</span>
    </div>
  `;
}

function renderTradeDetails(trades) {
  if (!trades.length) {
    els.tradeRows.innerHTML = `<tr><td colspan="10" class="empty">No simulated trades after slippage and threshold.</td></tr>`;
    return;
  }
  els.tradeRows.innerHTML = trades
    .map((trade) => {
      const buy = trade.buy || {};
      const sell = trade.sell || {};
      const settlement = trade.settlement || {};
      const rawNo = buy.raw_no_ask ?? buy.raw_no_price ?? trade.raw_no_ask ?? trade.no_price;
      const effectiveNo = buy.effective_no_ask ?? buy.effective_no_price ?? trade.effective_no_ask ?? trade.effective_no_price;
      const qty = trade.executed_qty ?? buy.executed_qty ?? buy.shares ?? trade.shares;
      const maxQty = trade.max_qty ?? buy.max_qty;
      const usedCapital = settlement.used_capital_usd ?? trade.used_capital_usd ?? settlement.cost_usd ?? buy.cost_usd;
      const unusedCapital = settlement.unused_capital_usd ?? trade.unused_capital_usd ?? 0;
      const rawSell = sell.raw_sum_other_yes_bid ?? sell.raw_sum_other_yes_price ?? trade.raw_sum_other_yes_bid ?? trade.sum_other_yes_price;
      const effectiveSell = sell.effective_sum_other_yes_bid ?? sell.effective_sum_other_yes ?? trade.effective_sum_other_yes_bid ?? trade.effective_sum_other_yes;
      const pnl = settlement.net_pnl_usd ?? settlement.pnl_usd ?? trade.simulated_pnl_usd;
      return `
        <tr>
          <td class="num">${formatTime(trade.timestamp)}</td>
          <td><div class="event-title" title="${escapeHtml(trade.event_title)}">${escapeHtml(trade.event_title)}</div></td>
          <td>
            <div class="trade-cell-main">${escapeHtml(shortText(buy.outcome || trade.best_market_question, 42))}</div>
            <div class="trade-cell-sub">${price(rawNo)} -> ${price(effectiveNo)}</div>
          </td>
          <td>
            <div class="trade-cell-main num">${num(qty, 2)}</div>
            <div class="trade-cell-sub">max ${num(maxQty, 2)}</div>
          </td>
          <td>
            <div class="trade-cell-main num">${money(usedCapital)}</div>
            <div class="trade-cell-sub">unused ${money(unusedCapital)}</div>
          </td>
          <td>
            <div class="trade-cell-main">${price(rawSell)} -> ${price(effectiveSell)}</div>
            <div class="trade-cell-sub">proceeds ${money(sell.proceeds_usd)}</div>
          </td>
          <td class="num profit">${pct(trade.gross_per_share ?? trade.gross_after_slippage)}</td>
          <td class="num total">${money(pnl)}</td>
          <td class="num">${num(settlement.return_pct, 2)}%</td>
          <td class="num">${money(settlement.equity_after)}</td>
        </tr>
      `;
    })
    .join("");
}

function renderTopEvents(rows) {
  if (!rows.length) {
    els.topEventRows.innerHTML = `<tr><td colspan="6" class="empty">No historical opportunities after slippage.</td></tr>`;
    return;
  }
  els.topEventRows.innerHTML = rows
    .slice(0, 10)
    .map(
      (row) => `
        <tr>
          <td><div class="event-title" title="${escapeHtml(row.event_title)}">${escapeHtml(row.event_title)}</div></td>
          <td class="num">${row.count}</td>
          <td class="num profit">${pct(row.max_gross_after_slippage)}</td>
          <td class="num total">${money(row.max_trade_pnl_usd)}</td>
          <td class="num">${money(row.total_pnl_usd)}</td>
          <td class="num">${formatTime(row.latest_at)}</td>
        </tr>
      `
    )
    .join("");
}

function setState(text, ok) {
  els.state.textContent = text;
  els.state.classList.toggle("connected", ok);
}

function coverageWindowText(coverage) {
  if (!coverage.coverage_start || !coverage.coverage_end) {
    return `0.00h / ${num(coverage.requested_hours ?? 24, 2)}h`;
  }
  return `${num(coverage.covered_hours ?? 0, 2)}h / ${num(coverage.requested_hours ?? 24, 2)}h`;
}

function coverageNoteText(coverage, sourceMode) {
  const note = coverage.source_note || "";
  if (sourceMode === "pmxt-archive") {
    const files = coverage.local_file_count ?? coverage.read_file_count ?? coverage.downloaded_file_count ?? 0;
    const requested = coverage.requested_file_count ?? 0;
    const gap = Number(coverage.file_gap_hours ?? 0);
    const gapText = Number.isFinite(gap) && gap > 0 ? ` Missing file hours: ${num(gap, 2)}.` : "";
    return `${note} Local files read: ${files}/${requested}.${gapText} Lookback hours is ignored; the replay window comes from local parquet file names.`;
  }
  if (sourceMode !== "book-snapshot") return note;
  if (coverage.is_complete_requested === false) {
    return `${note} Current request is only partially covered by local snapshots.`;
  }
  return note || "Strict mode uses locally recorded bid/ask/depth snapshots.";
}

function money(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${n < 0 ? "-" : ""}$${Math.abs(n).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function num(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function pct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${(n * 100).toFixed(2)}%`;
}

function formatTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString();
}

function formatHour(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function price(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(4);
}

function shortText(value, length) {
  const text = String(value || "");
  return text.length > length ? `${text.slice(0, length - 1)}...` : text;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

syncSourceControls();
runReplay(false);
