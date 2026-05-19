let selectedEventId = null;
let lastPayload = null;

const els = {
  connectionState: document.getElementById("connectionState"),
  eventCount: document.getElementById("eventCount"),
  tokenCount: document.getElementById("tokenCount"),
  shardCount: document.getElementById("shardCount"),
  oppCount: document.getElementById("oppCount"),
  lastRefresh: document.getElementById("lastRefresh"),
  lastScan: document.getElementById("lastScan"),
  lastError: document.getElementById("lastError"),
  thresholdText: document.getElementById("thresholdText"),
  opportunityRows: document.getElementById("opportunityRows"),
  detailBody: document.getElementById("detailBody"),
};

function connect() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${protocol}://${window.location.host}/ws/dashboard`);

  ws.onopen = () => setConnection(true, "Connected");
  ws.onclose = () => {
    setConnection(false, "Disconnected");
    setTimeout(connect, 1500);
  };
  ws.onerror = () => setConnection(false, "Error");
  ws.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    lastPayload = payload;
    render(payload);
  };
}

function setConnection(ok, text) {
  els.connectionState.textContent = text;
  els.connectionState.classList.toggle("connected", ok);
}

function render(payload) {
  const status = payload.status || {};
  const shards = status.shards || { connected: 0, total: 0 };
  els.eventCount.textContent = status.event_count ?? 0;
  els.tokenCount.textContent = status.token_count ?? 0;
  els.shardCount.textContent = `${shards.connected ?? 0}/${shards.total ?? 0}`;
  els.oppCount.textContent = status.opportunity_count ?? 0;
  els.lastRefresh.textContent = formatTime(status.last_event_refresh_at);
  els.lastScan.textContent = formatTime(status.last_scan_at);
  els.lastError.textContent = status.last_error || "-";
  const config = status.config || {};
  els.thresholdText.textContent = `gross >= ${pct(config.min_gross_profit)}, total >= $${num(config.min_total_usd, 2)}`;
  renderRows(payload.opportunities || []);
}

function renderRows(opps) {
  if (!opps.length) {
    els.opportunityRows.innerHTML = `<tr><td colspan="8" class="empty">No profitable paper opportunities right now.</td></tr>`;
    return;
  }

  els.opportunityRows.innerHTML = opps
    .map((opp) => {
      const selected = opp.event_id === selectedEventId ? "selected" : "";
      return `
        <tr class="${selected}" data-event-id="${escapeHtml(opp.event_id)}">
          <td><div class="event-title" title="${escapeHtml(opp.event_title)}">${escapeHtml(opp.event_title)}</div></td>
          <td class="num">${opp.n_markets}</td>
          <td class="num">${num(opp.sum_yes_ask, 4)}</td>
          <td>${escapeHtml(shortText(opp.best_market_question, 34))}</td>
          <td class="num profit">${pct(opp.gross_profit)}</td>
          <td class="num">${num(opp.max_qty, 1)}</td>
          <td class="num total">$${num(opp.total_usd, 2)}</td>
          <td class="num">${num(opp.book_age_seconds, 1)}s</td>
        </tr>
      `;
    })
    .join("");

  for (const row of els.opportunityRows.querySelectorAll("tr[data-event-id]")) {
    row.addEventListener("click", () => selectEvent(row.dataset.eventId));
  }

  if (!selectedEventId && opps[0]) {
    selectEvent(opps[0].event_id);
  }
}

async function selectEvent(eventId) {
  selectedEventId = eventId;
  for (const row of els.opportunityRows.querySelectorAll("tr")) {
    row.classList.toggle("selected", row.dataset.eventId === eventId);
  }

  try {
    const res = await fetch(`/api/events/${encodeURIComponent(eventId)}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const detail = await res.json();
    renderDetail(detail);
  } catch (err) {
    els.detailBody.innerHTML = `<p class="empty">Failed to load detail: ${escapeHtml(err.message)}</p>`;
  }
}

function renderDetail(detail) {
  const opp = detail.opportunity;
  const bestIdx = opp ? opp.best_no_idx : -1;
  const steps = opp
    ? `
      <div class="steps">
        <div class="step"><strong>1.</strong> ${escapeHtml(opp.simulation.step_1)}</div>
        <div class="step"><strong>2.</strong> ${escapeHtml(opp.simulation.step_2)}</div>
        <div class="step"><strong>3.</strong> ${escapeHtml(opp.simulation.step_3)}</div>
      </div>
    `
    : "";

  const markets = (detail.markets || [])
    .map((market) => {
      const badge = market.index === bestIdx ? "Target NO" : "Converted YES";
      return `
        <div class="market">
          <strong>#${market.index + 1} ${escapeHtml(badge)}: ${escapeHtml(market.question)}</strong>
          <dl>
            <div><dt>YES bid / ask</dt><dd>${price(market.yes_book.best_bid)} / ${price(market.yes_book.best_ask)}</dd></div>
            <div><dt>NO bid / ask</dt><dd>${price(market.no_book.best_bid)} / ${price(market.no_book.best_ask)}</dd></div>
            <div><dt>YES depth</dt><dd>${num(market.yes_book.bid_depth, 1)} bid / ${num(market.yes_book.ask_depth, 1)} ask</dd></div>
            <div><dt>NO depth</dt><dd>${num(market.no_book.bid_depth, 1)} bid / ${num(market.no_book.ask_depth, 1)} ask</dd></div>
            <div><dt>Tick</dt><dd>${price(market.min_tick_size || market.yes_book.tick_size)}</dd></div>
            <div><dt>Question index</dt><dd>${market.question_index ?? "-"}</dd></div>
          </dl>
        </div>
      `;
    })
    .join("");

  els.detailBody.innerHTML = `
    <div class="detail-title">${escapeHtml(detail.title)}</div>
    ${steps}
    <div class="market-list">${markets}</div>
  `;
}

function formatTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleTimeString();
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

function price(value) {
  if (value === null || value === undefined) return "-";
  return num(value, 4);
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

connect();
