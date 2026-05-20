let selectedEventId = null;
let lastPayload = null;
let viewMode = "opportunities";
let allEvents = [];
let eventsLoading = false;
let lastEventsLoadedAt = 0;

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
  tableTitle: document.getElementById("tableTitle"),
  tableHead: document.getElementById("monitorTableHead"),
  opportunitiesViewBtn: document.getElementById("opportunitiesViewBtn"),
  eventsViewBtn: document.getElementById("eventsViewBtn"),
  opportunityRows: document.getElementById("opportunityRows"),
  detailBody: document.getElementById("detailBody"),
};

els.opportunitiesViewBtn.addEventListener("click", () => setViewMode("opportunities"));
els.eventsViewBtn.addEventListener("click", () => setViewMode("events"));

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
  if (viewMode === "events") {
    loadEvents();
  } else {
    renderOpportunityRows(payload.opportunities || []);
  }
}

function setViewMode(mode) {
  viewMode = mode;
  selectedEventId = null;
  els.opportunitiesViewBtn.classList.toggle("active", mode === "opportunities");
  els.eventsViewBtn.classList.toggle("active", mode === "events");
  els.detailBody.innerHTML =
    mode === "events"
      ? `<p class="empty">Select any event to inspect every market's 1 share simulation.</p>`
      : `<p class="empty">Select an opportunity to inspect the paper execution path.</p>`;

  if (mode === "events") {
    renderEventRows(allEvents);
    loadEvents(true);
  } else {
    renderOpportunityRows(lastPayload?.opportunities || []);
  }
}

async function loadEvents(force = false) {
  if (eventsLoading) return;
  const now = Date.now();
  if (!force && allEvents.length && now - lastEventsLoadedAt < 5000) {
    renderEventRows(allEvents);
    return;
  }
  eventsLoading = true;
  try {
    const res = await fetch("/api/events");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    allEvents = await res.json();
    lastEventsLoadedAt = Date.now();
    if (viewMode === "events") renderEventRows(allEvents);
  } catch (err) {
    if (viewMode === "events") {
      els.opportunityRows.innerHTML = `<tr><td colspan="8" class="empty">Failed to load events: ${escapeHtml(err.message)}</td></tr>`;
    }
  } finally {
    eventsLoading = false;
  }
}

function renderOpportunityRows(opps) {
  els.tableTitle.textContent = "Opportunities";
  els.thresholdText.style.display = "";
  els.tableHead.innerHTML = `
    <tr>
      <th>Event</th>
      <th>Markets</th>
      <th>Sum YES Ask</th>
      <th>Best NO</th>
      <th>Gross</th>
      <th>Qty</th>
      <th>Total USD</th>
      <th>Book Age</th>
    </tr>
  `;
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

function renderEventRows(events) {
  els.tableTitle.textContent = "All Events";
  els.thresholdText.style.display = "none";
  els.tableHead.innerHTML = `
    <tr>
      <th>Event</th>
      <th>Markets</th>
      <th>Complete</th>
      <th>Best 1 Share NO</th>
      <th>1 Share PnL</th>
      <th>Return</th>
      <th>Max Qty</th>
      <th>Opportunity</th>
    </tr>
  `;

  if (!events.length) {
    els.opportunityRows.innerHTML = `<tr><td colspan="8" class="empty">No NegRisk events loaded yet.</td></tr>`;
    return;
  }

  els.opportunityRows.innerHTML = events
    .map((event) => {
      const selected = event.event_id === selectedEventId ? "selected" : "";
      return `
        <tr class="${selected}" data-event-id="${escapeHtml(event.event_id)}">
          <td><div class="event-title" title="${escapeHtml(event.event_title)}">${escapeHtml(event.event_title)}</div></td>
          <td class="num">${event.n_markets}</td>
          <td class="num">${event.complete_market_count ?? 0}/${event.n_markets}</td>
          <td>${escapeHtml(shortText(event.best_market_question || "-", 34))}</td>
          <td class="num ${pnlClass(event.best_profit_1_share)}">${money4(event.best_profit_1_share)}</td>
          <td class="num">${num(event.best_return_pct, 2)}%</td>
          <td class="num">${num(event.best_max_qty, 1)}</td>
          <td>${event.has_opportunity ? '<span class="pill positive">Yes</span>' : '<span class="pill">No</span>'}</td>
        </tr>
      `;
    })
    .join("");

  for (const row of els.opportunityRows.querySelectorAll("tr[data-event-id]")) {
    row.addEventListener("click", () => selectEvent(row.dataset.eventId));
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
  const simulations = detail.market_simulations || [];
  const steps = opp
    ? `
      <div class="steps">
        <div class="step"><strong>1.</strong> ${escapeHtml(opp.simulation.step_1)}</div>
        <div class="step"><strong>2.</strong> ${escapeHtml(opp.simulation.step_2)}</div>
        <div class="step"><strong>3.</strong> ${escapeHtml(opp.simulation.step_3)}</div>
      </div>
    `
    : "";

  const simulationList = simulations.length
    ? `
      <div class="one-share-panel">
        <div class="mini-title">1 Share Simulation</div>
        <div class="simulation-list">
          ${simulations.map((row) => renderOneShareSimulation(row, bestIdx)).join("")}
        </div>
      </div>
    `
    : "";

  const markets = (detail.markets || [])
    .map((market) => {
      const badge = market.index === bestIdx ? "Target NO" : "Converted YES";
      const sim = market.one_share_simulation || {};
      return `
        <div class="market">
          <strong>#${market.index + 1} ${escapeHtml(badge)}: ${escapeHtml(market.question)}</strong>
          <dl>
            <div><dt>YES bid / ask</dt><dd>${price(market.yes_book.best_bid)} / ${price(market.yes_book.best_ask)}</dd></div>
            <div><dt>NO bid / ask</dt><dd>${price(market.no_book.best_bid)} / ${price(market.no_book.best_ask)}</dd></div>
            <div><dt>1 share cost / proceeds</dt><dd>${money4(sim.buy_no_ask)} / ${money4(sim.sum_other_yes_bid)}</dd></div>
            <div><dt>1 share PnL</dt><dd class="${pnlClass(sim.profit_1_share)}">${money4(sim.profit_1_share)} (${num(sim.return_pct, 2)}%)</dd></div>
            <div><dt>YES depth</dt><dd>${num(market.yes_book.bid_depth, 1)} bid / ${num(market.yes_book.ask_depth, 1)} ask</dd></div>
            <div><dt>NO depth</dt><dd>${num(market.no_book.bid_depth, 1)} bid / ${num(market.no_book.ask_depth, 1)} ask</dd></div>
            <div><dt>Tick</dt><dd>${price(market.min_tick_size || market.yes_book.tick_size)}</dd></div>
            <div><dt>Question index</dt><dd>${market.question_index ?? "-"}</dd></div>
          </dl>
          ${renderOrderBook(market)}
        </div>
      `;
    })
    .join("");

  els.detailBody.innerHTML = `
    <div class="detail-title">${escapeHtml(detail.title)}</div>
    ${steps}
    ${simulationList}
    <div class="market-list">${markets}</div>
  `;
}

function renderOrderBook(market) {
  return `
    <div class="orderbook-block">
      <div class="mini-title">Order Book</div>
      <div class="book-grid">
        ${renderBookSide("YES Bids", market.yes_book.bid_levels)}
        ${renderBookSide("YES Asks", market.yes_book.ask_levels)}
        ${renderBookSide("NO Bids", market.no_book.bid_levels)}
        ${renderBookSide("NO Asks", market.no_book.ask_levels)}
      </div>
    </div>
  `;
}

function renderBookSide(title, levels) {
  const rows = Array.isArray(levels) && levels.length ? levels.map(renderBookLevel).join("") : `<div class="book-empty">-</div>`;
  return `
    <div class="book-side">
      <div class="book-title">${escapeHtml(title)}</div>
      <div class="book-levels">${rows}</div>
    </div>
  `;
}

function renderBookLevel(level) {
  return `
    <div class="book-level">
      <span>${price(level.price)} x ${num(level.size, 2)}</span>
      <em>${money4(level.notional)}</em>
    </div>
  `;
}

function renderOneShareSimulation(row, bestIdx) {
  const target = row.index === bestIdx ? " target" : "";
  const executable = row.executable_1_share ? "executable" : row.reason || "not executable";
  return `
    <div class="simulation-row${target}">
      <div class="simulation-market">
        <strong>#${row.index + 1} ${escapeHtml(shortText(row.question, 44))}</strong>
        <span>${escapeHtml(executable)}</span>
      </div>
      <dl>
        <div><dt>Buy NO</dt><dd>${money4(row.buy_no_ask)}</dd></div>
        <div><dt>Sell YES Sum</dt><dd>${money4(row.sum_other_yes_bid)}</dd></div>
        <div><dt>PnL / Share</dt><dd class="${pnlClass(row.profit_1_share)}">${money4(row.profit_1_share)}</dd></div>
        <div><dt>Return</dt><dd>${num(row.return_pct, 2)}%</dd></div>
        <div><dt>Max Qty</dt><dd>${num(row.max_qty, 2)}</dd></div>
      </dl>
    </div>
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

function money4(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${n < 0 ? "-" : ""}$${Math.abs(n).toFixed(4)}`;
}

function pnlClass(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n === 0) return "";
  return n > 0 ? "pnl-positive" : "pnl-negative";
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
