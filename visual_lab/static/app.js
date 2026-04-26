"use strict";

/**
 * Omega Visual Lab - app.js producción
 *
 * Arquitectura:
 *
 *   Binance WS
 *      ↓
 *   server.py / Market Hub
 *      ↓
 *   /ws/dashboard
 *      ↓
 *   app.js / Omega Visual Lab
 *
 * Este archivo:
 * - NO conecta directo a Binance.
 * - NO usa fetch polling.
 * - NO llama /api/snapshot.
 * - Recibe hidratación inicial por WebSocket.
 * - Recibe actualizaciones vivas por WebSocket.
 * - Renderiza watchlist compacta.
 * - Renderiza gráfico de velas con Lightweight Charts si está cargado.
 * - Renderiza paneles de scanner/geometría/plan.
 */

/* ============================================================
 * Estado global
 * ============================================================ */

const OMEGA_UI = {
  ws: null,
  reconnectTimer: null,
  pingTimer: null,
  reconnectAttempts: 0,
  manuallyClosed: false,

  connectedAt: null,
  lastServerMessageAt: null,
  lastStatus: null,

  symbols: new Map(),
  scannerAlerts: [],
  systemEvents: [],
};

const OMEGA_CHART = {
  chart: null,
  candleSeries: null,
  container: null,
  resizeObserver: null,

  selectedSymbol: null,
  selectedTf: "15m",

  candles: new Map(),
};

const OMEGA_CONFIG = {
  maxAlerts: 30,
  maxSystemEvents: 40,
  maxCandlesPerKey: 1000,

  reconnectMinMs: 1000,
  reconnectMaxMs: 15000,

  pingEveryMs: 15000,

  tfOrder: ["15m", "5m", "3m", "1m"],

  tfLabels: {
    "15m": "M15 Madre",
    "5m": "M5",
    "3m": "M3",
    "1m": "M1 Gatillo",
  },
};

/* ============================================================
 * Arranque
 * ============================================================ */

function bootOmegaVisualLab() {
  cleanupOldDashboardInjection();
  injectOmegaStyles();

  mountVisualSlots();
  hookExternalTimeframeButtons();

  connectDashboardWs();

  window.addEventListener("beforeunload", () => {
    stopDashboardWs({ silent: true });
  });
}

function cleanupOldDashboardInjection() {
  /**
   * Limpieza defensiva del dashboard anterior que inyectaba una tabla grande.
   */
  const oldDashboard = document.querySelector("#omega-dashboard-root");
  if (oldDashboard) oldDashboard.remove();

  const oldStyles = document.querySelector("#omega-dashboard-styles");
  if (oldStyles) oldStyles.remove();
}

/* ============================================================
 * WebSocket local del backend
 * ============================================================ */

function getDashboardWsUrl() {
  if (window.OMEGA_WS_URL) {
    return String(window.OMEGA_WS_URL);
  }

  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/ws/dashboard`;
}

function connectDashboardWs() {
  clearReconnectTimer();
  clearPingTimer();

  OMEGA_UI.manuallyClosed = false;

  setConnectionState("connecting", "Conectando backend WS…");

  const url = getDashboardWsUrl();

  try {
    const ws = new WebSocket(url);
    OMEGA_UI.ws = ws;

    ws.onopen = () => {
      OMEGA_UI.connectedAt = Date.now();
      OMEGA_UI.reconnectAttempts = 0;

      setConnectionState("online", "Backend WS conectado");

      sendWs({ type: "get_status" });
      startPingTimer();

      renderAll();
    };

    ws.onmessage = (event) => {
      OMEGA_UI.lastServerMessageAt = Date.now();

      let message;

      try {
        message = JSON.parse(event.data);
      } catch (err) {
        console.warn("[Omega UI] Mensaje WS no JSON:", err, event.data);
        return;
      }

      handleBackendMessage(message);
      renderAll();
    };

    ws.onerror = (event) => {
      console.warn("[Omega UI] Error WS:", event);
      setConnectionState("warning", "Error en WebSocket local");
    };

    ws.onclose = (event) => {
      console.warn("[Omega UI] WS cerrado:", event.code, event.reason || "");

      OMEGA_UI.ws = null;
      clearPingTimer();

      if (!OMEGA_UI.manuallyClosed) {
        scheduleReconnect();
      } else {
        setConnectionState("offline", "WS detenido");
      }
    };
  } catch (err) {
    console.error("[Omega UI] No se pudo abrir WebSocket:", err);
    scheduleReconnect();
  }
}

function stopDashboardWs(options = {}) {
  OMEGA_UI.manuallyClosed = true;

  clearReconnectTimer();
  clearPingTimer();

  if (OMEGA_UI.ws) {
    try {
      OMEGA_UI.ws.close(1000, "Manual stop");
    } catch (_) {
      // noop
    }
  }

  OMEGA_UI.ws = null;

  if (!options.silent) {
    setConnectionState("offline", "WS detenido");
    renderAll();
  }
}

function reconnectDashboardWs() {
  stopDashboardWs({ silent: true });

  setTimeout(() => {
    OMEGA_UI.manuallyClosed = false;
    connectDashboardWs();
  }, 250);
}

function scheduleReconnect() {
  OMEGA_UI.reconnectAttempts += 1;

  const exponential =
    OMEGA_CONFIG.reconnectMinMs *
    Math.pow(2, OMEGA_UI.reconnectAttempts - 1);

  const delay = Math.min(exponential, OMEGA_CONFIG.reconnectMaxMs);
  const jitter = Math.floor(Math.random() * 500);
  const finalDelay = delay + jitter;

  setConnectionState(
    "reconnecting",
    `Reconectando en ${(finalDelay / 1000).toFixed(1)}s…`
  );

  clearReconnectTimer();

  OMEGA_UI.reconnectTimer = setTimeout(() => {
    connectDashboardWs();
  }, finalDelay);
}

function clearReconnectTimer() {
  if (OMEGA_UI.reconnectTimer) {
    clearTimeout(OMEGA_UI.reconnectTimer);
    OMEGA_UI.reconnectTimer = null;
  }
}

function startPingTimer() {
  clearPingTimer();

  OMEGA_UI.pingTimer = setInterval(() => {
    sendWs({
      type: "ping",
      client_time_ms: Date.now(),
    });
  }, OMEGA_CONFIG.pingEveryMs);
}

function clearPingTimer() {
  if (OMEGA_UI.pingTimer) {
    clearInterval(OMEGA_UI.pingTimer);
    OMEGA_UI.pingTimer = null;
  }
}

function sendWs(payload) {
  if (!OMEGA_UI.ws || OMEGA_UI.ws.readyState !== WebSocket.OPEN) {
    return;
  }

  OMEGA_UI.ws.send(JSON.stringify(payload));
}

/* ============================================================
 * Router de mensajes WS
 * ============================================================ */

function handleBackendMessage(message) {
  switch (message.type) {
    case "hello":
      handleHello(message);
      break;

    case "status":
      handleStatus(message.data || message.status || message);
      break;

    case "snapshot":
      applySnapshot(message.data || message);
      break;

    case "market_batch":
      applyMarketBatch(message);
      break;

    case "scanner_alert":
      applyScannerAlert(message.data || message);
      break;

    case "pong":
      break;

    default:
      handleUnknownMessage(message);
      break;
  }
}

function handleHello(message) {
  const status = message.status || {};
  OMEGA_UI.lastStatus = status;

  if (status.binance_market_ws_connected) {
    setConnectionState("online", "Backend + Binance conectados");
  } else {
    setConnectionState("warning", "Backend conectado · Binance reconectando");
  }
}

function handleStatus(status) {
  OMEGA_UI.lastStatus = status || {};

  if (OMEGA_UI.lastStatus.binance_market_ws_connected) {
    setConnectionState("online", "Backend + Binance conectados");
  } else {
    setConnectionState("warning", "Backend conectado · Binance reconectando");
  }
}

function handleUnknownMessage(message) {
  if (message.event === "alert" || message.type === "alert") {
    applyScannerAlert(message.data || message);
    return;
  }

  if (message.type === "system") {
    applySystemEvent(message);
  }
}

function applyMarketBatch(message) {
  const events = Array.isArray(message.events) ? message.events : [];

  for (const event of events) {
    if (event.type === "market_update" && event.event === "mark_price") {
      applyMarkPrice(event.data);
      continue;
    }

    if (event.type === "market_update" && event.event === "kline") {
      applyKline(event.data);
      continue;
    }

    if (event.type === "scanner_alert") {
      applyScannerAlert(event.data || event);
      continue;
    }

    if (event.type === "system") {
      applySystemEvent(event);
      continue;
    }
  }
}

/* ============================================================
 * Snapshot WS inicial
 * ============================================================ */

function applySnapshot(payload) {
  /**
   * Esto NO es snapshot REST ni polling.
   * Es hidratación inicial por WebSocket desde server.py.
   */

  const data = payload.data || payload;
  const symbols = data.symbols || {};

  for (const [symbol, item] of Object.entries(symbols)) {
    const state = ensureSymbol(symbol);

    state.symbol = item.symbol || symbol;
    state.markPrice = item.mark_price ?? state.markPrice;
    state.markPriceMa = item.mark_price_ma ?? state.markPriceMa;
    state.indexPrice = item.index_price ?? state.indexPrice;
    state.estimatedSettlePrice =
      item.estimated_settle_price ?? state.estimatedSettlePrice;
    state.fundingRate = item.funding_rate ?? state.fundingRate;
    state.nextFundingTime = item.next_funding_time ?? state.nextFundingTime;
    state.lastEventAt =
      item.last_price_event_time ??
      item.last_server_update_ms ??
      state.lastEventAt;

    if (item.klines) {
      for (const [tf, tfData] of Object.entries(item.klines)) {
        const closed = Array.isArray(tfData.closed) ? tfData.closed : [];
        const current = tfData.current || null;

        const candles = closed
          .map(normalizeCandle)
          .filter(isValidChartCandle)
          .sort((a, b) => a.time - b.time);

        if (current) {
          const currentCandle = normalizeCandle(current);

          if (isValidChartCandle(currentCandle)) {
            upsertIntoArray(candles, currentCandle);
            state.klines.set(tf, currentCandle);
          }
        }

        setChartCandles(symbol, tf, candles);
      }
    }
  }

  autoSelectFirstSymbol();
  renderSelectedChart();
}

/* ============================================================
 * Actualizaciones de mercado
 * ============================================================ */

function applyMarkPrice(data) {
  if (!data || !data.symbol) return;

  const state = ensureSymbol(data.symbol);

  state.markPrice = data.mark_price ?? state.markPrice;
  state.markPriceMa = data.mark_price_ma ?? state.markPriceMa;
  state.indexPrice = data.index_price ?? state.indexPrice;
  state.estimatedSettlePrice =
    data.estimated_settle_price ?? state.estimatedSettlePrice;
  state.fundingRate = data.funding_rate ?? state.fundingRate;
  state.nextFundingTime = data.next_funding_time ?? state.nextFundingTime;
  state.lastEventAt = data.event_time ?? data.server_update_ms ?? Date.now();
}

function applyKline(data) {
  if (!data || !data.symbol || !data.timeframe) return;

  const state = ensureSymbol(data.symbol);
  const candle = normalizeCandle(data);

  if (!isValidChartCandle(candle)) return;

  state.klines.set(candle.timeframe, candle);
  state.lastEventAt = candle.eventTime || candle.serverUpdateMs || Date.now();

  upsertChartCandle(candle.symbol, candle.timeframe, candle);

  if (
    OMEGA_CHART.selectedSymbol === normalizeSymbol(candle.symbol) &&
    OMEGA_CHART.selectedTf === candle.timeframe
  ) {
    updateChartCandle(candle);
  }
}

function applyScannerAlert(alert) {
  if (!alert || typeof alert !== "object") return;

  OMEGA_UI.scannerAlerts.unshift({
    ...alert,
    receivedAt: Date.now(),
  });

  OMEGA_UI.scannerAlerts = OMEGA_UI.scannerAlerts.slice(
    0,
    OMEGA_CONFIG.maxAlerts
  );
}

function applySystemEvent(event) {
  OMEGA_UI.systemEvents.unshift({
    ...event,
    receivedAt: Date.now(),
  });

  OMEGA_UI.systemEvents = OMEGA_UI.systemEvents.slice(
    0,
    OMEGA_CONFIG.maxSystemEvents
  );

  if (event.event === "binance_connected") {
    setConnectionState("online", "Binance WS conectado");
  }

  if (event.event === "binance_disconnected") {
    setConnectionState("warning", "Binance WS reconectando");
  }

  if (event.event === "scanner_error") {
    setConnectionState("warning", "Scanner con error");
  }

  if (event.event === "notifier_error") {
    setConnectionState("warning", "Notifier con error");
  }
}

/* ============================================================
 * Estado interno
 * ============================================================ */

function ensureSymbol(symbol) {
  const normalized = normalizeSymbol(symbol);

  if (!OMEGA_UI.symbols.has(normalized)) {
    OMEGA_UI.symbols.set(normalized, {
      symbol: normalized,
      markPrice: null,
      markPriceMa: null,
      indexPrice: null,
      estimatedSettlePrice: null,
      fundingRate: null,
      nextFundingTime: null,
      lastEventAt: null,
      klines: new Map(),
    });
  }

  return OMEGA_UI.symbols.get(normalized);
}

function normalizeSymbol(symbol) {
  return String(symbol || "").trim().toUpperCase();
}

function normalizeCandle(data) {
  const openTime = toNumber(data.open_time);

  return {
    symbol: normalizeSymbol(data.symbol),
    timeframe: String(data.timeframe || "").toLowerCase(),

    openTime: data.open_time,
    closeTime: data.close_time,

    time: Number.isFinite(openTime) ? Math.floor(openTime / 1000) : null,

    open: toNumber(data.open),
    high: toNumber(data.high),
    low: toNumber(data.low),
    close: toNumber(data.close),

    volume: toNumber(data.volume),
    quoteVolume: toNumber(data.quote_volume),

    trades: data.trades,
    closed: Boolean(data.closed),

    eventTime: data.event_time,
    serverUpdateMs: data.server_update_ms,
  };
}

/* ============================================================
 * Montaje visual
 * ============================================================ */

function mountVisualSlots() {
  mountWatchlistPanel();
  mountRightPanels();
  mountChartPanel();
}

function mountWatchlistPanel() {
  const panel = findWatchlistPanel();

  if (!panel) {
    mountStandaloneFallback();
    return;
  }

  panel.classList.add("omega-watchlist-panel-mounted");

  panel.innerHTML = `
    <div class="omega-watchlist-head">
      <div>
        <div class="omega-panel-label">WATCHLIST</div>
        <div class="omega-title">Omega Realtime</div>
        <div class="omega-subtitle">Market Hub · Backend WS</div>
      </div>

      <div id="omegaConnectionDot" class="omega-conn-dot is-connecting"></div>
    </div>

    <div id="omegaConnectionText" class="omega-connection-text">
      Conectando…
    </div>

    <div class="omega-actions">
      <button id="omegaReconnectBtn" type="button">Reconectar</button>
      <button id="omegaStopBtn" type="button">Detener</button>
    </div>

    <div id="omegaWatchlistRows" class="omega-watchlist-rows"></div>
  `;

  const reconnectBtn = document.querySelector("#omegaReconnectBtn");
  const stopBtn = document.querySelector("#omegaStopBtn");
  const rows = document.querySelector("#omegaWatchlistRows");

  if (reconnectBtn) {
    reconnectBtn.addEventListener("click", reconnectDashboardWs);
  }

  if (stopBtn) {
    stopBtn.addEventListener("click", () => stopDashboardWs());
  }

  if (rows) {
    rows.addEventListener("click", (event) => {
      const row = event.target.closest("[data-omega-symbol]");
      if (!row) return;

      const symbol = row.getAttribute("data-omega-symbol");
      if (symbol) {
        selectChartSymbol(symbol);
      }
    });
  }
}

function mountRightPanels() {
  const decision = findPanelByTitle("DECISIÓN DEL SCANNER", {
    minWidth: 180,
    maxWidth: 520,
    minHeight: 40,
  });

  const geometry = findPanelByTitle("GEOMETRÍA M15", {
    minWidth: 180,
    maxWidth: 520,
    minHeight: 40,
  });

  const plan = findPanelByTitle("PLAN", {
    minWidth: 180,
    maxWidth: 520,
    minHeight: 40,
  });

  if (decision && !decision.querySelector("#omegaDecisionBody")) {
    decision.insertAdjacentHTML(
      "beforeend",
      `<div id="omegaDecisionBody" class="omega-right-body">Esperando scanner…</div>`
    );
  }

  if (geometry && !geometry.querySelector("#omegaGeometryBody")) {
    geometry.insertAdjacentHTML(
      "beforeend",
      `<div id="omegaGeometryBody" class="omega-right-body">Esperando estructura M15…</div>`
    );
  }

  if (plan && !plan.querySelector("#omegaPlanBody")) {
    plan.insertAdjacentHTML(
      "beforeend",
      `<div id="omegaPlanBody" class="omega-right-body">Sin plan activo.</div>`
    );
  }
}

function mountChartPanel() {
  if (document.querySelector("#omegaChartMount")) {
    OMEGA_CHART.container = document.querySelector("#omegaChartMount");
    return OMEGA_CHART.container;
  }

  const panel = findMainChartPanel();

  if (!panel) {
    console.warn("[Omega Chart] No encontré panel central para montar el gráfico.");
    return null;
  }

  panel.classList.add("omega-chart-panel-mounted");

  panel.innerHTML = `
    <div class="omega-chart-shell">
      <div class="omega-chart-toolbar">
        <div>
          <div id="omegaChartTitle" class="omega-chart-title">Omega Chart</div>
          <div id="omegaChartSubtitle" class="omega-chart-subtitle">
            Esperando datos OHLC…
          </div>
        </div>

        <div class="omega-chart-tf-buttons">
          <button type="button" data-chart-tf="15m">M15 Madre</button>
          <button type="button" data-chart-tf="5m">M5</button>
          <button type="button" data-chart-tf="3m">M3</button>
          <button type="button" data-chart-tf="1m">M1 Gatillo</button>
        </div>
      </div>

      <div class="omega-chart-area">
        <div id="omegaChartMount" class="omega-chart-mount"></div>
        <div id="omegaChartEmpty" class="omega-chart-empty">
          Esperando velas desde el Market Hub…
        </div>
      </div>
    </div>
  `;

  panel.querySelectorAll("[data-chart-tf]").forEach((button) => {
    button.addEventListener("click", () => {
      const tf = button.getAttribute("data-chart-tf");
      selectChartTf(tf);
    });
  });

  OMEGA_CHART.container = panel.querySelector("#omegaChartMount");

  return OMEGA_CHART.container;
}

function mountStandaloneFallback() {
  if (document.querySelector("#omegaStandaloneRoot")) return;

  const root = document.createElement("div");
  root.id = "omegaStandaloneRoot";
  root.className = "omega-standalone-root";

  root.innerHTML = `
    <div class="omega-watchlist-head">
      <div>
        <div class="omega-panel-label">OMEGA VISUAL LAB</div>
        <div class="omega-title">Omega Realtime</div>
        <div class="omega-subtitle">Market Hub · Backend WS</div>
      </div>

      <div id="omegaConnectionDot" class="omega-conn-dot is-connecting"></div>
    </div>

    <div id="omegaConnectionText" class="omega-connection-text">
      Conectando…
    </div>

    <div class="omega-actions">
      <button id="omegaReconnectBtn" type="button">Reconectar</button>
      <button id="omegaStopBtn" type="button">Detener</button>
    </div>

    <div id="omegaWatchlistRows" class="omega-watchlist-rows"></div>
  `;

  document.body.appendChild(root);
}

/* ============================================================
 * Selectores defensivos de layout
 * ============================================================ */

function findWatchlistPanel() {
  return (
    document.querySelector("#watchlist") ||
    document.querySelector("#watchlistPanel") ||
    document.querySelector("[data-panel='watchlist']") ||
    document.querySelector("[data-slot='watchlist']") ||
    document.querySelector(".watchlist-panel") ||
    document.querySelector(".watchlist") ||
    findPanelByTitle("WATCHLIST", {
      minWidth: 120,
      maxWidth: 360,
      minHeight: 120,
    })
  );
}

function findMainChartPanel() {
  return (
    document.querySelector("#chart") ||
    document.querySelector("#chartContainer") ||
    document.querySelector("#omegaChart") ||
    document.querySelector("[data-panel='chart']") ||
    document.querySelector("[data-slot='chart']") ||
    document.querySelector(".chart-panel") ||
    document.querySelector(".main-chart") ||
    document.querySelector(".visual-chart") ||
    findLargestEmptyPanel()
  );
}

function findPanelByTitle(title, options = {}) {
  const wanted = normalizeText(title);

  const minWidth = options.minWidth ?? 0;
  const maxWidth = options.maxWidth ?? Infinity;
  const minHeight = options.minHeight ?? 0;
  const maxHeight = options.maxHeight ?? Infinity;

  const candidates = Array.from(
    document.querySelectorAll("aside, section, article, div")
  );

  let best = null;
  let bestArea = Infinity;

  for (const element of candidates) {
    if (element.id && element.id.startsWith("omega")) continue;

    const rect = element.getBoundingClientRect();

    if (rect.width < minWidth || rect.width > maxWidth) continue;
    if (rect.height < minHeight || rect.height > maxHeight) continue;

    const text = normalizeText(element.textContent || "");
    if (!text.includes(wanted)) continue;

    const area = rect.width * rect.height;

    if (area < bestArea) {
      best = element;
      bestArea = area;
    }
  }

  return best;
}

function findLargestEmptyPanel() {
  const candidates = Array.from(
    document.querySelectorAll("section, article, div, main")
  );

  let best = null;
  let bestArea = 0;

  for (const element of candidates) {
    if (element.id && element.id.startsWith("omega")) continue;
    if (element.closest("#omegaStandaloneRoot")) continue;
    if (element.querySelector("#omegaWatchlistRows")) continue;
    if (element.querySelector("#omegaDecisionBody")) continue;
    if (element.querySelector("#omegaGeometryBody")) continue;
    if (element.querySelector("#omegaPlanBody")) continue;

    const rect = element.getBoundingClientRect();

    if (rect.width < 420 || rect.height < 260) continue;

    const text = String(element.textContent || "").trim();

    /**
     * Buscamos el panel central vacío. Si tiene mucho texto,
     * probablemente es un contenedor padre y no el chart panel real.
     */
    if (text.length > 120) continue;

    const area = rect.width * rect.height;

    if (area > bestArea) {
      best = element;
      bestArea = area;
    }
  }

  return best;
}

function hookExternalTimeframeButtons() {
  /**
   * Si la plantilla externa ya tiene botones arriba:
   * M15 Madre | M5 | M3 | M1 Gatillo
   * los usamos también.
   */

  const buttons = Array.from(document.querySelectorAll("button"));

  for (const button of buttons) {
    const tf = parseTfFromText(button.textContent || "");

    if (!tf) continue;

    button.addEventListener("click", () => {
      selectChartTf(tf);
    });
  }
}

function parseTfFromText(text) {
  const normalized = normalizeText(text);

  if (normalized.includes("M15")) return "15m";
  if (normalized.includes("M5")) return "5m";
  if (normalized.includes("M3")) return "3m";
  if (normalized.includes("M1")) return "1m";

  return null;
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase();
}

/* ============================================================
 * Render
 * ============================================================ */

function renderAll() {
  renderWatchlist();
  renderScannerPanels();
  renderConnectionMeta();
  syncChartButtons();
}

function renderWatchlist() {
  const rows = document.querySelector("#omegaWatchlistRows");
  if (!rows) return;

  const symbols = Array.from(OMEGA_UI.symbols.values()).sort((a, b) =>
    a.symbol.localeCompare(b.symbol)
  );

  if (!symbols.length) {
    rows.innerHTML = `
      <div class="omega-empty">
        Esperando datos del Market Hub…
      </div>
    `;
    return;
  }

  rows.innerHTML = symbols.map(renderSymbolRow).join("");
}

function renderSymbolRow(symbolState) {
  const live = isLive(symbolState.lastEventAt);
  const selected = OMEGA_CHART.selectedSymbol === symbolState.symbol;

  const m15 = symbolState.klines.get("15m");
  const m5 = symbolState.klines.get("5m");
  const m3 = symbolState.klines.get("3m");
  const m1 = symbolState.klines.get("1m");

  return `
    <div
      class="omega-symbol-row ${selected ? "is-selected" : ""}"
      data-omega-symbol="${escapeHtml(symbolState.symbol)}"
      title="Seleccionar ${escapeHtml(symbolState.symbol)}"
    >
      <div class="omega-symbol-top">
        <div class="omega-symbol-name">
          <span class="omega-live-dot ${live ? "is-live" : ""}"></span>
          <span>${escapeHtml(symbolState.symbol)}</span>
        </div>

        <div class="omega-symbol-price">
          ${formatPrice(symbolState.markPrice)}
        </div>
      </div>

      <div class="omega-tf-line">
        ${renderTfPill("15", m15)}
        ${renderTfPill("5", m5)}
        ${renderTfPill("3", m3)}
        ${renderTfPill("1", m1)}
      </div>
    </div>
  `;
}

function renderTfPill(label, candle) {
  if (!candle) {
    return `<span class="omega-tf-pill is-empty">${label}: —</span>`;
  }

  const direction = candleDirection(candle);
  const closedLabel = candle.closed ? "C" : "L";

  return `
    <span class="omega-tf-pill is-${direction}" title="${label}m · ${candle.closed ? "cerrada" : "viva"}">
      ${label}: ${directionIcon(direction)} ${closedLabel}
    </span>
  `;
}

function renderScannerPanels() {
  const latest = OMEGA_UI.scannerAlerts[0];

  const decision = document.querySelector("#omegaDecisionBody");
  const geometry = document.querySelector("#omegaGeometryBody");
  const plan = document.querySelector("#omegaPlanBody");

  if (decision) {
    if (!latest) {
      decision.innerHTML = `<span class="omega-muted">Sin alertas todavía.</span>`;
    } else {
      decision.innerHTML = `
        <div class="omega-decision-main">
          ${escapeHtml(latest.side || "—")} · ${escapeHtml(latest.symbol || "—")}
        </div>

        <div class="omega-decision-sub">
          ${escapeHtml(latest.type || latest.signal_type || "Señal")} ·
          Calidad: ${escapeHtml(latest.quality || latest.calidad || "—")}
        </div>

        <div class="omega-decision-reason">
          ${escapeHtml(latest.reason || latest.motivo || "Sin motivo detallado.")}
        </div>
      `;
    }
  }

  if (geometry) {
    if (!latest) {
      geometry.innerHTML = `<span class="omega-muted">Esperando estructura M15.</span>`;
    } else {
      geometry.innerHTML = `
        <div class="omega-kv">
          <span>Símbolo</span>
          <strong>${escapeHtml(latest.symbol || "—")}</strong>
        </div>

        <div class="omega-kv">
          <span>Fuente</span>
          <strong>${escapeHtml(latest.source_tf || latest.timeframe || "—")}</strong>
        </div>

        <div class="omega-kv">
          <span>Geometría</span>
          <strong>${escapeHtml(latest.geometry_score ?? "—")}</strong>
        </div>

        <div class="omega-kv">
          <span>Total</span>
          <strong>${escapeHtml(latest.total_score ?? latest.score ?? "—")}</strong>
        </div>
      `;
    }
  }

  if (plan) {
    if (!latest) {
      plan.innerHTML = `<span class="omega-muted">Sin plan activo.</span>`;
    } else {
      plan.innerHTML = `
        <div class="omega-kv">
          <span>Precio</span>
          <strong>${formatPrice(latest.price)}</strong>
        </div>

        <div class="omega-kv">
          <span>Tipo</span>
          <strong>${escapeHtml(latest.type || latest.signal_type || "—")}</strong>
        </div>

        <div class="omega-kv">
          <span>ID</span>
          <strong>${escapeHtml(String(latest.fingerprint || latest.plan_id || "—").slice(0, 14))}</strong>
        </div>

        <div class="omega-kv">
          <span>Hora</span>
          <strong>${formatTime(latest.server_time_ms || latest.receivedAt)}</strong>
        </div>
      `;
    }
  }
}

function renderConnectionMeta() {
  const text = document.querySelector("#omegaConnectionText");
  if (!text) return;

  const count = OMEGA_UI.symbols.size;
  const last = OMEGA_UI.lastServerMessageAt
    ? formatTime(OMEGA_UI.lastServerMessageAt)
    : "—";

  const binanceState = OMEGA_UI.lastStatus?.binance_market_ws_connected
    ? "Binance OK"
    : "Binance…";

  text.textContent = `${count} símbolos · ${binanceState} · Último evento: ${last}`;
}

function setConnectionState(kind, label) {
  const dot = document.querySelector("#omegaConnectionDot");
  const text = document.querySelector("#omegaConnectionText");

  if (dot) {
    dot.className = `omega-conn-dot is-${kind}`;
  }

  if (text) {
    text.textContent = label;
  }
}

/* ============================================================
 * Chart
 * ============================================================ */

function candleKey(symbol, timeframe) {
  return `${normalizeSymbol(symbol)}::${String(timeframe).toLowerCase()}`;
}

function setChartCandles(symbol, timeframe, candles) {
  const key = candleKey(symbol, timeframe);

  const cleaned = candles
    .filter(isValidChartCandle)
    .sort((a, b) => a.time - b.time)
    .slice(-OMEGA_CONFIG.maxCandlesPerKey);

  OMEGA_CHART.candles.set(key, cleaned);
}

function upsertChartCandle(symbol, timeframe, candle) {
  const key = candleKey(symbol, timeframe);
  const candles = OMEGA_CHART.candles.get(key) || [];

  upsertIntoArray(candles, candle);

  if (candles.length > OMEGA_CONFIG.maxCandlesPerKey) {
    candles.splice(0, candles.length - OMEGA_CONFIG.maxCandlesPerKey);
  }

  OMEGA_CHART.candles.set(key, candles);
}

function upsertIntoArray(candles, candle) {
  if (!isValidChartCandle(candle)) return candles;

  const last = candles[candles.length - 1];

  if (!last || last.time < candle.time) {
    candles.push(candle);
    return candles;
  }

  if (last.time === candle.time) {
    candles[candles.length - 1] = candle;
    return candles;
  }

  const index = candles.findIndex((item) => item.time === candle.time);

  if (index >= 0) {
    candles[index] = candle;
  } else {
    candles.push(candle);
    candles.sort((a, b) => a.time - b.time);
  }

  return candles;
}

function isValidChartCandle(candle) {
  return (
    candle &&
    Number.isFinite(candle.time) &&
    Number.isFinite(candle.open) &&
    Number.isFinite(candle.high) &&
    Number.isFinite(candle.low) &&
    Number.isFinite(candle.close)
  );
}

function autoSelectFirstSymbol() {
  if (OMEGA_CHART.selectedSymbol) return;

  const first = Array.from(OMEGA_UI.symbols.keys())[0];

  if (first) {
    OMEGA_CHART.selectedSymbol = first;
  }
}

function selectChartSymbol(symbol) {
  OMEGA_CHART.selectedSymbol = normalizeSymbol(symbol);
  renderSelectedChart();
  renderAll();
}

function selectChartTf(timeframe) {
  const tf = String(timeframe || "").toLowerCase();

  if (!OMEGA_CONFIG.tfOrder.includes(tf)) {
    return;
  }

  OMEGA_CHART.selectedTf = tf;
  renderSelectedChart();
  renderAll();
}

function ensureChart() {
  const container = mountChartPanel();

  if (!container) return false;

  if (!window.LightweightCharts) {
    showChartEmpty(
      "Lightweight Charts no está cargado. Hay que corregir index.html."
    );
    return false;
  }

  if (OMEGA_CHART.chart && OMEGA_CHART.candleSeries) {
    return true;
  }

  const { createChart } = window.LightweightCharts;

  try {
    OMEGA_CHART.chart = createChart(container, {
      layout: {
        background: { color: "#0f172a" },
        textColor: "#cbd5e1",
      },
      grid: {
        vertLines: { color: "rgba(148, 163, 184, 0.08)" },
        horzLines: { color: "rgba(148, 163, 184, 0.08)" },
      },
      rightPriceScale: {
        borderColor: "rgba(148, 163, 184, 0.18)",
      },
      timeScale: {
        borderColor: "rgba(148, 163, 184, 0.18)",
        timeVisible: true,
        secondsVisible: false,
      },
      crosshair: {
        mode: 1,
      },
      width: Math.max(320, container.clientWidth),
      height: Math.max(320, container.clientHeight),
    });

    OMEGA_CHART.candleSeries = createCandlestickSeriesCompat(
      OMEGA_CHART.chart
    );

    setupChartResizeObserver(container);

    return true;
  } catch (err) {
    console.error("[Omega Chart] Error creando chart:", err);
    showChartEmpty(`Error creando chart: ${err.message || err}`);
    return false;
  }
}

function createCandlestickSeriesCompat(chart) {
  const lightweight = window.LightweightCharts;

  /**
   * Lightweight Charts v4.
   */
  if (typeof chart.addCandlestickSeries === "function") {
    return chart.addCandlestickSeries({
      priceFormat: {
        type: "price",
        precision: 8,
        minMove: 0.00000001,
      },
    });
  }

  /**
   * Lightweight Charts v5.
   */
  if (
    typeof chart.addSeries === "function" &&
    lightweight &&
    lightweight.CandlestickSeries
  ) {
    return chart.addSeries(lightweight.CandlestickSeries, {
      priceFormat: {
        type: "price",
        precision: 8,
        minMove: 0.00000001,
      },
    });
  }

  throw new Error(
    "Versión incompatible de Lightweight Charts. No existe addCandlestickSeries ni addSeries(CandlestickSeries)."
  );
}

function setupChartResizeObserver(container) {
  if (OMEGA_CHART.resizeObserver) {
    OMEGA_CHART.resizeObserver.disconnect();
  }

  OMEGA_CHART.resizeObserver = new ResizeObserver(() => {
    if (!OMEGA_CHART.chart || !OMEGA_CHART.container) return;

    OMEGA_CHART.chart.applyOptions({
      width: Math.max(320, OMEGA_CHART.container.clientWidth),
      height: Math.max(320, OMEGA_CHART.container.clientHeight),
    });
  });

  OMEGA_CHART.resizeObserver.observe(container);
}

function renderSelectedChart() {
  autoSelectFirstSymbol();

  const symbol = OMEGA_CHART.selectedSymbol;
  const timeframe = OMEGA_CHART.selectedTf;

  updateChartHeader(symbol, timeframe);

  if (!symbol || !timeframe) {
    showChartEmpty("Seleccioná un símbolo para ver el gráfico.");
    return;
  }

  if (!ensureChart()) return;

  const key = candleKey(symbol, timeframe);
  const candles = OMEGA_CHART.candles.get(key) || [];

  updateChartHeader(symbol, timeframe, candles.length);

  if (!candles.length) {
    showChartEmpty(
      `Sin velas todavía para ${symbol} ${timeframe.toUpperCase()}. Esperando Market Hub…`
    );
    return;
  }

  hideChartEmpty();

  const chartData = candles.map(toChartCandle);

  try {
    OMEGA_CHART.candleSeries.setData(chartData);

    if (OMEGA_CHART.chart.timeScale) {
      OMEGA_CHART.chart.timeScale().fitContent();
    }
  } catch (err) {
    console.error("[Omega Chart] Error seteando datos:", err);
    showChartEmpty(`Error seteando datos del chart: ${err.message || err}`);
  }
}

function updateChartCandle(candle) {
  if (!ensureChart()) return;

  hideChartEmpty();

  try {
    OMEGA_CHART.candleSeries.update(toChartCandle(candle));

    const key = candleKey(candle.symbol, candle.timeframe);
    const candles = OMEGA_CHART.candles.get(key) || [];

    updateChartHeader(candle.symbol, candle.timeframe, candles.length);
  } catch (err) {
    console.error("[Omega Chart] Error actualizando vela:", err);
  }
}

function updateChartHeader(symbol, timeframe, candleCount = null) {
  const title = document.querySelector("#omegaChartTitle");
  const subtitle = document.querySelector("#omegaChartSubtitle");

  if (title) {
    if (symbol && timeframe) {
      title.textContent = `${symbol} · ${timeframe.toUpperCase()}`;
    } else {
      title.textContent = "Omega Chart";
    }
  }

  if (subtitle) {
    const countText =
      candleCount === null ? "Esperando velas" : `${candleCount} velas`;

    subtitle.textContent = `${countText} · Market Hub realtime`;
  }
}

function toChartCandle(candle) {
  return {
    time: candle.time,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
  };
}

function showChartEmpty(message) {
  const empty = document.querySelector("#omegaChartEmpty");
  if (!empty) return;

  empty.textContent = message;
  empty.classList.add("is-visible");
}

function hideChartEmpty() {
  const empty = document.querySelector("#omegaChartEmpty");
  if (!empty) return;

  empty.classList.remove("is-visible");
}

function syncChartButtons() {
  document.querySelectorAll("[data-chart-tf]").forEach((button) => {
    const tf = button.getAttribute("data-chart-tf");

    if (tf === OMEGA_CHART.selectedTf) {
      button.classList.add("is-active");
    } else {
      button.classList.remove("is-active");
    }
  });
}

/* ============================================================
 * Formato
 * ============================================================ */

function toNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function formatPrice(value) {
  const number = toNumber(value);

  if (number === null) return "—";

  if (number >= 1000) {
    return number.toLocaleString("en-US", {
      maximumFractionDigits: 2,
    });
  }

  if (number >= 1) {
    return number.toLocaleString("en-US", {
      maximumFractionDigits: 4,
    });
  }

  if (number >= 0.01) {
    return number.toLocaleString("en-US", {
      maximumFractionDigits: 5,
    });
  }

  return number.toLocaleString("en-US", {
    maximumFractionDigits: 8,
  });
}

function formatTime(ms) {
  const number = toNumber(ms);
  if (number === null) return "—";

  return new Date(number).toLocaleTimeString("es-AR", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function candleDirection(candle) {
  const open = toNumber(candle.open);
  const close = toNumber(candle.close);

  if (open === null || close === null) return "flat";
  if (close > open) return "up";
  if (close < open) return "down";
  return "flat";
}

function directionIcon(direction) {
  if (direction === "up") return "🟢";
  if (direction === "down") return "🔴";
  return "⚪";
}

function isLive(ms) {
  const number = toNumber(ms);
  if (number === null) return false;

  return Date.now() - number < 6000;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

/* ============================================================
 * CSS aislado
 * ============================================================ */

function injectOmegaStyles() {
  if (document.querySelector("#omega-visual-live-styles")) return;

  const style = document.createElement("style");
  style.id = "omega-visual-live-styles";

  style.textContent = `
    .omega-watchlist-panel-mounted {
      overflow: hidden !important;
      box-sizing: border-box !important;
    }

    .omega-watchlist-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 10px;
      margin-bottom: 10px;
    }

    .omega-panel-label {
      font-size: 11px;
      letter-spacing: .08em;
      color: #93a4c7;
      font-weight: 900;
      margin-bottom: 6px;
    }

    .omega-title {
      font-size: 18px;
      line-height: 1.1;
      color: #f8fafc;
      font-weight: 900;
      letter-spacing: -0.03em;
    }

    .omega-subtitle {
      font-size: 11px;
      color: #8ca0c4;
      margin-top: 4px;
    }

    .omega-connection-text {
      color: #8ca0c4;
      font-size: 11px;
      margin-bottom: 10px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .omega-conn-dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: #475569;
      flex: 0 0 auto;
      margin-top: 4px;
    }

    .omega-conn-dot.is-online {
      background: #22c55e;
      box-shadow: 0 0 14px rgba(34, 197, 94, .75);
    }

    .omega-conn-dot.is-connecting,
    .omega-conn-dot.is-reconnecting,
    .omega-conn-dot.is-warning {
      background: #f59e0b;
      box-shadow: 0 0 14px rgba(245, 158, 11, .6);
    }

    .omega-conn-dot.is-offline {
      background: #ef4444;
      box-shadow: 0 0 14px rgba(239, 68, 68, .55);
    }

    .omega-actions {
      display: flex;
      gap: 8px;
      margin-bottom: 12px;
    }

    .omega-actions button {
      border: 1px solid rgba(148, 163, 184, .22);
      background: rgba(15, 23, 42, .85);
      color: #e5e7eb;
      border-radius: 10px;
      padding: 7px 9px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
    }

    .omega-actions button:hover {
      background: rgba(30, 41, 59, .95);
    }

    .omega-watchlist-rows {
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-height: calc(100vh - 260px);
      overflow-y: auto;
      overflow-x: hidden;
      padding-right: 2px;
    }

    .omega-symbol-row {
      border: 1px solid rgba(148, 163, 184, .12);
      background: rgba(15, 23, 42, .58);
      border-radius: 12px;
      padding: 8px;
      width: 100%;
      box-sizing: border-box;
      cursor: pointer;
      transition:
        border-color .15s ease,
        background .15s ease,
        transform .15s ease;
    }

    .omega-symbol-row:hover {
      background: rgba(30, 41, 59, .7);
      border-color: rgba(56, 189, 248, .35);
    }

    .omega-symbol-row.is-selected {
      border-color: rgba(56, 189, 248, .65);
      background: rgba(14, 165, 233, .12);
    }

    .omega-symbol-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      margin-bottom: 7px;
    }

    .omega-symbol-name {
      display: flex;
      align-items: center;
      gap: 6px;
      color: #f8fafc;
      font-size: 12px;
      font-weight: 900;
      min-width: 0;
    }

    .omega-symbol-price {
      color: #e0f2fe;
      font-size: 12px;
      font-weight: 900;
      font-variant-numeric: tabular-nums;
      text-align: right;
    }

    .omega-live-dot {
      width: 7px;
      height: 7px;
      border-radius: 999px;
      background: #475569;
      flex: 0 0 auto;
    }

    .omega-live-dot.is-live {
      background: #22c55e;
      box-shadow: 0 0 10px rgba(34, 197, 94, .75);
    }

    .omega-tf-line {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 4px;
    }

    .omega-tf-pill {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 0;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, .12);
      background: rgba(2, 6, 23, .45);
      color: #94a3b8;
      padding: 3px 4px;
      font-size: 10px;
      font-weight: 800;
      overflow: hidden;
      white-space: nowrap;
    }

    .omega-tf-pill.is-up {
      color: #86efac;
      border-color: rgba(34, 197, 94, .22);
    }

    .omega-tf-pill.is-down {
      color: #fca5a5;
      border-color: rgba(239, 68, 68, .22);
    }

    .omega-tf-pill.is-flat,
    .omega-tf-pill.is-empty {
      color: #cbd5e1;
    }

    .omega-empty {
      color: #94a3b8;
      font-size: 12px;
      padding: 10px;
      border: 1px dashed rgba(148, 163, 184, .18);
      border-radius: 12px;
    }

    .omega-right-body {
      margin-top: 10px;
      color: #cbd5e1;
      font-size: 12px;
      line-height: 1.45;
    }

    .omega-decision-main {
      color: #f8fafc;
      font-weight: 900;
      font-size: 14px;
      margin-bottom: 3px;
    }

    .omega-decision-sub {
      color: #93c5fd;
      font-size: 12px;
      margin-bottom: 5px;
    }

    .omega-decision-reason {
      color: #94a3b8;
      font-size: 11px;
    }

    .omega-kv {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      padding: 4px 0;
      border-bottom: 1px solid rgba(148, 163, 184, .08);
    }

    .omega-kv span {
      color: #94a3b8;
    }

    .omega-kv strong {
      color: #f8fafc;
      text-align: right;
      font-variant-numeric: tabular-nums;
    }

    .omega-muted {
      color: #94a3b8;
    }

    .omega-chart-panel-mounted {
      overflow: hidden !important;
      box-sizing: border-box !important;
      padding: 0 !important;
    }

    .omega-chart-shell {
      width: 100%;
      height: 100%;
      min-height: 420px;
      display: flex;
      flex-direction: column;
      background: rgba(15, 23, 42, .72);
      border-radius: inherit;
      overflow: hidden;
    }

    .omega-chart-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 12px 14px;
      border-bottom: 1px solid rgba(148, 163, 184, .14);
      flex: 0 0 auto;
    }

    .omega-chart-title {
      color: #f8fafc;
      font-size: 15px;
      font-weight: 900;
    }

    .omega-chart-subtitle {
      color: #94a3b8;
      font-size: 11px;
      margin-top: 3px;
    }

    .omega-chart-tf-buttons {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .omega-chart-tf-buttons button {
      border: 1px solid rgba(56, 189, 248, .28);
      background: rgba(15, 23, 42, .8);
      color: #e0f2fe;
      border-radius: 10px;
      padding: 7px 10px;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
    }

    .omega-chart-tf-buttons button:hover,
    .omega-chart-tf-buttons button.is-active {
      background: rgba(14, 165, 233, .20);
      border-color: rgba(56, 189, 248, .70);
    }

    .omega-chart-area {
      position: relative;
      flex: 1;
      min-height: 360px;
      width: 100%;
    }

    .omega-chart-mount {
      position: absolute;
      inset: 0;
      width: 100%;
      height: 100%;
    }

    .omega-chart-empty {
      position: absolute;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      text-align: center;
      padding: 18px;
      color: #94a3b8;
      font-size: 13px;
      background: radial-gradient(circle at center, rgba(15, 23, 42, .45), rgba(15, 23, 42, .82));
      pointer-events: none;
      z-index: 5;
    }

    .omega-chart-empty.is-visible {
      display: flex;
    }

    .omega-standalone-root {
      position: fixed;
      left: 12px;
      top: 12px;
      width: 270px;
      max-height: calc(100vh - 24px);
      overflow: hidden;
      z-index: 9999;
      background: #0f172a;
      border: 1px solid rgba(148, 163, 184, .18);
      border-radius: 16px;
      padding: 14px;
      box-shadow: 0 20px 50px rgba(0,0,0,.35);
    }

    @media (max-width: 900px) {
      .omega-chart-toolbar {
        align-items: flex-start;
        flex-direction: column;
      }

      .omega-chart-tf-buttons {
        justify-content: flex-start;
      }
    }
  `;

  document.head.appendChild(style);
}

/* ============================================================
 * API pública para depuración desde consola
 * ============================================================ */

window.OmegaVisualLab = {
  reconnect: reconnectDashboardWs,
  stop: stopDashboardWs,

  selectSymbol: selectChartSymbol,
  selectTf: selectChartTf,
  renderChart: renderSelectedChart,

  state: OMEGA_UI,
  chart: OMEGA_CHART,
};

/* ============================================================
 * Start
 * ============================================================ */

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootOmegaVisualLab);
} else {
  bootOmegaVisualLab();
}