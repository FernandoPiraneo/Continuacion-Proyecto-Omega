"use strict";

const params = new URLSearchParams(window.location.search);
const targetSymbol = String(params.get("symbol") || "").toUpperCase();
const targetTf = String(params.get("tf") || "15m").toLowerCase();

let ws = null;
let latestVisualState = null;

function wsUrl() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}/ws/dashboard`;
}

function fmt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  if (n >= 1000) return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (n >= 1) return n.toLocaleString("en-US", { maximumFractionDigits: 4 });
  return n.toLocaleString("en-US", { maximumFractionDigits: 8 });
}

function t(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  const ms = n > 1e12 ? n : n * 1000;
  return new Date(ms).toLocaleString("es-AR");
}

function kv(label, value) {
  return `<div class="kv"><span>${label}</span><strong class="mono">${value ?? "—"}</strong></div>`;
}

function card(title, html) {
  return `<section class="card"><h3>${title}</h3>${html}</section>`;
}

function render() {
  const title = document.querySelector("#planTitle");
  const sub = document.querySelector("#planSub");
  const grid = document.querySelector("#planGrid");
  if (!title || !sub || !grid) return;

  if (!latestVisualState) {
    title.textContent = "Plan detail";
    sub.textContent = "Esperando visual_state...";
    grid.innerHTML = "";
    return;
  }

  const symbol = latestVisualState.symbol || targetSymbol || "—";
  const plan = latestVisualState?.best_plan_by_tf?.[targetTf] || latestVisualState?.trade_setups_by_tf?.[targetTf] || {};
  const tree = latestVisualState?.fractal_trade_tree || {};
  const scanner = latestVisualState?.scanner_view || {};
  const structure = latestVisualState?.structure_by_tf?.[targetTf] || {};
  const geometry = latestVisualState?.geometry_by_tf?.[targetTf] || {};
  const indicators = latestVisualState?.indicators_by_tf?.[targetTf] || {};
  const cross = (latestVisualState?.ema_crosses_by_tf?.[targetTf] || []).slice(-1)[0] || {};

  const rrBase = Math.abs(Number(plan.entry) - Number(plan.stop_loss));
  const rrTp3 = rrBase > 0 ? Math.abs(Number(plan.tp3) - Number(plan.entry)) / rrBase : null;

  title.textContent = `${symbol} · ${targetTf.toUpperCase()} · ${plan.direction || "—"} · ${plan.status || "—"}`;
  sub.textContent = `plan_id: ${plan.plan_id || "—"} · visual_only=true`;

  const timeline = [
    `EMA cross: ${cross.type || "—"} @ ${t(cross.time)}`,
    `Setup: ${plan.status || "—"}`,
    `M1 trigger: ${tree.m1_trigger_status || "—"}`,
    `Lifecycle: ${tree.status || "—"}`,
    `Rearm: ${String(tree.rearm_allowed ?? "—")}`,
  ];

  grid.innerHTML = [
    card("Setup", [
      kv("Entry", fmt(plan.entry)),
      kv("SL", fmt(plan.stop_loss)),
      kv("TP1", fmt(plan.tp1)),
      kv("TP2", fmt(plan.tp2)),
      kv("TP3", fmt(plan.tp3)),
      kv("R:R estimado", rrTp3 === null || !Number.isFinite(rrTp3) ? "—" : rrTp3.toFixed(2)),
    ].join("")),
    card("Cruces EMA", [
      kv("Tipo", cross.type || "—"),
      kv("Hora", t(cross.time)),
      kv("TF", targetTf.toUpperCase()),
      kv("Estado", cross.status || "—"),
    ].join("")),
    card("Estructura", [
      kv("Bias", structure.bias ?? "—"),
      kv("BOS", structure.bos ?? "—"),
      kv("CHoCH", structure.choch ?? "—"),
      kv("Pullback", structure.pullback ?? "—"),
      kv("HH/HL/LH/LL", `H ${structure.highs ?? "—"} · L ${structure.lows ?? "—"}`),
    ].join("")),
    card("Geometría", [
      kv("Bias", geometry.bias ?? "—"),
      kv("Confidence", geometry.confidence_score ?? "—"),
      kv("TL soporte", geometry.support_line ? "sí" : "no"),
      kv("TL resistencia", geometry.resistance_line ? "sí" : "no"),
      kv("Pivots relevantes", Array.isArray(geometry.swing_points) ? geometry.swing_points.length : 0),
      kv("Zonas", Array.isArray(geometry.zones) ? geometry.zones.length : 0),
    ].join("")),
    card("SMC / ICT", [
      kv("Liquidity", structure.liquidity ?? "—"),
      kv("Sweep", structure.sweep ?? "—"),
      kv("Reclaim", structure.reclaim ?? "—"),
      kv("Premium/Discount", structure.premium_discount ?? "—"),
      kv("Displacement", structure.displacement ?? "—"),
      kv("FVG / OB", `${structure.fvg ?? "—"} / ${structure.ob ?? "—"}`),
    ].join("")),
    card("Confirmaciones", [
      kv("ADX", indicators.adx_state ?? scanner.adx_state ?? "—"),
      kv("Squeeze", indicators.sqzmom_state ?? scanner.sqzmom_state ?? "—"),
      kv("MACD", indicators.macd_state ?? "—"),
      kv("Geometry score", scanner.geometry_score ?? "—"),
      kv("Indicator score", scanner.indicator_score ?? "—"),
      kv("Total score", scanner.total_score ?? scanner.score ?? "—"),
    ].join("")),
    card("Estado fractal", [
      kv("M15 macro", String(tree.macro_valid ?? "—")),
      kv("M5 confirming", String(tree.m5_confirming ?? "—")),
      kv("M3 transición", tree.m3_transition ?? "—"),
      kv("M1 trigger", tree.m1_trigger_status ?? "—"),
      kv("Rearm allowed", String(tree.rearm_allowed ?? "—")),
      kv("Recommended action", tree.recommended_action ?? "—"),
      kv("Reason", tree.reason ?? "—"),
    ].join("")),
    card("Timeline", `<ul class="timeline">${timeline.map((row) => `<li>${row}</li>`).join("")}</ul>`),
  ].join("");
}

function maybeHandleVisualState(event) {
  if (!event || String(event.type).toLowerCase() !== "visual_state") return;
  if (targetSymbol && String(event.symbol || "").toUpperCase() !== targetSymbol) return;
  latestVisualState = event;
  render();
}

function handleMessage(raw) {
  if (!raw || typeof raw !== "object") return;
  if (raw.type === "bootstrap_state" && Array.isArray(raw.events)) {
    for (const evt of raw.events) maybeHandleVisualState(evt);
    return;
  }
  maybeHandleVisualState(raw);
}

function connect() {
  ws = new WebSocket(wsUrl());
  ws.onmessage = (msg) => {
    try {
      handleMessage(JSON.parse(msg.data));
    } catch (_err) {
      // silenciar parsing parcial en vista detalle
    }
  };
  ws.onclose = () => setTimeout(connect, 1500);
}

render();
connect();
