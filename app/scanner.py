from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

from app.binance_client import BinanceAPIError, BinanceFuturesClient
from app.models import AlertEvent, AlertPriority, Candle, TradeSide
from app.storage import Storage
from app.strategy_engine import (
    analyze_adx_dmi,
    analyze_ema_signal,
    analyze_koncorde_lite,
    analyze_macd,
    analyze_sqzmom,
    evaluate_signal_quality,
)
from app.structure import analyze_structure
from app.timeframe import normalize_timeframe
from app.trade_manager import TradeManager
from app.geometric_core import analyze_geometry


def _is_geometry_enabled() -> bool:
    val = os.environ.get("ENABLE_GEOMETRY_CORE", "false").strip().lower()
    return val in {"1", "true", "yes", "on"}


def _serialize_geometry(geom_analysis) -> dict:
    if not geom_analysis:
        return {}
    return {
        "bias": geom_analysis.bias,
        "confidence_score": geom_analysis.confidence_score,
        "reason": geom_analysis.reason,
        "zones": [
            {
                "kind": zone.kind,
                "low": zone.low,
                "high": zone.high,
                "touches": zone.touches,
                "confidence_score": zone.confidence_score,
            }
            for zone in geom_analysis.zones
        ],
        "patterns": [
            {
                "type": p.pattern_type,
                "bias": p.side_bias,
                "score": p.confidence_score,
                "reason": p.reason,
                "key_levels": p.key_levels,
                "invalidation_level": p.invalidation_level
            }
            for p in geom_analysis.patterns
        ]
    }

DispatchAlerts = Callable[[list[AlertEvent]], Awaitable[int]]
SignalSentCallback = Callable[[AlertEvent], Awaitable[None]]

QUALITY_ORDER = {"BAJA": 1, "MEDIA": 2, "ALTA": 3, "MUY_ALTA": 4}
QUALITY_LABELS = ("BAJA", "MEDIA", "ALTA", "MUY_ALTA")
DEFAULT_ALLOWED_SIGNAL_TYPES = {
    "LONG_PULLBACK",
    "SHORT_PULLBACK",
    "LONG_CONTINUATION",
    "SHORT_CONTINUATION",
    "LONG_REVERSAL_EARLY",
    "SHORT_REVERSAL_EARLY",
}
GEOMETRY_WEIGHT_MAX = 70.0
INDICATOR_WEIGHT_MAX = 30.0
MIN_GEOMETRY_FOR_HIGH = 45.0
MIN_GEOMETRY_FOR_VERY_HIGH = 55.0
MIN_GEOMETRY_FOR_ALERT = 30.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _cap_quality(current: str, maximum: str) -> str:
    curr_idx = QUALITY_LABELS.index(current) if current in QUALITY_LABELS else 0
    max_idx = QUALITY_LABELS.index(maximum) if maximum in QUALITY_LABELS else 0
    return QUALITY_LABELS[min(curr_idx, max_idx)]


def _quality_from_total_score(total_score: float) -> str:
    if total_score >= 85:
        return "MUY_ALTA"
    if total_score >= 70:
        return "ALTA"
    if total_score >= 55:
        return "MEDIA"
    return "BAJA"


def _has_confirmed_geometry_pattern(geometry_m15: dict | None, direction: str) -> bool:
    if not isinstance(geometry_m15, dict):
        return False
    expected_bias = "LONG" if direction == "LONG" else "SHORT"
    for pattern in geometry_m15.get("patterns", []):
        if not isinstance(pattern, dict):
            continue
        pattern_type = str(pattern.get("type", ""))
        pattern_bias = str(pattern.get("bias", "NEUTRAL")).upper()
        score = float(pattern.get("score", 0.0) or 0.0)
        if pattern_type.startswith("POTENTIAL_"):
            continue
        if pattern_bias not in {expected_bias, "NEUTRAL"}:
            continue
        if score >= 65:
            return True
    return False


def _calculate_indicator_score(quality_payload: dict[str, object], direction: str) -> float:
    score = 0.0
    expected_bias = "BULL" if direction == "LONG" else "BEAR"
    opposite_bias = "BEAR" if direction == "LONG" else "BULL"

    # EMA / tendencia (max 7)
    ema_score = 0.0
    ema_human = quality_payload.get("ema_human", {})
    if isinstance(ema_human, dict):
        close_vs = str(ema_human.get("close_vs_ema200", "")).upper()
        ema55_vs = str(ema_human.get("ema55_vs_ema200", "")).upper()
        if close_vs == "ABOVE" and ema55_vs == "ABOVE" and expected_bias == "BULL":
            ema_score = 7.0
        elif close_vs == "BELOW" and ema55_vs == "BELOW" and expected_bias == "BEAR":
            ema_score = 7.0
        elif close_vs in {"ABOVE", "BELOW"} and ema55_vs in {"ABOVE", "BELOW"}:
            ema_score = 3.0
    score += min(7.0, ema_score)

    # ADX/DMI (max 7)
    adx_score = 0.0
    adx_human = quality_payload.get("adx_human", {})
    if isinstance(adx_human, dict):
        adx_state = str(adx_human.get("state", ""))
        if adx_state in {"ADX_FAVOR_STRONG", "ADX_EXTENDED"}:
            adx_score = 7.0
        elif adx_state == "ADX_FAVOR_OK":
            adx_score = 5.0
        elif adx_state == "ADX_WEAK":
            adx_score = 1.0
    score += min(7.0, adx_score)

    # Squeeze (max 6)
    sqz_score = 0.0
    sqz_human = quality_payload.get("sqzmom_human", {})
    if isinstance(sqz_human, dict):
        sqz_state = str(sqz_human.get("state", "NEUTRAL")).upper()
        if (direction == "LONG" and "LONG" in sqz_state) or (direction == "SHORT" and "SHORT" in sqz_state):
            sqz_score = 6.0 if "STRONG" in sqz_state else 4.0
        elif "NEUTRAL" in sqz_state:
            sqz_score = 2.0
    score += min(6.0, sqz_score)

    # MACD (max 5)
    macd_score = 0.0
    macd_human = quality_payload.get("macd_human", {})
    if isinstance(macd_human, dict):
        macd_state = str(macd_human.get("state", "NEUTRAL")).upper()
        if (direction == "LONG" and "LONG" in macd_state) or (direction == "SHORT" and "SHORT" in macd_state):
            macd_score = 5.0 if "STRONG" in macd_state else 3.0
        elif "NEUTRAL" in macd_state:
            macd_score = 1.0
    score += min(5.0, macd_score)

    # Flujo/Koncorde (max 5)
    kon_score = 0.0
    kon_human = quality_payload.get("koncorde_human", {})
    if isinstance(kon_human, dict):
        kon_state = str(kon_human.get("state", "NEUTRAL")).upper()
        if (direction == "LONG" and kon_state in {"LONG_STRONG", "LONG_OK"}) or (
            direction == "SHORT" and kon_state in {"SHORT_STRONG", "SHORT_OK"}
        ):
            kon_score = 5.0 if kon_state.endswith("STRONG") else 3.0
        elif kon_state in {"NEUTRAL", "ABSORPTION"}:
            kon_score = 1.0
        elif kon_state in {"CONTRARY", "DRY_VOLUME"}:
            kon_score = 0.0
    score += min(5.0, kon_score)

    # si los dos filtros más importantes están contrarios, recorte suave
    if isinstance(adx_human, dict) and isinstance(kon_human, dict):
        if bool(adx_human.get("is_contrary", False)) or bool(kon_human.get("is_contrary", False)):
            score -= 3.0
        if bool(adx_human.get("is_weak", False)) and str(kon_human.get("state", "")).upper() == "DRY_VOLUME":
            score -= 2.0

    return _clamp(score, 0.0, INDICATOR_WEIGHT_MAX)



def evaluate_auto_alert_gate(
    *,
    quality: dict[str, object],
    structure_m15: dict[str, object] | None,
    min_quality: str = "MEDIA",
    allow_low_quality: bool = False,
    allow_momentum_chase: bool = False,
    require_adx_not_weak: bool = True,
    require_structure_confirmation: bool = False,
    block_dry_volume: bool = True,
) -> tuple[bool, str]:
    if not bool(quality.get("alert_allowed", False)):
        return False, "ALERT_ALLOWED_FALSE"

    signal_type = str(quality.get("signal_type", "SIN_SEÑAL"))
    quality_label = str(quality.get("quality", "BAJA"))
    adx_state = str(quality.get("adx_human", {}).get("state", ""))
    kon_state = str(quality.get("koncorde_human", {}).get("state", ""))
    direction = str(quality.get("result", "SIN_SEÑAL"))
    choch = str((structure_m15 or {}).get("choch", "NONE"))

    if signal_type in {"SIN_SEÑAL", "CONFLICT"}:
        return False, signal_type
    if signal_type == "MOMENTUM_CHASE" and not allow_momentum_chase:
        return False, "MOMENTUM_CHASE_DISABLED"
    if signal_type not in DEFAULT_ALLOWED_SIGNAL_TYPES and signal_type != "MOMENTUM_CHASE":
        return False, "SIGNAL_TYPE_BLOCKED"

    if not allow_low_quality:
        threshold = QUALITY_ORDER.get(min_quality.upper(), QUALITY_ORDER["MEDIA"])
        current = QUALITY_ORDER.get(quality_label.upper(), QUALITY_ORDER["BAJA"])
        if current < threshold:
            return False, "LOW_QUALITY"

    if require_adx_not_weak and adx_state in {"ADX_WEAK", "ADX_CONTRARY"}:
        return False, "ADX_WEAK"

    if block_dry_volume and kon_state == "DRY_VOLUME":
        return False, "DRY_VOLUME"
    if kon_state == "CONTRARY":
        return False, "KONCORDE_CONTRARY"

    if signal_type.endswith("REVERSAL_EARLY"):
        if direction == "LONG" and choch == "BEAR":
            return False, "REVERSAL_CHOCH_CONTRA"
        if direction == "SHORT" and choch == "BULL":
            return False, "REVERSAL_CHOCH_CONTRA"

    structure = structure_m15 or {}
    bos_ok = str(structure.get("bos")) in {"BULL", "BEAR"}
    pullback_ok = str(structure.get("pullback")) in {"LONG", "SHORT"}
    if require_structure_confirmation and (not bos_ok or not pullback_ok):
        return False, "NO_STRUCTURE_CONFIRMATION"

    return True, "OK"


def effective_quality_label(
    *,
    quality_payload: dict[str, object],
    structure_m15: dict[str, object] | None,
) -> str:
    quality = str(quality_payload.get("quality", "BAJA"))
    adx_state = str(quality_payload.get("adx_human", {}).get("state", ""))
    kon_state = str(quality_payload.get("koncorde_human", {}).get("state", ""))
    pullback = str((structure_m15 or {}).get("pullback", "NONE"))
    bos = str((structure_m15 or {}).get("bos", "NONE"))
    if adx_state == "ADX_WEAK" and kon_state == "DRY_VOLUME" and pullback not in {"LONG", "SHORT"} and bos == "NONE":
        return "MEDIA" if quality in {"MUY_ALTA", "ALTA", "MEDIA"} else "BAJA"
    return quality


@dataclass(slots=True)
class ScannerSignal:
    event: AlertEvent
    direction: str
    level: str
    trigger_tf: str
    closed_candle_time: int


class SignalScanner:
    def __init__(
        self,
        *,
        binance_client: BinanceFuturesClient,
        trade_manager: TradeManager,
        storage: Storage,
        dispatch_alerts: DispatchAlerts,
        logger: logging.Logger,
        interval_seconds: int = 60,
        alerts_enabled: bool = True,
        structure_enabled: bool = True,
        structure_timeframes: tuple[str, ...] = ("15m", "5m", "3m"),
        pivot_window: int = 3,
        pullback_tolerance_mode: str = "atr",
        pullback_atr_mult: float = 0.25,
        pullback_pct: float = 0.15,
        alert_low_quality: bool = False,
        alert_momentum_chase: bool = False,
        min_quality: str = "MEDIA",
        require_adx_not_weak: bool = True,
        require_structure_confirmation: bool = False,
        block_dry_volume: bool = True,
        on_signal_sent: SignalSentCallback | None = None,
    ) -> None:
        self._binance_client = binance_client
        self._trade_manager = trade_manager
        self._storage = storage
        self._dispatch_alerts = dispatch_alerts
        self._logger = logger
        self._interval_seconds = max(10, int(interval_seconds))
        self._alerts_enabled = alerts_enabled
        self._structure_enabled = structure_enabled
        self._structure_timeframes = tuple(structure_timeframes)
        self._pivot_window = max(2, int(pivot_window))
        self._pullback_tolerance_mode = pullback_tolerance_mode
        self._pullback_atr_mult = pullback_atr_mult
        self._pullback_pct = pullback_pct
        self._alert_low_quality = alert_low_quality
        self._alert_momentum_chase = alert_momentum_chase
        self._min_quality = min_quality.upper()
        self._require_adx_not_weak = require_adx_not_weak
        self._require_structure_confirmation = require_structure_confirmation
        self._block_dry_volume = block_dry_volume
        self._on_signal_sent = on_signal_sent
        self._stop_event = asyncio.Event()
        self._running = False
        self._last_scan_state: dict[str, dict[str, object]] = {}

    @property
    def is_running(self) -> bool:
        return self._running and not self._stop_event.is_set()

    async def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        self._running = True
        self._logger.info(
            "Scanner start | alerts=%s | interval=%ss",
            self._alerts_enabled,
            self._interval_seconds,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await self.scan_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._logger.exception("Scanner error")

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
                except asyncio.TimeoutError:
                    continue
        finally:
            self._running = False
            self._logger.info("Scanner stop")

    async def scan_once(self) -> None:
        if hasattr(self._binance_client, "is_rest_paused") and self._binance_client.is_rest_paused():
            self._logger.warning("scanner paused due rate limit / ban guard")
            return
        symbols = self._trade_manager.get_watchlist_symbols()
        self._logger.info("scanner watchlist count=%s symbols=%s", len(symbols), symbols)
        if not symbols:
            return

        for symbol in symbols:
            self._logger.info("scan symbol %s start", symbol)
            try:
                signals = await self._scan_symbol(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.warning("error symbol=%s continue reason=%s", symbol, exc)
                self._set_last_scan_state(
                    symbol,
                    result_type="ERROR",
                    quality="-",
                    auto_alert_allowed=False,
                    block_reason="ERROR",
                    error=str(exc),
                )
                continue
            if not signals:
                self._set_last_scan_state(
                    symbol,
                    result_type="NO_SIGNAL",
                    quality="-",
                    auto_alert_allowed=False,
                    block_reason="NO_SIGNAL",
                )
                continue

            for signal in signals:
                if await self._storage.has_scanner_alert_state(signal.event.key):
                    self._logger.info("Scanner skip cooldown: %s", signal.event.key)
                    continue
                metadata = signal.event.metadata or {}
                quality_payload = metadata.get("quality_payload")
                structure_m15 = metadata.get("structure_m15")
                if isinstance(quality_payload, dict):
                    allowed, reason = evaluate_auto_alert_gate(
                        quality=quality_payload,
                        structure_m15=structure_m15 if isinstance(structure_m15, dict) else {},
                        min_quality=self._min_quality,
                        allow_low_quality=self._alert_low_quality,
                        allow_momentum_chase=self._alert_momentum_chase,
                        require_adx_not_weak=self._require_adx_not_weak,
                        require_structure_confirmation=self._require_structure_confirmation,
                        block_dry_volume=self._block_dry_volume,
                    )
                    if not allowed:
                        self._logger.info("skip %s reason=%s", symbol, reason)
                        self._set_last_scan_state(
                            symbol,
                            result_type=str(quality_payload.get("signal_type", signal.level)),
                            quality=str(quality_payload.get("quality", "-")),
                            auto_alert_allowed=False,
                            block_reason=reason,
                        )
                        continue

                self._logger.info(
                    "Scanner señal detectada: %s %s %s",
                    symbol,
                    signal.direction,
                    signal.level,
                )
                if self._on_signal_sent is not None:
                    try:
                        await self._on_signal_sent(signal.event)
                    except Exception as exc:
                        self._logger.warning("plan callback error %s: %s", symbol, exc)
                self._attach_plan_section(signal.event)
                if not self._alerts_enabled:
                    continue

                sent_count = await self._dispatch_alerts([signal.event])
                if sent_count <= 0:
                    continue

                await self._storage.record_scanner_alert_state(
                    key=signal.event.key,
                    symbol=symbol,
                    direction=signal.direction,
                    level=signal.level,
                    trigger_tf=signal.trigger_tf,
                    closed_candle_time=str(signal.closed_candle_time),
                )
                self._logger.info("Scanner alerta enviada: %s", signal.event.key)
                self._set_last_scan_state(
                    symbol,
                    result_type=str((signal.event.metadata or {}).get("signal_type", signal.level)),
                    quality=str((signal.event.metadata or {}).get("quality_payload", {}).get("quality", "-")),
                    auto_alert_allowed=True,
                    block_reason="",
                )

    @staticmethod
    def _severity_from_quality(quality_label: str, fallback_level: str) -> AlertPriority:
        if quality_label == "MUY_ALTA":
            return AlertPriority.CRITICAL
        if quality_label == "ALTA":
            return AlertPriority.WARNING
        if quality_label == "MEDIA":
            return AlertPriority.INFO if fallback_level != "FULL_4_4" else AlertPriority.WARNING
        return AlertPriority.INFO

    def _attach_plan_section(self, event: AlertEvent) -> None:
        metadata = event.metadata or {}
        if not bool(metadata.get("scanner", False)):
            return
        lines = [event.note or ""]
        plan_status = str(metadata.get("plan_status", "PLAN_REJECTED"))
        context_quality = str(metadata.get("context_quality", metadata.get("quality_payload", {}).get("quality", "BAJA")))
        setup_status = str(metadata.get("setup_status", "NOT_ARMED"))
        if metadata.get("plan_created") and metadata.get("plan_id") is not None:
            plan = metadata.get("plan_payload") or {}
            lines.extend(
                [
                    "",
                    "🟢 Plan generado:",
                    f"Plan ID: #{metadata.get('plan_id')}",
                    f"Estado: {plan_status}",
                    f"Entrada: {float(plan.get('entry_low', 0.0)):.6f} - {float(plan.get('entry_high', 0.0)):.6f}",
                    f"SL: {float(plan.get('stop_loss', 0.0)):.6f}",
                    f"TP1: {float(plan.get('tp1', 0.0)):.6f} | {float(plan.get('rr_tp1', 0.0)):.2f}R",
                    f"TP2: {float(plan.get('tp2', 0.0)):.6f} | {float(plan.get('rr_tp2', 0.0)):.2f}R",
                    f"TP3: {float(plan.get('tp3', 0.0)):.6f} | {float(plan.get('rr_tp3', 0.0)):.2f}R",
                ]
            )
            metadata["template_used"] = "SIGNAL_WITH_PLAN"
        else:
            prefix = "🟡 Contexto solamente:" if setup_status in {"CONTEXT_ONLY", "NOT_ARMED"} else "🟠 Setup armado:"
            lines.extend(
                [
                    "",
                    prefix,
                    f"Contexto: {context_quality}",
                    f"Setup: {setup_status}",
                    f"Plan status: {plan_status}",
                    "Plan: No generado.",
                    f"Motivo: {metadata.get('plan_block_reason', 'datos insuficientes')}",
                ]
            )
            metadata["template_used"] = "SIGNAL_NO_PLAN"
        event.note = "\n".join(lines).strip()
        event.metadata = metadata

    def _set_last_scan_state(
        self,
        symbol: str,
        *,
        result_type: str,
        quality: str,
        auto_alert_allowed: bool,
        block_reason: str,
        error: str = "",
    ) -> None:
        self._last_scan_state[symbol.upper()] = {
            "symbol": symbol.upper(),
            "scanned_at": datetime.now(tz=timezone.utc).isoformat(),
            "result_type": result_type,
            "quality": quality,
            "auto_alert_allowed": auto_alert_allowed,
            "block_reason": block_reason,
            "error": error,
        }

    def get_last_scan_state(self) -> dict[str, dict[str, object]]:
        return dict(self._last_scan_state)

    def _apply_geometry_weighted_scoring(
        self,
        *,
        quality: dict[str, object],
        geometry_m15: dict | None,
        direction: str,
    ) -> dict[str, object]:
        scored = dict(quality)

        geometry_confidence = 0.0
        geometry_bias = "NEUTRAL"
        geometry_reason = "sin geometría M15"
        if isinstance(geometry_m15, dict):
            geometry_confidence = float(geometry_m15.get("confidence_score", 0.0) or 0.0)
            geometry_bias = str(geometry_m15.get("bias", "NEUTRAL")).upper()
            geometry_reason = str(geometry_m15.get("reason", "sin razón geométrica"))

        geometry_score = _clamp(geometry_confidence * 0.70, 0.0, GEOMETRY_WEIGHT_MAX)
        indicator_score = _calculate_indicator_score(scored, direction)
        total_score = _clamp(geometry_score + indicator_score, 0.0, 100.0)

        quality_label = _quality_from_total_score(total_score)
        degraders = list(scored.get("degraders", [])) if isinstance(scored.get("degraders"), list) else []
        blockers = list(scored.get("blockers", [])) if isinstance(scored.get("blockers"), list) else []

        expected_geom_bias = "BULLISH" if direction == "LONG" else "BEARISH"
        opposite_geom_bias = "BEARISH" if direction == "LONG" else "BULLISH"
        confirmed_pattern = _has_confirmed_geometry_pattern(geometry_m15, direction)

        if geometry_score < MIN_GEOMETRY_FOR_ALERT:
            quality_label = _cap_quality(quality_label, "BAJA")
            degraders.append("geometría débil (<30/70)")
        elif geometry_score < MIN_GEOMETRY_FOR_HIGH:
            quality_label = _cap_quality(quality_label, "MEDIA")
            degraders.append("geometría insuficiente para ALTA (<45/70)")
        elif geometry_score < MIN_GEOMETRY_FOR_VERY_HIGH:
            quality_label = _cap_quality(quality_label, "ALTA")

        if geometry_bias == opposite_geom_bias:
            quality_label = _cap_quality(quality_label, "BAJA")
            blockers.append("M15 geométrico contrario a la señal")
            scored["signal_type"] = "CONFLICT"
        elif geometry_bias == "NEUTRAL" and not confirmed_pattern:
            quality_label = _cap_quality(quality_label, "MEDIA")
            degraders.append("M15 geométrico neutral")

        if geometry_score < MIN_GEOMETRY_FOR_ALERT:
            scored["alert_allowed"] = False
            if str(scored.get("signal_type", "")) not in {"SIN_SEÑAL", "CONFLICT"}:
                scored["signal_type"] = "CONFLICT"
            blockers.append("geometría M15 no armada para alerta fuerte")
        else:
            scored["alert_allowed"] = bool(scored.get("alert_allowed", False)) and "M15 geométrico contrario a la señal" not in blockers

        scored["quality"] = quality_label
        scored["final_quality"] = quality_label
        scored["raw_quality"] = quality_label
        scored["score"] = int(round(total_score))
        scored["score_total"] = int(round(total_score))
        scored["geometry_score"] = round(geometry_score, 2)
        scored["indicator_score"] = round(indicator_score, 2)
        scored["total_score"] = round(total_score, 2)
        scored["geometry_bias_m15"] = geometry_bias
        scored["geometry_reason_m15"] = geometry_reason
        scored["geometry_confirmed_pattern"] = confirmed_pattern
        scored["blockers"] = sorted(set(str(item) for item in blockers if str(item).strip()))
        scored["degraders"] = sorted(set(str(item) for item in degraders if str(item).strip()))
        if scored["blockers"]:
            scored["no_signal_reason"] = "; ".join(scored["blockers"])
        return scored

    async def _scan_symbol(self, symbol: str) -> list[ScannerSignal]:
        candles_by_tf: dict[str, list[Candle]] = {}
        tf_directions: dict[str, str] = {}
        tf_rows: dict[str, str] = {}
        m15_ema = None
        koncorde_m15 = None
        geometry_m15 = None
        structure_by_tf: dict[str, dict[str, str | bool | float | None]] = {}
        adx_by_tf: dict[str, dict[str, float]] = {}
        macd_by_tf: dict[str, dict[str, float]] = {}
        sqz_by_tf: dict[str, dict[str, float | bool]] = {}

        for interval, label in (("15m", "M15"), ("5m", "M5"), ("3m", "M3"), ("1m", "M1")):
            candles = await self._binance_client.get_klines(symbol, interval=interval, limit=210)
            candles_by_tf[interval] = candles
            ema_result = analyze_ema_signal(candles, fast_period=55, slow_period=200)
            adx = analyze_adx_dmi(candles)
            macd = analyze_macd([candle.close for candle in candles[:-1]])
            sqz = analyze_sqzmom(candles)
            adx_by_tf[interval] = adx
            macd_by_tf[interval] = macd
            sqz_by_tf[interval] = sqz

            direction = "NONE"
            if ema_result["trend"] == "BULL":
                direction = "LONG"
            elif ema_result["trend"] == "BEAR":
                direction = "SHORT"
            tf_directions[interval] = direction

            trend_icon = {"LONG": "🟢", "SHORT": "🔴", "NONE": "⚪"}[direction]
            sqz_icon = "⚪"
            if sqz["strong_bull"]:
                sqz_icon = "🟢"
            elif sqz["strong_bear"]:
                sqz_icon = "🔴"
            macd_icon = "⚪"
            if macd["histogram"] > 0:
                macd_icon = "🟢"
            elif macd["histogram"] < 0:
                macd_icon = "🔴"
            tf_rows[interval] = (
                f"{label} {trend_icon} ADX {adx['adx']:.0f} | SQZ {sqz_icon} | MACD {macd_icon}"
                if direction != "NONE"
                else f"{label} ⚪ neutral"
            )

            if interval == "15m":
                m15_ema = ema_result
                koncorde_m15 = analyze_koncorde_lite(candles)
            if self._structure_enabled and interval in self._structure_timeframes:
                try:
                    structure_by_tf[interval] = analyze_structure(
                        candles,
                        pivot_window=self._pivot_window,
                        pullback_tolerance_mode=self._pullback_tolerance_mode,
                        pullback_atr_mult=self._pullback_atr_mult,
                        pullback_pct=self._pullback_pct,
                        logger=self._logger,
                    )
                    self._logger.info(
                        "Structure score | %s %s bias=%s bos=%s choch=%s pullback=%s",
                        symbol,
                        interval,
                        structure_by_tf[interval].get("bias"),
                        structure_by_tf[interval].get("bos"),
                        structure_by_tf[interval].get("choch"),
                        structure_by_tf[interval].get("pullback"),
                    )
                except Exception as exc:
                    self._logger.warning("Structure error %s %s: %s", symbol, interval, exc)
                    structure_by_tf[interval] = {
                        "bias": "MIX",
                        "bos": "NONE",
                        "choch": "NONE",
                        "pullback": "NONE",
                        "summary": "error estructura",
                    }
                    
            if interval == "15m" and _is_geometry_enabled():
                try:
                    geometry_analysis = analyze_geometry(candles, source_timeframe="15m")
                    geometry_m15 = _serialize_geometry(geometry_analysis)
                except Exception as exc:
                    self._logger.warning("Geometry error %s %s: %s", symbol, interval, exc)

        assert m15_ema is not None and koncorde_m15 is not None
        quality = evaluate_signal_quality(
            tf_directions=tf_directions,
            m15_ema=m15_ema,
            koncorde_m15=koncorde_m15,
            adx_m15=adx_by_tf["15m"],
            macd_m15=macd_by_tf["15m"],
            sqzmom_m15=sqz_by_tf["15m"],
            structure_by_tf=structure_by_tf if self._structure_enabled else None,
        )

        result = str(quality.get("result", "SIN_SEÑAL"))
        if result not in {"LONG", "SHORT"}:
            return []

        quality = self._apply_geometry_weighted_scoring(
            quality=quality,
            geometry_m15=geometry_m15,
            direction=result,
        )

        if not bool(quality.get("alert_allowed", False)):
            self._logger.info(
                "Scanner bloqueado %s | signal_type=%s blockers=%s degraders=%s score=%s reason=%s",
                symbol,
                quality.get("signal_type", "SIN_SEÑAL"),
                quality.get("blockers", []),
                quality.get("degraders", []),
                quality.get("total_score", quality.get("score_total", quality.get("score", 0))),
                quality.get("no_signal_reason", ""),
            )
            return []

        signals: list[ScannerSignal] = []

        full_sync = tf_directions["1m"] == result
        m15_closed = self._last_closed_candle(candles_by_tf["15m"])
        m5_closed = self._last_closed_candle(candles_by_tf["5m"])
        m1_closed = self._last_closed_candle(candles_by_tf["1m"])
        if m15_closed is None or m5_closed is None or m1_closed is None:
            return []
        if full_sync:
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction=result,
                    level="FULL_4_4",
                    trigger_tf="1m",
                    closed_candle_time=m1_closed.close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_by_tf.get("15m", {}),
                    geometry_m15=geometry_m15,
                    m15_candle_open_time=m15_closed.open_time,
                    is_closed_candle=True,
                )
            )
        else:
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction=result,
                    level="SYNC_3_4",
                    trigger_tf="5m",
                    closed_candle_time=m5_closed.close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_by_tf.get("15m", {}),
                    geometry_m15=geometry_m15,
                    m15_candle_open_time=m15_closed.open_time,
                    is_closed_candle=True,
                )
            )
        return self._maybe_add_cross_signal(
            symbol=symbol,
            m15_ema=m15_ema,
            candles_by_tf=candles_by_tf,
            tf_rows=tf_rows,
            koncorde_m15=koncorde_m15,
            quality=quality,
            structure_m15=structure_by_tf.get("15m", {}),
            geometry_m15=geometry_m15,
            signals=signals,
        )

    def _maybe_add_cross_signal(
        self,
        *,
        symbol: str,
        m15_ema: dict[str, float | str | None],
        candles_by_tf: dict[str, list[Candle]],
        tf_rows: dict[str, str],
        koncorde_m15: dict[str, float | str | bool | None],
        quality: dict[str, int | str | dict | list],
        structure_m15: dict[str, str | bool | float | None],
        geometry_m15: dict | None,
        signals: list[ScannerSignal],
    ) -> list[ScannerSignal]:
        cross = str(m15_ema["cross"])
        m15_closed = self._last_closed_candle(candles_by_tf["15m"])
        if m15_closed is None:
            return signals
        if cross == "bull_cross":
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction="LONG",
                    level="EMA_CROSS_M15",
                    trigger_tf="15m",
                    closed_candle_time=m15_closed.close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_m15,
                    geometry_m15=geometry_m15,
                    m15_candle_open_time=m15_closed.open_time,
                    is_closed_candle=True,
                )
            )
        elif cross == "bear_cross":
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction="SHORT",
                    level="EMA_CROSS_M15",
                    trigger_tf="15m",
                    closed_candle_time=m15_closed.close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_m15,
                    m15_candle_open_time=m15_closed.open_time,
                    is_closed_candle=True,
                )
            )
        return signals

    def _build_signal(
        self,
        *,
        symbol: str,
        direction: str,
        level: str,
        trigger_tf: str,
        closed_candle_time: int,
        tf_rows: dict[str, str],
        m15_ema: dict[str, float | str | None],
        koncorde_m15: dict[str, float | str | bool | None],
        quality: dict[str, int | str | dict | list],
        structure_m15: dict[str, str | bool | float | None],
        geometry_m15: dict | None,
        m15_candle_open_time: int,
        is_closed_candle: bool,
    ) -> ScannerSignal:
        side = TradeSide.LONG if direction == "LONG" else TradeSide.SHORT
        trigger_tf = normalize_timeframe(trigger_tf)
        header_icon = "🟢" if direction == "LONG" else "🔴"
        signal_type = str(quality.get("signal_type", f"{direction}_CONTINUATION"))
        level_text = {
            "SYNC_3_4": f"{signal_type} sync 3/4",
            "FULL_4_4": f"{signal_type} sync 4/4",
            "EMA_CROSS_M15": f"{signal_type} EMA cross M15",
        }[level]
        cross_text = "none"
        if m15_ema["cross"] == "bull_cross":
            cross_text = "🟢 bull"
        elif m15_ema["cross"] == "bear_cross":
            cross_text = "🔴 bear"
        expected_bias = "BULL" if direction == "LONG" else "BEAR"
        expected_geometry_bias = "BULLISH" if direction == "LONG" else "BEARISH"

        effective_quality = effective_quality_label(quality_payload=quality, structure_m15=structure_m15)
        geometry_score = float(quality.get("geometry_score", 0.0) or 0.0)
        indicator_score = float(quality.get("indicator_score", 0.0) or 0.0)
        total_score = float(quality.get("total_score", quality.get("score_total", quality.get("score", 0.0))) or 0.0)
        geometry_bias = str(quality.get("geometry_bias_m15", geometry_m15.get("bias", "NEUTRAL") if isinstance(geometry_m15, dict) else "NEUTRAL")).upper()
        geometry_reason = str(quality.get("geometry_reason_m15", geometry_m15.get("reason", "") if isinstance(geometry_m15, dict) else "sin geometría M15"))
        geometry_patterns = geometry_m15.get("patterns", []) if isinstance(geometry_m15, dict) else []
        confirmed_patterns = [
            str(p.get("type", "UNKNOWN"))
            for p in geometry_patterns
            if isinstance(p, dict) and not str(p.get("type", "")).startswith("POTENTIAL_")
        ]
        potential_patterns = [
            str(p.get("type", "UNKNOWN"))
            for p in geometry_patterns
            if isinstance(p, dict) and str(p.get("type", "")).startswith("POTENTIAL_")
        ]
        dominant_zone = None
        if isinstance(geometry_m15, dict):
            zones = geometry_m15.get("zones", [])
            if isinstance(zones, list) and zones:
                zone0 = zones[0]
                if isinstance(zone0, dict):
                    try:
                        dominant_zone = (
                            f"{zone0.get('kind', 'N/A')} "
                            f"{float(zone0.get('low', 0.0)):.4f}-{float(zone0.get('high', 0.0)):.4f}"
                        )
                    except (TypeError, ValueError):
                        dominant_zone = str(zone0.get("kind", "N/A"))

        note = "\n".join(
            [
                "⚠️ Señal detectada",
                "",
                f"Símbolo: {symbol}",
                f"Tipo: {signal_type}",
                f"Side: {direction}",
                f"Calidad: {effective_quality}",
                f"Precio: {float(m15_ema['close']):.6f} USDT",
                "",
                "Lectura:",
                f"- Nivel: {level_text}",
                f"- Estructura M15: {structure_m15.get('bias', 'MIX')} | BOS {'✅' if str(structure_m15.get('bos')) == expected_bias else '❌'} | Pullback {'✅' if str(structure_m15.get('pullback')) == direction else '❌'}",
                f"- Momentum TF: M15 {tf_rows['15m'].split()[1]} | M5 {tf_rows['5m'].split()[1]} | M3 {tf_rows['3m'].split()[1]} | M1 {tf_rows['1m'].split()[1]}",
                f"- MAs: {header_icon} {m15_ema['relation']} | cruce {cross_text}",
                f"- ADX: {quality.get('adx_human', {}).get('status', '🟡 sin lectura')}",
                f"- Flujo: {quality.get('koncorde_human', {}).get('status', '🟡 flujo neutral')}",
                "",
                f"📐 Geometría: {geometry_score:.1f}/70",
                f"- M15: {geometry_bias}",
                f"- Sesgo esperado: {expected_geometry_bias}",
                f"- Razón: {geometry_reason}",
                f"- Patrón confirmado: {confirmed_patterns[0] if confirmed_patterns else 'none'}",
                f"- Patrón potencial: {potential_patterns[0] if potential_patterns else 'none'}",
                f"- Zona dominante: {dominant_zone or 'none'}",
                "",
                f"📊 Indicadores: {indicator_score:.1f}/30",
                f"🎯 Score total: {total_score:.1f}/100",
                "",
                "⚠ Solo alerta. No orden.",
            ]
        )
        quality_label = effective_quality
        event = AlertEvent(
            key=f"scanner::{symbol}::{direction}::{level}::{trigger_tf}::{closed_candle_time}",
            priority=self._severity_from_quality(quality_label, level),
            reason=level_text,
            symbol=symbol,
            side=side,
            current_price=float(m15_ema["close"]),
            note=note,
            cooldown_seconds=0,
            metadata={
                "scanner": True,
                "direction": direction,
                "signal_type": signal_type,
                "level": level,
                "source_tf": trigger_tf,
                "execution_tf": "15m",
                "trigger_tf": trigger_tf,
                "closed_candle_time": closed_candle_time,
                "m15_candle_open_time": m15_candle_open_time,
                "source_candle_closed": True,
                "execution_candle_closed": is_closed_candle,
                "is_closed_candle": is_closed_candle,
                "quality_payload": quality,
                "structure_m15": structure_m15,
                "geometry_m15": geometry_m15,
                "geometry_score": geometry_score,
                "indicator_score": indicator_score,
                "total_score": total_score,
                "m15_geometry_bias": geometry_bias,
                "m15_geometry_reason": geometry_reason,
                "severity": self._severity_from_quality(quality_label, level).value,
            },
        )
        return ScannerSignal(
            event=event,
            direction=direction,
            level=level,
            trigger_tf=trigger_tf,
            closed_candle_time=closed_candle_time,
        )

    @staticmethod
    def _closed_candles_only(candles: list[Candle]) -> list[Candle]:
        if len(candles) <= 1:
            return []
        return candles[:-1]

    @staticmethod
    def _last_closed_candle(candles: list[Candle]) -> Candle | None:
        closed = SignalScanner._closed_candles_only(candles)
        if not closed:
            return None
        return closed[-1]
