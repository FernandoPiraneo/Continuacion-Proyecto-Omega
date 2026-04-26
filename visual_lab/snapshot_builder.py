from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Callable

from app.binance_client import BinanceFuturesClient
from app.models import Candle
from app.strategy_engine import (
    analyze_adx_dmi,
    analyze_ema_signal,
    analyze_koncorde_lite,
    analyze_macd,
    analyze_sqzmom,
    evaluate_signal_quality,
)
from app.structure import analyze_structure
from app.geometric_core import analyze_geometry

try:
    from app.scanner import (
        _calculate_indicator_score,
        _quality_from_total_score,
        _clamp,
    )
except Exception:
    _calculate_indicator_score = None
    _quality_from_total_score = None
    _clamp = None


TIMEFRAMES = ("15m", "5m", "3m", "1m")
EMA_FAST_PERIOD = 55
EMA_SLOW_PERIOD = 200

# Binance USDⓈ-M Futures no acepta 2500 klines en una sola request.
# Estos límites mantienen el Visual Lab liviano y evitan snapshots vacíos por error de API.
KLINES_LIMIT_BY_TF = {
    "15m": 1500,
    "5m": 1000,
    "3m": 1000,
    "1m": 600,
}

TF_RANK = {"15m": 4, "5m": 3, "3m": 2, "1m": 1}


def jsonable(value: Any) -> Any:
    """
    Convierte valores raros/enums/dataclasses simples en tipos serializables por JSON.
    """
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        return value

    if hasattr(value, "value"):
        return value.value

    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]

    return str(value)


def candle_to_chart(candle: Candle) -> dict[str, float | int]:
    return {
        "time": int(candle.open_time // 1000),
        "open": float(candle.open),
        "high": float(candle.high),
        "low": float(candle.low),
        "close": float(candle.close),
        "volume": float(candle.volume),
        "open_time": int(candle.open_time),
        "close_time": int(candle.close_time),
    }


def build_candles_payload(
    *,
    symbol: str,
    timeframe: str,
    candles: list[Candle],
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": [candle_to_chart(candle) for candle in candles],
    }


def safe_call(
    name: str,
    fn: Callable[[], Any],
    fallback: Any = None,
) -> tuple[Any, str | None]:
    try:
        return fn(), None
    except Exception as exc:
        return fallback, f"{name}: {exc}"


def calculate_ema_line_series(
    candles: list[Candle],
    period: int,
    *,
    closed_only: bool = True,
) -> list[dict[str, float | int]]:
    """
    Calcula una serie completa de EMA alineada con el gráfico.

    Nota importante:
    - El scanner usa velas cerradas para EMA.
    - Por eso closed_only=True descarta la vela abierta actual.
    - La primera EMA se inicializa con SMA de los primeros `period` cierres,
      igual que calculate_ema() en app.strategy_engine.py.
    """
    if period <= 0:
        return []

    source_candles = candles[:-1] if closed_only and len(candles) > 1 else candles

    if len(source_candles) < period:
        return []

    closes = [float(candle.close) for candle in source_candles]
    multiplier = 2 / (period + 1)

    ema_value = sum(closes[:period]) / period

    series: list[dict[str, float | int]] = [
        {
            "time": int(source_candles[period - 1].open_time // 1000),
            "value": float(ema_value),
        }
    ]

    for index in range(period, len(source_candles)):
        close = closes[index]
        ema_value = ((close - ema_value) * multiplier) + ema_value

        series.append(
            {
                "time": int(source_candles[index].open_time // 1000),
                "value": float(ema_value),
            }
        )

    return series


def build_ema_payload(candles: list[Candle]) -> dict[str, Any]:
    """
    Payload de medias móviles para frontend.
    """
    return {
        "ema55": calculate_ema_line_series(
            candles,
            EMA_FAST_PERIOD,
            closed_only=True,
        ),
        "ema200": calculate_ema_line_series(
            candles,
            EMA_SLOW_PERIOD,
            closed_only=True,
        ),
        "periods": {
            "fast": EMA_FAST_PERIOD,
            "slow": EMA_SLOW_PERIOD,
        },
        "source": "closed_candles",
    }


def serialize_swing(point: Any) -> dict[str, Any] | None:
    if point is None:
        return None

    timestamp = getattr(point, "timestamp", None)

    return {
        "index": jsonable(getattr(point, "index", None)),
        "price": jsonable(getattr(point, "price", None)),
        "kind": jsonable(getattr(point, "kind", None)),
        "strength": jsonable(getattr(point, "strength", None)),
        "timestamp": jsonable(timestamp),
        "time": int(timestamp // 1000) if timestamp else None,
    }


def serialize_line(line: Any) -> dict[str, Any] | None:
    if line is None:
        return None

    return {
        "kind": jsonable(getattr(line, "kind", None)),
        "slope": jsonable(getattr(line, "slope", None)),
        "intercept": jsonable(getattr(line, "intercept", None)),
        "source_timeframe": jsonable(getattr(line, "source_timeframe", None)),
        "score": jsonable(getattr(line, "score", 0.0)),
        "touches": jsonable(getattr(line, "touches", 0)),
        "violations": jsonable(getattr(line, "violations", 0)),
        "start": serialize_swing(getattr(line, "start", None)),
        "end": serialize_swing(getattr(line, "end", None)),
    }


def serialize_geometry(analysis: Any) -> dict[str, Any]:
    if analysis is None:
        return {
            "bias": "NEUTRAL",
            "confidence_score": 0,
            "reason": "geometry unavailable",
            "swing_points": [],
            "support_line": None,
            "resistance_line": None,
            "zones": [],
            "patterns": [],
        }

    zones: list[dict[str, Any]] = []
    for zone in getattr(analysis, "zones", []) or []:
        zones.append(
            {
                "kind": jsonable(getattr(zone, "kind", None)),
                "low": jsonable(getattr(zone, "low", None)),
                "high": jsonable(getattr(zone, "high", None)),
                "touches": jsonable(getattr(zone, "touches", None)),
                "confidence_score": jsonable(
                    getattr(zone, "confidence_score", None)
                ),
            }
        )

    patterns: list[dict[str, Any]] = []
    for pattern in getattr(analysis, "patterns", []) or []:
        patterns.append(
            {
                "type": jsonable(getattr(pattern, "pattern_type", None)),
                "bias": jsonable(getattr(pattern, "side_bias", None)),
                "score": jsonable(getattr(pattern, "confidence_score", None)),
                "reason": jsonable(getattr(pattern, "reason", None)),
                "key_levels": jsonable(getattr(pattern, "key_levels", None)),
                "invalidation_level": jsonable(
                    getattr(pattern, "invalidation_level", None)
                ),
                "source_timeframe": jsonable(
                    getattr(pattern, "source_timeframe", None)
                ),
            }
        )

    return {
        "bias": jsonable(getattr(analysis, "bias", "NEUTRAL")),
        "confidence_score": jsonable(getattr(analysis, "confidence_score", 0)),
        "reason": jsonable(getattr(analysis, "reason", "")),
        "swing_points": [
            serialize_swing(point)
            for point in (getattr(analysis, "swing_points", []) or [])
        ],
        "support_line": serialize_line(getattr(analysis, "support_line", None)),
        "resistance_line": serialize_line(
            getattr(analysis, "resistance_line", None)
        ),
        "zones": zones,
        "patterns": patterns,
    }


def direction_from_ema(ema_result: dict[str, Any]) -> str:
    trend = str(ema_result.get("trend", "MIX")).upper()

    if trend == "BULL":
        return "LONG"

    if trend == "BEAR":
        return "SHORT"

    return "NONE"


def clamp(value: float, low: float, high: float) -> float:
    if _clamp is not None:
        return float(_clamp(value, low, high))
    return max(low, min(high, value))


def quality_from_total_score(total_score: float) -> str:
    if _quality_from_total_score is not None:
        return str(_quality_from_total_score(total_score))

    if total_score >= 85:
        return "MUY_ALTA"

    if total_score >= 70:
        return "ALTA"

    if total_score >= 55:
        return "MEDIA"

    return "BAJA"


def apply_visual_weighted_scoring(
    *,
    quality: dict[str, Any],
    geometry: dict[str, Any],
    direction: str,
) -> dict[str, Any]:
    scored = dict(quality)

    geometry_confidence = float(geometry.get("confidence_score") or 0.0)
    geometry_bias = str(geometry.get("bias", "NEUTRAL")).upper()

    geometry_score = clamp(geometry_confidence * 0.70, 0.0, 70.0)

    if _calculate_indicator_score is not None:
        indicator_score = float(_calculate_indicator_score(scored, direction))
    else:
        indicator_score = 0.0

    total_score = clamp(geometry_score + indicator_score, 0.0, 100.0)
    quality_label = quality_from_total_score(total_score)

    blockers = (
        list(scored.get("blockers", []))
        if isinstance(scored.get("blockers"), list)
        else []
    )
    degraders = (
        list(scored.get("degraders", []))
        if isinstance(scored.get("degraders"), list)
        else []
    )

    expected_bias = "BULLISH" if direction == "LONG" else "BEARISH"
    opposite_bias = "BEARISH" if direction == "LONG" else "BULLISH"

    if geometry_score < 30:
        degraders.append("geometría débil visual (<30/70)")
        blockers.append("geometría M15 insuficiente para alerta fuerte")
    elif geometry_score < 45:
        degraders.append("geometría insuficiente para calidad ALTA (<45/70)")

    if geometry_bias == opposite_bias:
        blockers.append("M15 geométrico contrario a la señal")
    elif geometry_bias == "NEUTRAL":
        degraders.append("M15 geométrico neutral")

    scored.update(
        {
            "quality": quality_label,
            "geometry_score": round(geometry_score, 2),
            "indicator_score": round(indicator_score, 2),
            "total_score": round(total_score, 2),
            "geometry_bias_m15": geometry_bias,
            "expected_geometry_bias": expected_bias,
            "blockers": sorted(
                set(str(item) for item in blockers if str(item).strip())
            ),
            "degraders": sorted(
                set(str(item) for item in degraders if str(item).strip())
            ),
            "visual_debug": True,
            "visual_note": (
                "Score calculado por plantilla externa, "
                "sin modificar scanner.py."
            ),
        }
    )

    return scored


def latest_plan_for_symbol(db_path: Path, symbol: str) -> dict[str, Any] | None:
    if not db_path.exists():
        return None

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT *
                FROM trade_plans
                WHERE symbol = ?
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            ).fetchone()

            if row is None:
                return None

            payload = dict(row)
            raw_json = payload.get("raw_json")

            if raw_json:
                try:
                    payload["raw"] = json.loads(raw_json)
                except json.JSONDecodeError:
                    payload["raw"] = None

            return jsonable(payload)
    except sqlite3.DatabaseError:
        return None


def latest_plan_events_for_symbol(
    db_path: Path,
    symbol: str,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    e.*,
                    p.symbol,
                    p.direction,
                    p.status AS plan_status
                FROM trade_plan_events e
                JOIN trade_plans p ON p.id = e.plan_id
                WHERE p.symbol = ?
                ORDER BY datetime(e.created_at) DESC, e.id DESC
                LIMIT 30
                """,
                (symbol.upper(),),
            ).fetchall()

            return [jsonable(dict(row)) for row in rows]
    except sqlite3.DatabaseError:
        return []


async def fetch_candles_by_timeframe(
    *,
    client: BinanceFuturesClient,
    symbol: str,
    errors: list[str],
) -> tuple[dict[str, list[Candle]], dict[str, list[dict[str, Any]]]]:
    candles_by_tf: dict[str, list[Candle]] = {}
    chart_by_tf: dict[str, list[dict[str, Any]]] = {}

    for tf in TIMEFRAMES:
        try:
            limit = KLINES_LIMIT_BY_TF.get(tf, 1000)
            candles = await client.get_klines(symbol, interval=tf, limit=limit)
            candles_by_tf[tf] = candles
            chart_by_tf[tf] = [candle_to_chart(candle) for candle in candles]
        except Exception as exc:
            errors.append(f"get_klines_{tf}: {exc}")
            candles_by_tf[tf] = []
            chart_by_tf[tf] = []

    return candles_by_tf, chart_by_tf


def build_indicators_and_directions(
    *,
    candles_by_tf: dict[str, list[Candle]],
    errors: list[str],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, str],
    dict[str, dict[str, Any]],
]:
    indicators_by_tf: dict[str, dict[str, Any]] = {}
    tf_directions: dict[str, str] = {}
    ema_series_by_tf: dict[str, dict[str, Any]] = {}

    for tf, candles in candles_by_tf.items():
        ema_series_by_tf[tf] = build_ema_payload(candles)

        if not candles:
            indicators_by_tf[tf] = {"error": "sin velas"}
            tf_directions[tf] = "NONE"
            continue

        ema_result, error = safe_call(
            "ema",
            lambda candles=candles: analyze_ema_signal(
                candles,
                fast_period=EMA_FAST_PERIOD,
                slow_period=EMA_SLOW_PERIOD,
            ),
            {},
        )
        if error:
            errors.append(f"{tf} {error}")

        adx_result, error = safe_call(
            "adx",
            lambda candles=candles: analyze_adx_dmi(candles),
            {},
        )
        if error:
            errors.append(f"{tf} {error}")

        macd_result, error = safe_call(
            "macd",
            lambda candles=candles: analyze_macd(
                [candle.close for candle in candles[:-1]]
            ),
            {},
        )
        if error:
            errors.append(f"{tf} {error}")

        sqz_result, error = safe_call(
            "sqzmom",
            lambda candles=candles: analyze_sqzmom(candles),
            {},
        )
        if error:
            errors.append(f"{tf} {error}")

        direction = direction_from_ema(ema_result)

        indicators_by_tf[tf] = {
            "ema": jsonable(ema_result),
            "adx": jsonable(adx_result),
            "macd": jsonable(macd_result),
            "sqzmom": jsonable(sqz_result),
            "direction": direction,
        }
        tf_directions[tf] = direction

    return indicators_by_tf, tf_directions, ema_series_by_tf


def build_structure_payload(
    *,
    candles_by_tf: dict[str, list[Candle]],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    structure_by_tf: dict[str, dict[str, Any]] = {}

    for tf in TIMEFRAMES:
        candles = candles_by_tf.get(tf, [])

        if not candles:
            structure_by_tf[tf] = {"error": "sin velas"}
            continue

        structure, error = safe_call(
            "structure",
            lambda candles=candles: analyze_structure(
                candles,
                pivot_window=3,
                pullback_tolerance_mode="atr",
                pullback_atr_mult=0.25,
                pullback_pct=0.15,
            ),
            {},
        )
        if error:
            errors.append(f"{tf} {error}")

        structure_by_tf[tf] = jsonable(structure)

    return structure_by_tf


def build_geometry_by_timeframe_payload(
    *,
    candles_by_tf: dict[str, list[Candle]],
    errors: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Calcula geometría fractal por temporalidad.

    M15 no desaparece: sigue siendo la estructura madre.
    Pero M5/M3/M1 también tienen sus propias TLs, bias, zonas y patrones.
    """
    geometry_by_tf: dict[str, dict[str, Any]] = {}

    for tf in TIMEFRAMES:
        analysis, error = safe_call(
            f"geometry_{tf}",
            lambda tf=tf: analyze_geometry(
                candles_by_tf.get(tf, []),
                source_timeframe=tf,
            ),
            None,
        )

        if error:
            errors.append(f"{tf} {error}")

        geometry_by_tf[tf] = serialize_geometry(analysis)

    return geometry_by_tf


def build_geometry_payload(
    *,
    candles_by_tf: dict[str, list[Candle]],
    errors: list[str],
) -> dict[str, Any]:
    # Compatibilidad con código anterior: devuelve solo la geometría madre M15.
    return build_geometry_by_timeframe_payload(
        candles_by_tf=candles_by_tf,
        errors=errors,
    ).get("15m", {})


def _trendline_role(tf: str) -> str:
    if tf == "15m":
        return "MACRO_STRUCTURE"
    if tf == "5m":
        return "MID_STRUCTURE"
    if tf == "3m":
        return "TRIGGER_CONTEXT"
    return "MICRO_TRIGGER"


def build_trendline_overlays(geometry_by_tf: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    overlays: list[dict[str, Any]] = []

    visible_on = {
        "15m": ["15m", "5m", "3m", "1m"],
        "5m": ["5m", "3m", "1m"],
        "3m": ["3m", "1m"],
        "1m": ["1m"],
    }

    for tf in TIMEFRAMES:
        geometry = geometry_by_tf.get(tf, {}) or {}
        role = _trendline_role(tf)

        for key, label_kind in (("support_line", "Soporte"), ("resistance_line", "Resistencia")):
            line = geometry.get(key)
            if not line:
                continue

            overlays.append(
                {
                    "id": f"{tf}:{key}",
                    "source_timeframe": tf,
                    "kind": line.get("kind") or ("SUPPORT" if key == "support_line" else "RESISTANCE"),
                    "role": role,
                    "label": f"TL {tf.upper()} · {label_kind}",
                    "visible_on": visible_on.get(tf, [tf]),
                    "line": line,
                    "bias": geometry.get("bias", "NEUTRAL"),
                    "confidence_score": geometry.get("confidence_score", 0),
                }
            )

    return overlays


def _norm_direction(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if text in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"
    return "NONE"


def _bias_for_direction(direction: str) -> str:
    return "BULLISH" if direction == "LONG" else "BEARISH" if direction == "SHORT" else "NEUTRAL"


def _opposite_bias(direction: str) -> str:
    return "BEARISH" if direction == "LONG" else "BULLISH" if direction == "SHORT" else "NEUTRAL"


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "si", "sí", "long", "short"}


def _tf_state_for_direction(
    *,
    tf: str,
    direction: str,
    geometry_by_tf: dict[str, dict[str, Any]],
    structure_by_tf: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    geometry = geometry_by_tf.get(tf, {}) or {}
    structure = structure_by_tf.get(tf, {}) or {}
    expected = _bias_for_direction(direction)
    opposite = _opposite_bias(direction)
    geometry_bias = str(geometry.get("bias", "NEUTRAL")).upper()
    structure_bias = str(structure.get("bias", "NEUTRAL")).upper()

    aligned = geometry_bias == expected or structure_bias == expected
    opposed = geometry_bias == opposite or structure_bias == opposite

    state = "UNKNOWN"
    if direction == "NONE":
        state = "NO_DIRECTION"
    elif opposed:
        state = "OPPOSED"
    elif aligned:
        state = "ALIGNED"
    else:
        state = "NEUTRAL"

    return {
        "tf": tf,
        "state": state,
        "geometry_bias": geometry_bias,
        "structure_bias": structure_bias,
        "confidence_score": geometry.get("confidence_score", 0),
        "bos": structure.get("bos"),
        "choch": structure.get("choch"),
        "pullback": structure.get("pullback"),
        "sweep": structure.get("sweep"),
    }


def build_fractal_trade_state(
    *,
    latest_plan: dict[str, Any] | None,
    latest_events: list[dict[str, Any]],
    scanner_view: dict[str, Any],
    geometry_by_tf: dict[str, dict[str, Any]],
    structure_by_tf: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Modelo de lectura fractal para Visual Lab.

    Regla conceptual:
    - M15 invalida la hipótesis madre.
    - M1/M3 invalidan intentos/gatillos, no necesariamente la idea madre.
    - Si hay sweep + reclaim y M15 sigue válido, el setup puede rearmarse.
    """
    direction = _norm_direction((latest_plan or {}).get("direction"))
    if direction == "NONE":
        direction = _norm_direction(scanner_view.get("result"))

    tf_states = {
        tf: _tf_state_for_direction(
            tf=tf,
            direction=direction,
            geometry_by_tf=geometry_by_tf,
            structure_by_tf=structure_by_tf,
        )
        for tf in TIMEFRAMES
    }

    plan_status = str((latest_plan or {}).get("status", "")).upper()
    m15_state = tf_states.get("15m", {})
    m5_state = tf_states.get("5m", {})
    m3_state = tf_states.get("3m", {})
    m1_state = tf_states.get("1m", {})

    macro_invalidated = (
        "INVALID" in plan_status
        or "CANCEL" in plan_status
        or m15_state.get("state") == "OPPOSED"
    )

    context_weakened = m5_state.get("state") == "OPPOSED"
    trigger_invalidated = (
        m1_state.get("state") == "OPPOSED"
        or m3_state.get("state") == "OPPOSED"
        or "M1_TRIGGER_INVALIDATED" in plan_status
    )

    sweep_detected = any(_is_truthy(tf_states[tf].get("sweep")) for tf in TIMEFRAMES if tf in tf_states)
    macro_valid = direction != "NONE" and not macro_invalidated
    rearm_allowed = bool(macro_valid and (trigger_invalidated or sweep_detected or context_weakened))

    if macro_invalidated:
        lifecycle_state = "STRUCTURE_INVALIDATED_M15"
        telegram_severity = "CRITICAL"
        action = "invalidar_plan_madre"
        reason = "M15 quedó opuesto o el plan ya figura como invalidado/cancelado."
    elif trigger_invalidated:
        lifecycle_state = "WAITING_REARM"
        telegram_severity = "WARNING"
        action = "cancelar_intento_micro_y_esperar_nuevo_gatillo"
        reason = "M1/M3 invalidó el gatillo, pero M15 no invalidó la hipótesis madre."
    elif sweep_detected and macro_valid:
        lifecycle_state = "LIQUIDITY_SWEEP_DETECTED"
        telegram_severity = "INFO"
        action = "esperar_reclaim_y_nuevo_gatillo"
        reason = "Hay sweep en alguna capa y M15 sigue vivo; setup potencialmente rearmable."
    elif context_weakened:
        lifecycle_state = "M5_CONTEXT_WEAKENED"
        telegram_severity = "WARNING"
        action = "pausar_entradas_y_esperar_recuperacion"
        reason = "M5 se debilitó/opuso; M15 no invalida todavía."
    elif macro_valid:
        lifecycle_state = "MACRO_SETUP_VALID"
        telegram_severity = "INFO"
        action = "esperar_gatillo_m1_m3"
        reason = "M15 sostiene la estructura madre."
    else:
        lifecycle_state = "NO_ACTIVE_FRACTAL_PLAN"
        telegram_severity = "INFO"
        action = "observar"
        reason = "No hay dirección LONG/SHORT suficiente para evaluar lifecycle fractal."

    return {
        "model": "fractal_trade_lifecycle_v1",
        "direction": direction,
        "anchor_tf": "15m",
        "trigger_tf": "1m",
        "lifecycle_state": lifecycle_state,
        "macro_valid": macro_valid,
        "macro_invalidated": macro_invalidated,
        "trigger_invalidated": trigger_invalidated,
        "context_weakened": context_weakened,
        "sweep_detected": sweep_detected,
        "rearm_allowed": rearm_allowed,
        "telegram_severity": telegram_severity,
        "recommended_action": action,
        "reason": reason,
        "tf_states": tf_states,
        "latest_event_types": [str(ev.get("event_type", "")) for ev in latest_events[:8]],
        "note": "Lectura del dashboard: no ejecuta órdenes ni modifica el plan real.",
    }


def build_koncorde_payload(
    *,
    candles_by_tf: dict[str, list[Candle]],
    errors: list[str],
) -> dict[str, Any]:
    koncorde_m15, error = safe_call(
        "koncorde_m15",
        lambda: analyze_koncorde_lite(candles_by_tf.get("15m", [])),
        {},
    )

    if error:
        errors.append(error)

    return jsonable(koncorde_m15)


def build_quality_payload(
    *,
    tf_directions: dict[str, str],
    indicators_by_tf: dict[str, dict[str, Any]],
    koncorde_m15: dict[str, Any],
    structure_by_tf: dict[str, dict[str, Any]],
    candles_by_tf: dict[str, list[Candle]],
    geometry: dict[str, Any],
    errors: list[str],
) -> dict[str, Any]:
    quality: dict[str, Any] = {
        "result": "SIN_SEÑAL",
        "signal_type": "SIN_SEÑAL",
        "quality": "BAJA",
        "alert_allowed": False,
    }

    if candles_by_tf.get("15m"):
        quality_result, error = safe_call(
            "evaluate_signal_quality",
            lambda: evaluate_signal_quality(
                tf_directions=tf_directions,
                m15_ema=indicators_by_tf.get("15m", {}).get("ema", {}),
                koncorde_m15=koncorde_m15,
                adx_m15=indicators_by_tf.get("15m", {}).get("adx", {}),
                macd_m15=indicators_by_tf.get("15m", {}).get("macd", {}),
                sqzmom_m15=indicators_by_tf.get("15m", {}).get("sqzmom", {}),
                structure_by_tf=structure_by_tf,
            ),
            quality,
        )

        if error:
            errors.append(error)

        quality = jsonable(quality_result)

    direction = str(quality.get("result", "SIN_SEÑAL"))

    if direction in {"LONG", "SHORT"}:
        return apply_visual_weighted_scoring(
            quality=quality,
            geometry=geometry,
            direction=direction,
        )

    scanner_view = dict(quality)
    scanner_view.update(
        {
            "geometry_score": 0,
            "indicator_score": 0,
            "total_score": 0,
            "geometry_bias_m15": geometry.get("bias", "NEUTRAL"),
            "expected_geometry_bias": "-",
            "visual_debug": True,
            "visual_note": "Sin dirección LONG/SHORT activa.",
        }
    )

    return jsonable(scanner_view)


async def build_snapshot_payload(
    *,
    client: BinanceFuturesClient,
    symbol: str,
    selected_timeframe: str,
    db_path: Path,
) -> dict[str, Any]:
    errors: list[str] = []

    candles_by_tf, chart_by_tf = await fetch_candles_by_timeframe(
        client=client,
        symbol=symbol,
        errors=errors,
    )

    (
        indicators_by_tf,
        tf_directions,
        ema_series_by_tf,
    ) = build_indicators_and_directions(
        candles_by_tf=candles_by_tf,
        errors=errors,
    )

    structure_by_tf = build_structure_payload(
        candles_by_tf=candles_by_tf,
        errors=errors,
    )

    geometry_by_tf = build_geometry_by_timeframe_payload(
        candles_by_tf=candles_by_tf,
        errors=errors,
    )
    geometry = geometry_by_tf.get("15m", {})

    koncorde_m15 = build_koncorde_payload(
        candles_by_tf=candles_by_tf,
        errors=errors,
    )

    scanner_view = build_quality_payload(
        tf_directions=tf_directions,
        indicators_by_tf=indicators_by_tf,
        koncorde_m15=koncorde_m15,
        structure_by_tf=structure_by_tf,
        candles_by_tf=candles_by_tf,
        geometry=geometry,
        errors=errors,
    )

    selected_candles = candles_by_tf.get(selected_timeframe, [])
    current_price = selected_candles[-1].close if selected_candles else None

    latest_plan = latest_plan_for_symbol(db_path, symbol)
    latest_events = latest_plan_events_for_symbol(db_path, symbol)
    trendline_overlays = build_trendline_overlays(geometry_by_tf)
    fractal_trade_state = build_fractal_trade_state(
        latest_plan=latest_plan,
        latest_events=latest_events,
        scanner_view=scanner_view,
        geometry_by_tf=geometry_by_tf,
        structure_by_tf=structure_by_tf,
    )

    return jsonable(
        {
            "symbol": symbol,
            "selected_timeframe": selected_timeframe,
            "current_price": current_price,
            "timeframes": list(TIMEFRAMES),
            "candles": chart_by_tf,
            "ema_series": ema_series_by_tf,
            "directions": tf_directions,
            "scanner": scanner_view,
            "geometry_m15": geometry,
            "geometry_by_tf": geometry_by_tf,
            "trendline_overlays": trendline_overlays,
            "fractal_trade_state": fractal_trade_state,
            "structure": structure_by_tf,
            "indicators": indicators_by_tf,
            "koncorde_m15": koncorde_m15,
            "latest_plan": latest_plan,
            "latest_plan_events": latest_events,
            "errors": errors,
        }
    )