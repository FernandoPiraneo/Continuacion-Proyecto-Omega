from __future__ import annotations

from typing import Any

from app.models import Candle
from visual_lab.snapshot_builder import (
    TIMEFRAMES,
    build_geometry_by_timeframe_payload,
    build_indicators_and_directions,
    build_koncorde_payload,
    build_quality_payload,
    build_structure_payload,
    build_trendline_overlays,
    build_fractal_trade_state,
)
from visual_lab.visual_setup_engine import (
    build_fractal_trade_tree,
    build_primary_trade,
    build_visual_trade_setups_by_tf,
    detect_ema_crosses_by_tf,
)

ALL_VISIBLE_TFS = ["15m", "5m", "3m", "1m"]
VISUAL_SETUP_TFS = ("15m", "5m", "1m")


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        n = float(value)
        if n != n:
            return None
        return n
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_candle(raw: dict[str, Any]) -> Candle | None:
    open_time = _to_int(raw.get("open_time"))
    close_time = _to_int(raw.get("close_time"))
    open_price = _to_float(raw.get("open"))
    high = _to_float(raw.get("high"))
    low = _to_float(raw.get("low"))
    close = _to_float(raw.get("close"))
    volume = _to_float(raw.get("volume"))

    if (
        open_time is None
        or close_time is None
        or open_price is None
        or high is None
        or low is None
        or close is None
    ):
        return None

    return Candle(
        open_time=open_time,
        open=open_price,
        high=high,
        low=low,
        close=close,
        close_time=close_time,
        volume=volume or 0.0,
    )


def _convert_payload_candles(
    symbol_payload: dict[str, Any],
    *,
    errors: list[str],
) -> dict[str, list[Candle]]:
    candles_by_tf: dict[str, list[Candle]] = {tf: [] for tf in TIMEFRAMES}
    raw_klines = symbol_payload.get("klines") if isinstance(symbol_payload, dict) else None

    if not isinstance(raw_klines, dict):
        errors.append("klines_missing_or_invalid")
        return candles_by_tf

    for tf in TIMEFRAMES:
        raw_candles = raw_klines.get(tf, [])

        if not isinstance(raw_candles, list):
            errors.append(f"{tf}: invalid_candle_list")
            continue

        parsed: dict[int, Candle] = {}

        for item in raw_candles:
            if not isinstance(item, dict):
                continue
            candle = _to_candle(item)
            if candle is None:
                continue
            parsed[candle.open_time] = candle

        candles_by_tf[tf] = [parsed[key] for key in sorted(parsed.keys())]

        if not candles_by_tf[tf]:
            errors.append(f"{tf}: no_valid_candles")

    return candles_by_tf


def _build_visual_setups(
    *,
    symbol: str,
    indicators_by_tf: dict[str, dict[str, Any]],
    structure_by_tf: dict[str, dict[str, Any]],
    geometry_by_tf: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    setups: list[dict[str, Any]] = []

    for tf in VISUAL_SETUP_TFS:
        direction = str((indicators_by_tf.get(tf) or {}).get("direction", "NONE"))
        structure = structure_by_tf.get(tf) or {}
        geometry = geometry_by_tf.get(tf) or {}
        reason = (
            f"visual setup {tf} | structure_bias={structure.get('bias', 'N/A')} "
            f"| geometry_bias={geometry.get('bias', 'N/A')}"
        )

        setups.append(
            {
                "tf": tf,
                "symbol": symbol,
                "direction": direction,
                "status": "VISUAL_ONLY",
                "reason": reason,
                "structure": structure,
                "geometry": geometry,
            }
        )

    return setups


def _normalize_trendlines_for_cross_tf(
    trendlines: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []

    for item in trendlines:
        if not isinstance(item, dict):
            continue

        source_tf = str(item.get("source_timeframe") or item.get("source_tf") or "")
        line = item.get("line") if isinstance(item.get("line"), dict) else {}

        normalized.append(
            {
                **item,
                "source_timeframe": source_tf,
                "source_tf": source_tf,
                "label": item.get("label") or f"TL {source_tf.upper()}" if source_tf else "TL",
                "visible_on": list(ALL_VISIBLE_TFS),
                "visible_on_tfs": list(ALL_VISIBLE_TFS),
                "line": {
                    "start": line.get("start"),
                    "end": line.get("end"),
                    "slope": line.get("slope"),
                    "intercept": line.get("intercept"),
                    "score": line.get("score", 0),
                    "touches": line.get("touches", 0),
                    "violations": line.get("violations", 0),
                },
            }
        )

    return normalized


def _build_visual_state_event(
    *,
    generated_at_ms: int,
    timeframes: list[str],
    symbol: str,
    symbol_payload: dict[str, Any],
) -> dict[str, Any]:
    errors: list[str] = []
    candles_by_tf = _convert_payload_candles(symbol_payload, errors=errors)

    indicators_by_tf, tf_directions, ema_series_by_tf = build_indicators_and_directions(
        candles_by_tf=candles_by_tf,
        errors=errors,
    )
    structure_by_tf = build_structure_payload(candles_by_tf=candles_by_tf, errors=errors)
    geometry_by_tf = build_geometry_by_timeframe_payload(candles_by_tf=candles_by_tf, errors=errors)
    geometry_m15 = geometry_by_tf.get("15m", {})

    koncorde_m15 = build_koncorde_payload(candles_by_tf=candles_by_tf, errors=errors)
    scanner_view = build_quality_payload(
        tf_directions=tf_directions,
        indicators_by_tf=indicators_by_tf,
        koncorde_m15=koncorde_m15,
        structure_by_tf=structure_by_tf,
        candles_by_tf=candles_by_tf,
        geometry=geometry_m15,
        errors=errors,
    )

    trendlines = _normalize_trendlines_for_cross_tf(build_trendline_overlays(geometry_by_tf))
    fractal_state = build_fractal_trade_state(
        latest_plan=None,
        latest_events=[],
        scanner_view=scanner_view,
        geometry_by_tf=geometry_by_tf,
        structure_by_tf=structure_by_tf,
    )

    setups = _build_visual_setups(
        symbol=symbol,
        indicators_by_tf=indicators_by_tf,
        structure_by_tf=structure_by_tf,
        geometry_by_tf=geometry_by_tf,
    )
    ema_crosses_by_tf = detect_ema_crosses_by_tf(ema_series_by_tf)
    trade_setups_by_tf = build_visual_trade_setups_by_tf(
        symbol=symbol,
        mark_price=_to_float(symbol_payload.get("mark_price")),
        structure_by_tf=structure_by_tf,
        geometry_by_tf=geometry_by_tf,
        ema_crosses_by_tf=ema_crosses_by_tf,
        scanner_view=scanner_view if isinstance(scanner_view, dict) else None,
    )
    primary_trade = build_primary_trade(
        symbol=symbol,
        trade_setups_by_tf=trade_setups_by_tf,
        ema_crosses_by_tf=ema_crosses_by_tf,
        structure_by_tf=structure_by_tf,
        geometry_by_tf=geometry_by_tf,
    )
    fractal_trade_tree = build_fractal_trade_tree(
        primary_trade=primary_trade,
        trade_setups_by_tf=trade_setups_by_tf,
        structure_by_tf=structure_by_tf,
        geometry_by_tf=geometry_by_tf,
    )

    return {
        "type": "visual_state",
        "symbol": symbol,
        "generated_at_ms": generated_at_ms,
        "timeframes": timeframes,
        "mark_price": symbol_payload.get("mark_price"),
        "index_price": symbol_payload.get("index_price"),
        "funding_rate": symbol_payload.get("funding_rate"),
        "structure_by_tf": structure_by_tf,
        "geometry_by_tf": geometry_by_tf,
        "indicators_by_tf": indicators_by_tf,
        "ema_series_by_tf": ema_series_by_tf,
        "overlays": {
            "trendlines": trendlines,
        },
        "ema_crosses_by_tf": ema_crosses_by_tf,
        "trade_setups_by_tf": trade_setups_by_tf,
        "primary_trade": primary_trade,
        "fractal_trade_tree": fractal_trade_tree,
        "setups": setups,
        "fractal_state": fractal_state,
        "scanner_view": scanner_view,
        "debug": {
            "errors": errors,
            "source": "visual_lab.scanner_adapter.scan_realtime_state",
            "read_only": True,
        },
    }


def scan_realtime_state(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    generated_at_ms = int(payload.get("generated_at_ms") or 0)
    symbols = payload.get("symbols")
    timeframes = payload.get("timeframes")

    if not isinstance(symbols, dict):
        return []

    tf_list = [str(tf) for tf in timeframes] if isinstance(timeframes, list) else list(TIMEFRAMES)

    events: list[dict[str, Any]] = []

    for symbol, symbol_payload in symbols.items():
        if not isinstance(symbol_payload, dict):
            continue

        events.append(
            _build_visual_state_event(
                generated_at_ms=generated_at_ms,
                timeframes=tf_list,
                symbol=str(symbol).upper(),
                symbol_payload=symbol_payload,
            )
        )

    return events
