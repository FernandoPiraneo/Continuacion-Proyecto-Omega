from __future__ import annotations

from typing import Any

TIMEFRAMES = ("15m", "5m", "3m", "1m")
EXCLUDED_PLAN_STATUSES = {
    "INSUFFICIENT_STRUCTURE",
    "INVALID_LEVEL_ORDER",
    "CONFLICT",
    "MACRO_SETUP_INVALIDATED",
    "SETUP_INVALIDATED",
}


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


def _norm_direction(value: Any) -> str:
    text = str(value or "").upper()
    if text in {"LONG", "BUY", "BULL", "BULLISH"}:
        return "LONG"
    if text in {"SHORT", "SELL", "BEAR", "BEARISH"}:
        return "SHORT"
    return "NONE"


def _pick_geometry_reference(geometry: dict[str, Any], kind: str) -> float | None:
    line = geometry.get("support_line" if kind == "support" else "resistance_line")
    if isinstance(line, dict):
        for key in ("end", "start"):
            point = line.get(key)
            if isinstance(point, dict):
                value = _to_float(point.get("price"))
                if value is not None:
                    return value

    zones = geometry.get("zones")
    if isinstance(zones, list):
        candidates: list[float] = []
        for zone in zones:
            if not isinstance(zone, dict):
                continue
            low = _to_float(zone.get("low"))
            high = _to_float(zone.get("high"))
            if low is not None:
                candidates.append(low)
            if high is not None:
                candidates.append(high)
        if candidates:
            return min(candidates) if kind == "support" else max(candidates)

    return None


def _nearest_levels(
    *,
    mark_price: float | None,
    structure: dict[str, Any],
    geometry: dict[str, Any],
) -> tuple[float | None, float | None]:
    support = _to_float(structure.get("last_low"))
    resistance = _to_float(structure.get("last_high"))

    if support is None:
        support = _pick_geometry_reference(geometry, "support")
    if resistance is None:
        resistance = _pick_geometry_reference(geometry, "resistance")

    if mark_price is not None:
        if support is not None and support >= mark_price:
            support = None
        if resistance is not None and resistance <= mark_price:
            resistance = None

    return support, resistance


def detect_ema_crosses_by_tf(ema_series_by_tf: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {tf: [] for tf in TIMEFRAMES}

    for tf in TIMEFRAMES:
        payload = ema_series_by_tf.get(tf, {}) if isinstance(ema_series_by_tf, dict) else {}
        ema55 = payload.get("ema55") if isinstance(payload, dict) else []
        ema200 = payload.get("ema200") if isinstance(payload, dict) else []

        if not isinstance(ema55, list) or not isinstance(ema200, list):
            continue

        fast_by_time = {
            _to_int(item.get("time")): _to_float(item.get("value"))
            for item in ema55
            if isinstance(item, dict)
        }
        slow_by_time = {
            _to_int(item.get("time")): _to_float(item.get("value"))
            for item in ema200
            if isinstance(item, dict)
        }

        common_times = sorted(
            time
            for time in set(fast_by_time).intersection(slow_by_time)
            if time is not None and fast_by_time.get(time) is not None and slow_by_time.get(time) is not None
        )

        if len(common_times) < 2:
            continue

        crosses: list[dict[str, Any]] = []

        for idx in range(1, len(common_times)):
            prev_t = common_times[idx - 1]
            curr_t = common_times[idx]
            prev_fast = float(fast_by_time[prev_t])
            prev_slow = float(slow_by_time[prev_t])
            curr_fast = float(fast_by_time[curr_t])
            curr_slow = float(slow_by_time[curr_t])

            if prev_fast <= prev_slow and curr_fast > curr_slow:
                crosses.append(
                    {
                        "tf": tf,
                        "type": "BULL_CROSS",
                        "direction": "LONG",
                        "time": int(curr_t),
                        "ema55": curr_fast,
                        "ema200": curr_slow,
                        "status": "VISUAL_ONLY",
                    }
                )
            elif prev_fast >= prev_slow and curr_fast < curr_slow:
                crosses.append(
                    {
                        "tf": tf,
                        "type": "BEAR_CROSS",
                        "direction": "SHORT",
                        "time": int(curr_t),
                        "ema55": curr_fast,
                        "ema200": curr_slow,
                        "status": "VISUAL_ONLY",
                    }
                )

        out[tf] = crosses[-50:]

    return out


def _validate_level_order(direction: str, entry: float | None, sl: float | None, tp1: float | None, tp2: float | None, tp3: float | None) -> bool:
    if any(level is None for level in (entry, sl, tp1, tp2, tp3)):
        return False

    if direction == "LONG":
        return bool(sl < entry < tp1 < tp2 < tp3)

    if direction == "SHORT":
        return bool(tp3 < tp2 < tp1 < entry < sl)

    return False


def _build_setup_levels(
    *,
    direction: str,
    mark_price: float | None,
    support: float | None,
    resistance: float | None,
) -> dict[str, float | None]:
    entry = mark_price

    if direction == "LONG":
        sl = support
        tp1 = resistance
        if entry is not None and sl is not None and tp1 is not None:
            risk = max(1e-9, entry - sl)
            tp2 = max(tp1, entry + (risk * 2))
            tp3 = max(tp2, entry + (risk * 3))
        else:
            tp2 = None
            tp3 = None
    elif direction == "SHORT":
        sl = resistance
        tp1 = support
        if entry is not None and sl is not None and tp1 is not None:
            risk = max(1e-9, sl - entry)
            tp2 = min(tp1, entry - (risk * 2))
            tp3 = min(tp2, entry - (risk * 3))
        else:
            tp2 = None
            tp3 = None
    else:
        sl = tp1 = tp2 = tp3 = None

    return {
        "entry": entry,
        "stop_loss": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "invalidation_level": sl,
    }


def _is_setup_invalidated(*, direction: str, mark_price: float | None, invalidation_level: float | None) -> bool:
    if mark_price is None or invalidation_level is None:
        return False
    if direction == "LONG":
        return mark_price <= invalidation_level
    if direction == "SHORT":
        return mark_price >= invalidation_level
    return False


def build_visual_trade_setups_by_tf(
    *,
    symbol: str,
    mark_price: float | None,
    structure_by_tf: dict[str, dict[str, Any]],
    geometry_by_tf: dict[str, dict[str, Any]],
    ema_crosses_by_tf: dict[str, list[dict[str, Any]]],
    scanner_view: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    setups: dict[str, dict[str, Any]] = {}

    scanner_direction = _norm_direction((scanner_view or {}).get("result"))

    for tf in TIMEFRAMES:
        crosses = ema_crosses_by_tf.get(tf, []) if isinstance(ema_crosses_by_tf, dict) else []
        structure = structure_by_tf.get(tf, {}) if isinstance(structure_by_tf, dict) else {}
        geometry = geometry_by_tf.get(tf, {}) if isinstance(geometry_by_tf, dict) else {}

        latest_cross = crosses[-1] if crosses else None

        if latest_cross is None:
            setups[tf] = {
                "symbol": symbol,
                "tf": tf,
                "status": "NO_EMA_CROSS",
                "direction": "NONE",
                "source_event": "EMA_CROSS",
                "ema_cross_type": None,
                "source_cross": None,
                "entry": None,
                "stop_loss": None,
                "tp1": None,
                "tp2": None,
                "tp3": None,
                "invalidation_level": None,
                "structure_bias": structure.get("bias"),
                "geometry_bias": geometry.get("bias"),
                "bos": structure.get("bos"),
                "choch": structure.get("choch"),
                "pullback": structure.get("pullback"),
                "nearest_support": None,
                "nearest_resistance": None,
                "trendline_reference": geometry.get("support_line") or geometry.get("resistance_line"),
                "reason": "Sin cruce EMA55/EMA200 reciente.",
                "visual_only": True,
            }
            continue

        direction = _norm_direction(latest_cross.get("direction"))
        support, resistance = _nearest_levels(mark_price=mark_price, structure=structure, geometry=geometry)
        levels = _build_setup_levels(
            direction=direction,
            mark_price=mark_price,
            support=support,
            resistance=resistance,
        )

        status = "EMA_CROSS_DETECTED"
        reason = "Cruce EMA detectado, setup visual generado."

        if any(levels[key] is None for key in ("entry", "stop_loss", "tp1")):
            status = "INSUFFICIENT_STRUCTURE"
            reason = "Faltan niveles estructurales mínimos (entry/SL/TP1)."
        elif not _validate_level_order(
            direction,
            levels["entry"],
            levels["stop_loss"],
            levels["tp1"],
            levels["tp2"],
            levels["tp3"],
        ):
            status = "INVALID_LEVEL_ORDER"
            reason = "Orden inválido de niveles para la dirección del setup."
        else:
            structure_bias = str(structure.get("bias", "")).upper()
            if _is_setup_invalidated(
                direction=direction,
                mark_price=mark_price,
                invalidation_level=levels.get("invalidation_level"),
            ):
                status = "SETUP_INVALIDATED"
                reason = "Precio actual cruzó nivel de invalidación del setup."
            elif tf == "15m":
                if structure_bias not in {"BULL", "BEAR", ""}:
                    status = "INSUFFICIENT_STRUCTURE"
                    reason = "M15 sin bias estructural claro para hipótesis macro."
                else:
                    status = "MACRO_SETUP_VALID"
                    reason = "M15 válido como hipótesis macro visual."
            elif tf == "5m":
                if scanner_direction != "NONE" and direction != scanner_direction:
                    status = "M5_CONTEXT_WEAKENED"
                    reason = "M5 debilitado respecto del sesgo principal."
                else:
                    status = "M5_CONTEXT_CONFIRMING"
                    reason = "M5 confirma el contexto macro visual."
            elif tf == "3m":
                status = "M3_TRANSITIONAL"
                reason = "M3 en transición microestructural."
            elif tf == "1m":
                if scanner_direction != "NONE" and direction != scanner_direction:
                    status = "M1_TRIGGER_INVALIDATED"
                    reason = "M1 invalida gatillo respecto del contexto dominante."
                else:
                    status = "M1_TRIGGER_ARMED"
                    reason = "M1 armado como gatillo visual."

        setups[tf] = {
            "symbol": symbol,
            "tf": tf,
            "status": status,
            "direction": direction,
            "source_event": "EMA_CROSS",
            "ema_cross_type": latest_cross.get("type"),
            "source_cross": latest_cross,
            **levels,
            "structure_bias": structure.get("bias"),
            "geometry_bias": geometry.get("bias"),
            "bos": structure.get("bos"),
            "choch": structure.get("choch"),
            "pullback": structure.get("pullback"),
            "nearest_support": support,
            "nearest_resistance": resistance,
            "trendline_reference": geometry.get("support_line") or geometry.get("resistance_line"),
            "reason": reason,
            "visual_only": True,
        }

    return setups


def build_primary_trade(
    *,
    symbol: str,
    trade_setups_by_tf: dict[str, dict[str, Any]],
    ema_crosses_by_tf: dict[str, list[dict[str, Any]]],
    structure_by_tf: dict[str, dict[str, Any]],
    geometry_by_tf: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    m15 = trade_setups_by_tf.get("15m", {}) if isinstance(trade_setups_by_tf, dict) else {}
    cross = (ema_crosses_by_tf.get("15m") or [None])[-1] if isinstance(ema_crosses_by_tf, dict) else None

    if not m15:
        return {
            "symbol": symbol,
            "anchor_tf": "15m",
            "direction": "NONE",
            "status": "INSUFFICIENT_STRUCTURE",
            "entry": None,
            "stop_loss": None,
            "tp1": None,
            "tp2": None,
            "tp3": None,
            "source_event": "EMA_CROSS",
            "source_cross": cross,
            "visual_only": True,
            "reason": "No hay setup M15 disponible.",
        }

    status = str(m15.get("status", "INSUFFICIENT_STRUCTURE"))
    if status not in {"MACRO_SETUP_VALID", "M5_CONTEXT_CONFIRMING", "M5_CONTEXT_WEAKENED", "M3_TRANSITIONAL", "M1_TRIGGER_ARMED", "M1_TRIGGER_INVALIDATED"}:
        status = "INSUFFICIENT_STRUCTURE"

    return {
        "symbol": symbol,
        "anchor_tf": "15m",
        "direction": m15.get("direction", "NONE"),
        "status": status,
        "entry": m15.get("entry"),
        "stop_loss": m15.get("stop_loss"),
        "tp1": m15.get("tp1"),
        "tp2": m15.get("tp2"),
        "tp3": m15.get("tp3"),
        "source_event": "EMA_CROSS",
        "source_cross": cross,
        "visual_only": True,
        "reason": m15.get("reason") or "Trade principal derivado del setup M15.",
        "structure_bias": (structure_by_tf.get("15m") or {}).get("bias") if isinstance(structure_by_tf, dict) else None,
        "geometry_bias": (geometry_by_tf.get("15m") or {}).get("bias") if isinstance(geometry_by_tf, dict) else None,
    }


def build_fractal_trade_tree(
    *,
    primary_trade: dict[str, Any],
    trade_setups_by_tf: dict[str, dict[str, Any]],
    structure_by_tf: dict[str, dict[str, Any]],
    geometry_by_tf: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    m15 = trade_setups_by_tf.get("15m", {})
    m5 = trade_setups_by_tf.get("5m", {})
    m3 = trade_setups_by_tf.get("3m", {})
    m1 = trade_setups_by_tf.get("1m", {})

    m15_status = str(m15.get("status", "INSUFFICIENT_STRUCTURE"))
    m5_status = str(m5.get("status", "INSUFFICIENT_STRUCTURE"))
    m3_status = str(m3.get("status", "INSUFFICIENT_STRUCTURE"))
    m1_status = str(m1.get("status", "INSUFFICIENT_STRUCTURE"))

    macro_invalidated = m15_status in {"MACRO_SETUP_INVALIDATED", "INSUFFICIENT_STRUCTURE", "INVALID_LEVEL_ORDER", "NO_EMA_CROSS"}
    macro_valid = not macro_invalidated and _norm_direction(primary_trade.get("direction")) in {"LONG", "SHORT"}

    m5_confirming = m5_status == "M5_CONTEXT_CONFIRMING"
    m5_weakened = m5_status == "M5_CONTEXT_WEAKENED"

    if macro_invalidated:
        lifecycle = "MACRO_SETUP_INVALIDATED"
        rearm_allowed = False
        recommended_action = "descartar_setups_micro"
        reason = "M15 invalidó la hipótesis principal."
    elif m1_status == "M1_TRIGGER_INVALIDATED":
        lifecycle = "WAITING_REARM"
        rearm_allowed = True
        recommended_action = "esperar_rearm_m1"
        reason = "M1 invalidó el gatillo, pero M15 mantiene hipótesis macro."
    elif m1_status == "M1_TRIGGER_ARMED" and m5_confirming:
        lifecycle = "M1_TRIGGER_ARMED"
        rearm_allowed = False
        recommended_action = "observar_confirmacion_final"
        reason = "M1 armado con soporte de M15/M5."
    elif m5_weakened:
        lifecycle = "M5_CONTEXT_WEAKENED"
        rearm_allowed = macro_valid
        recommended_action = "reducir_confianza_y_esperar"
        reason = "M5 debilitado, macro M15 aún vigente."
    else:
        lifecycle = "MACRO_SETUP_VALID" if macro_valid else "INSUFFICIENT_STRUCTURE"
        rearm_allowed = macro_valid
        recommended_action = "esperar_gatillo_m1"
        reason = "Esperando mayor confirmación fractal."

    return {
        "primary_tf": "15m",
        "trigger_tf": "1m",
        "confirmation_tfs": ["5m", "3m"],
        "macro_valid": macro_valid,
        "macro_invalidated": macro_invalidated,
        "m5_confirming": m5_confirming,
        "m3_transition": m3_status,
        "m1_trigger_status": m1_status,
        "rearm_allowed": rearm_allowed,
        "status": lifecycle,
        "recommended_action": recommended_action,
        "reason": reason,
        "children": {
            "15m": m15,
            "5m": m5,
            "3m": m3,
            "1m": m1,
        },
        "debug": {
            "structure_m15": (structure_by_tf.get("15m") or {}).get("bias") if isinstance(structure_by_tf, dict) else None,
            "geometry_m15": (geometry_by_tf.get("15m") or {}).get("bias") if isinstance(geometry_by_tf, dict) else None,
        },
        "visual_only": True,
    }


def _safe_score(value: Any) -> float:
    score = _to_float(value)
    return float(score or 0.0)


def _risk_reward_to_tp2_tp3(setup: dict[str, Any]) -> float:
    entry = _to_float(setup.get("entry"))
    sl = _to_float(setup.get("stop_loss"))
    tp2 = _to_float(setup.get("tp2"))
    tp3 = _to_float(setup.get("tp3"))
    if entry is None or sl is None:
        return 0.0
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0
    rr2 = abs((tp2 or entry) - entry) / risk if tp2 is not None else 0.0
    rr3 = abs((tp3 or entry) - entry) / risk if tp3 is not None else 0.0
    return rr2 + rr3


def _plan_quality_score(
    *,
    tf: str,
    setup: dict[str, Any],
    scanner_view: dict[str, Any] | None,
    indicators_by_tf: dict[str, dict[str, Any]] | None,
    macro_direction: str,
) -> float:
    status = str(setup.get("status", "")).upper()
    if status in EXCLUDED_PLAN_STATUSES:
        return -9999.0

    direction = _norm_direction(setup.get("direction"))
    score = 0.0
    if direction in {"LONG", "SHORT"}:
        score += 25.0
    if macro_direction in {"LONG", "SHORT"} and direction == macro_direction:
        score += 20.0
    if status in {"MACRO_SETUP_VALID", "M5_CONTEXT_CONFIRMING", "M1_TRIGGER_ARMED"}:
        score += 18.0
    elif "WEAKENED" in status or "REARM" in status:
        score += 6.0
    score += _safe_score((scanner_view or {}).get("geometry_score")) * 0.2
    score += _safe_score((scanner_view or {}).get("indicator_score")) * 0.2
    score += _risk_reward_to_tp2_tp3(setup) * 10.0
    tf_ind = (indicators_by_tf or {}).get(tf, {}) if isinstance(indicators_by_tf, dict) else {}
    if str(tf_ind.get("adx_state", "")).upper() in {"STRONG", "TRENDING", "HIGH"}:
        score += 6.0
    if str(tf_ind.get("sqzmom_state", "")).upper() in {"EXPANSION", "BULLISH", "BEARISH"}:
        score += 4.0
    if setup.get("source_cross"):
        score += 3.0
    return score


def build_plan_compression_views(
    *,
    trade_setups_by_tf: dict[str, dict[str, Any]],
    primary_trade: dict[str, Any],
    fractal_trade_tree: dict[str, Any],
    scanner_view: dict[str, Any] | None,
    indicators_by_tf: dict[str, dict[str, Any]] | None,
    ema_crosses_by_tf: dict[str, list[dict[str, Any]]] | None,
) -> dict[str, Any]:
    best_plan_by_tf: dict[str, dict[str, Any] | None] = {}
    candidate_plans_by_tf: dict[str, list[dict[str, Any]]] = {}
    best_alerts_by_tf: dict[str, dict[str, Any] | None] = {}
    chart_plan_overlays: dict[str, dict[str, Any]] = {}

    macro_direction = _norm_direction(primary_trade.get("direction"))

    for tf in TIMEFRAMES:
        setup = trade_setups_by_tf.get(tf, {}) if isinstance(trade_setups_by_tf, dict) else {}
        if not isinstance(setup, dict) or not setup:
            best_plan_by_tf[tf] = None
            candidate_plans_by_tf[tf] = []
            best_alerts_by_tf[tf] = None
            chart_plan_overlays[tf] = {"entry": None, "stop_loss": None, "tp1": None, "tp2": None, "tp3": None}
            continue

        plan = dict(setup)
        plan["quality_rank_score"] = _plan_quality_score(
            tf=tf,
            setup=plan,
            scanner_view=scanner_view,
            indicators_by_tf=indicators_by_tf,
            macro_direction=macro_direction,
        )
        plan["is_canonical"] = plan["quality_rank_score"] > -9999
        plan["plan_id"] = f"{plan.get('symbol', 'SYMBOL')}:{tf}:{plan.get('status', 'UNKNOWN')}"

        if not plan["is_canonical"]:
            best_plan_by_tf[tf] = None
            candidate_plans_by_tf[tf] = [plan]
            best_alerts_by_tf[tf] = {
                "tf": tf,
                "type": "INSUFFICIENT_STRUCTURE",
                "severity": "warning",
                "reason": plan.get("reason"),
                "time": ((plan.get("source_cross") or {}).get("time")) if isinstance(plan.get("source_cross"), dict) else None,
                "visual_only": True,
            }
            chart_plan_overlays[tf] = {"entry": None, "stop_loss": None, "tp1": None, "tp2": None, "tp3": None}
            continue

        best_plan_by_tf[tf] = plan
        candidate_plans_by_tf[tf] = [plan]
        cross = ((ema_crosses_by_tf or {}).get(tf) or [None])[-1] if isinstance(ema_crosses_by_tf, dict) else None
        best_alerts_by_tf[tf] = {
            "tf": tf,
            "type": "LONG_SETUP_VISUAL" if _norm_direction(plan.get("direction")) == "LONG" else "SHORT_SETUP_VISUAL",
            "severity": "info" if "WEAKENED" not in str(plan.get("status", "")).upper() else "warning",
            "reason": plan.get("reason"),
            "time": (cross or {}).get("time") if isinstance(cross, dict) else None,
            "status": plan.get("status"),
            "direction": plan.get("direction"),
            "visual_only": True,
        }
        chart_plan_overlays[tf] = {
            "entry": plan.get("entry"),
            "stop_loss": plan.get("stop_loss"),
            "tp1": plan.get("tp1"),
            "tp2": plan.get("tp2"),
            "tp3": plan.get("tp3"),
            "status": plan.get("status"),
            "direction": plan.get("direction"),
            "plan_id": plan.get("plan_id"),
        }

    tree_status = str((fractal_trade_tree or {}).get("status", "")).upper()
    if tree_status in {"WAITING_REARM", "MACRO_SETUP_INVALIDATED", "M5_CONTEXT_WEAKENED"}:
        best_alerts_by_tf["15m"] = {
            "tf": "15m",
            "type": tree_status,
            "severity": "warning" if tree_status != "MACRO_SETUP_INVALIDATED" else "invalidated",
            "reason": (fractal_trade_tree or {}).get("reason"),
            "time": (((primary_trade or {}).get("source_cross") or {}).get("time")) if isinstance((primary_trade or {}).get("source_cross"), dict) else None,
            "visual_only": True,
        }

    return {
        "best_plan_by_tf": best_plan_by_tf,
        "candidate_plans_by_tf": candidate_plans_by_tf,
        "best_alerts_by_tf": best_alerts_by_tf,
        "chart_plan_overlays": chart_plan_overlays,
    }
