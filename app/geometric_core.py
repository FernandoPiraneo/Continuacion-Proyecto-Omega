from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.models import Candle


@dataclass(slots=True)
class SwingPoint:
    index: int
    price: float
    kind: Literal["HIGH", "LOW"]
    strength: float = 0.0
    timestamp: int | None = None


@dataclass(slots=True)
class TrendLine:
    start: SwingPoint
    end: SwingPoint
    slope: float
    intercept: float | None
    kind: Literal["SUPPORT", "RESISTANCE", "UNKNOWN"]


@dataclass(slots=True)
class GeometryZone:
    kind: Literal["SUPPORT", "RESISTANCE", "SUPPLY", "DEMAND", "NEUTRAL"]
    low: float
    high: float
    touches: int = 0
    confidence_score: float = 0.0


@dataclass(slots=True)
class GeometryPattern:
    pattern_type: str
    side_bias: Literal["LONG", "SHORT", "NEUTRAL"]
    confidence_score: float
    key_levels: list[float] = field(default_factory=list)
    invalidation_level: float | None = None
    reason: str = ""
    source_timeframe: str | None = None


@dataclass(slots=True)
class GeometryAnalysis:
    bias: Literal["BULLISH", "BEARISH", "NEUTRAL"]
    confidence_score: float
    swing_points: list[SwingPoint] = field(default_factory=list)
    support_line: TrendLine | None = None
    resistance_line: TrendLine | None = None
    zones: list[GeometryZone] = field(default_factory=list)
    patterns: list[GeometryPattern] = field(default_factory=list)
    reason: str = ""


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _pct_distance(base: float, target: float) -> float:
    if base <= 0 or target <= 0:
        return 0.0
    return abs((base - target) / base) * 100.0


def calculate_price_distance_percent(price_a: float, price_b: float) -> float:
    return _pct_distance(price_a, price_b)


def calculate_atr(candles: list[Candle], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    period = max(2, int(period))
    start_index = max(1, len(candles) - period)
    true_ranges: list[float] = []
    for idx in range(start_index, len(candles)):
        current = candles[idx]
        prev_close = candles[idx - 1].close
        tr = max(
            current.high - current.low,
            abs(current.high - prev_close),
            abs(current.low - prev_close),
        )
        true_ranges.append(max(0.0, tr))
    if not true_ranges:
        return 0.0
    return sum(true_ranges) / len(true_ranges)


def calculate_dynamic_tolerance_pct(candles: list[Candle], fallback_pct: float = 0.5) -> float:
    if not candles:
        return _clamp(float(fallback_pct), 0.15, 1.25)
    current_price = candles[-1].close
    if current_price <= 0:
        return _clamp(float(fallback_pct), 0.15, 1.25)
    atr = calculate_atr(candles, period=14)
    if atr <= 0:
        return _clamp(float(fallback_pct), 0.15, 1.25)
    tolerance_pct = (atr / current_price) * 100.0 * 0.75
    return _clamp(tolerance_pct, 0.15, 1.25)


def _is_similar(price_a: float, price_b: float, tolerance_pct: float = 0.5) -> bool:
    return _pct_distance(price_a, price_b) <= tolerance_pct


def detect_swing_points(candles: list[Candle], left: int = 2, right: int = 2) -> list[SwingPoint]:
    if not candles:
        return []
    if len(candles) < (left + right + 1):
        return []

    points: list[SwingPoint] = []

    for i in range(left, len(candles) - right):
        current = candles[i]
        left_window = candles[i - left : i]
        right_window = candles[i + 1 : i + 1 + right]
        window = left_window + right_window

        if not window:
            continue

        max_high = max(c.high for c in window)
        min_low = min(c.low for c in window)
        local_range = max(max_high - min_low, 0.0)
        denom = max(abs(current.close), 1e-9)

        if all(current.high > c.high for c in window):
            prominence_pct = ((current.high - max_high) / denom) * 100.0
            forward_min_low = min(c.low for c in right_window) if right_window else current.low
            reaction_pct = ((current.high - forward_min_low) / denom) * 100.0
            range_pct = (local_range / denom) * 100.0
            strength = _clamp((prominence_pct * 20.0) + (reaction_pct * 20.0) + (range_pct * 10.0), 0.0, 100.0)
            points.append(
                SwingPoint(
                    index=i,
                    price=current.high,
                    kind="HIGH",
                    strength=strength,
                    timestamp=current.open_time,
                )
            )

        if all(current.low < c.low for c in window):
            prominence_pct = ((min_low - current.low) / denom) * 100.0
            forward_max_high = max(c.high for c in right_window) if right_window else current.high
            reaction_pct = ((forward_max_high - current.low) / denom) * 100.0
            range_pct = (local_range / denom) * 100.0
            strength = _clamp((prominence_pct * 20.0) + (reaction_pct * 20.0) + (range_pct * 10.0), 0.0, 100.0)
            points.append(
                SwingPoint(
                    index=i,
                    price=current.low,
                    kind="LOW",
                    strength=strength,
                    timestamp=current.open_time,
                )
            )

    return points


def calculate_trendline(point_a: SwingPoint, point_b: SwingPoint) -> TrendLine:
    if point_a.index == point_b.index:
        return TrendLine(start=point_a, end=point_b, slope=0.0, intercept=point_a.price, kind="UNKNOWN")

    dx = float(point_b.index - point_a.index)
    dy = point_b.price - point_a.price
    slope = dy / dx
    intercept = point_a.price - (slope * point_a.index)

    kind: Literal["SUPPORT", "RESISTANCE", "UNKNOWN"] = "UNKNOWN"
    if point_a.kind == "LOW" and point_b.kind == "LOW":
        kind = "SUPPORT"
    elif point_a.kind == "HIGH" and point_b.kind == "HIGH":
        kind = "RESISTANCE"

    return TrendLine(start=point_a, end=point_b, slope=slope, intercept=intercept, kind=kind)


def trendline_price_at(line: TrendLine | None, index: int) -> float | None:
    if line is None or line.intercept is None:
        return None
    return (line.slope * float(index)) + float(line.intercept)


def distance_to_trendline_pct(price: float, line: TrendLine | None, index: int) -> float:
    line_price = trendline_price_at(line, index)
    if line_price is None or line_price <= 0 or price <= 0:
        return float("inf")
    return _pct_distance(price, line_price)


def is_price_near_trendline(price: float, line: TrendLine | None, index: int, tolerance_pct: float) -> bool:
    return distance_to_trendline_pct(price, line, index) <= max(0.0, tolerance_pct)


def estimate_support_resistance_lines(swing_points: list[SwingPoint]) -> tuple[TrendLine | None, TrendLine | None]:
    lows = [p for p in swing_points if p.kind == "LOW"]
    highs = [p for p in swing_points if p.kind == "HIGH"]

    support = calculate_trendline(lows[-2], lows[-1]) if len(lows) >= 2 else None
    resistance = calculate_trendline(highs[-2], highs[-1]) if len(highs) >= 2 else None
    return support, resistance


def _neckline_between(points: list[SwingPoint], start_index: int, end_index: int, mode: Literal["min", "max"]) -> float | None:
    between = [p.price for p in points if start_index < p.index < end_index]
    if not between:
        return None
    return min(between) if mode == "min" else max(between)


def detect_double_top(
    highs: list[SwingPoint],
    lows: list[SwingPoint] | None = None,
    tolerance_pct: float = 0.5,
    current_price: float | None = None,
) -> list[GeometryPattern]:
    if len(highs) < 2:
        return []
    h1, h2 = highs[-2], highs[-1]
    if not _is_similar(h1.price, h2.price, tolerance_pct) or (h2.index - h1.index) <= 2:
        return []
    neckline = _neckline_between(lows or [], h1.index, h2.index, mode="min")
    is_confirmed = bool(
        neckline is not None
        and current_price is not None
        and current_price < neckline * (1.0 - (tolerance_pct / 100.0) * 0.25)
    )
    return [
        GeometryPattern(
            pattern_type="DOUBLE_TOP" if is_confirmed else "POTENTIAL_DOUBLE_TOP",
            side_bias="SHORT",
            confidence_score=72.0 if is_confirmed else 58.0,
            key_levels=[h1.price, h2.price] + ([neckline] if neckline is not None else []),
            invalidation_level=max(h1.price, h2.price) * 1.005,
            reason=(
                "Doble techo confirmado por ruptura de neckline"
                if is_confirmed
                else "Posible doble techo sin ruptura confirmada"
            ),
        )
    ]


def detect_double_bottom(
    lows: list[SwingPoint],
    highs: list[SwingPoint] | None = None,
    tolerance_pct: float = 0.5,
    current_price: float | None = None,
) -> list[GeometryPattern]:
    if len(lows) < 2:
        return []
    l1, l2 = lows[-2], lows[-1]
    if not _is_similar(l1.price, l2.price, tolerance_pct) or (l2.index - l1.index) <= 2:
        return []
    neckline = _neckline_between(highs or [], l1.index, l2.index, mode="max")
    is_confirmed = bool(
        neckline is not None
        and current_price is not None
        and current_price > neckline * (1.0 + (tolerance_pct / 100.0) * 0.25)
    )
    return [
        GeometryPattern(
            pattern_type="DOUBLE_BOTTOM" if is_confirmed else "POTENTIAL_DOUBLE_BOTTOM",
            side_bias="LONG",
            confidence_score=72.0 if is_confirmed else 58.0,
            key_levels=[l1.price, l2.price] + ([neckline] if neckline is not None else []),
            invalidation_level=min(l1.price, l2.price) * 0.995,
            reason=(
                "Doble suelo confirmado por ruptura de neckline"
                if is_confirmed
                else "Posible doble suelo sin ruptura confirmada"
            ),
        )
    ]


def detect_triple_top(
    highs: list[SwingPoint],
    lows: list[SwingPoint] | None = None,
    tolerance_pct: float = 0.5,
    current_price: float | None = None,
) -> list[GeometryPattern]:
    if len(highs) < 3:
        return []
    h1, h2, h3 = highs[-3], highs[-2], highs[-1]
    if not (_is_similar(h1.price, h2.price, tolerance_pct) and _is_similar(h2.price, h3.price, tolerance_pct)):
        return []
    if (h3.index - h2.index) <= 2 or (h2.index - h1.index) <= 2:
        return []
    neckline = _neckline_between(lows or [], h1.index, h3.index, mode="min")
    is_confirmed = bool(
        neckline is not None
        and current_price is not None
        and current_price < neckline * (1.0 - (tolerance_pct / 100.0) * 0.25)
    )
    return [
        GeometryPattern(
            pattern_type="TRIPLE_TOP" if is_confirmed else "POTENTIAL_TRIPLE_TOP",
            side_bias="SHORT",
            confidence_score=80.0 if is_confirmed else 62.0,
            key_levels=[h1.price, h2.price, h3.price] + ([neckline] if neckline is not None else []),
            invalidation_level=max(h1.price, h2.price, h3.price) * 1.005,
            reason=(
                "Triple techo confirmado por cierre bajo neckline"
                if is_confirmed
                else "Triple techo potencial sin confirmación"
            ),
        )
    ]


def detect_triple_bottom(
    lows: list[SwingPoint],
    highs: list[SwingPoint] | None = None,
    tolerance_pct: float = 0.5,
    current_price: float | None = None,
) -> list[GeometryPattern]:
    if len(lows) < 3:
        return []
    l1, l2, l3 = lows[-3], lows[-2], lows[-1]
    if not (_is_similar(l1.price, l2.price, tolerance_pct) and _is_similar(l2.price, l3.price, tolerance_pct)):
        return []
    if (l3.index - l2.index) <= 2 or (l2.index - l1.index) <= 2:
        return []
    neckline = _neckline_between(highs or [], l1.index, l3.index, mode="max")
    is_confirmed = bool(
        neckline is not None
        and current_price is not None
        and current_price > neckline * (1.0 + (tolerance_pct / 100.0) * 0.25)
    )
    return [
        GeometryPattern(
            pattern_type="TRIPLE_BOTTOM" if is_confirmed else "POTENTIAL_TRIPLE_BOTTOM",
            side_bias="LONG",
            confidence_score=80.0 if is_confirmed else 62.0,
            key_levels=[l1.price, l2.price, l3.price] + ([neckline] if neckline is not None else []),
            invalidation_level=min(l1.price, l2.price, l3.price) * 0.995,
            reason=(
                "Triple suelo confirmado por cierre sobre neckline"
                if is_confirmed
                else "Triple suelo potencial sin confirmación"
            ),
        )
    ]


def detect_head_and_shoulders(
    highs: list[SwingPoint],
    lows: list[SwingPoint],
    tolerance_pct: float = 1.5,
    current_price: float | None = None,
) -> list[GeometryPattern]:
    if len(highs) < 3 or len(lows) < 2:
        return []
    h1, h2, h3 = highs[-3], highs[-2], highs[-1]
    if not (h2.price > h1.price and h2.price > h3.price):
        return []
    if not _is_similar(h1.price, h3.price, tolerance_pct):
        return []

    neckline_lows = [l for l in lows if h1.index < l.index < h3.index]
    neckline = sum(l.price for l in neckline_lows) / len(neckline_lows) if neckline_lows else None
    is_confirmed = bool(
        neckline is not None
        and current_price is not None
        and current_price < neckline * (1.0 - (tolerance_pct / 100.0) * 0.2)
    )
    return [
        GeometryPattern(
            pattern_type="HEAD_AND_SHOULDERS" if is_confirmed else "POTENTIAL_HEAD_AND_SHOULDERS",
            side_bias="SHORT",
            confidence_score=78.0 if is_confirmed else 60.0,
            key_levels=[h1.price, h2.price, h3.price] + ([neckline] if neckline else []),
            invalidation_level=h2.price * 1.005,
            reason=(
                "HCH confirmado por ruptura de neckline"
                if is_confirmed
                else "HCH potencial sin ruptura confirmada"
            ),
        )
    ]


def detect_inverse_head_and_shoulders(
    highs: list[SwingPoint],
    lows: list[SwingPoint],
    tolerance_pct: float = 1.5,
    current_price: float | None = None,
) -> list[GeometryPattern]:
    if len(lows) < 3 or len(highs) < 2:
        return []
    l1, l2, l3 = lows[-3], lows[-2], lows[-1]
    if not (l2.price < l1.price and l2.price < l3.price):
        return []
    if not _is_similar(l1.price, l3.price, tolerance_pct):
        return []

    neckline_highs = [h for h in highs if l1.index < h.index < l3.index]
    neckline = sum(h.price for h in neckline_highs) / len(neckline_highs) if neckline_highs else None
    is_confirmed = bool(
        neckline is not None
        and current_price is not None
        and current_price > neckline * (1.0 + (tolerance_pct / 100.0) * 0.2)
    )
    return [
        GeometryPattern(
            pattern_type=(
                "INVERSE_HEAD_AND_SHOULDERS"
                if is_confirmed
                else "POTENTIAL_INVERSE_HEAD_AND_SHOULDERS"
            ),
            side_bias="LONG",
            confidence_score=78.0 if is_confirmed else 60.0,
            key_levels=[l1.price, l2.price, l3.price] + ([neckline] if neckline else []),
            invalidation_level=l2.price * 0.995,
            reason=(
                "HCH invertido confirmado por ruptura de neckline"
                if is_confirmed
                else "HCH invertido potencial sin ruptura confirmada"
            ),
        )
    ]


def detect_ascending_triangle(support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if not (support and resistance):
        return []
    avg_price = (support.start.price + resistance.start.price) / 2
    rel_res_slope = resistance.slope / avg_price if avg_price > 0 else 0
    rel_supp_slope = support.slope / avg_price if avg_price > 0 else 0

    if abs(rel_res_slope) < 0.0005 and rel_supp_slope > 0.0005:
        return [
            GeometryPattern(
                pattern_type="ASCENDING_TRIANGLE",
                side_bias="LONG",
                confidence_score=60.0,
                key_levels=[resistance.end.price],
                invalidation_level=support.end.price * 0.99,
                reason="Resistencia plana y mínimos ascendentes",
            )
        ]
    return []


def detect_descending_triangle(support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if not (support and resistance):
        return []
    avg_price = (support.start.price + resistance.start.price) / 2
    rel_res_slope = resistance.slope / avg_price if avg_price > 0 else 0
    rel_supp_slope = support.slope / avg_price if avg_price > 0 else 0

    if abs(rel_supp_slope) < 0.0005 and rel_res_slope < -0.0005:
        return [
            GeometryPattern(
                pattern_type="DESCENDING_TRIANGLE",
                side_bias="SHORT",
                confidence_score=60.0,
                key_levels=[support.end.price],
                invalidation_level=resistance.end.price * 1.01,
                reason="Soporte plano y máximos descendentes",
            )
        ]
    return []


def detect_symmetrical_triangle(support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if not (support and resistance):
        return []
    avg_price = (support.start.price + resistance.start.price) / 2
    rel_res_slope = resistance.slope / avg_price if avg_price > 0 else 0
    rel_supp_slope = support.slope / avg_price if avg_price > 0 else 0

    if rel_supp_slope > 0.0005 and rel_res_slope < -0.0005:
        return [
            GeometryPattern(
                pattern_type="SYMMETRICAL_TRIANGLE",
                side_bias="NEUTRAL",
                confidence_score=56.0,
                key_levels=[support.end.price, resistance.end.price],
                invalidation_level=None,
                reason="Compresión: máximos descendentes y mínimos ascendentes",
            )
        ]
    return []


def detect_clean_pullback(
    current_price: float,
    bias: str,
    support: TrendLine | None,
    resistance: TrendLine | None,
    current_index: int | None = None,
    tolerance_pct: float = 0.5,
) -> list[GeometryPattern]:
    idx = current_index if current_index is not None else max(
        support.end.index if support is not None else 0,
        resistance.end.index if resistance is not None else 0,
    )
    if bias == "BULLISH" and support:
        line_price_now = trendline_price_at(support, idx)
        if line_price_now is not None and is_price_near_trendline(current_price, support, idx, tolerance_pct):
            return [
                GeometryPattern(
                    pattern_type="CLEAN_PULLBACK",
                    side_bias="LONG",
                    confidence_score=58.0,
                    key_levels=[line_price_now],
                    invalidation_level=line_price_now * 0.99,
                    reason="Retroceso a soporte proyectado en tendencia alcista",
                )
            ]
    if bias == "BEARISH" and resistance:
        line_price_now = trendline_price_at(resistance, idx)
        if line_price_now is not None and is_price_near_trendline(current_price, resistance, idx, tolerance_pct):
            return [
                GeometryPattern(
                    pattern_type="CLEAN_PULLBACK",
                    side_bias="SHORT",
                    confidence_score=58.0,
                    key_levels=[line_price_now],
                    invalidation_level=line_price_now * 1.01,
                    reason="Retroceso a resistencia proyectada en tendencia bajista",
                )
            ]
    return []


def _cluster_points(points: list[SwingPoint], tolerance_pct: float) -> list[list[SwingPoint]]:
    if not points:
        return []
    sorted_points = sorted(points, key=lambda p: p.price)
    clusters: list[list[SwingPoint]] = []
    for point in sorted_points:
        added = False
        for cluster in clusters:
            center = sum(p.price for p in cluster) / len(cluster)
            if _is_similar(point.price, center, tolerance_pct):
                cluster.append(point)
                added = True
                break
        if not added:
            clusters.append([point])
    return clusters


def build_geometry_zones(
    swing_points: list[SwingPoint],
    candles: list[Candle],
    tolerance_pct: float,
) -> list[GeometryZone]:
    highs = [p for p in swing_points if p.kind == "HIGH"]
    lows = [p for p in swing_points if p.kind == "LOW"]
    last_price = candles[-1].close if candles else 0.0

    zones: list[GeometryZone] = []
    for kind, points in (("RESISTANCE", highs), ("SUPPORT", lows)):
        for cluster in _cluster_points(points, tolerance_pct):
            touches = len(cluster)
            if touches < 2:
                continue
            low_price = min(p.price for p in cluster)
            high_price = max(p.price for p in cluster)
            zone_mid = (low_price + high_price) / 2 if (low_price + high_price) > 0 else 0.0
            width_pct = _pct_distance(zone_mid, high_price) if zone_mid > 0 else 0.0
            avg_strength = sum(p.strength for p in cluster) / touches
            confidence = _clamp((touches * 18.0) + (avg_strength * 0.25) - (width_pct * 16.0), 0.0, 100.0)

            # penalización si la zona está claramente rota por el último precio
            if kind == "SUPPORT" and last_price > 0 and last_price < low_price * (1.0 - tolerance_pct / 100.0):
                confidence *= 0.7
            if kind == "RESISTANCE" and last_price > 0 and last_price > high_price * (1.0 + tolerance_pct / 100.0):
                confidence *= 0.7

            zones.append(
                GeometryZone(
                    kind=kind,
                    low=low_price,
                    high=high_price,
                    touches=touches,
                    confidence_score=_clamp(confidence, 0.0, 100.0),
                )
            )

    zones.sort(key=lambda z: z.confidence_score, reverse=True)
    return zones[:8]


def detect_geometry_patterns(
    highs: list[SwingPoint],
    lows: list[SwingPoint],
    support: TrendLine | None,
    resistance: TrendLine | None,
    current_price: float,
    bias: str,
    current_index: int,
    tolerance_pct: float,
) -> list[GeometryPattern]:
    patterns: list[GeometryPattern] = []

    t_top = detect_triple_top(highs, lows=lows, tolerance_pct=tolerance_pct, current_price=current_price)
    patterns.extend(t_top or detect_double_top(highs, lows=lows, tolerance_pct=tolerance_pct, current_price=current_price))

    t_bottom = detect_triple_bottom(lows, highs=highs, tolerance_pct=tolerance_pct, current_price=current_price)
    patterns.extend(
        t_bottom or detect_double_bottom(lows, highs=highs, tolerance_pct=tolerance_pct, current_price=current_price)
    )

    patterns.extend(
        detect_head_and_shoulders(
            highs,
            lows,
            tolerance_pct=max(1.0, tolerance_pct * 2.5),
            current_price=current_price,
        )
    )
    patterns.extend(
        detect_inverse_head_and_shoulders(
            highs,
            lows,
            tolerance_pct=max(1.0, tolerance_pct * 2.5),
            current_price=current_price,
        )
    )

    patterns.extend(detect_ascending_triangle(support, resistance))
    patterns.extend(detect_descending_triangle(support, resistance))
    patterns.extend(detect_symmetrical_triangle(support, resistance))

    patterns.extend(
        detect_clean_pullback(
            current_price=current_price,
            bias=bias,
            support=support,
            resistance=resistance,
            current_index=current_index,
            tolerance_pct=tolerance_pct,
        )
    )

    return patterns


def score_geometry_quality(
    bias: Literal["BULLISH", "BEARISH", "NEUTRAL"],
    swing_points: list[SwingPoint],
    support: TrendLine | None,
    resistance: TrendLine | None,
    zones: list[GeometryZone] | None = None,
    patterns: list[GeometryPattern] | None = None,
    current_price: float | None = None,
) -> float:
    if len(swing_points) < 2:
        return _clamp(len(swing_points) * 4.0, 0.0, 15.0)

    zones = zones or []
    patterns = patterns or []

    score = 0.0

    pivots_component = min(12, len(swing_points)) / 12.0
    avg_strength = sum(p.strength for p in swing_points) / max(1, len(swing_points))
    score += (pivots_component * 15.0) + (avg_strength / 100.0 * 10.0)

    score += 20.0 if bias in {"BULLISH", "BEARISH"} else 6.0

    line_score = 0.0
    if support is not None:
        line_score += 10.0
        if support.end.index > support.start.index:
            line_score += 2.0
    if resistance is not None:
        line_score += 10.0
        if resistance.end.index > resistance.start.index:
            line_score += 2.0
    score += min(20.0, line_score)

    if zones:
        top_zones = sorted(zones, key=lambda z: z.confidence_score, reverse=True)[:2]
        score += min(20.0, sum(z.confidence_score for z in top_zones) / 10.0)

    confirmed = [p for p in patterns if not p.pattern_type.startswith("POTENTIAL_")]
    potential = [p for p in patterns if p.pattern_type.startswith("POTENTIAL_")]
    score += min(15.0, len(confirmed) * 7.0 + len(potential) * 3.0)

    if len(swing_points) < 4:
        score -= 8.0
    if bias == "NEUTRAL":
        score -= 6.0
    if support is None and resistance is None:
        score -= 8.0
    if not confirmed and potential:
        score -= 4.0

    if current_price is not None and current_price > 0 and (support or resistance):
        near_support = distance_to_trendline_pct(current_price, support, swing_points[-1].index) if support else float("inf")
        near_resistance = (
            distance_to_trendline_pct(current_price, resistance, swing_points[-1].index)
            if resistance
            else float("inf")
        )
        if min(near_support, near_resistance) > 3.0:
            score -= 6.0

    return _clamp(score, 0.0, 100.0)


def _build_geometry_reason(
    bias: Literal["BULLISH", "BEARISH", "NEUTRAL"],
    support: TrendLine | None,
    resistance: TrendLine | None,
    zones: list[GeometryZone],
    patterns: list[GeometryPattern],
) -> str:
    confirmed = [p for p in patterns if not p.pattern_type.startswith("POTENTIAL_")]
    potential = [p for p in patterns if p.pattern_type.startswith("POTENTIAL_")]
    top_zone = zones[0] if zones else None

    if bias == "BULLISH":
        base = "Bullish: HH/HL activo"
        if support is not None:
            base += ", soporte ascendente proyectado"
    elif bias == "BEARISH":
        base = "Bearish: LH/LL activo"
        if resistance is not None:
            base += ", resistencia descendente proyectada"
    else:
        base = "Estructura ambigua: pivotes mixtos"

    if top_zone is not None:
        zone_kind = "demanda" if top_zone.kind == "SUPPORT" else "oferta"
        base += f", zona dominante de {zone_kind} ({top_zone.touches} toques)"

    if confirmed:
        base += f", patrón confirmado: {confirmed[0].pattern_type}"
    elif potential:
        base += f", patrón potencial: {potential[0].pattern_type}"

    return base


def analyze_geometry(candles: list[Candle], source_timeframe: str | None = None) -> GeometryAnalysis:
    if not candles or len(candles) < 5:
        return GeometryAnalysis(bias="NEUTRAL", confidence_score=0.0, reason="Sin velas suficientes")

    points = detect_swing_points(candles, left=2, right=2)
    if len(points) < 2:
        return GeometryAnalysis(bias="NEUTRAL", confidence_score=10.0, reason="Sin pivotes suficientes")

    lows = [p for p in points if p.kind == "LOW"]
    highs = [p for p in points if p.kind == "HIGH"]

    bias: Literal["BULLISH", "BEARISH", "NEUTRAL"] = "NEUTRAL"
    has_higher_highs = len(highs) >= 2 and highs[-1].price > highs[-2].price
    has_higher_lows = len(lows) >= 2 and lows[-1].price > lows[-2].price
    has_lower_highs = len(highs) >= 2 and highs[-1].price < highs[-2].price
    has_lower_lows = len(lows) >= 2 and lows[-1].price < lows[-2].price

    if has_higher_highs and has_higher_lows:
        bias = "BULLISH"
    elif has_lower_highs and has_lower_lows:
        bias = "BEARISH"

    support, resistance = estimate_support_resistance_lines(points)
    current_price = candles[-1].close
    current_index = len(candles) - 1
    tolerance_pct = calculate_dynamic_tolerance_pct(candles, fallback_pct=0.5)

    zones = build_geometry_zones(points, candles, tolerance_pct)
    detected_patterns = detect_geometry_patterns(
        highs=highs,
        lows=lows,
        support=support,
        resistance=resistance,
        current_price=current_price,
        bias=bias,
        current_index=current_index,
        tolerance_pct=tolerance_pct,
    )

    for pattern in detected_patterns:
        if source_timeframe:
            pattern.source_timeframe = source_timeframe

    score = score_geometry_quality(
        bias,
        points,
        support,
        resistance,
        zones=zones,
        patterns=detected_patterns,
        current_price=current_price,
    )
    reason = _build_geometry_reason(bias, support, resistance, zones, detected_patterns)

    return GeometryAnalysis(
        bias=bias,
        confidence_score=score,
        swing_points=points,
        support_line=support,
        resistance_line=resistance,
        zones=zones,
        patterns=detected_patterns,
        reason=reason,
    )
