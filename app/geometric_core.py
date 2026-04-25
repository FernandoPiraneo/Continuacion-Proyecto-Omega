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

        if all(current.high > c.high for c in window):
            points.append(
                SwingPoint(
                    index=i,
                    price=current.high,
                    kind="HIGH",
                    strength=1.0,
                    timestamp=current.open_time
                )
            )
        if all(current.low < c.low for c in window):
            points.append(
                SwingPoint(
                    index=i,
                    price=current.low,
                    kind="LOW",
                    strength=1.0,
                    timestamp=current.open_time
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


def estimate_support_resistance_lines(swing_points: list[SwingPoint]) -> tuple[TrendLine | None, TrendLine | None]:
    lows = [p for p in swing_points if p.kind == "LOW"]
    highs = [p for p in swing_points if p.kind == "HIGH"]

    support = None
    resistance = None

    if len(lows) >= 2:
        support = calculate_trendline(lows[-2], lows[-1])
    if len(highs) >= 2:
        resistance = calculate_trendline(highs[-2], highs[-1])

    return support, resistance


def calculate_price_distance_percent(price_a: float, price_b: float) -> float:
    if price_a <= 0 or price_b <= 0:
        return 0.0
    return abs((price_a - price_b) / price_a) * 100.0


def score_geometry_quality(
    bias: Literal["BULLISH", "BEARISH", "NEUTRAL"],
    swing_points: list[SwingPoint],
    support: TrendLine | None,
    resistance: TrendLine | None
) -> float:
    score = 0.0
    if len(swing_points) < 4:
        return max(0.0, min(20.0, len(swing_points) * 5.0))
    
    score += 40.0
    
    if bias != "NEUTRAL":
        score += 20.0
    
    if support is not None:
        score += 20.0
        
    if resistance is not None:
        score += 20.0
        
    return max(0.0, min(100.0, score))


def _is_similar(price_a: float, price_b: float, tolerance_pct: float = 0.5) -> bool:
    if price_a <= 0: return False
    return abs(price_a - price_b) / price_a * 100 <= tolerance_pct


def detect_double_top(highs: list[SwingPoint]) -> list[GeometryPattern]:
    if len(highs) < 2: return []
    h1, h2 = highs[-2], highs[-1]
    if _is_similar(h1.price, h2.price, 0.5):
        if h2.index - h1.index > 2:
            return [GeometryPattern(
                pattern_type="DOUBLE_TOP",
                side_bias="SHORT",
                confidence_score=70.0,
                key_levels=[h1.price, h2.price],
                invalidation_level=max(h1.price, h2.price) * 1.005,
                reason="Dos máximos consecutivos al mismo nivel (resistencia)"
            )]
    return []


def detect_double_bottom(lows: list[SwingPoint]) -> list[GeometryPattern]:
    if len(lows) < 2: return []
    l1, l2 = lows[-2], lows[-1]
    if _is_similar(l1.price, l2.price, 0.5):
        if l2.index - l1.index > 2:
            return [GeometryPattern(
                pattern_type="DOUBLE_BOTTOM",
                side_bias="LONG",
                confidence_score=70.0,
                key_levels=[l1.price, l2.price],
                invalidation_level=min(l1.price, l2.price) * 0.995,
                reason="Dos mínimos consecutivos al mismo nivel (soporte)"
            )]
    return []


def detect_triple_top(highs: list[SwingPoint]) -> list[GeometryPattern]:
    if len(highs) < 3: return []
    h1, h2, h3 = highs[-3], highs[-2], highs[-1]
    if _is_similar(h1.price, h2.price, 0.5) and _is_similar(h2.price, h3.price, 0.5):
        if h3.index - h2.index > 2 and h2.index - h1.index > 2:
            return [GeometryPattern(
                pattern_type="TRIPLE_TOP",
                side_bias="SHORT",
                confidence_score=80.0,
                key_levels=[h1.price, h2.price, h3.price],
                invalidation_level=max(h1.price, h2.price, h3.price) * 1.005,
                reason="Tres máximos consecutivos en nivel similar"
            )]
    return []


def detect_triple_bottom(lows: list[SwingPoint]) -> list[GeometryPattern]:
    if len(lows) < 3: return []
    l1, l2, l3 = lows[-3], lows[-2], lows[-1]
    if _is_similar(l1.price, l2.price, 0.5) and _is_similar(l2.price, l3.price, 0.5):
        if l3.index - l2.index > 2 and l2.index - l1.index > 2:
            return [GeometryPattern(
                pattern_type="TRIPLE_BOTTOM",
                side_bias="LONG",
                confidence_score=80.0,
                key_levels=[l1.price, l2.price, l3.price],
                invalidation_level=min(l1.price, l2.price, l3.price) * 0.995,
                reason="Tres mínimos consecutivos en nivel similar"
            )]
    return []


def detect_head_and_shoulders(highs: list[SwingPoint], lows: list[SwingPoint]) -> list[GeometryPattern]:
    if len(highs) < 3 or len(lows) < 2: return []
    h1, h2, h3 = highs[-3], highs[-2], highs[-1]
    
    if h2.price > h1.price and h2.price > h3.price:
        if _is_similar(h1.price, h3.price, 1.5):
            neckline_lows = [l for l in lows if h1.index < l.index < h3.index]
            neckline = sum(l.price for l in neckline_lows) / len(neckline_lows) if neckline_lows else None
            
            return [GeometryPattern(
                pattern_type="HEAD_AND_SHOULDERS",
                side_bias="SHORT",
                confidence_score=75.0,
                key_levels=[h1.price, h2.price, h3.price] + ([neckline] if neckline else []),
                invalidation_level=h2.price * 1.005,
                reason="HCH: Cabeza alta y hombros alineados"
            )]
    return []


def detect_inverse_head_and_shoulders(highs: list[SwingPoint], lows: list[SwingPoint]) -> list[GeometryPattern]:
    if len(lows) < 3 or len(highs) < 2: return []
    l1, l2, l3 = lows[-3], lows[-2], lows[-1]
    
    if l2.price < l1.price and l2.price < l3.price:
        if _is_similar(l1.price, l3.price, 1.5):
            neckline_highs = [h for h in highs if l1.index < h.index < l3.index]
            neckline = sum(h.price for h in neckline_highs) / len(neckline_highs) if neckline_highs else None
            
            return [GeometryPattern(
                pattern_type="INVERSE_HEAD_AND_SHOULDERS",
                side_bias="LONG",
                confidence_score=75.0,
                key_levels=[l1.price, l2.price, l3.price] + ([neckline] if neckline else []),
                invalidation_level=l2.price * 0.995,
                reason="HCH invertido: Cabeza profunda y hombros alineados"
            )]
    return []


def detect_ascending_triangle(support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if not (support and resistance): return []
    avg_price = (support.start.price + resistance.start.price) / 2
    rel_res_slope = resistance.slope / avg_price if avg_price > 0 else 0
    rel_supp_slope = support.slope / avg_price if avg_price > 0 else 0
    
    is_res_flat = abs(rel_res_slope) < 0.0005
    is_supp_asc = rel_supp_slope > 0.0005
    
    if is_res_flat and is_supp_asc:
        return [GeometryPattern(
            pattern_type="ASCENDING_TRIANGLE",
            side_bias="LONG",
            confidence_score=65.0,
            key_levels=[resistance.end.price],
            invalidation_level=support.end.price * 0.99,
            reason="Resistencia plana y mínimos ascendentes"
        )]
    return []


def detect_descending_triangle(support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if not (support and resistance): return []
    avg_price = (support.start.price + resistance.start.price) / 2
    rel_res_slope = resistance.slope / avg_price if avg_price > 0 else 0
    rel_supp_slope = support.slope / avg_price if avg_price > 0 else 0
    
    is_supp_flat = abs(rel_supp_slope) < 0.0005
    is_res_desc = rel_res_slope < -0.0005
    
    if is_supp_flat and is_res_desc:
        return [GeometryPattern(
            pattern_type="DESCENDING_TRIANGLE",
            side_bias="SHORT",
            confidence_score=65.0,
            key_levels=[support.end.price],
            invalidation_level=resistance.end.price * 1.01,
            reason="Soporte plano y máximos descendentes"
        )]
    return []


def detect_symmetrical_triangle(support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if not (support and resistance): return []
    avg_price = (support.start.price + resistance.start.price) / 2
    rel_res_slope = resistance.slope / avg_price if avg_price > 0 else 0
    rel_supp_slope = support.slope / avg_price if avg_price > 0 else 0
    
    is_supp_asc = rel_supp_slope > 0.0005
    is_res_desc = rel_res_slope < -0.0005
    
    if is_supp_asc and is_res_desc:
        return [GeometryPattern(
            pattern_type="SYMMETRICAL_TRIANGLE",
            side_bias="NEUTRAL",
            confidence_score=60.0,
            key_levels=[support.end.price, resistance.end.price],
            invalidation_level=None,
            reason="Compresión: máximos descendentes y mínimos ascendentes"
        )]
    return []


def detect_clean_pullback(current_price: float, bias: str, support: TrendLine | None, resistance: TrendLine | None) -> list[GeometryPattern]:
    if bias == "BULLISH" and support:
        if _is_similar(current_price, support.end.price, 0.5):
            return [GeometryPattern(
                pattern_type="CLEAN_PULLBACK",
                side_bias="LONG",
                confidence_score=60.0,
                key_levels=[support.end.price],
                invalidation_level=support.end.price * 0.99,
                reason="Retroceso a soporte en tendencia alcista"
            )]
    if bias == "BEARISH" and resistance:
        if _is_similar(current_price, resistance.end.price, 0.5):
            return [GeometryPattern(
                pattern_type="CLEAN_PULLBACK",
                side_bias="SHORT",
                confidence_score=60.0,
                key_levels=[resistance.end.price],
                invalidation_level=resistance.end.price * 1.01,
                reason="Retroceso a resistencia en tendencia bajista"
            )]
    return []


def detect_geometry_patterns(highs: list[SwingPoint], lows: list[SwingPoint], support: TrendLine | None, resistance: TrendLine | None, current_price: float, bias: str) -> list[GeometryPattern]:
    patterns = []
    
    t_top = detect_triple_top(highs)
    if t_top:
        patterns.extend(t_top)
    else:
        patterns.extend(detect_double_top(highs))
        
    t_bottom = detect_triple_bottom(lows)
    if t_bottom:
        patterns.extend(t_bottom)
    else:
        patterns.extend(detect_double_bottom(lows))
        
    patterns.extend(detect_head_and_shoulders(highs, lows))
    patterns.extend(detect_inverse_head_and_shoulders(highs, lows))
    
    patterns.extend(detect_ascending_triangle(support, resistance))
    patterns.extend(detect_descending_triangle(support, resistance))
    patterns.extend(detect_symmetrical_triangle(support, resistance))
    
    patterns.extend(detect_clean_pullback(current_price, bias, support, resistance))
    
    return patterns


def analyze_geometry(candles: list[Candle], source_timeframe: str | None = None) -> GeometryAnalysis:
    if not candles or len(candles) < 5:
        return GeometryAnalysis(bias="NEUTRAL", confidence_score=0.0, reason="Sin velas suficientes")

    points = detect_swing_points(candles, left=2, right=2)
    if len(points) < 2:
        return GeometryAnalysis(bias="NEUTRAL", confidence_score=10.0, reason="Sin pivotes suficientes")

    lows = [p for p in points if p.kind == "LOW"]
    highs = [p for p in points if p.kind == "HIGH"]

    bias: Literal["BULLISH", "BEARISH", "NEUTRAL"] = "NEUTRAL"
    reason = "Estructura lateral o ambigua"

    has_higher_highs = len(highs) >= 2 and highs[-1].price > highs[-2].price
    has_higher_lows = len(lows) >= 2 and lows[-1].price > lows[-2].price
    
    has_lower_highs = len(highs) >= 2 and highs[-1].price < highs[-2].price
    has_lower_lows = len(lows) >= 2 and lows[-1].price < lows[-2].price

    if has_higher_highs and has_higher_lows:
        bias = "BULLISH"
        reason = "Máximos y mínimos crecientes"
    elif has_lower_highs and has_lower_lows:
        bias = "BEARISH"
        reason = "Máximos y mínimos decrecientes"
        
    support, resistance = estimate_support_resistance_lines(points)
    
    current_price = candles[-1].close if candles else 0.0
    detected_patterns = detect_geometry_patterns(
        highs=highs,
        lows=lows,
        support=support,
        resistance=resistance,
        current_price=current_price,
        bias=bias
    )
    
    for p in detected_patterns:
        if source_timeframe:
            p.source_timeframe = source_timeframe

    score = score_geometry_quality(bias, points, support, resistance)

    return GeometryAnalysis(
        bias=bias,
        confidence_score=score,
        swing_points=points,
        support_line=support,
        resistance_line=resistance,
        patterns=detected_patterns,
        reason=reason
    )

