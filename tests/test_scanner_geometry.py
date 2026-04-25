import pytest
import asyncio
import os
import json
from unittest.mock import AsyncMock, patch, MagicMock

from app.scanner import SignalScanner
from app.models import Candle

@pytest.fixture
def fake_candles():
    return [
        Candle(
            open_time=1000 * i, 
            open=100.0, 
            high=110.0, 
            low=90.0, 
            close=105.0, 
            close_time=1000 * i + 999, 
            volume=10.0
        )
        for i in range(210)
    ]

@pytest.fixture
def scanner_deps():
    client = AsyncMock()
    client.is_rest_paused = MagicMock(return_value=False)
    tm = MagicMock()
    tm.get_watchlist_symbols.return_value = ["BTCUSDT"]
    storage = AsyncMock()
    storage.has_scanner_alert_state.return_value = False
    dispatch = AsyncMock(return_value=1)
    logger = MagicMock()
    
    return {
        "binance_client": client,
        "trade_manager": tm,
        "storage": storage,
        "dispatch_alerts": dispatch,
        "logger": logger,
        "alerts_enabled": True
    }

def test_geometry_flag_off_by_default(scanner_deps, fake_candles, monkeypatch):
    async def _test():
        monkeypatch.delenv("ENABLE_GEOMETRY_CORE", raising=False)
        scanner_deps["binance_client"].get_klines.return_value = fake_candles
        scanner = SignalScanner(**scanner_deps)
        with patch("app.scanner.analyze_geometry") as mock_analyze:
            await scanner.scan_once()
            mock_analyze.assert_not_called()
    asyncio.run(_test())

def test_geometry_flag_explicitly_false(scanner_deps, fake_candles, monkeypatch):
    async def _test():
        monkeypatch.setenv("ENABLE_GEOMETRY_CORE", "false")
        scanner_deps["binance_client"].get_klines.return_value = fake_candles
        scanner = SignalScanner(**scanner_deps)
        with patch("app.scanner.analyze_geometry") as mock_analyze:
            await scanner.scan_once()
            mock_analyze.assert_not_called()
    asyncio.run(_test())

def test_geometry_flag_true_calls_analyze(scanner_deps, fake_candles, monkeypatch):
    async def _test():
        monkeypatch.setenv("ENABLE_GEOMETRY_CORE", "true")
        scanner_deps["binance_client"].get_klines.return_value = fake_candles
        scanner = SignalScanner(**scanner_deps)
        with patch("app.scanner.analyze_geometry") as mock_analyze:
            from app.geometric_core import GeometryAnalysis
            mock_analyze.return_value = GeometryAnalysis(bias="BULLISH", confidence_score=80.0, patterns=[], swing_points=[])
            await scanner.scan_once()
            mock_analyze.assert_called_once()
            args, kwargs = mock_analyze.call_args
            assert kwargs.get("source_timeframe") == "15m"
    asyncio.run(_test())

def test_geometry_metadata_serialized(scanner_deps, fake_candles, monkeypatch):
    async def _test():
        monkeypatch.setenv("ENABLE_GEOMETRY_CORE", "true")
        scanner_deps["binance_client"].get_klines.return_value = fake_candles
        scanner = SignalScanner(**scanner_deps)
        with patch("app.scanner.analyze_geometry") as mock_analyze, \
             patch("app.scanner.evaluate_signal_quality") as mock_eval:
            from app.geometric_core import GeometryAnalysis, GeometryPattern
            p = GeometryPattern("DOUBLE_TOP", "SHORT", 90.0, [100.0], None, "reason")
            mock_analyze.return_value = GeometryAnalysis(bias="BULLISH", confidence_score=80.0, patterns=[p], swing_points=[])
            mock_eval.return_value = {
                "alert_allowed": True,
                "result": "LONG",
                "signal_type": "LONG_CONTINUATION",
                "quality": "ALTA"
            }
            await scanner.scan_once()
            dispatch = scanner_deps["dispatch_alerts"]
            assert dispatch.call_count == 1
            events = dispatch.call_args[0][0]
            assert len(events) > 0
            event = events[0]
            geom = event.metadata.get("geometry_m15")
            assert geom is not None
            assert geom["bias"] == "BULLISH"
            assert geom["confidence_score"] == 80.0
            assert len(geom["patterns"]) == 1
            assert geom["patterns"][0]["type"] == "DOUBLE_TOP"
            assert geom["patterns"][0]["key_levels"] == [100.0]
            json_str = json.dumps(geom)
            assert "DOUBLE_TOP" in json_str
    asyncio.run(_test())

def test_geometry_exception_does_not_crash(scanner_deps, fake_candles, monkeypatch):
    async def _test():
        monkeypatch.setenv("ENABLE_GEOMETRY_CORE", "true")
        scanner_deps["binance_client"].get_klines.return_value = fake_candles
        scanner = SignalScanner(**scanner_deps)
        with patch("app.scanner.analyze_geometry") as mock_analyze, \
             patch("app.scanner.evaluate_signal_quality") as mock_eval:
            mock_analyze.side_effect = Exception("Simulated Fake Crash in core")
            mock_eval.return_value = {
                "alert_allowed": True,
                "result": "LONG",
                "signal_type": "LONG_CONTINUATION",
                "quality": "ALTA"
            }
            await scanner.scan_once()
            dispatch = scanner_deps["dispatch_alerts"]
            assert dispatch.call_count == 1
            events = dispatch.call_args[0][0]
            event = events[0]
            assert event.metadata.get("geometry_m15") is None
    asyncio.run(_test())

def test_geometry_no_plan_creation_and_no_scoring_change(scanner_deps, fake_candles, monkeypatch):
    async def _test():
        monkeypatch.setenv("ENABLE_GEOMETRY_CORE", "true")
        scanner_deps["binance_client"].get_klines.return_value = fake_candles
        scanner = SignalScanner(**scanner_deps)
        with patch("app.scanner.analyze_geometry") as mock_analyze, \
             patch("app.scanner.evaluate_signal_quality") as mock_eval:
            from app.geometric_core import GeometryAnalysis
            mock_analyze.return_value = GeometryAnalysis(bias="BULLISH", confidence_score=80.0, patterns=[], swing_points=[])
            quality_mock_return = {
                "alert_allowed": True,
                "result": "LONG",
                "signal_type": "LONG_CONTINUATION",
                "quality": "MEDIA",
                "score": 50.0
            }
            mock_eval.return_value = quality_mock_return
            await scanner.scan_once()
            dispatch = scanner_deps["dispatch_alerts"]
            event = dispatch.call_args[0][0][0]
            assert event.metadata.get("quality_payload") == quality_mock_return
    asyncio.run(_test())
