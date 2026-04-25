import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from app.models import AlertEngineSettings, TradeStatus
from app.storage import Storage
from app.telegram_command_bot import TelegramCommandBot
from app.trade_manager import TradeManager

def run(coro):
    return asyncio.run(coro)

def make_update(chat_id: str, text: str, update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": chat_id, "type": "private"},
            "text": text,
        },
    }

async def build_bot_for_geom(
    tmp_path: Path,
    *,
    geom_enabled: bool = True,
):
    sent_messages: list[tuple[str, str]] = []
    storage = Storage(tmp_path / "bot_geom.sqlite")
    await storage.initialize()
    trade_manager = TradeManager(storage)
    await trade_manager.bootstrap_trades([])

    async def sender(chat_id: str, text: str, reply_markup: dict | None = None) -> None:
        sent_messages.append((chat_id, text))

    def status_provider() -> dict:
        return {
            "DRY_RUN": "true",
            "BINANCE_TESTNET": "false",
            "BINANCE_PRIVATE_ACCOUNT_SYNC": "true",
            "BINANCE_MARKET_SOURCE": "mark_price",
            "SCANNER_ENABLED": "true",
            "SCANNER_ALERTS_ENABLED": "true",
            "SCANNER_INTERVAL_SECONDS": 60,
            "active_trades": 0,
            "ENABLE_GEOMETRY_CORE": geom_enabled
        }

    bot = TelegramCommandBot(
        bot_token="test-token",
        authorized_chat_id="123",
        trade_manager=trade_manager,
        alert_settings=AlertEngineSettings(),
        logger=logging.getLogger("test.geometria"),
        status_provider=status_provider,
        sender=sender,
        storage=storage,
    )
    return bot, storage, sent_messages

def test_geometria_registered(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path)
        try:
            # We check if /geometria is in the handlers by calling it
            await bot.handle_update(make_update("123", "/geometria"))
            assert any("Auditoría privada del Core Geométrico" in msg for _, msg in sent)
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_unauthorized(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path)
        try:
            await bot.handle_update(make_update("999", "/geometria"))
            assert sent == [("999", "Unauthorized")]
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_core_off(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=False)
        try:
            await bot.handle_update(make_update("123", "/geometria"))
            assert any("Estado: OFF ⚪" in msg for _, msg in sent)
            assert any("ENABLE_GEOMETRY_CORE=true" in msg for _, msg in sent)
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_core_on_no_readings(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        try:
            # Storage is empty
            await bot.handle_update(make_update("123", "/geometria"))
            assert any("Estado: ON ✅" in msg for _, msg in sent)
            assert any("Sin lecturas geométricas registradas todavía" in msg for _, msg in sent)
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_core_on_valid_reading(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        try:
            # Inject a valid alert into alert_history
            geom_data = {
                "bias": "LONG",
                "confidence_score": 75,
                "patterns": [
                    {
                        "type": "DOUBLE_BOTTOM",
                        "bias": "LONG",
                        "score": 80,
                        "reason": "Clear structure",
                        "key_levels": 65000.5,
                        "invalidation_level": 64000.0
                    }
                ]
            }
            payload = {
                "metadata": {
                    "geometry_m15": geom_data
                }
            }
            
            # Use record_alert sync via thread if needed, but here we can mock list_recent_alerts or use real DB
            with storage._thread_lock:
                storage._connection.execute(
                    "INSERT INTO alert_history (alert_key, symbol, side, priority, reason, sent_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("test_key", "BTCUSDT", "LONG", "INFO", "LONG_PULLBACK", "2026-04-25T18:50:00Z", json.dumps(payload))
                )
                storage._connection.commit()

            await bot.handle_update(make_update("123", "/geometria"))
            msg = sent[-1][1]
            assert "BTCUSDT" in msg
            assert "Patrón: DOUBLE_BOTTOM" in msg
            assert "Sesgo: LONG 🟢" in msg
            assert "Confianza: 80/100" in msg
            assert "Nivel clave: 65000.5" in msg
            assert "Invalidación: 64000.0" in msg
            assert "Alerta base: LONG_PULLBACK" in msg
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_max_5_readings(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        try:
            # Inject 10 valid alerts
            for i in range(10):
                geom_data = {"patterns": [{"type": "TEST_PAT", "bias": "LONG", "score": 50}]}
                payload = {"metadata": {"geometry_m15": geom_data}}
                with storage._thread_lock:
                    storage._connection.execute(
                        "INSERT INTO alert_history (alert_key, symbol, side, priority, reason, sent_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (f"key_{i}", f"SYM_{i}", "LONG", "INFO", "reason", f"2026-04-25T18:{i:02d}:00Z", json.dumps(payload))
                    )
                    storage._connection.commit()

            await bot.handle_update(make_update("123", "/geometria"))
            msg = sent[-1][1]
            # Should have 1., 2., 3., 4., 5. but not 6.
            assert "1. SYM_9" in msg # SYM_9 is latest if sorted DESC
            assert "5. SYM_5" in msg
            assert "6. SYM_4" not in msg
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_malformed_metadata(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        try:
            malformed_cases = [
                "INVALID_JSON",
                json.dumps({"metadata": "not_a_dict"}),
                json.dumps({"metadata": {"geometry_m15": "not_a_dict"}}),
                json.dumps({"metadata": {"geometry_m15": {"patterns": "not_a_list"}}})
            ]
            
            for i, case in enumerate(malformed_cases):
                with storage._thread_lock:
                    storage._connection.execute(
                        "INSERT INTO alert_history (alert_key, symbol, side, priority, reason, sent_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (f"mal_{i}", f"SYM_MAL_{i}", "LONG", "INFO", "reason", f"2026-04-25T18:{i:02d}:00Z", case)
                    )
                    storage._connection.commit()

            await bot.handle_update(make_update("123", "/geometria"))
            msg = sent[-1][1]
            # Should fall back to "Sin lecturas" if all were malformed
            assert "Sin lecturas geométricas registradas todavía" in msg
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_no_dicts_crudos_no_secrets(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        try:
            geom_data = {"patterns": [{"type": "SECRET_TEST", "bias": "LONG", "score": 99}]}
            payload = {"metadata": {"geometry_m15": geom_data}}
            with storage._thread_lock:
                storage._connection.execute(
                    "INSERT INTO alert_history (alert_key, symbol, side, priority, reason, sent_at, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    ("secret_key", "SECRET_BTC", "LONG", "INFO", "reason", "2026-04-25T18:00:00Z", json.dumps(payload))
                )
                storage._connection.commit()

            await bot.handle_update(make_update("123", "/geometria"))
            msg = sent[-1][1]
            
            # No dict reprs
            assert "{'geometry_m15'" not in msg
            assert "GeometryPattern(" not in msg
            
            # No secrets (just in case they were leaked in some way)
            assert "BINANCE_API_KEY" not in msg
            assert "TELEGRAM_BOT_TOKEN" not in msg
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_no_binance_no_analyze_calls(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        bot._binance_client = MagicMock()
        
        # We need to mock analyze_geometry in app.geometric_core
        with patch("app.telegram_command_bot.json.loads") as mock_json_loads:
            # We don't really need to mock json.loads, just showing we can use patch
            pass
            
        try:
            with patch("app.scanner.analyze_geometry") as mock_analyze:
                await bot.handle_update(make_update("123", "/geometria"))
                # Ensure no calls to binance_client methods
                assert not bot._binance_client.method_calls
                # Ensure no calls to analyze_geometry
                assert not mock_analyze.called
        finally:
            await bot.close()
            await storage.close()
    run(scenario())

def test_geometria_storage_read_only(tmp_path: Path):
    async def scenario():
        bot, storage, sent = await build_bot_for_geom(tmp_path, geom_enabled=True)
        
        # We want to ensure it only calls list_recent_alerts and not record_alert or others
        storage.list_recent_alerts = MagicMock(side_effect=storage.list_recent_alerts)
        storage.record_alert = MagicMock()
        storage.create_trade_plan = MagicMock()
        
        try:
            await bot.handle_update(make_update("123", "/geometria"))
            assert storage.list_recent_alerts.called
            assert not storage.record_alert.called
            assert not storage.create_trade_plan.called
        finally:
            await bot.close()
            await storage.close()
    run(scenario())
