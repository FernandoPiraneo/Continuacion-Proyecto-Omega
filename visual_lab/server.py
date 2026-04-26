from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib
import inspect
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable

import httpx
import uvicorn
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("omega.dashboard.server")


# ============================================================
# Configuración general
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_FILE = STATIC_DIR / "index.html"

BINANCE_MARKET_WS_BASE = "wss://fstream.binance.com/market/stream?streams="
BINANCE_FUTURES_REST_KLINES = "https://fapi.binance.com/fapi/v1/klines"

DEFAULT_WATCHLIST = [
    "ADAUSDT",
    "ALGOUSDT",
    "AXSUSDT",
    "BANDUSDT",
    "BTCUSDT",
    "CRVUSDT",
    "DOTUSDT",
    "ETCUSDT",
    "ETHUSDT",
    "HOOKUSDT",
    "IOTAUSDT",
    "KAVAUSDT",
    "LINKUSDT",
    "LTCUSDT",
    "PEPEUSDT",
    "RAVEUSDT",
    "REEFUSDT",
    "ROSEUSDT",
    "SHIBAUSDT",
    "TRBUSDT",
    "XTZUSDT",
]

DEFAULT_TIMEFRAMES = ["1m", "3m", "5m", "15m"]

MAX_CANDLES_PER_TF = int(os.getenv("OMEGA_MAX_CANDLES_PER_TF", "500"))
BACKFILL_ENABLED = os.getenv("OMEGA_ENABLE_BACKFILL", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
BACKFILL_LIMIT = int(os.getenv("OMEGA_BACKFILL_LIMIT", "300"))

BROADCAST_INTERVAL_SECONDS = float(os.getenv("OMEGA_BROADCAST_INTERVAL_SECONDS", "0.25"))

SCANNER_ENABLED = os.getenv("OMEGA_ENABLE_SCANNER", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

SCANNER_INTERVAL_SECONDS = float(os.getenv("OMEGA_SCANNER_INTERVAL_SECONDS", "1.0"))
SCANNER_MODULE = os.getenv("OMEGA_SCANNER_MODULE", "app.scanner")
SCANNER_FUNCTION = os.getenv("OMEGA_SCANNER_FUNCTION", "scan_realtime_state")

NOTIFIER_ENABLED = os.getenv("OMEGA_ENABLE_NOTIFIER", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

NOTIFIER_MODULE = os.getenv("OMEGA_NOTIFIER_MODULE", "app.notification_engine")
NOTIFIER_FUNCTION = os.getenv("OMEGA_NOTIFIER_FUNCTION", "send_telegram_alert")

ALERT_DEDUP_SECONDS = float(os.getenv("OMEGA_ALERT_DEDUP_SECONDS", "180"))


def load_watchlist() -> list[str]:
    raw = os.getenv("OMEGA_WATCHLIST", "").strip()

    if not raw:
        return DEFAULT_WATCHLIST

    symbols = [
        item.strip().upper()
        for item in raw.split(",")
        if item.strip()
    ]

    return symbols or DEFAULT_WATCHLIST


def load_timeframes() -> list[str]:
    raw = os.getenv("OMEGA_TIMEFRAMES", "").strip()

    if not raw:
        return DEFAULT_TIMEFRAMES

    tfs = [
        item.strip().lower()
        for item in raw.split(",")
        if item.strip()
    ]

    return tfs or DEFAULT_TIMEFRAMES


WATCHLIST = load_watchlist()
TIMEFRAMES = load_timeframes()


# ============================================================
# Helpers
# ============================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def now_seconds() -> float:
    return time.time()


def to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        n = float(value)
        if n != n:
            return None
        return n
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def stream_symbol(symbol: str) -> str:
    return normalize_symbol(symbol).lower()


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def safe_task_name(task: asyncio.Task[Any] | None) -> str:
    if task is None:
        return "none"

    try:
        return task.get_name()
    except Exception:
        return "unknown"


# ============================================================
# CandleCache / MarketState
# ============================================================

class MarketCache:
    """
    Estado vivo de mercado.

    Guarda:
    - mark price
    - index price
    - funding rate
    - próxima hora de funding
    - vela viva por timeframe
    - últimas velas cerradas por timeframe

    Este objeto es la futura fuente de verdad para scanner.py.
    """

    def __init__(self, symbols: list[str], timeframes: list[str]) -> None:
        self.symbols = [normalize_symbol(s) for s in symbols]
        self.timeframes = list(timeframes)
        self._lock = asyncio.Lock()

        self._state: dict[str, dict[str, Any]] = {}

        for symbol in self.symbols:
            self._state[symbol] = {
                "symbol": symbol,
                "mark_price": None,
                "mark_price_ma": None,
                "index_price": None,
                "estimated_settle_price": None,
                "funding_rate": None,
                "next_funding_time": None,
                "last_price_event_time": None,
                "last_server_update_ms": None,
                "klines": {
                    tf: {
                        "current": None,
                        "closed": deque(maxlen=MAX_CANDLES_PER_TF),
                    }
                    for tf in self.timeframes
                },
            }

    async def update_mark_price(self, data: dict[str, Any]) -> dict[str, Any] | None:
        symbol = normalize_symbol(data.get("s"))

        if symbol not in self._state:
            return None

        event = {
            "symbol": symbol,
            "event_time": to_int(data.get("E")),
            "mark_price": to_float(data.get("p")),
            "mark_price_ma": to_float(data.get("ap")),
            "index_price": to_float(data.get("i")),
            "estimated_settle_price": to_float(data.get("P")),
            "funding_rate": to_float(data.get("r")),
            "next_funding_time": to_int(data.get("T")),
            "server_update_ms": now_ms(),
        }

        async with self._lock:
            item = self._state[symbol]
            item["mark_price"] = event["mark_price"]
            item["mark_price_ma"] = event["mark_price_ma"]
            item["index_price"] = event["index_price"]
            item["estimated_settle_price"] = event["estimated_settle_price"]
            item["funding_rate"] = event["funding_rate"]
            item["next_funding_time"] = event["next_funding_time"]
            item["last_price_event_time"] = event["event_time"]
            item["last_server_update_ms"] = event["server_update_ms"]

        return event

    async def update_kline(self, data: dict[str, Any]) -> dict[str, Any] | None:
        symbol = normalize_symbol(data.get("s"))
        k = data.get("k") or {}

        if symbol not in self._state:
            return None

        tf = str(k.get("i") or "").lower()

        if tf not in self.timeframes:
            return None

        candle = {
            "symbol": symbol,
            "timeframe": tf,
            "event_time": to_int(data.get("E")),
            "open_time": to_int(k.get("t")),
            "close_time": to_int(k.get("T")),
            "open": to_float(k.get("o")),
            "high": to_float(k.get("h")),
            "low": to_float(k.get("l")),
            "close": to_float(k.get("c")),
            "volume": to_float(k.get("v")),
            "quote_volume": to_float(k.get("q")),
            "trades": to_int(k.get("n")),
            "taker_buy_base_volume": to_float(k.get("V")),
            "taker_buy_quote_volume": to_float(k.get("Q")),
            "closed": bool(k.get("x")),
            "server_update_ms": now_ms(),
        }

        async with self._lock:
            tf_state = self._state[symbol]["klines"][tf]
            tf_state["current"] = candle

            if candle["closed"]:
                closed: deque[dict[str, Any]] = tf_state["closed"]

                if closed and closed[-1].get("open_time") == candle["open_time"]:
                    closed[-1] = candle
                else:
                    closed.append(candle)

        return candle

    async def export_state(self) -> dict[str, Any]:
        async with self._lock:
            symbols: dict[str, Any] = {}

            for symbol, item in self._state.items():
                klines: dict[str, Any] = {}

                for tf, tf_state in item["klines"].items():
                    klines[tf] = {
                        "current": tf_state["current"],
                        "closed": list(tf_state["closed"]),
                    }

                symbols[symbol] = {
                    "symbol": symbol,
                    "mark_price": item["mark_price"],
                    "mark_price_ma": item["mark_price_ma"],
                    "index_price": item["index_price"],
                    "estimated_settle_price": item["estimated_settle_price"],
                    "funding_rate": item["funding_rate"],
                    "next_funding_time": item["next_funding_time"],
                    "last_price_event_time": item["last_price_event_time"],
                    "last_server_update_ms": item["last_server_update_ms"],
                    "klines": klines,
                }

        return {
            "type": "bootstrap_state",
            "source": "omega_market_cache",
            "generated_at_ms": now_ms(),
            "symbols": symbols,
            "watchlist": self.symbols,
            "timeframes": self.timeframes,
        }

    async def seed_klines(
        self,
        *,
        symbol: str,
        timeframe: str,
        candles: list[dict[str, Any]],
    ) -> int:
        normalized_symbol = normalize_symbol(symbol)
        tf = str(timeframe or "").lower()

        if normalized_symbol not in self._state or tf not in self.timeframes:
            return 0

        valid = [
            candle
            for candle in candles
            if candle.get("open_time") is not None
        ]

        valid.sort(key=lambda item: int(item.get("open_time") or 0))
        valid = valid[-MAX_CANDLES_PER_TF:]

        async with self._lock:
            tf_state = self._state[normalized_symbol]["klines"][tf]
            closed: deque[dict[str, Any]] = tf_state["closed"]
            closed.clear()

            for candle in valid:
                closed.append(candle)

            tf_state["current"] = valid[-1] if valid else None

        return len(valid)

    async def scanner_payload(self) -> dict[str, Any]:
        """
        Payload simplificado para scanner.py.

        Forma sugerida para scanner.py:

        {
          "symbols": {
             "BTCUSDT": {
                "mark_price": ...,
                "klines": {
                   "15m": [velas cerradas..., vela actual],
                   "5m":  [...]
                }
             }
          }
        }
        """

        snap = await self.export_state()
        out_symbols: dict[str, Any] = {}

        for symbol, item in snap["symbols"].items():
            klines: dict[str, list[dict[str, Any]]] = {}

            for tf, tf_data in item["klines"].items():
                closed = list(tf_data.get("closed") or [])
                current = tf_data.get("current")

                candles = closed[:]

                if current:
                    if not candles or candles[-1].get("open_time") != current.get("open_time"):
                        candles.append(current)
                    elif not candles[-1].get("closed"):
                        candles[-1] = current

                klines[tf] = candles

            out_symbols[symbol] = {
                "symbol": symbol,
                "mark_price": item["mark_price"],
                "index_price": item["index_price"],
                "funding_rate": item["funding_rate"],
                "next_funding_time": item["next_funding_time"],
                "klines": klines,
            }

        return {
            "generated_at_ms": now_ms(),
            "watchlist": snap["watchlist"],
            "timeframes": snap["timeframes"],
            "symbols": out_symbols,
        }


# ============================================================
# Dashboard WS clients
# ============================================================

class DashboardClients:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()

        async with self._lock:
            self._clients.add(websocket)

        logger.info("Dashboard conectado. clients=%s", await self.count())

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(websocket)

        logger.info("Dashboard desconectado. clients=%s", await self.count())

    async def count(self) -> int:
        async with self._lock:
            return len(self._clients)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        message = json_dumps(payload)

        async with self._lock:
            clients = list(self._clients)

        if not clients:
            return

        dead: list[WebSocket] = []

        for ws in clients:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)

        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)


# ============================================================
# Scanner / Notifier integration
# ============================================================

def resolve_callable(module_name: str, function_name: str) -> Callable[..., Any] | None:
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        logger.warning("No se pudo importar %s: %s", module_name, exc)
        return None

    func = getattr(module, function_name, None)

    if not callable(func):
        logger.warning("No existe función callable %s.%s", module_name, function_name)
        return None

    return func


async def call_maybe_async(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)

    result = await asyncio.to_thread(func, *args, **kwargs)

    if inspect.isawaitable(result):
        return await result

    return result


def normalize_alerts(result: Any) -> list[dict[str, Any]]:
    if result is None:
        return []

    if isinstance(result, list):
        return [
            item
            for item in result
            if isinstance(item, dict)
        ]

    if isinstance(result, dict):
        if isinstance(result.get("alerts"), list):
            return [
                item
                for item in result["alerts"]
                if isinstance(item, dict)
            ]

        if "symbol" in result or "type" in result or "side" in result:
            return [result]

    return []


def alert_fingerprint(alert: dict[str, Any]) -> str:
    explicit = alert.get("id") or alert.get("fingerprint") or alert.get("plan_id")

    if explicit:
        return str(explicit)

    core = {
        "symbol": alert.get("symbol"),
        "side": alert.get("side"),
        "type": alert.get("type") or alert.get("signal_type"),
        "timeframe": alert.get("timeframe") or alert.get("tf") or alert.get("source_tf"),
        "level": alert.get("level"),
        "price": alert.get("price"),
        "reason": alert.get("reason") or alert.get("motivo"),
    }

    raw = json_dumps(core)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


# ============================================================
# Omega realtime hub
# ============================================================

class OmegaRealtimeHub:
    def __init__(self, symbols: list[str], timeframes: list[str]) -> None:
        self.symbols = [normalize_symbol(s) for s in symbols]
        self.timeframes = list(timeframes)

        self.cache = MarketCache(self.symbols, self.timeframes)
        self.clients = DashboardClients()

        self._stop_event = asyncio.Event()

        self._market_task: asyncio.Task[Any] | None = None
        self._broadcast_task: asyncio.Task[Any] | None = None
        self._scanner_task: asyncio.Task[Any] | None = None

        self._pending_events: list[dict[str, Any]] = []
        self._pending_lock = asyncio.Lock()

        self._binance_connected = False
        self._binance_connected_at_ms: int | None = None
        self._last_binance_event_at_ms: int | None = None
        self._last_binance_error: str | None = None
        self._reconnect_attempts = 0

        self._seen_alerts: dict[str, float] = {}

    def build_streams(self) -> list[str]:
        streams: list[str] = []

        for symbol in self.symbols:
            s = stream_symbol(symbol)

            streams.append(f"{s}@markPrice@1s")

            for tf in self.timeframes:
                streams.append(f"{s}@kline_{tf}")

        return streams

    def build_binance_url(self) -> str:
        return BINANCE_MARKET_WS_BASE + "/".join(self.build_streams())

    async def start(self) -> None:
        logger.info(
            "Iniciando OmegaRealtimeHub symbols=%s timeframes=%s streams=%s scanner=%s notifier=%s",
            len(self.symbols),
            self.timeframes,
            len(self.build_streams()),
            SCANNER_ENABLED,
            NOTIFIER_ENABLED,
        )

        self._stop_event.clear()

        if BACKFILL_ENABLED:
            await self.seed_historical_klines()

        self._market_task = asyncio.create_task(
            self._market_loop(),
            name="omega-binance-market-loop",
        )

        self._broadcast_task = asyncio.create_task(
            self._broadcast_loop(),
            name="omega-dashboard-broadcast-loop",
        )

        if SCANNER_ENABLED:
            self._scanner_task = asyncio.create_task(
                self._scanner_loop(),
                name="omega-scanner-loop",
            )

    async def stop(self) -> None:
        logger.info("Deteniendo OmegaRealtimeHub")

        self._stop_event.set()

        tasks = [
            self._market_task,
            self._broadcast_task,
            self._scanner_task,
        ]

        for task in tasks:
            if task:
                task.cancel()

        for task in tasks:
            if task:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        logger.info("OmegaRealtimeHub detenido")

    async def status(self) -> dict[str, Any]:
        client_count = await self.clients.count()

        return {
            "ok": True,
            "mode": "production_backend_ws_hub",
            "binance_market_ws_connected": self._binance_connected,
            "binance_connected_at_ms": self._binance_connected_at_ms,
            "last_binance_event_at_ms": self._last_binance_event_at_ms,
            "last_binance_error": self._last_binance_error,
            "reconnect_attempts": self._reconnect_attempts,
            "dashboard_clients": client_count,
            "watchlist": self.symbols,
            "timeframes": self.timeframes,
            "stream_count": len(self.build_streams()),
            "scanner_enabled": SCANNER_ENABLED,
            "notifier_enabled": NOTIFIER_ENABLED,
            "tasks": {
                "market": safe_task_name(self._market_task),
                "broadcast": safe_task_name(self._broadcast_task),
                "scanner": safe_task_name(self._scanner_task),
            },
        }

    async def queue_event(self, event: dict[str, Any]) -> None:
        async with self._pending_lock:
            self._pending_events.append(event)

            if len(self._pending_events) > 5_000:
                self._pending_events = self._pending_events[-2_500:]

    async def _broadcast_loop(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(BROADCAST_INTERVAL_SECONDS)

            async with self._pending_lock:
                events = self._pending_events
                self._pending_events = []

            if not events:
                continue

            await self.clients.broadcast(
                {
                    "type": "market_batch",
                    "server_time_ms": now_ms(),
                    "events": events,
                }
            )

    async def _market_loop(self) -> None:
        while not self._stop_event.is_set():
            url = self.build_binance_url()

            try:
                logger.info("Conectando Binance Market WS streams=%s", len(self.build_streams()))

                async with websockets.connect(
                    url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    max_queue=4096,
                    max_size=8 * 1024 * 1024,
                ) as ws:
                    self._binance_connected = True
                    self._binance_connected_at_ms = now_ms()
                    self._last_binance_error = None
                    self._reconnect_attempts = 0

                    await self.queue_event(
                        {
                            "type": "system",
                            "event": "binance_connected",
                            "connected_at_ms": self._binance_connected_at_ms,
                            "stream_count": len(self.build_streams()),
                        }
                    )

                    logger.info("Binance Market WS conectado")

                    async for raw in ws:
                        if self._stop_event.is_set():
                            break

                        self._last_binance_event_at_ms = now_ms()
                        await self._handle_binance_message(raw)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._binance_connected = False
                self._last_binance_error = repr(exc)
                self._reconnect_attempts += 1

                logger.exception("Binance Market WS error: %s", exc)

                await self.queue_event(
                    {
                        "type": "system",
                        "event": "binance_disconnected",
                        "error": repr(exc),
                        "reconnect_attempts": self._reconnect_attempts,
                        "server_time_ms": now_ms(),
                    }
                )

                delay = min(30.0, 1.0 * (2 ** min(self._reconnect_attempts, 5)))
                await asyncio.sleep(delay)

            finally:
                self._binance_connected = False

    async def _handle_binance_message(self, raw: str | bytes) -> None:
        try:
            packet = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Mensaje WS inválido no JSON: %r", raw[:200] if isinstance(raw, str) else raw[:200])
            return

        data = packet.get("data", packet)

        if not isinstance(data, dict):
            return

        event_type = data.get("e")

        if event_type == "markPriceUpdate":
            event = await self.cache.update_mark_price(data)

            if event:
                await self.queue_event(
                    {
                        "type": "market_update",
                        "event": "mark_price",
                        "data": event,
                    }
                )

            return

        if event_type == "kline":
            candle = await self.cache.update_kline(data)

            if candle:
                await self.queue_event(
                    {
                        "type": "market_update",
                        "event": "kline",
                        "data": candle,
                    }
                )

            return

    async def _scanner_loop(self) -> None:
        scanner_func = resolve_callable(SCANNER_MODULE, SCANNER_FUNCTION)

        if scanner_func is None:
            await self.queue_event(
                {
                    "type": "system",
                    "event": "scanner_disabled",
                    "reason": f"No se encontró {SCANNER_MODULE}.{SCANNER_FUNCTION}",
                    "server_time_ms": now_ms(),
                }
            )
            return

        notifier_func = None

        if NOTIFIER_ENABLED:
            notifier_func = resolve_callable(NOTIFIER_MODULE, NOTIFIER_FUNCTION)

            if notifier_func is None:
                await self.queue_event(
                    {
                        "type": "system",
                        "event": "notifier_disabled",
                        "reason": f"No se encontró {NOTIFIER_MODULE}.{NOTIFIER_FUNCTION}",
                        "server_time_ms": now_ms(),
                    }
                )

        logger.info(
            "Scanner loop activo: %s.%s interval=%ss",
            SCANNER_MODULE,
            SCANNER_FUNCTION,
            SCANNER_INTERVAL_SECONDS,
        )

        while not self._stop_event.is_set():
            await asyncio.sleep(SCANNER_INTERVAL_SECONDS)

            try:
                payload = await self.cache.scanner_payload()
                result = await call_maybe_async(scanner_func, payload)
                alerts = normalize_alerts(result)

                if not alerts:
                    continue

                for alert in alerts:
                    await self._handle_scanner_alert(alert, notifier_func)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.exception("Error en scanner loop: %s", exc)

                await self.queue_event(
                    {
                        "type": "system",
                        "event": "scanner_error",
                        "error": repr(exc),
                        "server_time_ms": now_ms(),
                    }
                )

    async def _handle_scanner_alert(
        self,
        alert: dict[str, Any],
        notifier_func: Callable[..., Any] | None,
    ) -> None:
        fingerprint = alert_fingerprint(alert)
        ts = now_seconds()

        self._prune_seen_alerts(ts)

        if fingerprint in self._seen_alerts:
            return

        self._seen_alerts[fingerprint] = ts

        normalized_alert = {
            **alert,
            "fingerprint": fingerprint,
            "server_time_ms": now_ms(),
        }

        await self.queue_event(
            {
                "type": "scanner_alert",
                "event": "alert",
                "data": normalized_alert,
            }
        )

        if notifier_func is not None:
            try:
                await call_maybe_async(notifier_func, normalized_alert)
            except Exception as exc:
                logger.exception("Error enviando alerta a Telegram/notifier: %s", exc)

                await self.queue_event(
                    {
                        "type": "system",
                        "event": "notifier_error",
                        "error": repr(exc),
                        "alert_fingerprint": fingerprint,
                        "server_time_ms": now_ms(),
                    }
                )

    def _prune_seen_alerts(self, ts: float) -> None:
        expired = [
            key
            for key, seen_ts in self._seen_alerts.items()
            if ts - seen_ts > ALERT_DEDUP_SECONDS
        ]

        for key in expired:
            self._seen_alerts.pop(key, None)

    async def seed_historical_klines(self) -> None:
        logger.info(
            "Iniciando backfill REST inicial symbols=%s timeframes=%s limit=%s",
            len(self.symbols),
            self.timeframes,
            BACKFILL_LIMIT,
        )

        timeout = httpx.Timeout(12.0, connect=8.0)
        loaded = 0

        async with httpx.AsyncClient(timeout=timeout) as client:
            for symbol in self.symbols:
                for timeframe in self.timeframes:
                    params = {
                        "symbol": symbol,
                        "interval": timeframe,
                        "limit": max(80, min(BACKFILL_LIMIT, MAX_CANDLES_PER_TF)),
                    }

                    try:
                        response = await client.get(BINANCE_FUTURES_REST_KLINES, params=params)
                        response.raise_for_status()
                        rows = response.json()
                    except Exception as exc:
                        logger.warning(
                            "Backfill falló symbol=%s tf=%s error=%s",
                            symbol,
                            timeframe,
                            exc,
                        )
                        continue

                    if not isinstance(rows, list):
                        continue

                    candles: list[dict[str, Any]] = []

                    for row in rows:
                        if not isinstance(row, list) or len(row) < 11:
                            continue

                        candle = {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "event_time": None,
                            "open_time": to_int(row[0]),
                            "close_time": to_int(row[6]),
                            "open": to_float(row[1]),
                            "high": to_float(row[2]),
                            "low": to_float(row[3]),
                            "close": to_float(row[4]),
                            "volume": to_float(row[5]),
                            "quote_volume": to_float(row[7]),
                            "trades": to_int(row[8]),
                            "taker_buy_base_volume": to_float(row[9]),
                            "taker_buy_quote_volume": to_float(row[10]),
                            "closed": True,
                            "server_update_ms": now_ms(),
                        }

                        if candle["open_time"] is None:
                            continue

                        candles.append(candle)

                    loaded += await self.cache.seed_klines(
                        symbol=symbol,
                        timeframe=timeframe,
                        candles=candles,
                    )

        logger.info("Backfill REST inicial completado velas=%s", loaded)


hub = OmegaRealtimeHub(WATCHLIST, TIMEFRAMES)


# ============================================================
# FastAPI app
# ============================================================

@asynccontextmanager
async def lifespan(_: FastAPI):
    await hub.start()

    try:
        yield
    finally:
        await hub.stop()


app = FastAPI(
    title="Omega Realtime Dashboard",
    version="3.0.0",
    description="Backend realtime con Binance WS, CandleCache, dashboard WS, scanner y notifier adapter.",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    if INDEX_FILE.exists():
        return FileResponse(INDEX_FILE)

    return JSONResponse(
        {
            "ok": True,
            "service": "Omega Realtime Dashboard",
            "message": "index.html no encontrado; API y WebSocket activos.",
            "expected_index_path": str(INDEX_FILE),
        }
    )


@app.get("/health")
async def health() -> JSONResponse:
    status = await hub.status()
    return JSONResponse(status)


@app.get("/api/status")
async def api_status() -> JSONResponse:
    status = await hub.status()
    return JSONResponse(status)


@app.get("/api/watchlist")
async def api_watchlist() -> JSONResponse:
    return JSONResponse(
        {
            "watchlist": WATCHLIST,
            "timeframes": TIMEFRAMES,
            "stream_count": len(hub.build_streams()),
        }
    )


@app.get("/api/market-state")
async def api_market_state() -> JSONResponse:
    state = await hub.cache.export_state()
    return JSONResponse(state)


@app.get("/api/scanner-payload")
async def api_scanner_payload() -> JSONResponse:
    payload = await hub.cache.scanner_payload()
    return JSONResponse(payload)


@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket) -> None:
    await hub.clients.connect(websocket)

    try:
        await websocket.send_text(
            json_dumps(
                {
                    "type": "hello",
                    "server_time_ms": now_ms(),
                    "status": await hub.status(),
                }
            )
        )

        await websocket.send_text(
            json_dumps(
                {
                    "type": "bootstrap_state",
                    "server_time_ms": now_ms(),
                    "data": await hub.cache.export_state(),
                }
            )
        )

        while True:
            raw = await websocket.receive_text()

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                msg = {"type": "raw", "data": raw}

            msg_type = msg.get("type")

            if msg_type == "ping":
                await websocket.send_text(
                    json_dumps(
                        {
                            "type": "pong",
                            "server_time_ms": now_ms(),
                        }
                    )
                )

            elif msg_type == "get_snapshot":
                # Compat legacy: mantener comando viejo del cliente,
                # pero responder con el nuevo contrato semántico.
                await websocket.send_text(
                    json_dumps(
                        {
                            "type": "bootstrap_state",
                            "server_time_ms": now_ms(),
                            "data": await hub.cache.export_state(),
                        }
                    )
                )

            elif msg_type == "get_status":
                await websocket.send_text(
                    json_dumps(
                        {
                            "type": "status",
                            "server_time_ms": now_ms(),
                            "data": await hub.status(),
                        }
                    )
                )

    except WebSocketDisconnect:
        pass

    except Exception as exc:
        logger.warning("Dashboard WS cerrado con error: %s", exc)

    finally:
        await hub.clients.disconnect(websocket)


if STATIC_DIR.exists():
    app.mount(
        "/static",
        StaticFiles(directory=STATIC_DIR),
        name="static",
    )


def main() -> None:
    uvicorn.run(
        "server:app",
        host=os.getenv("OMEGA_HOST", "127.0.0.1"),
        port=int(os.getenv("OMEGA_PORT", "8000")),
        reload=os.getenv("OMEGA_RELOAD", "1").strip().lower() in {"1", "true", "yes", "on"},
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":
    main()
