from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable

from app.alert_engine import AlertEngine
from app.binance_ws import BinanceMarketStream, PriceCallback, StatusCallback
from app.models import AlertEvent, AlertPriority, PriceUpdate

DispatchAlertsCallback = Callable[[list[AlertEvent]], Awaitable[int]]


class MarketWebSocketSupervisor:
    """Supervisa estado y alertas del market websocket sin duplicar su lógica de conexión."""

    STALE_SECONDS = 180
    STALE_CHECK_SECONDS = 30

    def __init__(
        self,
        *,
        market_stream: BinanceMarketStream,
        dispatch_alerts: DispatchAlertsCallback | None,
        alert_engine: AlertEngine | None,
        logger: logging.Logger,
    ) -> None:
        self._market_stream = market_stream
        self._dispatch_alerts = dispatch_alerts
        self._alert_engine = alert_engine
        self._logger = logger

        self.current_state = "INITIALIZING"
        self.connected_at: datetime | None = None
        self.last_message_at: datetime | None = None
        self.reconnect_count = 0
        self.consecutive_failures = 0
        self.last_disconnect_at: datetime | None = None
        self.last_error: str | None = None
        self.last_retry_in_seconds: float | None = None

        self._stop_event = asyncio.Event()
        self._stale_monitor_task: asyncio.Task[None] | None = None
        self._last_alert_sent_monotonic: dict[str, float] = {}
        self._warned_missing_alert_engine = False

    async def stop(self) -> None:
        self.current_state = "STOPPED"
        self._stop_event.set()
        if self._stale_monitor_task is not None:
            self._stale_monitor_task.cancel()
            await asyncio.gather(self._stale_monitor_task, return_exceptions=True)
            self._stale_monitor_task = None
        await self._market_stream.stop()

    async def update_symbols(self, symbols: list[str]) -> None:
        await self._market_stream.update_symbols(symbols)

    async def run(self, on_price: PriceCallback, on_status: StatusCallback) -> None:
        self.current_state = "CONNECTING"
        self._stop_event.clear()
        self._ensure_stale_monitor_task()
        try:
            await self._market_stream.run(
                self._wrap_on_price(on_price),
                self._wrap_on_status(on_status),
            )
        finally:
            self.current_state = "STOPPED"
            if self._stale_monitor_task is not None:
                self._stale_monitor_task.cancel()
                await asyncio.gather(self._stale_monitor_task, return_exceptions=True)
                self._stale_monitor_task = None

    def get_status(self) -> dict[str, object]:
        now = datetime.now(tz=timezone.utc)
        seconds_since_last_message = None
        uptime_seconds = None
        if self.last_message_at is not None:
            seconds_since_last_message = max(
                0.0,
                (now - self.last_message_at).total_seconds(),
            )
        if self.connected_at is not None and self.current_state == "CONNECTED":
            uptime_seconds = max(0.0, (now - self.connected_at).total_seconds())
        return {
            "current_state": self.current_state,
            "connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
            "reconnect_count": self.reconnect_count,
            "consecutive_failures": self.consecutive_failures,
            "seconds_since_last_message": seconds_since_last_message,
            "uptime_seconds": uptime_seconds,
            "last_error": self.last_error,
            "last_retry_in_seconds": self.last_retry_in_seconds,
        }

    def _ensure_stale_monitor_task(self) -> None:
        if self._stale_monitor_task is None or self._stale_monitor_task.done():
            self._stale_monitor_task = asyncio.create_task(self._stale_data_monitor_loop())

    def _wrap_on_price(self, on_price: PriceCallback) -> PriceCallback:
        async def wrapped(update: PriceUpdate) -> None:
            now = datetime.now(tz=timezone.utc)
            previous_state = self.current_state
            had_failures = self.consecutive_failures > 0

            self.last_message_at = now
            if previous_state in {"INITIALIZING", "CONNECTING", "RECONNECTING"}:
                self.current_state = "CONNECTED"
                self.connected_at = now
                if had_failures:
                    self.consecutive_failures = 0
                    await self._emit_system_alert(
                        key="market_ws_reconnected",
                        reason="WebSocket de mercado reconectado.",
                        priority=AlertPriority.INFO,
                        note=None,
                        cooldown_seconds=60,
                        local_cooldown_seconds=60,
                    )
            await on_price(update)

        return wrapped

    def _wrap_on_status(self, on_status: StatusCallback) -> StatusCallback:
        async def wrapped(kind: str, payload: dict) -> None:
            if kind != "market_ws_disconnected":
                await on_status(kind, payload)
                return

            now = datetime.now(tz=timezone.utc)
            self.consecutive_failures += 1
            self.reconnect_count += 1
            self.current_state = "RECONNECTING"
            self.connected_at = None
            self.last_disconnect_at = now
            self.last_error = str(payload.get("error", "desconocido"))
            self.last_retry_in_seconds = (
                float(payload["retry_in_seconds"])
                if payload.get("retry_in_seconds") is not None
                else None
            )

            priority = (
                AlertPriority.WARNING
                if self.consecutive_failures < 3
                else AlertPriority.ERROR
            )
            local_cooldown = 60 if priority == AlertPriority.WARNING else 120
            note_parts: list[str] = []
            if self.last_retry_in_seconds is not None:
                note_parts.append(f"Reintento en {self.last_retry_in_seconds}s")
            if payload.get("attempt") is not None:
                note_parts.append(f"Intento #{payload.get('attempt')}")
            if self.last_error:
                note_parts.append(self.last_error)

            await self._emit_system_alert(
                key=(
                    "market_ws_disconnected_warning"
                    if priority == AlertPriority.WARNING
                    else "market_ws_disconnected_error"
                ),
                reason="WebSocket de mercado desconectado.",
                priority=priority,
                note=" | ".join(note_parts) if note_parts else None,
                cooldown_seconds=60 if priority == AlertPriority.WARNING else 120,
                local_cooldown_seconds=local_cooldown,
            )

        return wrapped

    async def _stale_data_monitor_loop(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(self.STALE_CHECK_SECONDS)
            if self.current_state != "CONNECTED":
                continue
            if self.last_message_at is None:
                continue

            elapsed = (
                datetime.now(tz=timezone.utc) - self.last_message_at
            ).total_seconds()
            if elapsed < self.STALE_SECONDS:
                continue

            # cooldown local extra anti-loop; no reemplaza storage.should_send_alert.
            await self._emit_system_alert(
                key=f"market_ws_stale_{int(time.time() // 300)}",
                reason=(
                    f"Sin datos de mercado por {int(elapsed)} segundos. "
                    "Posible socket zombie o stream silencioso."
                ),
                priority=AlertPriority.CRITICAL,
                note=None,
                cooldown_seconds=0,
                local_cooldown_seconds=300,
            )

    async def _emit_system_alert(
        self,
        *,
        key: str,
        reason: str,
        priority: AlertPriority,
        note: str | None,
        cooldown_seconds: int,
        local_cooldown_seconds: int,
    ) -> None:
        if not self._can_emit_with_local_cooldown(key, local_cooldown_seconds):
            return
        if self._dispatch_alerts is None:
            return
        if self._alert_engine is None:
            if not self._warned_missing_alert_engine:
                self._logger.warning(
                    "alert_engine no disponible; se omiten alertas del supervisor de market WS."
                )
                self._warned_missing_alert_engine = True
            return
        try:
            event = self._alert_engine.build_system_alert(
                key=key,
                reason=reason,
                priority=priority,
                note=note,
                cooldown_seconds=cooldown_seconds,
            )
            await self._dispatch_alerts([event])
        except Exception as exc:  # defensivo para no tumbar el bot
            self._logger.warning("No se pudo despachar alerta de Market WS supervisor: %s", exc)

    def _can_emit_with_local_cooldown(self, key: str, cooldown_seconds: int) -> bool:
        now = time.monotonic()
        last_sent = self._last_alert_sent_monotonic.get(key)
        if last_sent is not None and (now - last_sent) < cooldown_seconds:
            return False
        self._last_alert_sent_monotonic[key] = now
        return True
