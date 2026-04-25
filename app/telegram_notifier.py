from __future__ import annotations

import html
import math
from typing import Any

import httpx

from app.models import AlertEvent, AlertPriority


class TelegramNotificationError(RuntimeError):
    pass


class TelegramNotifier:
    PRIORITY_ICON = {
        AlertPriority.INFO: "ℹ️",
        AlertPriority.WARNING: "⚠️",
        AlertPriority.ERROR: "❌",
        AlertPriority.CRITICAL: "🚨",
    }

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        *,
        parse_mode: str = "HTML",
        timeout_seconds: float = 10.0,
    ) -> None:
        if not bot_token:
            raise TelegramNotificationError("TELEGRAM_BOT_TOKEN no configurado.")
        if not chat_id:
            raise TelegramNotificationError("TELEGRAM_CHAT_ID no configurado.")

        self._bot_token = bot_token
        self._chat_id = chat_id
        self._parse_mode = parse_mode
        self._client = httpx.AsyncClient(
            base_url=f"https://api.telegram.org/bot{bot_token}",
            timeout=httpx.Timeout(timeout_seconds),
        )

    async def close(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _format_number(value: float | None, digits: int = 6) -> str:
        if value is None:
            return "N/D"
        return f"{value:.{digits}f}"

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None: return None
        try:
            val = float(value)
            if math.isnan(val) or math.isinf(val):
                return None
            return val
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _format_geometry_side(value: Any) -> str:
        val_str = str(value).strip().upper()
        mapping = {
            "LONG": "LONG 🟢",
            "SHORT": "SHORT 🔴",
            "NEUTRAL": "NEUTRAL 🟡",
            "BULLISH": "Alcista 🟢",
            "BEARISH": "Bajista 🔴"
        }
        return mapping.get(val_str, val_str)

    @staticmethod
    def _truncate_geometry_reason(text: Any, max_len: int = 90) -> str:
        if not text:
            return ""
        text_str = str(text).strip()
        if len(text_str) > max_len:
            return text_str[:max_len-3] + "..."
        return text_str

    @staticmethod
    def _select_best_geometry_pattern(patterns: Any) -> dict | None:
        if not isinstance(patterns, list):
            return None
        valid = [p for p in patterns if isinstance(p, dict)]
        if not valid:
            return None
        valid.sort(key=lambda p: TelegramNotifier._safe_float(p.get("score")) or 0.0, reverse=True)
        return valid[0]

    @staticmethod
    def _format_geometry_block(metadata: dict) -> str:
        if not metadata: return ""
        geom = metadata.get("geometry_m15")
        if not isinstance(geom, dict): return ""
        
        try:
            bias = geom.get("bias")
            conf_overall = geom.get("confidence_score")
            patterns = geom.get("patterns")
            reason_overall = geom.get("reason")
            
            if not bias and not conf_overall and not patterns and not reason_overall:
                return ""
            
            best_pattern = TelegramNotifier._select_best_geometry_pattern(patterns)
            
            lines = []
            lines.append("🧩 <b>Geometría M15:</b>")
            
            if best_pattern:
                p_type = best_pattern.get("type", "N/D")
                p_side = TelegramNotifier._format_geometry_side(best_pattern.get("bias", "NEUTRAL"))
                p_conf = TelegramNotifier._safe_float(best_pattern.get("score")) or 0.0
                
                lines.append(f"- Patrón: {html.escape(str(p_type))}")
                lines.append(f"- Sesgo: {p_side}")
                lines.append(f"- Confianza: {p_conf:.0f}/100")
                
                kls = best_pattern.get("key_levels")
                if isinstance(kls, list) and kls:
                    lvl = TelegramNotifier._safe_float(kls[0])
                    if lvl is not None:
                        lines.append(f"- Nivel clave: {TelegramNotifier._format_number(lvl, 5)}")
                elif isinstance(kls, dict) and kls:
                    k, v = next(iter(kls.items()))
                    lvl = TelegramNotifier._safe_float(v)
                    if lvl is not None:
                        lines.append(f"- Nivel {html.escape(str(k))}: {TelegramNotifier._format_number(lvl, 5)}")
                        
                inv = TelegramNotifier._safe_float(best_pattern.get("invalidation_level"))
                if inv is not None:
                    lines.append(f"- Invalidación: {TelegramNotifier._format_number(inv, 5)}")
                    
                reas = best_pattern.get("reason")
                if reas:
                    lines.append(f"- Lectura: {html.escape(TelegramNotifier._truncate_geometry_reason(reas))}")
            else:
                lines.append(f"- Sesgo: {TelegramNotifier._format_geometry_side(bias or 'NEUTRAL')}")
                c_overall = TelegramNotifier._safe_float(conf_overall) or 0.0
                if c_overall > 0:
                    lines.append(f"- Confianza: {c_overall:.0f}/100")
                if reason_overall:
                    lines.append(f"- Lectura: {html.escape(TelegramNotifier._truncate_geometry_reason(reason_overall))}")
                else:
                    lines.append("- Lectura: sin patrón dominante")
                    
            return "\n".join(lines)
        except Exception:
            return ""

    def _build_message(self, event: AlertEvent) -> str:
        geom_block = self._format_geometry_block(event.metadata) if isinstance(event.metadata, dict) else ""

        if bool((event.metadata or {}).get("scanner", False)) and event.note:
            text = event.note
            if geom_block:
                if "🧭 Plan:" in text:
                    text = text.replace("🧭 Plan:", f"{geom_block}\n\n🧭 Plan:")
                elif "⚠ Solo alerta." in text:
                    text = text.replace("⚠ Solo alerta.", f"{geom_block}\n\n⚠ Solo alerta.")
                else:
                    text = f"{text.rstrip()}\n\n{geom_block}"
            return text

        icon = self.PRIORITY_ICON.get(event.priority, "ℹ️")
        lines = [f"<b>{icon} {html.escape(event.priority.value)} | Trading Alert</b>"]

        if event.symbol != "SYSTEM":
            lines.extend(
                [
                    f"<b>Símbolo:</b> {html.escape(event.symbol)}",
                    f"<b>Side:</b> {html.escape(event.side.value if event.side else 'N/D')}",
                    f"<b>Precio actual:</b> {self._format_number(event.current_price)}",
                    f"<b>Entrada:</b> {self._format_number(event.entry)}",
                    f"<b>SL:</b> {self._format_number(event.stop_loss)}",
                ]
            )
            if event.take_profits:
                lines.append(
                    "<b>TPs:</b> "
                    + " / ".join(self._format_number(price) for price in event.take_profits)
                )
            pnl_parts: list[str] = []
            if event.approx_pnl_usdt is not None:
                pnl_parts.append(f"{event.approx_pnl_usdt:+.4f} USDT")
            if event.approx_pnl_pct is not None:
                pnl_parts.append(f"{event.approx_pnl_pct:+.2f}%")
            lines.append(f"<b>PnL aprox:</b> {' | '.join(pnl_parts) if pnl_parts else 'N/D'}")

            if event.bias is not None or event.ema_fast is not None or event.ema_slow is not None:
                lines.append(
                    "<b>Bias:</b> "
                    f"{html.escape(event.bias.value if event.bias else 'neutral')} | "
                    f"EMAf {self._format_number(event.ema_fast, 4)} | "
                    f"EMAs {self._format_number(event.ema_slow, 4)} | "
                    f"Struct {html.escape(event.structure or 'neutral')}"
                )

        lines.append(f"<b>Motivo:</b> {html.escape(event.reason)}")
        if event.note:
            lines.append(f"<b>Nota:</b> {html.escape(event.note)}")
            
        if geom_block:
            lines.append("")
            lines.append(geom_block)
            
        return "\n".join(lines)

    async def send_alert(self, event: AlertEvent) -> dict[str, Any]:
        payload = {
            "chat_id": self._chat_id,
            "text": self._build_message(event),
            "parse_mode": self._parse_mode,
            "disable_notification": event.priority == AlertPriority.INFO,
        }
        try:
            response = await self._client.post("/sendMessage", json=payload)
        except httpx.HTTPError as exc:
            raise TelegramNotificationError(f"Error HTTP enviando a Telegram: {exc}") from exc

        data = response.json()
        if response.status_code >= 400 or not data.get("ok", False):
            raise TelegramNotificationError(
                data.get("description", f"Telegram respondió con {response.status_code}")
            )
        return data
