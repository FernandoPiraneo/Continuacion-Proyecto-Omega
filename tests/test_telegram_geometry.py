import pytest
from app.telegram_notifier import TelegramNotifier
from app.models import AlertEvent, AlertPriority, TradeSide

@pytest.fixture
def notifier():
    return TelegramNotifier("fake_token", "fake_chat", parse_mode="HTML")

def build_scanner_event(metadata):
    note = "Testing Note Base\n\n⚠ Solo alerta.\n\n🧭 Plan:\nDetalles del plan."
    event = AlertEvent(
        key="test",
        priority=AlertPriority.INFO,
        reason="test scanner",
        symbol="BTCUSDT",
        note=note,
        metadata={"scanner": True, **metadata}
    )
    return event

def build_classic_event(metadata=None):
    return AlertEvent(
        key="test",
        priority=AlertPriority.INFO,
        reason="test classic",
        symbol="BTCUSDT",
        side=TradeSide.LONG,
        current_price=10.5,
        entry=10.0,
        stop_loss=9.5,
        metadata=metadata or {}
    )

def test_no_metadata_or_geometry_m15(notifier):
    # 1. Sin metadata
    event = build_classic_event()
    event.metadata = None
    msg = notifier._build_message(event)
    assert "Geometría M15" not in msg
    assert "BTCUSDT" in msg

    # 2. Metadata sin geometry_m15
    event = build_classic_event(metadata={"other": 123})
    msg = notifier._build_message(event)
    assert "Geometría M15" not in msg
    assert "BTCUSDT" in msg

    # 3. geometry_m15 = None
    event = build_classic_event(metadata={"geometry_m15": None})
    msg = notifier._build_message(event)
    assert "Geometría M15" not in msg

def test_malformed_geometry(notifier):
    # 4. Malformados: string, list, int, dict vacío
    for bad_geom in ["hello", [1, 2], 123, {}]:
        event = build_classic_event(metadata={"geometry_m15": bad_geom})
        msg = notifier._build_message(event)
        assert "{" not in msg
        assert "[" not in msg
        assert "Geometría M15" not in msg

def test_valid_pattern(notifier):
    # 5. Patrón válido
    geom = {
        "bias": "BULLISH",
        "confidence_score": 50,
        "patterns": [
            {
                "type": "DOUBLE_BOTTOM",
                "bias": "LONG",
                "score": 85.5,
                "key_levels": [0.1097],
                "invalidation_level": 0.1089,
                "reason": "Test reason"
            }
        ]
    }
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "🧩 <b>Geometría M15:</b>" in msg
    assert "DOUBLE_BOTTOM" in msg
    assert "LONG 🟢" in msg
    assert "86/100" in msg
    assert "0.1097" in msg
    assert "0.1089" in msg
    assert "Test reason" in msg

def test_select_best_pattern(notifier):
    # 6. Selección de patrón principal
    geom = {
        "patterns": [
            {"type": "LOW_CONF", "score": 20},
            {"type": "HIGH_CONF", "score": 90},
            {"type": "MID_CONF", "score": 50}
        ]
    }
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "HIGH_CONF" in msg
    assert "LOW_CONF" not in msg
    assert "MID_CONF" not in msg

def test_patterns_with_weird_elements(notifier):
    # 7. Patterns con elementos raros
    geom = {
        "patterns": [
            None,
            "string",
            {"score": "not_a_number", "type": "BAD_DICT"},
            {"score": 95, "type": "GOOD_DICT"}
        ]
    }
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "GOOD_DICT" in msg
    assert "BAD_DICT" not in msg

def test_no_patterns_fallback(notifier):
    # 8. Sin patterns pero con bias/conf/reason
    geom = {
        "bias": "BEARISH",
        "confidence_score": 45.1,
        "reason": "Solo contexto"
    }
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "Bajista 🔴" in msg
    assert "45/100" in msg
    assert "Solo contexto" in msg
    assert "sin patrón dominante" not in msg

def test_empty_patterns(notifier):
    # 9. Patterns vacío
    geom = {"patterns": [], "bias": "NEUTRAL"}
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "NEUTRAL 🟡" in msg
    assert "sin patrón dominante" in msg
    assert "[" not in msg

def test_html_escaping(notifier):
    # 10. HTML escaping
    geom = {
        "patterns": [
            {"type": "<BAD>", "reason": "a & b \"c'"}
        ]
    }
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "&lt;BAD&gt;" in msg
    assert "a &amp; b &quot;c&#x27;" in msg
    assert "<BAD>" not in msg

def test_long_reason(notifier):
    # 11. Reason largo
    long_text = "A" * 150
    geom = {"patterns": [{"type": "P1", "reason": long_text}]}
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "A" * 87 + "..." in msg
    assert len(msg) < 500

def test_key_levels_list_and_dict(notifier):
    # 12 & 13. Key levels as list and dict
    geom1 = {"patterns": [{"type": "P1", "key_levels": [0.1097, 0.1102]}]}
    event1 = build_classic_event(metadata={"geometry_m15": geom1})
    msg1 = notifier._build_message(event1)
    assert "0.10970" in msg1

    geom2 = {"patterns": [{"type": "P2", "key_levels": {"Neckline <": 0.555}}]}
    event2 = build_classic_event(metadata={"geometry_m15": geom2})
    msg2 = notifier._build_message(event2)
    assert "Nivel Neckline &lt;" in msg2
    assert "0.55500" in msg2

def test_invalidation_absent_and_confidence_invalid(notifier):
    # 14 & 15. Invalidation absent and confidence invalid
    geom = {
        "patterns": [
            {
                "type": "P1",
                "score": "NaN",  # invalid
                # invalidation missing
            }
        ]
    }
    event = build_classic_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    assert "Invalidación" not in msg
    assert "0/100" in msg

def test_block_location(notifier):
    # 16. Ubicación del bloque en eventos de scanner
    geom = {"bias": "BULLISH", "confidence_score": 90.0}
    event = build_scanner_event(metadata={"geometry_m15": geom})
    msg = notifier._build_message(event)
    
    # Geometría entra antes de PLAN
    idx_geom = msg.find("Geometría M15")
    idx_plan = msg.find("🧭 Plan:")
    idx_nota = msg.find("⚠ Solo alerta.")
    
    assert idx_geom != -1
    # Actually my logic inserted before PLAN if 'Plan:' exists
    assert idx_geom < idx_plan
    # But wait, does it insert before "Solo alerta." or before "Plan:"? 
    # The logic in notifier: 
    # if "🧭 Plan:" in text: text.replace("🧭 Plan:", f"{geom_block}\n\n🧭 Plan:")
    # else if "⚠ Solo alerta.": ...
    
    # Geometría will be before Plan:
    assert msg.find("🧩") < msg.find("🧭 Plan:")

def test_classic_alert_intact(notifier):
    # 17. Alerta clásica sin geometría no se mutila
    event = build_classic_event()
    msg = notifier._build_message(event)
    assert "Geometría" not in msg
    assert "BTCUSDT" in msg
    assert "Precio actual:</b> " in msg
    assert "Entrada:</b> " in msg
    assert "test classic" in msg
