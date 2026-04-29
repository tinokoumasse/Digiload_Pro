"""
Digiload Pro — Plugin: LED Control
Controls WLED LED strip based on gate state changes.

Hooks used:
    on_start        — reads LED config
    on_state_change — sends correct preset to WLED
    on_stop         — turns LED off

Config (config.json):
    "plugins": {
        "led_control": {"enabled": true}
    }
    "led": {
        "ip": "192.168.1.100",
        "presets": { ... }
    }
"""

import threading
import logging
import requests
import numpy as np

log = logging.getLogger("digiload.plugins.led_control")

# ── State ─────────────────────────────────────────────────────────────────────
_led_ip    = ""
_presets   = {}
_net_status = "—"

WLED_EFFECTS = {"static": 0, "blink": 1, "breath": 2, "strobe": 51}

_PRESET_FOR_MODE = {
    "IDLE":           "off",
    "STANDBY":        "standby",
    "ARMED":          "armed",
    "ERROR_SSCC":     "error",
    "ERROR_FORKLIFT": "error",
    "VALIDATED":      "confirmed",
}

# ── Plugin interface ──────────────────────────────────────────────────────────
def on_start(config: dict):
    global _led_ip, _presets
    _led_ip  = config.get("led", {}).get("ip", "")
    _presets = config.get("led", {}).get("presets", {})
    if _led_ip:
        log.info(f"[led_control] Initialized — controller: {_led_ip}")
    else:
        log.warning("[led_control] No LED IP configured")

def on_state_change(mode: str, ctx: dict):
    preset_name = _PRESET_FOR_MODE.get(mode, "off")
    _send_preset(preset_name)

def on_stop():
    _send_preset("off")
    log.info("[led_control] LED off — shutting down")

# ── WLED helpers ──────────────────────────────────────────────────────────────
def _build_payload(r, g, b, effect="static", brightness=200, speed=128, on=True):
    return {
        "on":  on,
        "bri": int(np.clip(brightness, 0, 255)),
        "seg": [{"col": [[int(r), int(g), int(b)]],
                 "fx":  WLED_EFFECTS.get(effect, 0),
                 "sx":  int(np.clip(speed, 0, 255))}]
    }

def _send_preset(name: str):
    global _net_status
    if not _led_ip:
        return
    p = _presets.get(name, _presets.get("off", {}))
    if not p:
        return
    payload = _build_payload(
        p.get("r", 0), p.get("g", 0), p.get("b", 0),
        p.get("effect", "static"),
        p.get("brightness", 200),
        p.get("speed", 128),
        on=(name != "off")
    )

    def _do():
        global _net_status
        try:
            requests.post(f"http://{_led_ip}/json/state", json=payload, timeout=1.5)
            _net_status = "OK"
        except Exception as e:
            _net_status = "ERR"
            log.debug(f"[led_control] Send failed: {e}")

    threading.Thread(target=_do, daemon=True).start()

def get_status() -> str:
    """Returns LED network status — called by core for HUD display."""
    return _net_status
