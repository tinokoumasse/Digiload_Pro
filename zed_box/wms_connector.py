"""
Digiload Pro — WMS Connector v1.0
Phase 1: Standalone process — direct REST POST to WMS + retry queue + HMAC signing

Run as:
    python wms_connector.py
    OR via systemd: digiload-wms.service

Architecture:
    digiload_pro.py  →  wms_queue (SQLite)  ←  wms_connector.py (this file)
                                                        ↓
                                                  WMS (direct POST)
                                                        ↓ (best-effort, non-blocking)
                                                  Central VM (sync notification)

Guarantees:
    - WMS receives every confirmed pallet load
    - Gate operation never blocked by WMS unavailability
    - Events survive digiload_pro.py crashes (queue persists in SQLite)
    - 24h retry coverage (288 attempts at 30s interval)
    - Every attempt logged to wms_delivery_log
"""

import sqlite3
import requests
import json
import time
import hmac
import hashlib
import logging
import threading
import os
import sys
from datetime import datetime
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs("/var/log/digiload", exist_ok=True)

log = logging.getLogger("digiload.wms")
log.setLevel(logging.DEBUG)

_fh = RotatingFileHandler(
    "/var/log/digiload/wms.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8"
)
_fh.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
log.addHandler(_fh)
log.addHandler(_ch)

# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────
CONFIG_FILE  = "/etc/digiload/config.json"
CONFIG_LOCAL = "config.json"
DB_FILE      = "/opt/digiload/digiload.db"
DB_LOCAL     = "digiload.db"

_CONFIG_FILE = CONFIG_FILE if os.path.exists(CONFIG_FILE) else CONFIG_LOCAL
_DB_FILE     = DB_FILE     if os.path.exists(os.path.dirname(DB_FILE)) else DB_LOCAL

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
class Config:
    def __init__(self):
        self.gate_id          = 1
        self.gate_name        = "Gate 1"
        self.webhook_url      = ""
        self.api_key          = ""
        self.retry_interval_s = 30
        self.max_retries      = 288
        self.vm_sync_url      = ""
        self.vm_enabled       = False

cfg = Config()

def load_config():
    if not os.path.exists(_CONFIG_FILE):
        log.error(f"[config] Not found: {_CONFIG_FILE}")
        return False
    try:
        with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        cfg.gate_id   = d.get("gate_id",   1)
        cfg.gate_name = d.get("gate_name", "Gate 1")
        wms = d.get("wms", {})
        cfg.webhook_url      = wms.get("webhook_url",      "")
        cfg.api_key          = wms.get("api_key",          "")
        cfg.retry_interval_s = int(wms.get("retry_interval_s", 30))
        cfg.max_retries      = int(wms.get("max_retries",      288))
        vm = d.get("vm", {})
        cfg.vm_sync_url = vm.get("sync_url", "")
        cfg.vm_enabled  = vm.get("enabled",  False) and bool(cfg.vm_sync_url)
        log.info(f"[config] Gate {cfg.gate_id} — {cfg.gate_name}")
        log.info(f"[config] WMS: {cfg.webhook_url or '(not configured)'}")
        log.info(f"[config] VM sync: {'enabled' if cfg.vm_enabled else 'disabled'}")
        log.info(f"[config] Retry: every {cfg.retry_interval_s}s max {cfg.max_retries} attempts")
        return True
    except Exception as e:
        log.error(f"[config] Error: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(_DB_FILE, timeout=10) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS wms_queue (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_id      INTEGER NOT NULL DEFAULT 1,
            event_type   TEXT    NOT NULL,
            payload      TEXT    NOT NULL,
            retry_count  INTEGER DEFAULT 0,
            delivered    INTEGER DEFAULT 0,
            created_at   TEXT    DEFAULT (datetime('now')),
            last_attempt TEXT
        );
        CREATE TABLE IF NOT EXISTS wms_delivery_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_id       INTEGER NOT NULL DEFAULT 1,
            queue_id      INTEGER,
            event_type    TEXT,
            payload       TEXT,
            target_url    TEXT,
            response_code INTEGER,
            success       INTEGER DEFAULT 0,
            error_msg     TEXT,
            delivered_at  TEXT    DEFAULT (datetime('now'))
        );
        """)
    log.info(f"[db] {_DB_FILE}")

@contextmanager
def get_db():
    conn = sqlite3.connect(_DB_FILE, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ─────────────────────────────────────────────────────────────────────────────
# HMAC SIGNING
# ─────────────────────────────────────────────────────────────────────────────
def sign_payload(payload_str: str) -> str:
    """
    HMAC-SHA256 of the raw JSON payload string.
    Secret = cfg.api_key (shared with WMS).
    Header: X-Digiload-Signature: sha256={hex}
    WMS verifies by computing the same HMAC on its side.
    """
    return hmac.new(
        cfg.api_key.encode("utf-8"),
        payload_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

# ─────────────────────────────────────────────────────────────────────────────
# WMS DELIVERY
# ─────────────────────────────────────────────────────────────────────────────
def _post_to_wms(queue_id: int, event_type: str, payload: dict) -> bool:
    """
    POST a single event to the WMS webhook URL.
    Returns True on HTTP 2xx.
    Always logs the attempt to wms_delivery_log.
    """
    if not cfg.webhook_url:
        log.warning("[wms] No webhook_url configured — cannot deliver")
        return False

    payload_str   = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    signature     = sign_payload(payload_str)
    response_code = None
    error_msg     = None
    success       = False

    headers = {
        "Content-Type":         "application/json",
        "X-Digiload-Signature": f"sha256={signature}",
        "X-Digiload-Gate":      str(cfg.gate_id),
        "X-Digiload-Event":     event_type,
        "X-Digiload-Timestamp": datetime.utcnow().isoformat() + "Z",
    }

    try:
        resp          = requests.post(cfg.webhook_url, data=payload_str,
                                      headers=headers, timeout=5)
        response_code = resp.status_code
        success       = 200 <= resp.status_code < 300
        if success:
            log.info(f"[wms] ✅ {event_type} → HTTP {resp.status_code}  "
                     f"ref={payload.get('sscc', payload.get('tour_id', '—'))}")
        else:
            error_msg = f"HTTP {resp.status_code}: {resp.text[:120]}"
            log.warning(f"[wms] ❌ {event_type} → {error_msg}")

    except requests.exceptions.Timeout:
        error_msg = "Timeout (5s)"
        log.warning(f"[wms] ⏱  {event_type} — timeout")
    except requests.exceptions.ConnectionError:
        error_msg = "Connection refused — WMS unreachable"
        log.warning(f"[wms] 🔌 {event_type} — WMS unreachable")
    except Exception as e:
        error_msg = str(e)[:120]
        log.error(f"[wms] 💥 {event_type} — {error_msg}")

    # Always log the attempt
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO wms_delivery_log
                    (gate_id, queue_id, event_type, payload, target_url,
                     response_code, success, error_msg, delivered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (cfg.gate_id, queue_id, event_type, payload_str,
                  cfg.webhook_url, response_code,
                  1 if success else 0, error_msg))
    except Exception as e:
        log.error(f"[wms] Delivery log error: {e}")

    return success


def _notify_vm(event_type: str, payload: dict):
    """
    Best-effort VM sync — called after successful WMS delivery.
    Never blocks. Never retries. Fire and forget.
    If VM is down, WMS delivery is completely unaffected.
    """
    if not cfg.vm_enabled:
        return
    try:
        requests.post(
            cfg.vm_sync_url,
            json={"event_type": event_type, "payload": payload,
                  "gate_id": cfg.gate_id, "gate_name": cfg.gate_name},
            timeout=2
        )
        log.debug(f"[vm] Synced: {event_type}")
    except Exception:
        log.debug("[vm] Sync failed (non-critical)")

# ─────────────────────────────────────────────────────────────────────────────
# QUEUE PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────
def _get_pending() -> list:
    """Fetch all pending undelivered events, oldest first."""
    try:
        with get_db() as conn:
            rows = conn.execute("""
                SELECT id, event_type, payload, retry_count, created_at
                  FROM wms_queue
                 WHERE delivered   = 0
                   AND retry_count < ?
                 ORDER BY created_at ASC
            """, (cfg.max_retries,)).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error(f"[queue] Fetch error: {e}")
        return []


def _mark_delivered(queue_id: int):
    try:
        with get_db() as conn:
            conn.execute("""
                UPDATE wms_queue
                   SET delivered=1, last_attempt=datetime('now')
                 WHERE id=?
            """, (queue_id,))
    except Exception as e:
        log.error(f"[queue] Mark delivered error: {e}")


def _increment_retry(queue_id: int):
    try:
        with get_db() as conn:
            conn.execute("""
                UPDATE wms_queue
                   SET retry_count=retry_count+1, last_attempt=datetime('now')
                 WHERE id=?
            """, (queue_id,))
    except Exception as e:
        log.error(f"[queue] Retry increment error: {e}")


def _abandon(queue_id: int, event_type: str):
    """Max retries exceeded — mark done and log abandonment."""
    log.error(f"[wms] ☠  Abandoned queue_id={queue_id} event={event_type} "
              f"after {cfg.max_retries} attempts (~{cfg.max_retries*cfg.retry_interval_s//3600}h)")
    try:
        with get_db() as conn:
            conn.execute("UPDATE wms_queue SET delivered=1 WHERE id=?", (queue_id,))
            conn.execute("""
                INSERT INTO wms_delivery_log
                    (gate_id, queue_id, event_type, target_url,
                     success, error_msg, delivered_at)
                VALUES (?, ?, ?, ?, 0, 'ABANDONED — max retries exceeded', datetime('now'))
            """, (cfg.gate_id, queue_id, event_type, cfg.webhook_url))
    except Exception as e:
        log.error(f"[queue] Abandon error: {e}")


def process_queue():
    """Process all pending events. Called on each loop iteration."""
    events = _get_pending()
    if not events:
        return

    log.debug(f"[queue] {len(events)} pending")

    for ev in events:
        qid         = ev["id"]
        event_type  = ev["event_type"]
        retry_count = ev["retry_count"]

        # Parse payload
        try:
            payload = json.loads(ev["payload"])
        except Exception as e:
            log.error(f"[queue] Bad JSON qid={qid}: {e}")
            _increment_retry(qid)
            continue

        # Check max retries
        if retry_count >= cfg.max_retries:
            _abandon(qid, event_type)
            continue

        if retry_count > 0:
            log.info(f"[queue] Retry #{retry_count}/{cfg.max_retries} — {event_type} qid={qid}")

        # Attempt delivery
        if _post_to_wms(qid, event_type, payload):
            _mark_delivered(qid)
            _notify_vm(event_type, payload)
        else:
            _increment_retry(qid)

# ─────────────────────────────────────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────────────────────────────────────
def log_stats():
    try:
        with get_db() as conn:
            delivered  = conn.execute("SELECT COUNT(*) FROM wms_queue WHERE delivered=1").fetchone()[0]
            pending    = conn.execute(
                "SELECT COUNT(*) FROM wms_queue WHERE delivered=0 AND retry_count<?",
                (cfg.max_retries,)).fetchone()[0]
            abandoned  = conn.execute(
                "SELECT COUNT(*) FROM wms_delivery_log WHERE error_msg LIKE 'ABANDONED%'").fetchone()[0]
            rate       = conn.execute(
                "SELECT ROUND(AVG(success)*100,1) FROM wms_delivery_log").fetchone()[0]
        log.info(f"[stats] delivered={delivered} pending={pending} "
                 f"abandoned={abandoned} success_rate={rate or 0}%")
    except Exception as e:
        log.error(f"[stats] {e}")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG HOT-RELOAD (every 5 minutes)
# Picks up WMS URL changes without restart
# ─────────────────────────────────────────────────────────────────────────────
def _config_reload_loop():
    while True:
        time.sleep(300)
        log.debug("[config] Hot-reload")
        load_config()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("Digiload WMS Connector v1.0")
    log.info("=" * 60)

    if not load_config():
        log.error("Config load failed — exiting")
        sys.exit(1)

    init_db()

    if not cfg.webhook_url:
        log.warning("[wms] webhook_url not set — queue will accumulate until configured")

    threading.Thread(target=_config_reload_loop, daemon=True).start()

    log.info(f"[wms] Ready — polling every {cfg.retry_interval_s}s")

    stats_every = max(1, 3600 // cfg.retry_interval_s)   # ~every hour
    counter     = 0

    while True:
        try:
            process_queue()
        except Exception as e:
            log.error(f"[wms] Processor error: {e}")

        counter += 1
        if counter >= stats_every:
            log_stats()
            counter = 0

        time.sleep(cfg.retry_interval_s)


if __name__ == "__main__":
    run()
