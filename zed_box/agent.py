"""
Digiload Pro — Fleet Agent v1.0
Phase 2: Heartbeat, config push, file deploy, remote commands, log stream, MJPEG preview

Runs as: digiload-agent.service (systemd)
Port:    5002 (restricted to VM IP only via iptables)

Responsibilities:
    1. Heartbeat every 30s → Central VM
    2. Receive and apply config push from VM
    3. Receive and apply file updates (digiload_pro.py, wms_connector.py)
    4. Execute remote commands (restart_app, restart_wms, reboot, clear_clips)
    5. Serve log tail endpoint
    6. Serve MJPEG camera preview (5fps) for gate zone editor in admin console
    7. Gate zone push (browser draw → config.json → app restart)

Security:
    - Shared secret authentication (X-Agent-Secret header)
    - Secret stored in /etc/digiload/agent.secret (chmod 600)
    - Port 5002 restricted to VM IP only via iptables (set by install.sh)
    - File updates verified by MD5 checksum before writing
    - /status and /health are public (no auth) for local technician access

Note:
    Status endpoint (/status, /health) is served here on port 5002.
    digiload_pro.py no longer needs its own Flask server.
"""

import os
import sys
import json
import time
import hmac
import hashlib
import logging
import threading
import subprocess
import shutil
import sqlite3
import signal
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler

import requests
from flask import Flask, request, jsonify, Response

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs("/var/log/digiload", exist_ok=True)

log = logging.getLogger("digiload.agent")
log.setLevel(logging.DEBUG)

_fh = RotatingFileHandler(
    "/var/log/digiload/agent.log",
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
SECRET_FILE  = "/etc/digiload/agent.secret"
SECRET_LOCAL = "agent.secret"
APP_FILE     = "/opt/digiload/digiload_pro.py"
APP_LOCAL    = "digiload_pro.py"
WMS_FILE     = "/opt/digiload/wms_connector.py"
WMS_LOCAL    = "wms_connector.py"
CLIPS_DIR    = "/opt/digiload/clips"
CLIPS_LOCAL  = "clips"

_CONFIG_FILE = CONFIG_FILE if os.path.exists(CONFIG_FILE) else CONFIG_LOCAL
_DB_FILE     = DB_FILE     if os.path.exists(os.path.dirname(DB_FILE)) else DB_LOCAL
_SECRET_FILE = SECRET_FILE if os.path.exists(SECRET_FILE) else SECRET_LOCAL
_APP_FILE    = APP_FILE    if os.path.exists(os.path.dirname(APP_FILE)) else APP_LOCAL
_WMS_FILE    = WMS_FILE    if os.path.exists(os.path.dirname(WMS_FILE)) else WMS_LOCAL
_CLIPS_DIR   = CLIPS_DIR   if os.path.exists(os.path.dirname(CLIPS_DIR)) else CLIPS_LOCAL

AGENT_VERSION = "1.0"

def _get_app_version():
    try:
        import re
        with open(_APP_FILE, "r") as f:
            for line in f:
                m = re.search(r"v(\d+\.\d+)", line)
                if m: return m.group(0)
    except Exception:
        pass
    return "unknown"

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
class AgentConfig:
    def __init__(self):
        self.gate_id        = 1
        self.gate_name      = "Gate 1"
        self.central_url    = ""
        self.ip_mode        = "dhcp"
        self.static_ip      = ""
        self.modules_active = []

cfg = AgentConfig()

def load_config():
    if not os.path.exists(_CONFIG_FILE):
        log.error(f"[config] Not found: {_CONFIG_FILE}")
        return False
    try:
        with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        cfg.gate_id   = d.get("gate_id",   1)
        cfg.gate_name = d.get("gate_name", "Gate 1")
        cfg.ip_mode   = d.get("ip_mode",   "dhcp")
        cfg.static_ip = d.get("ip",        "")
        sync_url      = d.get("vm", {}).get("sync_url", "")
        cfg.central_url = sync_url.replace("/api/sync", "").rstrip("/") if sync_url else ""
        mods = d.get("modules", {})
        cfg.modules_active = [k for k,v in mods.items() if v.get("enabled", False)]
        log.info(f"[config] Gate {cfg.gate_id} — {cfg.gate_name}")
        log.info(f"[config] Central: {cfg.central_url or '(not configured)'}")
        return True
    except Exception as e:
        log.error(f"[config] {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# SECRET
# ─────────────────────────────────────────────────────────────────────────────
_secret = None

def load_secret():
    global _secret
    if os.path.exists(_SECRET_FILE):
        try:
            with open(_SECRET_FILE, "r") as f:
                _secret = f.read().strip()
            log.info(f"[secret] Loaded from {_SECRET_FILE}")
            return
        except Exception as e:
            log.error(f"[secret] {e}")
    import secrets as _s
    _secret = _s.token_hex(32)
    target = _SECRET_FILE if os.path.exists(os.path.dirname(_SECRET_FILE)) else SECRET_LOCAL
    try:
        with open(target, "w") as f: f.write(_secret)
        if target == _SECRET_FILE: os.chmod(target, 0o600)
        log.warning(f"[secret] Generated new secret → {target}")
    except Exception as e:
        log.warning(f"[secret] Could not save: {e}")

def verify_secret(provided: str) -> bool:
    if not _secret or not provided:
        return False
    return hmac.compare_digest(provided.strip(), _secret.strip())

def require_secret(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        provided = request.headers.get("X-Agent-Secret", "")
        if not verify_secret(provided):
            log.warning(f"[auth] Rejected {request.method} {request.path} from {request.remote_addr}")
            return jsonify({"ok": False, "error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM METRICS
# ─────────────────────────────────────────────────────────────────────────────
def get_cpu_pct():
    try:
        with open("/proc/loadavg") as f:
            load = float(f.read().split()[0])
        return round(min(load / (os.cpu_count() or 1) * 100, 100), 1)
    except Exception:
        return 0.0

def get_ram_mb():
    try:
        mem = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    mem[parts[0].rstrip(":")] = int(parts[1])
        return round((mem.get("MemTotal",0) - mem.get("MemAvailable",0)) / 1024, 1)
    except Exception:
        return 0.0

def get_disk_free_gb():
    try:
        d = _CLIPS_DIR if os.path.exists(_CLIPS_DIR) else "."
        return round(shutil.disk_usage(d).free / 1024**3, 2)
    except Exception:
        return 0.0

def get_uptime_s():
    try:
        with open("/proc/uptime") as f:
            return int(float(f.read().split()[0]))
    except Exception:
        return 0

def get_local_ip():
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "unknown"

def is_camera_ok():
    try:
        r = subprocess.run(["systemctl","is-active","digiload-app"],
                           capture_output=True, text=True)
        if r.stdout.strip() == "active": return True
    except Exception:
        pass
    try:
        r = subprocess.run(["pgrep","-f","digiload_pro.py"],
                           capture_output=True, text=True)
        return bool(r.stdout.strip())
    except Exception:
        return False

def read_app_state():
    try:
        conn = sqlite3.connect(_DB_FILE, timeout=2)
        row  = conn.execute(
            "SELECT app_mode, active_tour_id, current_sscc FROM system_state WHERE id=1"
        ).fetchone()
        conn.close()
        if row: return row[0], row[1], row[2]
    except Exception:
        pass
    return "UNKNOWN", None, None

# ─────────────────────────────────────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────────────────────────────────────
def build_heartbeat() -> dict:
    mode, tour, sscc = read_app_state()
    return {
        "gate_id":       cfg.gate_id,
        "gate_name":     cfg.gate_name,
        "agent_version": AGENT_VERSION,
        "app_version":   _get_app_version(),
        "app_mode":      mode,
        "active_tour_id":tour,
        "current_sscc":  sscc,
        "ip":            get_local_ip(),
        "ip_mode":       cfg.ip_mode,
        "cpu_pct":       get_cpu_pct(),
        "ram_mb":        get_ram_mb(),
        "disk_free_gb":  get_disk_free_gb(),
        "uptime_s":      get_uptime_s(),
        "camera_ok":     is_camera_ok(),
        "modules_active":cfg.modules_active,
        "timestamp":     datetime.utcnow().isoformat() + "Z",
    }

def heartbeat_loop():
    while True:
        if cfg.central_url:
            try:
                hb   = build_heartbeat()
                resp = requests.post(
                    f"{cfg.central_url}/agent/heartbeat",
                    json=hb,
                    headers={"X-Agent-Secret": _secret or ""},
                    timeout=5
                )
                if resp.status_code == 200:
                    log.debug(f"[heartbeat] ✅")
                    data = resp.json()
                    if data.get("pending_config"):
                        log.info("[heartbeat] Pending config from VM — pulling")
                        _pull_config_from_vm()
                else:
                    log.warning(f"[heartbeat] VM → {resp.status_code}")
            except requests.exceptions.ConnectionError:
                log.debug("[heartbeat] VM unreachable (non-critical)")
            except Exception as e:
                log.warning(f"[heartbeat] {e}")
        time.sleep(30)

def _pull_config_from_vm():
    try:
        resp = requests.get(
            f"{cfg.central_url}/agent/config/{cfg.gate_id}",
            headers={"X-Agent-Secret": _secret or ""},
            timeout=5
        )
        if resp.status_code == 200:
            new_cfg = resp.json().get("config")
            if new_cfg: _apply_config(new_cfg)
    except Exception as e:
        log.warning(f"[config_pull] {e}")

# ─────────────────────────────────────────────────────────────────────────────
# MJPEG PREVIEW
# ─────────────────────────────────────────────────────────────────────────────
_preview_frame = None
_preview_lock  = threading.Lock()

def update_preview(frame_bytes: bytes):
    global _preview_frame
    with _preview_lock:
        _preview_frame = frame_bytes

def _generate_mjpeg():
    import cv2, numpy as np
    while True:
        with _preview_lock:
            frame = _preview_frame
        if frame is not None:
            yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + frame + b"\r\n")
        else:
            blank = np.full((360, 640, 3), 40, dtype="uint8")
            ok, buf = cv2.imencode(".jpg", blank, [cv2.IMWRITE_JPEG_QUALITY, 60])
            if ok:
                yield (b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + bytes(buf) + b"\r\n")
        time.sleep(0.2)    # 5fps

# ─────────────────────────────────────────────────────────────────────────────
# FILE OPERATIONS
# ─────────────────────────────────────────────────────────────────────────────
def _verify_md5(content: str, expected: str) -> bool:
    if not expected: return True
    return hashlib.md5(content.encode()).hexdigest() == expected

def _apply_config(new_config: dict) -> dict:
    backup = _CONFIG_FILE + ".bak"
    try:
        if os.path.exists(_CONFIG_FILE):
            shutil.copy2(_CONFIG_FILE, backup)
        with open(_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(new_config, f, indent=4)
        load_config()
        log.info("[config] Applied new config from VM")
        return {"ok": True}
    except Exception as e:
        log.error(f"[config] Apply error: {e}")
        if os.path.exists(backup):
            shutil.copy2(backup, _CONFIG_FILE)
        return {"ok": False, "error": str(e)}

def _apply_file(target: str, content: str, md5: str = "") -> dict:
    try:
        if not _verify_md5(content, md5):
            return {"ok": False, "error": "MD5 mismatch — file rejected"}
        backup = target + ".bak"
        if os.path.exists(target):
            shutil.copy2(target, backup)
        with open(target, "w", encoding="utf-8") as f:
            f.write(content)
        log.info(f"[deploy] Written: {target} ({len(content)} chars)")
        return {"ok": True, "path": target}
    except Exception as e:
        log.error(f"[deploy] {e}")
        return {"ok": False, "error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEMD / COMMANDS
# ─────────────────────────────────────────────────────────────────────────────
def _systemctl(action: str, service: str) -> dict:
    try:
        r  = subprocess.run(["systemctl", action, service],
                            capture_output=True, text=True, timeout=15)
        ok = r.returncode == 0
        log.info(f"[systemctl] {action} {service} → {'OK' if ok else 'FAILED'}")
        return {"ok": ok, "service": service, "action": action,
                "stderr": r.stderr[:200] if not ok else ""}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _clear_clips() -> dict:
    try:
        count = 0
        d = _CLIPS_DIR if os.path.exists(_CLIPS_DIR) else CLIPS_LOCAL
        for f in os.listdir(d):
            fp = os.path.join(d, f)
            if os.path.isfile(fp) and f.endswith(".mp4"):
                os.remove(fp); count += 1
        log.info(f"[clear_clips] Deleted {count} clips")
        return {"ok": True, "deleted": count}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _reboot() -> dict:
    log.warning("[reboot] In 5 seconds...")
    threading.Thread(target=lambda: (time.sleep(5), subprocess.run(["reboot"])),
                     daemon=True).start()
    return {"ok": True, "message": "Rebooting in 5 seconds"}

ALLOWED_COMMANDS = {
    "restart_app":   lambda: _systemctl("restart", "digiload-app"),
    "restart_wms":   lambda: _systemctl("restart", "digiload-wms"),
    "restart_agent": lambda: _systemctl("restart", "digiload-agent"),
    "stop_app":      lambda: _systemctl("stop",    "digiload-app"),
    "start_app":     lambda: _systemctl("start",   "digiload-app"),
    "clear_clips":   _clear_clips,
    "reboot":        _reboot,
}

# ─────────────────────────────────────────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────────────────────────────────────────
import logging as _plog
_plog.getLogger("werkzeug").setLevel(_plog.ERROR)

app = Flask("digiload_agent")

import queue

# ─────────────────────────────────────────────────────────────────────────────
# DRIVER SSE STATE (DL-028 — tablet connects directly, no VM)
# ─────────────────────────────────────────────────────────────────────────────
# Connected driver tablet SSE clients — one queue per connection
_driver_clients: list = []
_driver_lock    = threading.Lock()
_last_state     = {"mode": None, "tour": None, "sscc": None}

def _push_driver_event(mode, tour, sscc, loaded=0, total=0):
    """Push state update to all connected driver tablets."""
    import json as _json
    payload = _json.dumps({
        "mode":    mode,
        "tour_id": tour,
        "sscc":    sscc,
        "loaded":  loaded,
        "total":   total,
        "gate_id": cfg.gate_id,
        "ts":      datetime.utcnow().isoformat() + "Z",
    })
    data = f"data: {payload}\n\n"
    with _driver_lock:
        dead = []
        for q in _driver_clients:
            try:
                q.put_nowait(data)
            except Exception:
                dead.append(q)
        for q in dead:
            _driver_clients.remove(q)

def _driver_poll_loop():
    """
    Polls SQLite every 500ms for state changes.
    Pushes SSE event to all connected tablets when state changes.
    VM-independent — works even if central VM is down.
    """
    global _last_state
    while True:
        try:
            mode, tour, sscc = read_app_state()
            # Also get progress if mission active
            loaded, total = 0, 0
            if tour:
                try:
                    conn = sqlite3.connect(_DB_FILE, timeout=2)
                    row  = conn.execute(
                        "SELECT COUNT(*) FROM pallets WHERE tour_id=? AND status='LOADED'",
                        (tour,)
                    ).fetchone()
                    tot = conn.execute(
                        "SELECT COUNT(*) FROM pallets WHERE tour_id=?",
                        (tour,)
                    ).fetchone()
                    conn.close()
                    loaded = row[0] if row else 0
                    total  = tot[0] if tot else 0
                except Exception:
                    pass

            current = {"mode": mode, "tour": tour, "sscc": sscc}
            if current != _last_state:
                _last_state = current
                _push_driver_event(mode, tour, sscc, loaded, total)
        except Exception as e:
            log.debug(f"[driver_poll] {e}")
        time.sleep(0.5)

def status():
    mode, tour, sscc = read_app_state()
    return jsonify({
        "gate_id":        cfg.gate_id,
        "gate_name":      cfg.gate_name,
        "agent_version":  AGENT_VERSION,
        "app_version":    _get_app_version(),
        "app_mode":       mode,
        "active_tour_id": tour,
        "current_sscc":   sscc,
        "camera_ok":      is_camera_ok(),
        "cpu_pct":        get_cpu_pct(),
        "ram_mb":         get_ram_mb(),
        "disk_free_gb":   get_disk_free_gb(),
        "uptime_s":       get_uptime_s(),
        "modules_active": cfg.modules_active,
        "timestamp":      datetime.utcnow().isoformat() + "Z",
    })

@app.route("/health")
def health():
    return jsonify({"ok": True, "gate_id": cfg.gate_id})

# ─────────────────────────────────────────────────────────────────────────────
# DRIVER TABLET ENDPOINTS — PUBLIC (no secret required, DL-028)
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/driver/stream")
def driver_stream():
    """
    SSE stream for driver tablet — pushed on every state change.
    Tablet connects here directly — no VM in the loop.
    Public: no authentication required.
    """
    q = queue.Queue(maxsize=20)
    with _driver_lock:
        _driver_clients.append(q)

    def generate():
        # Send current state immediately on connect
        mode, tour, sscc = read_app_state()
        import json as _json
        yield f"data: {_json.dumps({'mode':mode,'tour_id':tour,'sscc':sscc,'gate_id':cfg.gate_id})}\n\n"
        # Then stream changes
        while True:
            try:
                data = q.get(timeout=25)
                yield data
            except queue.Empty:
                yield ": keepalive\n\n"   # prevents proxy timeout
            except GeneratorExit:
                break
        with _driver_lock:
            try: _driver_clients.remove(q)
            except ValueError: pass

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering":"no",       # disable nginx buffering
            "Connection":       "keep-alive",
        }
    )

@app.route("/driver/status")
def driver_status():
    """
    Polling fallback for tablets that don't support SSE.
    Public: no authentication required.
    """
    mode, tour, sscc = read_app_state()
    loaded, total = 0, 0
    if tour:
        try:
            conn = sqlite3.connect(_DB_FILE, timeout=2)
            row  = conn.execute(
                "SELECT COUNT(*) FROM pallets WHERE tour_id=? AND status='LOADED'",
                (tour,)
            ).fetchone()
            tot  = conn.execute(
                "SELECT COUNT(*) FROM pallets WHERE tour_id=?",
                (tour,)
            ).fetchone()
            conn.close()
            loaded = row[0] if row else 0
            total  = tot[0]  if tot  else 0
        except Exception:
            pass
    return jsonify({
        "gate_id":  cfg.gate_id,
        "gate_name":cfg.gate_name,
        "mode":     mode,
        "tour_id":  tour,
        "sscc":     sscc,
        "loaded":   loaded,
        "total":    total,
        "ts":       datetime.utcnow().isoformat() + "Z",
    })


@require_secret
def apply_config():
    data = request.get_json(silent=True) or {}
    new_cfg = data.get("config")
    if not new_cfg:
        return jsonify({"ok": False, "error": "No config provided"}), 400
    result  = _apply_config(new_cfg)
    if result["ok"] and data.get("restart_app", False):
        threading.Thread(
            target=lambda: (time.sleep(1), _systemctl("restart","digiload-app")),
            daemon=True
        ).start()
        result["app_restarted"] = True
    return jsonify(result)

@app.route("/agent/deploy", methods=["POST"])
@require_secret
def deploy():
    data    = request.get_json(silent=True) or {}
    version = data.get("version", "unknown")
    files   = data.get("files",   [])
    restart = data.get("restart", True)
    target_map = {
        "digiload_pro.py":  _APP_FILE,
        "wms_connector.py": _WMS_FILE,
    }
    log.info(f"[deploy] v{version} — {len(files)} file(s)")
    results = []
    for f in files:
        fname  = f.get("path","")
        target = target_map.get(os.path.basename(fname))
        if not target:
            results.append({"path": fname, "ok": False, "error": "Unknown target"}); continue
        results.append({"path": fname, **_apply_file(target, f.get("content",""), f.get("md5",""))})
    all_ok = all(r.get("ok") for r in results)
    if all_ok and restart:
        threading.Thread(
            target=lambda: (time.sleep(1),
                            _systemctl("restart","digiload-app"),
                            _systemctl("restart","digiload-wms")),
            daemon=True
        ).start()
    return jsonify({"ok": all_ok, "version": version, "results": results})

@app.route("/agent/command", methods=["POST"])
@require_secret
def command():
    cmd = (request.get_json(silent=True) or {}).get("cmd","")
    if cmd not in ALLOWED_COMMANDS:
        return jsonify({"ok": False, "error": f"Unknown: {cmd}",
                        "allowed": list(ALLOWED_COMMANDS.keys())}), 400
    log.info(f"[command] {cmd}")
    return jsonify(ALLOWED_COMMANDS[cmd]())

@app.route("/agent/gate-zone", methods=["POST"])
@require_secret
def gate_zone():
    rect = (request.get_json(silent=True) or {}).get("rect")
    if not rect or len(rect) != 4:
        return jsonify({"ok": False, "error": "rect must be [x, y, w, h]"}), 400
    try:
        with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        config.setdefault("gate",{})["rect"] = [int(v) for v in rect]
        with open(_CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4)
        log.info(f"[gate_zone] {rect}")
        threading.Thread(
            target=lambda: (time.sleep(0.5), _systemctl("restart","digiload-app")),
            daemon=True
        ).start()
        return jsonify({"ok": True, "rect": rect, "app_restarted": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/agent/preview")
@require_secret
def preview():
    return Response(_generate_mjpeg(),
                    mimetype="multipart/x-mixed-replace; boundary=frame")

@app.route("/agent/logs")
@require_secret
def logs():
    n = int(request.args.get("n", 100))
    try:
        r = subprocess.run(["tail","-n",str(n),"/var/log/digiload/app.log"],
                           capture_output=True, text=True, timeout=5)
        return jsonify({"ok": True, "lines": r.stdout.splitlines()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/agent/ping")
@require_secret
def ping():
    return jsonify({"ok": True, "gate_id": cfg.gate_id,
                    "ts": datetime.utcnow().isoformat()})

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG HOT-RELOAD + GRACEFUL SHUTDOWN
# ─────────────────────────────────────────────────────────────────────────────
def _config_reload_loop():
    while True:
        time.sleep(300)
        load_config()

signal.signal(signal.SIGTERM, lambda s,f: sys.exit(0))

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info(f"Digiload Agent v{AGENT_VERSION} — Starting")
    log.info("=" * 60)
    load_secret()
    load_config()
    log.info(f"[agent] Gate {cfg.gate_id} — {cfg.gate_name}")
    log.info(f"[agent] Central: {cfg.central_url or '(not configured)'}")
    threading.Thread(target=heartbeat_loop,      daemon=True).start()
    threading.Thread(target=_config_reload_loop,  daemon=True).start()
    threading.Thread(target=_driver_poll_loop,    daemon=True).start()
    log.info("[agent] Port 5002 — ready")
    app.run(host="0.0.0.0", port=5002, debug=False, threaded=True)

if __name__ == "__main__":
    run()
