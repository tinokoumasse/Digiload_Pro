"""
Digiload Pro — Central VM Application v1.0
Phase 3: Global dashboard, admin console, agent API, WMS REST API, auth

Runs on: Central VM (Docker, port 5001)
Access:  http://vm-ip:5001 (via Nginx in production)

Responsibilities:
    - Receive heartbeats from all ZED Box agents
    - Push config / deploy files / send commands to agents
    - Global dashboard (all gates, live status via SocketIO)
    - Gate detail view (MJPEG proxy, zone editor, logs)
    - Mission management (CSV import + JSON API)
    - Auth (JWT + bcrypt, 3 roles)
    - REST API for WMS integration (API key auth)
    - Serve ZED Box release files (digiload_pro.py etc.)
"""

import os
import io
import csv
import json
import time
import hmac
import hashlib
import logging
import threading
import uuid
import datetime as dt
from functools import wraps
from logging.handlers import RotatingFileHandler

import psycopg2
import psycopg2.extras
import requests
import jwt
import bcrypt
from flask import (Flask, render_template, request, jsonify,
                   redirect, url_for, session, send_from_directory,
                   Response, stream_with_context, flash)
from flask_socketio import SocketIO, emit

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("digiload.central")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SECRET_KEY       = os.environ.get("SECRET_KEY",       "dev-secret-change-in-production")
LICENSE_SECRET   = os.environ.get("LICENSE_SECRET",   "DIGILOAD_LICENSE_SECRET")
DB_HOST          = os.environ.get("DB_HOST",          "localhost")
DB_PORT          = int(os.environ.get("DB_PORT",      5432))
DB_NAME          = os.environ.get("DB_NAME",          "digiload")
DB_USER          = os.environ.get("DB_USER",          "digiload")
DB_PASSWORD      = os.environ.get("DB_PASSWORD",      "")
CLIPS_DIR        = os.environ.get("CLIPS_DIR",        "clips")
REPORTS_DIR      = os.environ.get("REPORTS_DIR",      "reports")
RELEASES_DIR     = os.environ.get("RELEASES_DIR",     "releases")
RETENTION_DAYS   = int(os.environ.get("CLIP_RETENTION_DAYS", 30))
JWT_EXPIRY_HOURS = 8
MAX_LOGIN_FAILS  = 10

os.makedirs(CLIPS_DIR,    exist_ok=True)
os.makedirs(REPORTS_DIR,  exist_ok=True)
os.makedirs(RELEASES_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# FLASK + SOCKETIO
# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["JSON_SORT_KEYS"] = False

sio = SocketIO(app, cors_allowed_origins="*", async_mode="gevent",
               logger=False, engineio_logger=False)

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.RealDictCursor
    )

def init_db():
    """Create all tables if they don't exist."""
    sql = """
    CREATE TABLE IF NOT EXISTS users (
        id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        email         TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role          TEXT NOT NULL CHECK (role IN ('ADMIN','SUPERVISOR','OPERATOR')),
        active        BOOLEAN DEFAULT true,
        login_fails   INTEGER DEFAULT 0,
        created_at    TIMESTAMPTZ DEFAULT now(),
        last_login    TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS user_gates (
        user_id  UUID REFERENCES users(id) ON DELETE CASCADE,
        gate_id  INTEGER NOT NULL,
        PRIMARY KEY (user_id, gate_id)
    );

    CREATE TABLE IF NOT EXISTS api_keys (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name        TEXT NOT NULL,
        key_hash    TEXT NOT NULL,
        created_at  TIMESTAMPTZ DEFAULT now(),
        expires_at  TIMESTAMPTZ,
        active      BOOLEAN DEFAULT true,
        last_used   TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS gates (
        id                INTEGER PRIMARY KEY,
        name              TEXT NOT NULL DEFAULT 'Gate',
        ip_mode           TEXT DEFAULT 'dhcp',
        ip                TEXT,
        last_heartbeat_ip TEXT,
        status            TEXT DEFAULT 'OFFLINE',
        last_heartbeat    TIMESTAMPTZ,
        app_version       TEXT,
        agent_version     TEXT,
        app_mode          TEXT DEFAULT 'IDLE',
        active_tour_id    UUID,
        cpu_pct           NUMERIC,
        ram_mb            NUMERIC,
        disk_free_gb      NUMERIC,
        uptime_s          INTEGER,
        camera_ok         BOOLEAN,
        modules_active    TEXT[] DEFAULT '{}',
        pending_config    BOOLEAN DEFAULT false
    );

    CREATE TABLE IF NOT EXISTS missions (
        id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        wms_mission_id TEXT UNIQUE,
        gate_id        INTEGER REFERENCES gates(id),
        name           TEXT NOT NULL,
        truck_id       TEXT,
        scheduled_at   TIMESTAMPTZ,
        activated_at   TIMESTAMPTZ,
        completed_at   TIMESTAMPTZ,
        status         TEXT DEFAULT 'WAITING'
            CHECK (status IN ('WAITING','ACTIVE','COMPLETED','CANCELLED','ARCHIVED')),
        total_pallets  INTEGER DEFAULT 0,
        source         TEXT DEFAULT 'csv',
        created_at     TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS pallets (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        mission_id  UUID REFERENCES missions(id) ON DELETE CASCADE,
        gate_id     INTEGER,
        sscc        TEXT NOT NULL,
        sku         TEXT,
        weight_kg   NUMERIC,
        status      TEXT DEFAULT 'WAITING'
            CHECK (status IN ('WAITING','LOADED','FLAGGED')),
        scan_time   TIMESTAMPTZ,
        loaded_at   TIMESTAMPTZ,
        forklift_id INTEGER,
        UNIQUE(mission_id, sscc)
    );

    CREATE TABLE IF NOT EXISTS clips (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        mission_id  UUID REFERENCES missions(id),
        gate_id     INTEGER,
        sscc        TEXT,
        event_type  TEXT DEFAULT 'VALIDATED',
        filename    TEXT NOT NULL,
        expires_at  TIMESTAMPTZ DEFAULT (now() + INTERVAL '30 days'),
        deleted     BOOLEAN DEFAULT false,
        created_at  TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS audit_log (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        user_id     UUID,
        user_email  TEXT,
        action      TEXT NOT NULL,
        target_type TEXT,
        target_id   TEXT,
        details     JSONB,
        ip_address  TEXT,
        created_at  TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS gate_config_queue (
        id          SERIAL PRIMARY KEY,
        gate_id     INTEGER NOT NULL,
        config      JSONB NOT NULL,
        created_at  TIMESTAMPTZ DEFAULT now(),
        applied_at  TIMESTAMPTZ
    );
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    log.info("[db] Schema initialised")

def _q(sql, params=None, fetchone=False, fetchall=False):
    """Quick query helper."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            if fetchone:  return cur.fetchone()
            if fetchall:  return cur.fetchall()
            conn.commit()

# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY GATE STATE
# Updated on every heartbeat — used for SocketIO push
# ─────────────────────────────────────────────────────────────────────────────
_gate_state = {}   # gate_id → heartbeat dict
_gate_lock  = threading.Lock()

def update_gate_state(gate_id: int, hb: dict):
    with _gate_lock:
        _gate_state[gate_id] = {**hb, "last_seen": time.time()}

def get_all_gate_states() -> list:
    with _gate_lock:
        return list(_gate_state.values())

# ─────────────────────────────────────────────────────────────────────────────
# AUTH HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _hash_password(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt(12)).decode()

def _check_password(pw: str, hashed: str) -> bool:
    return bcrypt.checkpw(pw.encode(), hashed.encode())

def _make_jwt(user_id: str, email: str, role: str, gate_ids: list) -> str:
    payload = {
        "sub":      user_id,
        "email":    email,
        "role":     role,
        "gates":    gate_ids,
        "exp":      dt.datetime.utcnow() + dt.timedelta(hours=JWT_EXPIRY_HOURS),
        "iat":      dt.datetime.utcnow(),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def _verify_jwt(token: str) -> dict | None:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

def _verify_api_key(raw_key: str) -> bool:
    """Check raw API key against bcrypt hashes in DB."""
    rows = _q("SELECT key_hash FROM api_keys WHERE active=true", fetchall=True)
    if not rows:
        return False
    for row in rows:
        if bcrypt.checkpw(raw_key.encode(), row["key_hash"].encode()):
            _q("UPDATE api_keys SET last_used=now() WHERE key_hash=%s",
               (row["key_hash"],))
            return True
    return False

def _audit(action, user_id=None, user_email=None,
           target_type=None, target_id=None, details=None):
    try:
        _q("""INSERT INTO audit_log
              (user_id, user_email, action, target_type, target_id, details, ip_address)
              VALUES (%s,%s,%s,%s,%s,%s,%s)""",
           (user_id, user_email, action, target_type, str(target_id) if target_id else None,
            json.dumps(details) if details else None,
            request.remote_addr if request else None))
    except Exception as e:
        log.error(f"[audit] {e}")

# ─────────────────────────────────────────────────────────────────────────────
# AUTH DECORATORS
# ─────────────────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.cookies.get("token") or \
                request.headers.get("Authorization","").replace("Bearer ","")
        payload = _verify_jwt(token)
        if not payload:
            if request.is_json:
                return jsonify({"ok": False, "error": "Unauthorized"}), 401
            return redirect(url_for("login_page"))
        request.user = payload
        return f(*args, **kwargs)
    return decorated

def role_required(*roles):
    def decorator(f):
        @wraps(f)
        @login_required
        def decorated(*args, **kwargs):
            if request.user.get("role") not in roles:
                return jsonify({"ok": False, "error": "Forbidden"}), 403
            return f(*args, **kwargs)
        return decorated
    return decorator

def api_key_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization","")
        if not auth.startswith("Bearer "):
            return jsonify({"ok": False, "error": "API key required"}), 401
        raw = auth.replace("Bearer ","").strip()
        if not _verify_api_key(raw):
            return jsonify({"ok": False, "error": "Invalid API key"}), 401
        return f(*args, **kwargs)
    return decorated

def agent_secret_required(f):
    """For agent heartbeat endpoint — validates X-Agent-Secret."""
    @wraps(f)
    def decorated(*args, **kwargs):
        # Agent secrets are per-gate — we just verify it's non-empty
        # Full per-gate secret validation added in Phase 5
        secret = request.headers.get("X-Agent-Secret","")
        if not secret:
            return jsonify({"ok": False, "error": "Agent secret required"}), 401
        return f(*args, **kwargs)
    return decorated

def _can_access_gate(user_payload: dict, gate_id: int) -> bool:
    if user_payload.get("role") == "ADMIN":
        return True
    return gate_id in (user_payload.get("gates") or [])

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: push command to ZED Box agent
# ─────────────────────────────────────────────────────────────────────────────
def _gate_ip(gate_id: int) -> str | None:
    row = _q("SELECT last_heartbeat_ip, ip, ip_mode FROM gates WHERE id=%s",
             (gate_id,), fetchone=True)
    if not row: return None
    if row["ip_mode"] == "static" and row["ip"]:
        return row["ip"]
    return row["last_heartbeat_ip"]

def _push_to_agent(gate_id: int, endpoint: str, data: dict,
                   agent_secret: str = "") -> dict:
    ip = _gate_ip(gate_id)
    if not ip:
        return {"ok": False, "error": "Gate IP unknown"}
    url = f"http://{ip}:5002{endpoint}"
    try:
        resp = requests.post(url, json=data,
                             headers={"X-Agent-Secret": agent_secret},
                             timeout=8)
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {"ok": False, "error": "Agent unreachable"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _get_from_agent(gate_id: int, endpoint: str,
                    agent_secret: str = "") -> dict:
    ip = _gate_ip(gate_id)
    if not ip:
        return {"ok": False, "error": "Gate IP unknown"}
    url = f"http://{ip}:5002{endpoint}"
    try:
        resp = requests.get(url,
                            headers={"X-Agent-Secret": agent_secret},
                            timeout=8)
        return resp.json()
    except requests.exceptions.ConnectionError:
        return {"ok": False, "error": "Agent unreachable"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND: broadcast gate states via SocketIO every 2s
# ─────────────────────────────────────────────────────────────────────────────
def _broadcast_loop():
    while True:
        time.sleep(2)
        try:
            states = get_all_gate_states()
            # Mark gates as OFFLINE if no heartbeat in 60s
            now = time.time()
            for s in states:
                if now - s.get("last_seen", 0) > 60:
                    s["status"] = "OFFLINE"
                else:
                    s["status"] = "ONLINE"
            sio.emit("fleet_update", {"gates": states})
        except Exception as e:
            log.error(f"[broadcast] {e}")

threading.Thread(target=_broadcast_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# ── AUTH ROUTES ──────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/login", methods=["GET"])
def login_page():
    return render_template("login.html")

@app.route("/login", methods=["POST"])
def login():
    data  = request.get_json(silent=True) or request.form
    email = (data.get("email") or "").strip().lower()
    pw    = data.get("password") or ""

    user = _q("SELECT * FROM users WHERE email=%s AND active=true",
              (email,), fetchone=True)

    if not user or not _check_password(pw, user["password_hash"]):
        if user:
            fails = user["login_fails"] + 1
            _q("UPDATE users SET login_fails=%s WHERE id=%s",
               (fails, user["id"]))
            if fails >= MAX_LOGIN_FAILS:
                _q("UPDATE users SET active=false WHERE id=%s", (user["id"],))
                log.warning(f"[auth] Account locked: {email}")
        log.warning(f"[auth] Failed login: {email}")
        _audit("login.failed", user_email=email)
        if request.is_json:
            return jsonify({"ok": False, "error": "Invalid credentials"}), 401
        flash("Invalid email or password")
        return redirect(url_for("login_page"))

    # Reset fail counter
    _q("UPDATE users SET login_fails=0, last_login=now() WHERE id=%s",
       (user["id"],))

    # Get gate assignments
    gates = [r["gate_id"] for r in
             _q("SELECT gate_id FROM user_gates WHERE user_id=%s",
                (user["id"],), fetchall=True) or []]

    token = _make_jwt(str(user["id"]), user["email"], user["role"], gates)
    _audit("login.success", user_id=user["id"], user_email=user["email"])

    if request.is_json:
        return jsonify({"ok": True, "token": token, "role": user["role"]})

    resp = redirect(url_for("dashboard"))
    resp.set_cookie("token", token, httponly=True, max_age=JWT_EXPIRY_HOURS*3600)
    return resp

@app.route("/logout")
def logout():
    resp = redirect(url_for("login_page"))
    resp.delete_cookie("token")
    return resp

# ─────────────────────────────────────────────────────────────────────────────
# ── DASHBOARD ROUTES ─────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
@login_required
def dashboard():
    gates = _q("SELECT * FROM gates ORDER BY id", fetchall=True) or []
    return render_template("index.html",
                           user=request.user,
                           gates=gates)

@app.route("/gate/<int:gate_id>")
@login_required
def gate_detail(gate_id):
    if not _can_access_gate(request.user, gate_id):
        return redirect(url_for("dashboard"))
    gate = _q("SELECT * FROM gates WHERE id=%s", (gate_id,), fetchone=True)
    if not gate:
        return redirect(url_for("dashboard"))
    missions = _q("""SELECT * FROM missions WHERE gate_id=%s
                     ORDER BY created_at DESC LIMIT 20""",
                  (gate_id,), fetchall=True) or []
    return render_template("gate.html",
                           user=request.user,
                           gate=gate,
                           missions=missions)

@app.route("/missions")
@login_required
def missions_page():
    role = request.user.get("role")
    if role == "ADMIN":
        missions = _q("""SELECT m.*, g.name as gate_name
                         FROM missions m LEFT JOIN gates g ON m.gate_id=g.id
                         WHERE m.status != 'ARCHIVED'
                         ORDER BY m.created_at DESC""", fetchall=True) or []
    else:
        gate_ids = request.user.get("gates") or []
        if not gate_ids:
            missions = []
        else:
            missions = _q("""SELECT m.*, g.name as gate_name
                             FROM missions m LEFT JOIN gates g ON m.gate_id=g.id
                             WHERE m.gate_id = ANY(%s) AND m.status != 'ARCHIVED'
                             ORDER BY m.created_at DESC""",
                          (gate_ids,), fetchall=True) or []
    gates = _q("SELECT id, name FROM gates ORDER BY id", fetchall=True) or []
    return render_template("missions.html",
                           user=request.user,
                           missions=missions,
                           gates=gates)

@app.route("/admin")
@role_required("ADMIN")
def admin_page():
    users = _q("SELECT id,email,role,active,created_at,last_login FROM users ORDER BY created_at",
               fetchall=True) or []
    gates = _q("SELECT * FROM gates ORDER BY id", fetchall=True) or []
    api_keys = _q("SELECT id,name,created_at,expires_at,active,last_used FROM api_keys ORDER BY created_at",
                  fetchall=True) or []
    return render_template("admin.html",
                           user=request.user,
                           users=users,
                           gates=gates,
                           api_keys=api_keys)

@app.route("/audit")
@role_required("ADMIN")
def audit_page():
    logs = _q("""SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 500""",
              fetchall=True) or []
    return render_template("audit.html", user=request.user, logs=logs)

# ─────────────────────────────────────────────────────────────────────────────
# ── MISSION API (dashboard) ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/missions", methods=["GET"])
@login_required
def api_missions():
    role     = request.user.get("role")
    gate_ids = request.user.get("gates") or []
    if role == "ADMIN":
        rows = _q("""SELECT m.*, g.name as gate_name,
                     (SELECT COUNT(*) FROM pallets p WHERE p.mission_id=m.id AND p.status='LOADED') as loaded
                     FROM missions m LEFT JOIN gates g ON m.gate_id=g.id
                     WHERE m.status != 'ARCHIVED' ORDER BY m.created_at DESC""",
                  fetchall=True)
    else:
        rows = _q("""SELECT m.*, g.name as gate_name,
                     (SELECT COUNT(*) FROM pallets p WHERE p.mission_id=m.id AND p.status='LOADED') as loaded
                     FROM missions m LEFT JOIN gates g ON m.gate_id=g.id
                     WHERE m.gate_id=ANY(%s) AND m.status != 'ARCHIVED'
                     ORDER BY m.created_at DESC""",
                  (gate_ids,), fetchall=True)
    active = _q("SELECT id FROM missions WHERE status='ACTIVE' LIMIT 1", fetchone=True)
    return jsonify({
        "ok": True,
        "missions": [dict(r) for r in (rows or [])],
        "active_tour_id": str(active["id"]) if active else None
    })

@app.route("/api/missions/import", methods=["POST"])
@login_required
def import_mission():
    if request.user.get("role") == "OPERATOR":
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    name     = (request.form.get("name") or "").strip()
    gate_id  = request.form.get("gate_id")
    csv_file = request.files.get("csv")

    if not name:
        return jsonify({"ok": False, "error": "Mission name required"}), 400
    if not gate_id:
        return jsonify({"ok": False, "error": "Gate ID required"}), 400
    if not csv_file:
        return jsonify({"ok": False, "error": "CSV file required"}), 400

    gate_id = int(gate_id)
    if not _can_access_gate(request.user, gate_id):
        return jsonify({"ok": False, "error": "No access to this gate"}), 403

    # Parse CSV
    try:
        content   = csv_file.read().decode("utf-8-sig")
        reader    = csv.DictReader(io.StringIO(content))
        sscc_col  = None
        pallets   = []
        for row in reader:
            # Auto-detect SSCC column
            if sscc_col is None:
                for col in row.keys():
                    if "sscc" in col.lower() or "barcode" in col.lower() or "code" in col.lower():
                        sscc_col = col
                        break
                if sscc_col is None:
                    sscc_col = list(row.keys())[0]
            sscc = str(row.get(sscc_col, "")).strip()
            if sscc:
                pallets.append({
                    "sscc":      sscc,
                    "sku":       row.get("sku", row.get("SKU","")),
                    "weight_kg": row.get("weight", row.get("weight_kg","")) or None,
                })
    except Exception as e:
        return jsonify({"ok": False, "error": f"CSV parse error: {e}"}), 400

    if not pallets:
        return jsonify({"ok": False, "error": "No pallets found in CSV"}), 400

    # Insert mission + pallets
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO missions (gate_id, name, total_pallets, source)
                           VALUES (%s,%s,%s,'csv') RETURNING id""",
                        (gate_id, name, len(pallets)))
            mission_id = cur.fetchone()["id"]
            for p in pallets:
                cur.execute("""INSERT INTO pallets (mission_id, gate_id, sscc, sku, weight_kg)
                               VALUES (%s,%s,%s,%s,%s)
                               ON CONFLICT (mission_id, sscc) DO NOTHING""",
                            (mission_id, gate_id, p["sscc"],
                             p["sku"] or None,
                             float(p["weight_kg"]) if p["weight_kg"] else None))
        conn.commit()

    _audit("mission.imported", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="mission", target_id=mission_id,
           details={"name": name, "gate_id": gate_id, "pallets": len(pallets)})
    log.info(f"[mission] Imported '{name}' gate={gate_id} pallets={len(pallets)}")
    return jsonify({"ok": True, "mission_id": str(mission_id),
                    "count": len(pallets)})

@app.route("/api/missions/<mission_id>/activate", methods=["POST"])
@login_required
def activate_mission(mission_id):
    if request.user.get("role") == "OPERATOR":
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    mission = _q("SELECT * FROM missions WHERE id=%s", (mission_id,), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Mission not found"}), 404
    if not _can_access_gate(request.user, mission["gate_id"]):
        return jsonify({"ok": False, "error": "No access to this gate"}), 403
    if mission["status"] not in ("WAITING",):
        return jsonify({"ok": False, "error": f"Cannot activate: status={mission['status']}"}), 400
    # Check no other mission active on this gate
    active = _q("SELECT id FROM missions WHERE gate_id=%s AND status='ACTIVE'",
                (mission["gate_id"],), fetchone=True)
    if active:
        return jsonify({"ok": False, "error": "Another mission is already active on this gate"}), 400
    _q("UPDATE missions SET status='ACTIVE', activated_at=now() WHERE id=%s", (mission_id,))
    _audit("mission.activated", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="mission", target_id=mission_id)
    log.info(f"[mission] Activated {mission['name']} gate={mission['gate_id']}")
    return jsonify({"ok": True})

@app.route("/api/missions/<mission_id>/deactivate", methods=["POST"])
@login_required
def deactivate_mission(mission_id):
    if request.user.get("role") == "OPERATOR":
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    mission = _q("SELECT * FROM missions WHERE id=%s", (mission_id,), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Not found"}), 404
    if not _can_access_gate(request.user, mission["gate_id"]):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    _q("UPDATE missions SET status='WAITING', activated_at=NULL WHERE id=%s", (mission_id,))
    _audit("mission.deactivated", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="mission", target_id=mission_id)
    return jsonify({"ok": True})

@app.route("/api/missions/<mission_id>/cancel", methods=["POST"])
@login_required
def cancel_mission(mission_id):
    if request.user.get("role") == "OPERATOR":
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    _q("UPDATE missions SET status='CANCELLED' WHERE id=%s", (mission_id,))
    _audit("mission.cancelled", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="mission", target_id=mission_id)
    return jsonify({"ok": True})

@app.route("/api/missions/<mission_id>/archive", methods=["POST"])
@login_required
def archive_mission(mission_id):
    if request.user.get("role") == "OPERATOR":
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    _q("UPDATE missions SET status='ARCHIVED' WHERE id=%s", (mission_id,))
    return jsonify({"ok": True})

@app.route("/api/missions/<mission_id>/pallets")
@login_required
def mission_pallets(mission_id):
    mission = _q("SELECT * FROM missions WHERE id=%s", (mission_id,), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Not found"}), 404
    if not _can_access_gate(request.user, mission["gate_id"]):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    pallets = _q("SELECT * FROM pallets WHERE mission_id=%s ORDER BY id",
                 (mission_id,), fetchall=True) or []
    return jsonify({"ok": True, "pallets": [dict(p) for p in pallets]})

@app.route("/api/clips")
@login_required
def list_clips():
    gate_id    = request.args.get("gate_id")
    mission_id = request.args.get("mission_id")
    if gate_id and not _can_access_gate(request.user, int(gate_id)):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    params = []
    where  = ["deleted=false"]
    if gate_id:    where.append("gate_id=%s");    params.append(gate_id)
    if mission_id: where.append("mission_id=%s"); params.append(mission_id)
    sql   = "SELECT * FROM clips WHERE " + " AND ".join(where) + " ORDER BY created_at DESC"
    clips = _q(sql, params, fetchall=True) or []
    return jsonify({"ok": True, "clips": [dict(c) for c in clips]})

# ─────────────────────────────────────────────────────────────────────────────
# ── GATE API (dashboard fleet management) ────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/gates")
@login_required
def api_gates():
    role = request.user.get("role")
    if role == "ADMIN":
        gates = _q("SELECT * FROM gates ORDER BY id", fetchall=True) or []
    else:
        gate_ids = request.user.get("gates") or []
        gates = _q("SELECT * FROM gates WHERE id=ANY(%s) ORDER BY id",
                   (gate_ids,), fetchall=True) or []
    return jsonify({"ok": True, "gates": [dict(g) for g in gates]})

@app.route("/api/gates/<int:gate_id>/status")
@login_required
def gate_status(gate_id):
    if not _can_access_gate(request.user, gate_id):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    with _gate_lock:
        state = _gate_state.get(gate_id)
    return jsonify({"ok": True, "state": state})

@app.route("/api/gates/<int:gate_id>/config", methods=["POST"])
@role_required("ADMIN")
def push_config(gate_id):
    data        = request.get_json(silent=True) or {}
    new_config  = data.get("config")
    restart_app = data.get("restart_app", True)
    if not new_config:
        return jsonify({"ok": False, "error": "No config provided"}), 400
    result = _push_to_agent(gate_id, "/agent/apply-config",
                            {"config": new_config, "restart_app": restart_app})
    _audit("config.pushed", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="gate", target_id=gate_id,
           details={"fields": list(new_config.keys())})
    return jsonify(result)

@app.route("/api/gates/<int:gate_id>/command", methods=["POST"])
@role_required("ADMIN")
def gate_command(gate_id):
    data = request.get_json(silent=True) or {}
    cmd  = data.get("cmd","")
    result = _push_to_agent(gate_id, "/agent/command", {"cmd": cmd})
    _audit(f"gate.command.{cmd}", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="gate", target_id=gate_id)
    return jsonify(result)

@app.route("/api/gates/<int:gate_id>/zone", methods=["POST"])
@role_required("ADMIN")
def push_gate_zone(gate_id):
    rect = (request.get_json(silent=True) or {}).get("rect")
    if not rect or len(rect) != 4:
        return jsonify({"ok": False, "error": "rect must be [x, y, w, h]"}), 400
    result = _push_to_agent(gate_id, "/agent/gate-zone", {"rect": rect})
    _audit("gate.zone_updated", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="gate", target_id=gate_id,
           details={"rect": rect})
    return jsonify(result)

@app.route("/api/gates/<int:gate_id>/logs")
@role_required("ADMIN")
def gate_logs(gate_id):
    n      = int(request.args.get("n", 100))
    result = _get_from_agent(gate_id, f"/agent/logs?n={n}")
    return jsonify(result)

@app.route("/api/gates/<int:gate_id>/preview")
@role_required("ADMIN")
def gate_preview_proxy(gate_id):
    """Proxy the MJPEG stream from the ZED Box agent."""
    ip = _gate_ip(gate_id)
    if not ip:
        return jsonify({"ok": False, "error": "Gate IP unknown"}), 404
    url = f"http://{ip}:5002/agent/preview"

    def generate():
        try:
            with requests.get(url, stream=True, timeout=10) as r:
                for chunk in r.iter_content(chunk_size=4096):
                    yield chunk
        except Exception:
            pass

    return Response(stream_with_context(generate()),
                    content_type="multipart/x-mixed-replace; boundary=frame")

@app.route("/api/gates/<int:gate_id>/deploy", methods=["POST"])
@role_required("ADMIN")
def deploy_to_gate(gate_id):
    data    = request.get_json(silent=True) or {}
    version = data.get("version","unknown")
    files   = data.get("files",[])
    restart = data.get("restart", True)
    result  = _push_to_agent(gate_id, "/agent/deploy",
                              {"version": version, "files": files, "restart": restart})
    _audit("update.deployed", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="gate", target_id=gate_id,
           details={"version": version})
    return jsonify(result)

# ─────────────────────────────────────────────────────────────────────────────
# ── AGENT API (ZED Boxes talk to this) ──────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/agent/heartbeat", methods=["POST"])
@agent_secret_required
def agent_heartbeat():
    hb      = request.get_json(silent=True) or {}
    gate_id = hb.get("gate_id")
    if not gate_id:
        return jsonify({"ok": False, "error": "gate_id required"}), 400

    # Upsert gate record
    _q("""INSERT INTO gates (id, name, last_heartbeat_ip, status, last_heartbeat,
                             app_version, agent_version, app_mode, active_tour_id,
                             cpu_pct, ram_mb, disk_free_gb, uptime_s,
                             camera_ok, modules_active, ip_mode)
          VALUES (%s,%s,%s,'ONLINE',now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
          ON CONFLICT (id) DO UPDATE SET
              last_heartbeat_ip = EXCLUDED.last_heartbeat_ip,
              status            = 'ONLINE',
              last_heartbeat    = now(),
              app_version       = EXCLUDED.app_version,
              agent_version     = EXCLUDED.agent_version,
              app_mode          = EXCLUDED.app_mode,
              active_tour_id    = EXCLUDED.active_tour_id,
              cpu_pct           = EXCLUDED.cpu_pct,
              ram_mb            = EXCLUDED.ram_mb,
              disk_free_gb      = EXCLUDED.disk_free_gb,
              uptime_s          = EXCLUDED.uptime_s,
              camera_ok         = EXCLUDED.camera_ok,
              modules_active    = EXCLUDED.modules_active,
              ip_mode           = EXCLUDED.ip_mode""",
       (gate_id,
        hb.get("gate_name", f"Gate {gate_id}"),
        hb.get("ip"),
        hb.get("app_version"),
        hb.get("agent_version"),
        hb.get("app_mode","IDLE"),
        hb.get("active_tour_id"),
        hb.get("cpu_pct"),
        hb.get("ram_mb"),
        hb.get("disk_free_gb"),
        hb.get("uptime_s"),
        hb.get("camera_ok"),
        hb.get("modules_active",[]),
        hb.get("ip_mode","dhcp")))

    # Update in-memory state
    update_gate_state(gate_id, {**hb, "status": "ONLINE"})

    # Check for pending config
    pending = _q("""SELECT config FROM gate_config_queue
                    WHERE gate_id=%s AND applied_at IS NULL
                    ORDER BY created_at ASC LIMIT 1""",
                 (gate_id,), fetchone=True)

    log.debug(f"[heartbeat] Gate {gate_id} — {hb.get('app_mode')} cpu={hb.get('cpu_pct')}%")
    return jsonify({"ok": True, "pending_config": pending is not None})

@app.route("/agent/config/<int:gate_id>", methods=["GET"])
@agent_secret_required
def agent_get_config(gate_id):
    """Agent pulls pending config from VM."""
    row = _q("""SELECT id, config FROM gate_config_queue
                WHERE gate_id=%s AND applied_at IS NULL
                ORDER BY created_at ASC LIMIT 1""",
             (gate_id,), fetchone=True)
    if not row:
        return jsonify({"ok": True, "config": None})
    _q("UPDATE gate_config_queue SET applied_at=now() WHERE id=%s", (row["id"],))
    return jsonify({"ok": True, "config": row["config"]})

@app.route("/agent/sync/event", methods=["POST"])
@agent_secret_required
def agent_sync_event():
    """ZED Box notifies VM of events (best-effort after WMS delivery)."""
    data       = request.get_json(silent=True) or {}
    gate_id    = data.get("gate_id")
    event_type = data.get("event_type")
    payload    = data.get("payload", {})

    if event_type == "pallet.loaded":
        sscc       = payload.get("sscc")
        mission_id = _q("""SELECT id FROM missions
                           WHERE gate_id=%s AND status='ACTIVE' LIMIT 1""",
                        (gate_id,), fetchone=True)
        if mission_id and sscc:
            _q("""UPDATE pallets SET status='LOADED', loaded_at=now(),
                  forklift_id=%s WHERE mission_id=%s AND sscc=%s""",
               (payload.get("forklift_id"), mission_id["id"], sscc))
            log.info(f"[sync] Pallet loaded gate={gate_id} sscc={sscc}")

    elif event_type == "mission.completed":
        _q("""UPDATE missions SET status='COMPLETED', completed_at=now()
              WHERE gate_id=%s AND status='ACTIVE'""", (gate_id,))
        log.info(f"[sync] Mission completed gate={gate_id}")

    return jsonify({"ok": True})

@app.route("/agent/releases/<filename>")
def agent_releases(filename):
    """Serve app files to ZED Boxes on install/update."""
    allowed = {"digiload_pro.py", "wms_connector.py", "agent.py"}
    if filename not in allowed:
        return jsonify({"ok": False, "error": "Not found"}), 404
    return send_from_directory(RELEASES_DIR, filename)

# ─────────────────────────────────────────────────────────────────────────────
# ── WMS REST API ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/v1/missions", methods=["POST"])
@api_key_required
def wms_create_mission():
    """WMS creates a mission via JSON POST."""
    data = request.get_json(silent=True) or {}

    gate_id        = data.get("gate_id")
    name           = data.get("name") or data.get("mission_id") or "WMS Mission"
    wms_mission_id = data.get("mission_id")
    truck_id       = data.get("truck_id")
    scheduled_at   = data.get("scheduled_at")
    forklift_ids   = data.get("forklift_ids", [])
    pallets_data   = data.get("pallets", [])

    if not gate_id:
        return jsonify({"ok": False, "error": "gate_id required"}), 400
    if not pallets_data:
        return jsonify({"ok": False, "error": "pallets required"}), 400

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO missions
                           (gate_id, name, wms_mission_id, truck_id,
                            scheduled_at, total_pallets, source)
                           VALUES (%s,%s,%s,%s,%s,%s,'api')
                           RETURNING id""",
                        (gate_id, name, wms_mission_id, truck_id,
                         scheduled_at, len(pallets_data)))
            mission_id = cur.fetchone()["id"]
            for p in pallets_data:
                cur.execute("""INSERT INTO pallets (mission_id, gate_id, sscc, sku, weight_kg)
                               VALUES (%s,%s,%s,%s,%s)
                               ON CONFLICT (mission_id, sscc) DO NOTHING""",
                            (mission_id, gate_id,
                             p.get("sscc"),
                             p.get("sku"),
                             p.get("weight_kg")))
        conn.commit()

    log.info(f"[wms_api] Mission created '{name}' gate={gate_id} pallets={len(pallets_data)}")
    return jsonify({
        "ok":         True,
        "mission_id": str(mission_id),
        "count":      len(pallets_data)
    }), 201

@app.route("/api/v1/missions/<mission_id>/activate", methods=["POST"])
@api_key_required
def wms_activate_mission(mission_id):
    mission = _q("SELECT * FROM missions WHERE id=%s OR wms_mission_id=%s",
                 (mission_id, mission_id), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Mission not found"}), 404
    if mission["status"] != "WAITING":
        return jsonify({"ok": False, "error": f"Status is {mission['status']}"}), 400
    active = _q("SELECT id FROM missions WHERE gate_id=%s AND status='ACTIVE'",
                (mission["gate_id"],), fetchone=True)
    if active:
        return jsonify({"ok": False, "error": "Gate already has active mission"}), 409
    _q("UPDATE missions SET status='ACTIVE', activated_at=now() WHERE id=%s",
       (mission["id"],))
    log.info(f"[wms_api] Mission activated {mission['name']}")
    return jsonify({"ok": True})

@app.route("/api/v1/missions/<mission_id>/cancel", methods=["POST"])
@api_key_required
def wms_cancel_mission(mission_id):
    mission = _q("SELECT * FROM missions WHERE id=%s OR wms_mission_id=%s",
                 (mission_id, mission_id), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Not found"}), 404
    _q("UPDATE missions SET status='CANCELLED' WHERE id=%s", (mission["id"],))
    log.info(f"[wms_api] Mission cancelled {mission['name']}")
    return jsonify({"ok": True})

@app.route("/api/v1/gates")
@api_key_required
def wms_list_gates():
    gates = _q("SELECT id, name, status, app_mode, last_heartbeat FROM gates ORDER BY id",
               fetchall=True) or []
    return jsonify({"ok": True, "gates": [dict(g) for g in gates]})

@app.route("/api/v1/gates/<int:gate_id>/status")
@api_key_required
def wms_gate_status(gate_id):
    gate = _q("SELECT * FROM gates WHERE id=%s", (gate_id,), fetchone=True)
    if not gate:
        return jsonify({"ok": False, "error": "Not found"}), 404
    return jsonify({"ok": True, "gate": dict(gate)})

# ─────────────────────────────────────────────────────────────────────────────
# ── ADMIN API ────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/admin/users", methods=["POST"])
@role_required("ADMIN")
def create_user():
    data  = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    pw    = data.get("password","")
    role  = data.get("role","OPERATOR")
    gates = data.get("gates",[])

    if not email or not pw:
        return jsonify({"ok": False, "error": "email and password required"}), 400
    if role not in ("ADMIN","SUPERVISOR","OPERATOR"):
        return jsonify({"ok": False, "error": "Invalid role"}), 400
    if len(pw) < 10:
        return jsonify({"ok": False, "error": "Password min 10 characters"}), 400

    with get_db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""INSERT INTO users (email, password_hash, role)
                               VALUES (%s,%s,%s) RETURNING id""",
                            (email, _hash_password(pw), role))
                user_id = cur.fetchone()["id"]
                for gid in gates:
                    cur.execute("INSERT INTO user_gates VALUES (%s,%s) ON CONFLICT DO NOTHING",
                                (user_id, gid))
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                return jsonify({"ok": False, "error": "Email already exists"}), 409

    _audit("user.created", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="user", target_id=user_id,
           details={"email": email, "role": role})
    return jsonify({"ok": True, "user_id": str(user_id)})

@app.route("/api/admin/users/<user_id>/deactivate", methods=["POST"])
@role_required("ADMIN")
def deactivate_user(user_id):
    _q("UPDATE users SET active=false WHERE id=%s", (user_id,))
    _audit("user.deactivated", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="user", target_id=user_id)
    return jsonify({"ok": True})

@app.route("/api/admin/gates", methods=["POST"])
@role_required("ADMIN")
def register_gate():
    data = request.get_json(silent=True) or {}
    gid  = data.get("id")
    name = data.get("name", f"Gate {gid}")
    if not gid:
        return jsonify({"ok": False, "error": "gate id required"}), 400
    _q("""INSERT INTO gates (id, name) VALUES (%s,%s)
          ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name""",
       (gid, name))
    return jsonify({"ok": True, "gate_id": gid})

@app.route("/api/admin/api-keys", methods=["POST"])
@role_required("ADMIN")
def create_api_key():
    data    = request.get_json(silent=True) or {}
    name    = data.get("name","unnamed")
    raw_key = "dlpro_" + uuid.uuid4().hex + uuid.uuid4().hex
    hashed  = _hash_password(raw_key)
    _q("INSERT INTO api_keys (name, key_hash) VALUES (%s,%s)", (name, hashed))
    _audit("apikey.created", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"name": name})
    return jsonify({"ok": True, "key": raw_key,
                    "note": "Save this key — it will not be shown again"})

@app.route("/api/config")
@login_required
def get_config():
    return jsonify({"ok": True})

@app.route("/api/config", methods=["POST"])
@role_required("ADMIN")
def save_global_config():
    return jsonify({"ok": True})

# ─────────────────────────────────────────────────────────────────────────────
# ── INSTALL SCRIPT ENDPOINT ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/install")
def serve_install_script():
    """
    Serves install.sh to ZED Boxes.
    Usage on ZED Box:
        curl -sSL http://vm-ip:5001/install | bash -s -- --gate-id=3
    """
    install_path = os.path.join(RELEASES_DIR, "install.sh")
    if not os.path.exists(install_path):
        return "Install script not found on server", 404
    with open(install_path, "r") as f:
        content = f.read()
    # Inject the VM IP so ZED Boxes can find home
    vm_host = request.host.split(":")[0]
    content = content.replace(
        "CENTRAL_URL=\"http://${CENTRAL_IP}:5001\"",
        f"CENTRAL_URL=\"http://{vm_host}:5001\""
    )
    return Response(content, mimetype="text/plain")

# ─────────────────────────────────────────────────────────────────────────────
# ── SOCKETIO ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@sio.on("connect")
def on_connect():
    log.debug(f"[sio] Client connected: {request.sid}")
    states = get_all_gate_states()
    emit("fleet_update", {"gates": states})

@sio.on("disconnect")
def on_disconnect():
    log.debug(f"[sio] Client disconnected: {request.sid}")

# ─────────────────────────────────────────────────────────────────────────────
# ── BACKGROUND: offline gate detection ───────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
def _offline_check_loop():
    """Mark gates as OFFLINE in DB if no heartbeat for 60s."""
    while True:
        time.sleep(30)
        try:
            _q("""UPDATE gates SET status='OFFLINE'
                  WHERE last_heartbeat < now() - INTERVAL '60 seconds'
                  AND status = 'ONLINE'""")
        except Exception as e:
            log.error(f"[offline_check] {e}")

threading.Thread(target=_offline_check_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# ── STARTUP ──────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
def create_default_admin():
    """Create default admin if no users exist."""
    existing = _q("SELECT COUNT(*) as n FROM users", fetchone=True)
    if existing and existing["n"] > 0:
        return
    default_pw = "Digiload2024!"
    _q("""INSERT INTO users (email, password_hash, role)
          VALUES (%s,%s,'ADMIN')""",
       ("admin@digiload.local", _hash_password(default_pw)))
    log.warning("=" * 50)
    log.warning("DEFAULT ADMIN CREATED")
    log.warning("  Email:    admin@digiload.local")
    log.warning(f"  Password: {default_pw}")
    log.warning("  CHANGE THIS IMMEDIATELY")
    log.warning("=" * 50)

# ─────────────────────────────────────────────────────────────────────────────
# ── MAIN ─────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 60)
    log.info("Digiload Pro — Central VM v1.0")
    log.info("=" * 60)

    # Wait for PostgreSQL
    for attempt in range(30):
        try:
            with get_db() as conn:
                conn.cursor().execute("SELECT 1")
            log.info("[db] PostgreSQL connected")
            break
        except Exception:
            log.info(f"[db] Waiting for PostgreSQL... ({attempt+1}/30)")
            time.sleep(2)
    else:
        log.error("[db] PostgreSQL unavailable — exiting")
        raise SystemExit(1)

    init_db()
    create_default_admin()

    port = int(os.environ.get("PORT", 5001))
    log.info(f"[app] Listening on port {port}")
    log.info(f"[app] Dashboard: http://0.0.0.0:{port}")

    sio.run(app, host="0.0.0.0", port=port, debug=False)
