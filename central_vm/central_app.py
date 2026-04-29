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
        role          TEXT NOT NULL CHECK (role IN ('SUPER_ADMIN','ADMIN','SUPERVISOR','OPERATOR')),
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
        extra_fields JSONB DEFAULT '{}',
        status      TEXT DEFAULT 'WAITING'
            CHECK (status IN ('WAITING','LOADED','FLAGGED')),
        scan_time   TIMESTAMPTZ,
        loaded_at   TIMESTAMPTZ,
        forklift_id INTEGER,
        UNIQUE(mission_id, sscc)
    );

    -- Configurable CSV column mapping per gate/installation (DL-022)
    CREATE TABLE IF NOT EXISTS csv_mappings (
        id          SERIAL PRIMARY KEY,
        gate_id     INTEGER REFERENCES gates(id) ON DELETE CASCADE,
        name        TEXT NOT NULL DEFAULT 'Default',
        mapping     JSONB NOT NULL DEFAULT '{}',
        is_default  BOOLEAN DEFAULT true,
        created_at  TIMESTAMPTZ DEFAULT now(),
        updated_at  TIMESTAMPTZ DEFAULT now()
    );

    -- Add extra_fields to pallets if upgrading from older schema
    ALTER TABLE pallets ADD COLUMN IF NOT EXISTS extra_fields JSONB DEFAULT '{}';

    -- Allow SUPER_ADMIN role on existing installations (DL-030)
    DO $$ BEGIN
        ALTER TABLE users DROP CONSTRAINT IF EXISTS users_role_check;
        ALTER TABLE users ADD CONSTRAINT users_role_check
            CHECK (role IN ('SUPER_ADMIN','ADMIN','SUPERVISOR','OPERATOR'));
    EXCEPTION WHEN OTHERS THEN NULL;
    END $$;

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

def super_admin_required(f):
    """For internal MDM routes — only SUPER_ADMIN role can access."""
    @wraps(f)
    @login_required
    def decorated(*args, **kwargs):
        if request.user.get("role") != "SUPER_ADMIN":
            return jsonify({"ok": False, "error": "Super admin only"}), 403
        return f(*args, **kwargs)
    return decorated

def _can_access_gate(user_payload: dict, gate_id: int) -> bool:
    if user_payload.get("role") in ("SUPER_ADMIN", "ADMIN"):
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
# ─────────────────────────────────────────────────────────────────────────────
# ── CSV MAPPING ENGINE (DL-022) ───────────────────────────────────────────────
# Maps customer column names → Digiload standard fields
# ─────────────────────────────────────────────────────────────────────────────

# Standard Digiload field names
_STANDARD_FIELDS = {
    "sscc", "sku", "weight_kg", "mission_name", "truck_id", "gate_id",
    "carrier", "customer_order", "delivery_note", "destination",
    "temp_requirement", "dangerous_goods", "priority",
    "custom_1", "custom_2", "custom_3", "custom_4", "custom_5",
}

# Auto-detection fallbacks when no mapping configured
_AUTO_DETECT = {
    "sscc":       ["sscc","barcode","code","sscc_code","nr_sscc","code_barre"],
    "sku":        ["sku","article","product","ref","reference","item"],
    "weight_kg":  ["weight_kg","weight","poids","poids_brut","kg","masse"],
    "truck_id":   ["truck_id","truck","camion","vehicle","vehicule"],
    "carrier":    ["carrier","transporteur","spediteur","transport"],
    "customer_order": ["customer_order","order","commande","n_commande","auftrag"],
    "delivery_note":  ["delivery_note","bl","bon_livraison","n_bl","lieferschein"],
    "destination":    ["destination","dest","zielort"],
    "gate_id":        ["gate_id","gate","quai","dock","tor"],
}

def get_csv_mapping(gate_id: int) -> dict:
    """Load CSV mapping for a gate. Returns empty dict if none configured."""
    row = _q(
        "SELECT mapping FROM csv_mappings WHERE gate_id=%s AND is_default=true LIMIT 1",
        (gate_id,), fetchone=True
    )
    return row["mapping"] if row else {}

def apply_csv_mapping(row: dict, mapping: dict) -> dict:
    """
    Map a CSV row to Digiload standard fields using configured mapping.
    Falls back to auto-detection for unmapped fields.
    Extra columns not in standard fields → extra_fields JSONB.

    Returns:
        {
            "sscc": "...", "sku": "...", "weight_kg": ...,
            "extra_fields": {"carrier": "DHL", "customer_order": "..."}
        }
    """
    result      = {}
    extra_fields = {}

    # Build reverse mapping: csv_col → standard_field
    reverse = {v: k for k, v in mapping.items()}  # csv_col → digiload_field

    for csv_col, value in row.items():
        value = str(value).strip() if value else ""
        if not value:
            continue
        col_lower = csv_col.lower().strip()

        # Check configured mapping first
        if col_lower in reverse:
            field = reverse[col_lower]
            if field in _STANDARD_FIELDS:
                result[field] = value
            continue

        # Auto-detect standard fields
        matched = False
        for field, aliases in _AUTO_DETECT.items():
            if col_lower in aliases or col_lower == field:
                result[field] = value
                matched = True
                break

        # Everything else → extra_fields
        if not matched and csv_col not in ("", " "):
            extra_fields[csv_col] = value

    result["extra_fields"] = extra_fields
    return result

def parse_csv_with_mapping(content: str, gate_id: int) -> list:
    """
    Parse a CSV file using the configured column mapping for this gate.
    Auto-detects delimiter (comma or semicolon) and encoding.
    Returns list of normalised pallet dicts.
    """
    # Detect delimiter
    sample = content[:2000]
    delimiter = ";" if sample.count(";") > sample.count(",") else ","

    mapping = get_csv_mapping(gate_id)
    reader  = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    pallets = []

    for row in reader:
        mapped = apply_csv_mapping(row, mapping)
        sscc   = mapped.get("sscc", "").strip()
        if not sscc:
            continue
        pallets.append({
            "sscc":        sscc,
            "sku":         mapped.get("sku") or None,
            "weight_kg":   _safe_float(mapped.get("weight_kg")),
            "extra_fields": mapped.get("extra_fields", {}),
            "gate_id_csv": mapped.get("gate_id") or None,
        })

    return pallets

def _safe_float(v):
    try:    return float(str(v).replace(",",".")) if v else None
    except: return None


# ─────────────────────────────────────────────────────────────────────────────
# ── SFTP WATCHER (DL-022) ────────────────────────────────────────────────────
# Watches /sftp/incoming/ for new CSV files, auto-imports them
# ─────────────────────────────────────────────────────────────────────────────
import hashlib as _hashlib

SFTP_INCOMING = os.environ.get("SFTP_INCOMING_DIR", "/sftp/incoming")
SFTP_ARCHIVE  = os.environ.get("SFTP_ARCHIVE_DIR",  "/sftp/archive")
SFTP_FAILED   = os.environ.get("SFTP_FAILED_DIR",   "/sftp/failed")
SFTP_POLL_S   = 60

def _sftp_md5(path: str) -> str:
    h = _hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _sftp_already_imported(md5: str) -> bool:
    row = _q(
        "SELECT id FROM audit_log WHERE action='mission.sftp_imported' "
        "AND details->>'md5'=%s LIMIT 1",
        (md5,), fetchone=True
    )
    return row is not None

def _sftp_import_file(filepath: str):
    """Process a single CSV file from SFTP incoming folder."""
    filename = os.path.basename(filepath)
    md5      = _sftp_md5(filepath)

    if _sftp_already_imported(md5):
        log.info(f"[sftp] Skipping duplicate: {filename}")
        os.makedirs(SFTP_ARCHIVE, exist_ok=True)
        os.rename(filepath, os.path.join(SFTP_ARCHIVE, filename))
        return

    try:
        with open(filepath, "r", encoding="utf-8-sig", errors="replace") as f:
            content = f.read()

        # Try to infer gate_id from filename: gate3_missions.csv → 3
        gate_id = None
        import re as _re
        m = _re.search(r"gate[_\-]?(\d+)", filename.lower())
        if m:
            gate_id = int(m.group(1))

        pallets = parse_csv_with_mapping(content, gate_id or 0)
        if not pallets:
            raise ValueError("No pallets found in file")

        # Use gate_id from CSV rows if not in filename
        if not gate_id and pallets[0].get("gate_id_csv"):
            gate_id = int(pallets[0]["gate_id_csv"])
        if not gate_id:
            raise ValueError("Cannot determine gate_id from filename or CSV content")

        # Create mission
        mission_name = os.path.splitext(filename)[0]
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO missions (gate_id, name, total_pallets, source)
                               VALUES (%s,%s,%s,'sftp') RETURNING id""",
                            (gate_id, mission_name, len(pallets)))
                mission_id = cur.fetchone()["id"]
                for p in pallets:
                    import json as _json
                    cur.execute("""INSERT INTO pallets
                                   (mission_id, gate_id, sscc, sku, weight_kg, extra_fields)
                                   VALUES (%s,%s,%s,%s,%s,%s)
                                   ON CONFLICT (mission_id, sscc) DO NOTHING""",
                                (mission_id, gate_id, p["sscc"],
                                 p["sku"], p["weight_kg"],
                                 _json.dumps(p["extra_fields"])))
            conn.commit()

        _audit("mission.sftp_imported",
               details={"filename": filename, "gate_id": gate_id,
                        "pallets": len(pallets), "md5": md5})
        log.info(f"[sftp] Imported {filename} → gate {gate_id}, {len(pallets)} pallets")

        # Archive
        os.makedirs(SFTP_ARCHIVE, exist_ok=True)
        os.rename(filepath, os.path.join(SFTP_ARCHIVE, filename))

    except Exception as e:
        log.error(f"[sftp] Failed {filename}: {e}")
        os.makedirs(SFTP_FAILED, exist_ok=True)
        try: os.rename(filepath, os.path.join(SFTP_FAILED, filename))
        except Exception: pass

def _sftp_watcher_loop():
    """Background thread — polls SFTP incoming folder every 60s."""
    log.info(f"[sftp] Watcher started — watching {SFTP_INCOMING} every {SFTP_POLL_S}s")
    while True:
        try:
            if os.path.isdir(SFTP_INCOMING):
                for fname in sorted(os.listdir(SFTP_INCOMING)):
                    if fname.lower().endswith(".csv") and not fname.startswith("."):
                        fpath = os.path.join(SFTP_INCOMING, fname)
                        _sftp_import_file(fpath)
        except Exception as e:
            log.error(f"[sftp] Watcher error: {e}")
        time.sleep(SFTP_POLL_S)


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
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
def audit_page():
    logs = _q("""SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 500""",
              fetchall=True) or []
    return render_template("audit.html", user=request.user, logs=logs)

# ─────────────────────────────────────────────────────────────────────────────
# ── SUPER ADMIN PANEL — MDM (Internal Use, DL-030) ────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/admin/super")
@super_admin_required
def super_admin_page():
    """
    Internal MDM panel — for Digiload team only.
    SUPER_ADMIN role required. Configure all gates remotely from one place.
    """
    return render_template("super_admin.html", user=request.user)

@app.route("/api/super/gates")
@super_admin_required
def super_list_gates():
    """List all gates with full status + config snapshot."""
    gates = _q("SELECT * FROM gates ORDER BY id", fetchall=True) or []
    states = get_all_gate_states()
    state_by_id = {s["gate_id"]: s for s in states}
    result = []
    for g in gates:
        s = state_by_id.get(g["id"], {})
        result.append({
            **dict(g),
            "online":         s.get("status") == "ONLINE",
            "ip":             s.get("ip"),
            "app_mode":       s.get("app_mode"),
            "app_version":    s.get("app_version"),
            "agent_version":  s.get("agent_version"),
            "cpu_pct":        s.get("cpu_pct"),
            "ram_mb":         s.get("ram_mb"),
            "disk_free_gb":   s.get("disk_free_gb"),
            "camera_ok":      s.get("camera_ok"),
            "modules_active": s.get("modules_active", []),
            "last_heartbeat": s.get("last_heartbeat"),
        })
    return jsonify({"ok": True, "gates": result})

@app.route("/api/super/gate/<int:gate_id>/config", methods=["GET"])
@super_admin_required
def super_get_gate_config(gate_id):
    """Pull current config from ZED Box agent."""
    config = _get_from_agent(gate_id, "/agent/config-current") or {}
    if not config:
        # Fallback — read from VM gate_config_queue
        row = _q("""SELECT config FROM gate_config_queue
                    WHERE gate_id=%s ORDER BY created_at DESC LIMIT 1""",
                 (gate_id,), fetchone=True)
        config = row["config"] if row else {}
    return jsonify({"ok": True, "config": config, "gate_id": gate_id})

@app.route("/api/super/gate/<int:gate_id>/config", methods=["POST"])
@super_admin_required
def super_push_gate_config(gate_id):
    """Push partial config update to ZED Box. Agent merges with existing."""
    data        = request.get_json(silent=True) or {}
    config      = data.get("config", {})
    restart_app = data.get("restart_app", False)

    if not config:
        return jsonify({"ok": False, "error": "No config provided"}), 400

    result = _push_to_agent(gate_id, "/agent/apply-config", {
        "config":      config,
        "restart_app": restart_app
    })

    _audit("super.config_pushed", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"gate_id": gate_id, "keys": list(config.keys()),
                    "restart": restart_app})
    log.info(f"[super] Config push to gate {gate_id} — {list(config.keys())}")
    return jsonify({"ok": True, "result": result})

@app.route("/api/super/gates/all/config", methods=["POST"])
@super_admin_required
def super_push_all_gates():
    """Bulk push config to ALL gates."""
    data        = request.get_json(silent=True) or {}
    config      = data.get("config", {})
    restart_app = data.get("restart_app", False)

    if not config:
        return jsonify({"ok": False, "error": "No config provided"}), 400

    gates = _q("SELECT id FROM gates", fetchall=True) or []
    results = []
    for g in gates:
        try:
            r = _push_to_agent(g["id"], "/agent/apply-config", {
                "config": config, "restart_app": restart_app
            })
            results.append({"gate_id": g["id"], "ok": True, "result": r})
        except Exception as e:
            results.append({"gate_id": g["id"], "ok": False, "error": str(e)})

    _audit("super.bulk_config_pushed", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"gate_count": len(gates), "keys": list(config.keys())})

    return jsonify({"ok": True, "gates_processed": len(gates), "results": results})

@app.route("/api/super/gate/<int:gate_id>", methods=["PATCH"])
@super_admin_required
def super_edit_gate(gate_id):
    """Edit gate name."""
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400
    _q("UPDATE gates SET name=%s WHERE id=%s", (name, gate_id))
    _audit("super.gate_renamed", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"gate_id": gate_id, "name": name})
    return jsonify({"ok": True})

@app.route("/api/super/gate/<int:gate_id>", methods=["DELETE"])
@super_admin_required
def super_delete_gate(gate_id):
    """Delete (decommission) a gate."""
    _q("DELETE FROM gates WHERE id=%s", (gate_id,))
    _audit("super.gate_deleted", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"gate_id": gate_id})
    log.warning(f"[super] Gate {gate_id} DELETED")
    return jsonify({"ok": True})

@app.route("/api/super/sftp/files")
@super_admin_required
def super_sftp_files():
    """List files in SFTP folders."""
    def _list(folder):
        if not os.path.isdir(folder): return []
        return sorted([
            {"name": f, "size": os.path.getsize(os.path.join(folder, f)),
             "mtime": os.path.getmtime(os.path.join(folder, f))}
            for f in os.listdir(folder)
            if not f.startswith(".")
        ], key=lambda x: -x["mtime"])
    return jsonify({
        "ok": True,
        "incoming": _list(SFTP_INCOMING),
        "archive":  _list(SFTP_ARCHIVE)[:50],
        "failed":   _list(SFTP_FAILED)[:50],
    })

@app.route("/api/super/license/generate", methods=["POST"])
@super_admin_required
def super_generate_license():
    """Generate a license key for a gate + module."""
    data       = request.get_json(silent=True) or {}
    gate_id    = data.get("gate_id")
    module     = data.get("module", "video_tracking")
    customer   = data.get("customer", "DEFAULT")
    years      = int(data.get("years", 1))

    if not gate_id:
        return jsonify({"ok": False, "error": "gate_id required"}), 400

    secret = os.environ.get("DIGILOAD_LICENSE_SECRET", "")
    if not secret:
        return jsonify({"ok": False, "error": "DIGILOAD_LICENSE_SECRET not set"}), 500

    import json as _json
    import base64 as _b64
    payload = {
        "gate_id":  int(gate_id),
        "module":   module,
        "customer": customer,
        "issued":   dt.datetime.utcnow().isoformat(),
        "expires":  (dt.datetime.utcnow() + dt.timedelta(days=365*years)).isoformat(),
    }
    payload_json = _json.dumps(payload, sort_keys=True)
    sig = hmac.new(secret.encode(), payload_json.encode(), hashlib.sha256).digest()
    payload_b64 = _b64.urlsafe_b64encode(payload_json.encode()).decode().rstrip("=")
    sig_b64     = _b64.urlsafe_b64encode(sig).decode().rstrip("=")
    license_key = f"{payload_b64}.{sig_b64}"

    _audit("super.license_generated", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"gate_id": gate_id, "module": module, "customer": customer,
                    "expires": payload["expires"]})

    return jsonify({"ok": True, "license_key": license_key, "payload": payload})



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

    # Parse CSV using configurable column mapping (DL-022)
    try:
        content = csv_file.read().decode("utf-8-sig", errors="replace")
        pallets = parse_csv_with_mapping(content, gate_id)
    except Exception as e:
        return jsonify({"ok": False, "error": f"CSV parse error: {e}"}), 400

    if not pallets:
        return jsonify({"ok": False, "error": "No pallets found in CSV"}), 400

    # Insert mission + pallets
    import json as _json
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO missions (gate_id, name, total_pallets, source)
                           VALUES (%s,%s,%s,'csv') RETURNING id""",
                        (gate_id, name, len(pallets)))
            mission_id = cur.fetchone()["id"]
            for p in pallets:
                cur.execute("""INSERT INTO pallets
                               (mission_id, gate_id, sscc, sku, weight_kg, extra_fields)
                               VALUES (%s,%s,%s,%s,%s,%s)
                               ON CONFLICT (mission_id, sscc) DO NOTHING""",
                            (mission_id, gate_id, p["sscc"],
                             p["sku"],
                             p["weight_kg"],
                             _json.dumps(p["extra_fields"])))
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
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
def gate_command(gate_id):
    data = request.get_json(silent=True) or {}
    cmd  = data.get("cmd","")
    result = _push_to_agent(gate_id, "/agent/command", {"cmd": cmd})
    _audit(f"gate.command.{cmd}", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="gate", target_id=gate_id)
    return jsonify(result)

@app.route("/api/gates/<int:gate_id>/zone", methods=["POST"])
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
def gate_logs(gate_id):
    n      = int(request.args.get("n", 100))
    result = _get_from_agent(gate_id, f"/agent/logs?n={n}")
    return jsonify(result)

@app.route("/api/gates/<int:gate_id>/preview")
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
def deactivate_user(user_id):
    _q("UPDATE users SET active=false WHERE id=%s", (user_id,))
    _audit("user.deactivated", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="user", target_id=user_id)
    return jsonify({"ok": True})

# ─────────────────────────────────────────────────────────────────────────────
# ── CSV MAPPING API (DL-022) ──────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/csv-mapping/<int:gate_id>", methods=["GET"])
@login_required
def get_mapping(gate_id):
    """Get current CSV column mapping for a gate."""
    row = _q("SELECT * FROM csv_mappings WHERE gate_id=%s AND is_default=true",
             (gate_id,), fetchone=True)
    if not row:
        return jsonify({"ok": True, "mapping": {}, "name": "Default (auto-detect)"})
    return jsonify({"ok": True, "mapping": row["mapping"], "name": row["name"],
                    "id": row["id"]})

@app.route("/api/csv-mapping/<int:gate_id>", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR")
def save_mapping(gate_id):
    """
    Save or update CSV column mapping for a gate.
    mapping format: {"customer_col": "digiload_field", ...}
    Example: {"NR_SSCC": "sscc", "POIDS_BRUT": "weight_kg", "QUAI": "gate_id"}
    """
    data    = request.get_json(silent=True) or {}
    mapping = data.get("mapping", {})
    name    = data.get("name", "Custom mapping")

    import json as _json
    # Validate all target fields are known
    unknown = [v for v in mapping.values() if v not in _STANDARD_FIELDS]
    if unknown:
        return jsonify({"ok": False,
                        "error": f"Unknown target fields: {unknown}",
                        "valid_fields": sorted(_STANDARD_FIELDS)}), 400

    existing = _q("SELECT id FROM csv_mappings WHERE gate_id=%s AND is_default=true",
                  (gate_id,), fetchone=True)
    if existing:
        _q("UPDATE csv_mappings SET mapping=%s, name=%s, updated_at=now() WHERE id=%s",
           (_json.dumps(mapping), name, existing["id"]))
    else:
        _q("INSERT INTO csv_mappings (gate_id, name, mapping, is_default) VALUES (%s,%s,%s,true)",
           (gate_id, name, _json.dumps(mapping)))

    _audit("csv_mapping.saved", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           details={"gate_id": gate_id, "fields": len(mapping)})
    log.info(f"[csv_mapping] Gate {gate_id} — {len(mapping)} field(s) mapped")
    return jsonify({"ok": True, "gate_id": gate_id, "fields": len(mapping)})

@app.route("/api/csv-mapping/<int:gate_id>", methods=["DELETE"])
@login_required
@role_required("SUPER_ADMIN", "ADMIN")
def delete_mapping(gate_id):
    """Reset to auto-detect (delete custom mapping)."""
    _q("DELETE FROM csv_mappings WHERE gate_id=%s", (gate_id,))
    return jsonify({"ok": True, "message": "Mapping reset to auto-detect"})

@app.route("/api/csv-mapping/preview", methods=["POST"])
@login_required
def preview_mapping():
    """
    Test a mapping against a sample CSV row.
    Useful for the admin UI to preview what will be imported.
    """
    data    = request.get_json(silent=True) or {}
    row     = data.get("row", {})
    mapping = data.get("mapping", {})
    result  = apply_csv_mapping(row, mapping)
    return jsonify({"ok": True, "result": result})

@app.route("/api/csv-mapping/fields")
@login_required
def list_standard_fields():
    """List all valid Digiload standard field names for mapping UI."""
    return jsonify({"ok": True, "fields": sorted(_STANDARD_FIELDS)})


@app.route("/api/admin/gates", methods=["POST"])
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
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
@role_required("SUPER_ADMIN", "ADMIN")
def save_global_config():
    return jsonify({"ok": True})

# ─────────────────────────────────────────────────────────────────────────────
# ── FLEET HEALTH ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/fleet/health")
@login_required
def fleet_health():
    """
    Aggregate health metrics across all gates.
    Used by the fleet health dashboard screen.
    """
    gates = _q("SELECT * FROM gates ORDER BY id", fetchall=True) or []
    now   = time.time()

    online    = [g for g in gates if g["status"] == "ONLINE"]
    offline   = [g for g in gates if g["status"] != "ONLINE"]
    camera_ok = [g for g in online if g.get("camera_ok")]

    # Uptime percentage (gates online in last hour)
    uptime_pct = round(len(online) / len(gates) * 100, 1) if gates else 0

    # Average CPU and disk across online gates
    cpu_vals  = [g["cpu_pct"]      for g in online if g.get("cpu_pct")      is not None]
    disk_vals = [g["disk_free_gb"] for g in online if g.get("disk_free_gb") is not None]
    avg_cpu   = round(sum(cpu_vals)  / len(cpu_vals),  1) if cpu_vals  else 0
    avg_disk  = round(sum(disk_vals) / len(disk_vals), 1) if disk_vals else 0

    # WMS delivery stats (last 24h)
    wms_stats = _q("""
        SELECT
            COUNT(*)                                   as total,
            SUM(CASE WHEN success THEN 1 ELSE 0 END)  as delivered,
            SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed
        FROM wms_delivery_log
        WHERE delivered_at > now() - INTERVAL '24 hours'
    """, fetchone=True)

    wms_total     = int(wms_stats["total"])     if wms_stats else 0
    wms_delivered = int(wms_stats["delivered"]) if wms_stats else 0
    wms_rate      = round(wms_delivered / wms_total * 100, 1) if wms_total > 0 else 100

    # Alerts
    alerts = []
    for g in gates:
        if g["status"] != "ONLINE":
            since = g.get("last_heartbeat")
            alerts.append({
                "level":   "error",
                "gate_id": g["id"],
                "gate":    g["name"],
                "message": f"Offline since {since.strftime('%H:%M') if since else 'unknown'}"
            })
        elif g.get("cpu_pct") and g["cpu_pct"] > 80:
            alerts.append({
                "level":   "warning",
                "gate_id": g["id"],
                "gate":    g["name"],
                "message": f"High CPU: {g['cpu_pct']}%"
            })
        elif g.get("disk_free_gb") is not None and g["disk_free_gb"] < 10:
            alerts.append({
                "level":   "warning",
                "gate_id": g["id"],
                "gate":    g["name"],
                "message": f"Low disk: {g['disk_free_gb']} GB free"
            })
        elif not g.get("camera_ok") and g["status"] == "ONLINE":
            alerts.append({
                "level":   "warning",
                "gate_id": g["id"],
                "gate":    g["name"],
                "message": "Camera not responding"
            })

    return jsonify({
        "ok":           True,
        "total_gates":  len(gates),
        "online":       len(online),
        "offline":      len(offline),
        "camera_ok":    len(camera_ok),
        "uptime_pct":   uptime_pct,
        "avg_cpu":      avg_cpu,
        "avg_disk_gb":  avg_disk,
        "wms_total":    wms_total,
        "wms_delivered":wms_delivered,
        "wms_rate":     wms_rate,
        "alerts":       alerts,
        "gates":        [dict(g) for g in gates],
    })

@app.route("/health-dashboard")
@login_required
def health_dashboard():
    return render_template("health.html", user=request.user)


# ─────────────────────────────────────────────────────────────────────────────
# ── ROLLING DEPLOY ────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
_deploy_jobs = {}   # job_id → { status, progress, results }

@app.route("/api/fleet/deploy", methods=["POST"])
@role_required("SUPER_ADMIN", "ADMIN")
def fleet_deploy():
    """
    Rolling deploy to multiple gates.
    Reads files from RELEASES_DIR and pushes to selected gates via agent.
    Strategy: rolling (one at a time) | all (parallel)
    """
    data     = request.get_json(silent=True) or {}
    gate_ids = data.get("gate_ids", [])
    version  = data.get("version", "latest")
    strategy = data.get("strategy", "rolling")   # 'rolling' | 'all'
    restart  = data.get("restart", True)

    if not gate_ids:
        return jsonify({"ok": False, "error": "gate_ids required"}), 400

    # Read release files
    files = []
    for fname in ["digiload_pro.py", "wms_connector.py", "agent.py"]:
        fpath = os.path.join(RELEASES_DIR, fname)
        if os.path.exists(fpath):
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
            files.append({
                "path":    fname,
                "content": content,
                "md5":     hashlib.md5(content.encode()).hexdigest()
            })

    if not files:
        return jsonify({"ok": False, "error": "No release files found in releases/"}), 400

    job_id = uuid.uuid4().hex[:8]
    _deploy_jobs[job_id] = {
        "status":   "running",
        "version":  version,
        "strategy": strategy,
        "gates":    {gid: "pending" for gid in gate_ids},
        "results":  {}
    }

    def _run_deploy():
        job = _deploy_jobs[job_id]
        for gate_id in gate_ids:
            job["gates"][gate_id] = "deploying"
            result = _push_to_agent(
                gate_id, "/agent/deploy",
                {"version": version, "files": files, "restart": restart}
            )
            job["gates"][gate_id]    = "done" if result.get("ok") else "failed"
            job["results"][gate_id]  = result
            _audit("update.deployed",
                   user_id=request.user.get("sub"),
                   user_email=request.user.get("email"),
                   target_type="gate", target_id=gate_id,
                   details={"version": version, "job_id": job_id})
            if strategy == "rolling":
                time.sleep(3)   # wait between gates in rolling mode
        job["status"] = "complete"
        log.info(f"[deploy] Job {job_id} complete — {len(gate_ids)} gates")

    threading.Thread(target=_run_deploy, daemon=True).start()

    return jsonify({
        "ok":     True,
        "job_id": job_id,
        "gates":  len(gate_ids),
        "files":  [f["path"] for f in files]
    })

@app.route("/api/fleet/deploy/<job_id>")
@role_required("SUPER_ADMIN", "ADMIN")
def deploy_job_status(job_id):
    """Poll deploy job progress."""
    job = _deploy_jobs.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    return jsonify({"ok": True, **job})


# ─────────────────────────────────────────────────────────────────────────────
# ── DRIVER VIEW (DL-024) ──────────────────────────────────────────────────────
# Public — no login required
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/gate/<int:gate_id>/driver")
def driver_view(gate_id):
    gate = _q("SELECT * FROM gates WHERE id=%s", (gate_id,), fetchone=True)
    gate_name  = gate["name"] if gate else f"Gate {gate_id}"
    driver_url = request.url_root.rstrip("/") + f"/gate/{gate_id}/driver"

    # ZED Box IP from last heartbeat — tablet connects directly (DL-028)
    with _gate_lock:
        state  = _gate_state.get(gate_id, {})
    zed_ip = state.get("ip", "")
    zed_url = f"http://{zed_ip}:5002" if zed_ip else ""

    return render_template("driver.html",
                           gate_id=gate_id,
                           gate_name=gate_name,
                           driver_url=driver_url,
                           zed_url=zed_url)

@app.route("/gate/<int:gate_id>/qr")
def gate_qr(gate_id):
    gate = _q("SELECT * FROM gates WHERE id=%s", (gate_id,), fetchone=True)
    gate_name  = gate["name"] if gate else f"Gate {gate_id}"
    driver_url = request.url_root.rstrip("/") + f"/gate/{gate_id}/driver"
    return render_template("qr.html",
                           gate_id=gate_id,
                           gate_name=gate_name,
                           driver_url=driver_url)

@app.route("/gate/<int:gate_id>/driver/activate", methods=["POST"])
def driver_activate(gate_id):
    """
    Driver activates a mission by scanning forklift ID + WMS order number.
    DL-023: dynamic forklift assignment — no pre-assignment needed.
    Public endpoint — no login required.
    """
    data        = request.get_json(silent=True) or {}
    forklift_id = str(data.get("forklift_id", "")).strip()
    mission_ref = str(data.get("mission_ref",  "")).strip()

    if not forklift_id:
        return jsonify({"ok": False, "error": "Forklift ID required"}), 400
    if not mission_ref:
        return jsonify({"ok": False, "error": "Mission reference required"}), 400

    # Find mission by WMS order number or internal ID on this gate
    mission = _q("""SELECT * FROM missions
                    WHERE (wms_mission_id=%s OR id::text=%s)
                    AND gate_id=%s AND status='WAITING'
                    ORDER BY created_at DESC LIMIT 1""",
                 (mission_ref, mission_ref, gate_id), fetchone=True)

    # Not found — check if mission exists on another gate (wrong gate detection)
    if not mission:
        other = _q("""SELECT gate_id FROM missions
                      WHERE (wms_mission_id=%s OR id::text=%s)
                      AND status='WAITING' LIMIT 1""",
                   (mission_ref, mission_ref), fetchone=True)
        if other:
            return jsonify({
                "ok":           False,
                "error":        "Mission is assigned to a different gate",
                "correct_gate": other["gate_id"]
            }), 409
        return jsonify({"ok": False, "error": "Mission not found"}), 404

    # Check no other mission active on this gate
    active = _q("SELECT id FROM missions WHERE gate_id=%s AND status='ACTIVE'",
                (gate_id,), fetchone=True)
    if active:
        return jsonify({"ok": False, "error": "Gate already has an active mission"}), 409

    # Activate mission
    _q("UPDATE missions SET status='ACTIVE', activated_at=now() WHERE id=%s",
       (mission["id"],))

    # Push session forklift_id to ZED Box agent (DL-023)
    # Sets authorized forklift for this session only — not saved to config.json
    _push_to_agent(gate_id, "/agent/apply-config", {
        "config":      {"system": {"session_forklift_id": forklift_id}},
        "restart_app": False
    })

    pallet_count = _q("SELECT COUNT(*) as n FROM pallets WHERE mission_id=%s",
                      (mission["id"],), fetchone=True)
    total = pallet_count["n"] if pallet_count else 0

    _audit("mission.driver_activated",
           target_type="mission", target_id=mission["id"],
           details={"gate_id": gate_id, "forklift_id": forklift_id, "mission_ref": mission_ref})

    log.info(f"[driver] Gate {gate_id} forklift={forklift_id} mission={mission['name']} activated")

    return jsonify({
        "ok":            True,
        "mission_id":    str(mission["id"]),
        "mission_name":  mission["name"],
        "total_pallets": total,
        "loaded":        0,
        "forklift_id":   forklift_id,
    })


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
# ── SIGNED URLS ───────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
def _make_signed_url(clip_id: str, expires_in: int = 900) -> str:
    """
    Generate a signed URL for clip access.
    expires_in: seconds (default 15 min)
    Token = HMAC-SHA256(clip_id + expires_ts, SECRET_KEY)
    """
    expires_ts = int(time.time()) + expires_in
    payload    = f"{clip_id}:{expires_ts}"
    sig        = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return url_for("stream_clip", clip_id=clip_id,
                   expires=expires_ts, sig=sig, _external=True)

def _verify_signed_url(clip_id: str, expires: str, sig: str) -> bool:
    try:
        if int(expires) < int(time.time()):
            return False   # expired
        payload  = f"{clip_id}:{expires}"
        expected = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, sig)
    except Exception:
        return False


@app.route("/clips/stream/<clip_id>")
def stream_clip(clip_id):
    """Serve clip via signed URL — no direct auth required, URL is the token."""
    expires = request.args.get("expires","")
    sig     = request.args.get("sig","")
    if not _verify_signed_url(clip_id, expires, sig):
        return "Link expired or invalid", 403
    clip = _q("SELECT * FROM clips WHERE id=%s AND deleted=false",
              (clip_id,), fetchone=True)
    if not clip:
        return "Clip not found", 404
    _audit("clip.viewed", details={"clip_id": clip_id, "sscc": clip.get("sscc")})
    return send_from_directory(CLIPS_DIR, clip["filename"])


@app.route("/api/clips/<clip_id>/signed-url")
@login_required
def get_signed_url(clip_id):
    """Generate a signed URL for a clip — checks RBAC first."""
    clip = _q("SELECT * FROM clips WHERE id=%s AND deleted=false",
              (clip_id,), fetchone=True)
    if not clip:
        return jsonify({"ok": False, "error": "Not found"}), 404
    if not _can_access_gate(request.user, clip["gate_id"]):
        return jsonify({"ok": False, "error": "Forbidden"}), 403
    url = _make_signed_url(clip_id)
    return jsonify({"ok": True, "url": url, "expires_in": 900})


# ─────────────────────────────────────────────────────────────────────────────
# ── REPORTS ──────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/api/missions/<mission_id>/report/pdf")
@login_required
def mission_report_pdf(mission_id):
    """Generate and serve PDF proof of delivery."""
    mission = _q("SELECT * FROM missions WHERE id=%s", (mission_id,), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Not found"}), 404
    if not _can_access_gate(request.user, mission["gate_id"]):
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    pallets = _q("SELECT * FROM pallets WHERE mission_id=%s ORDER BY id",
                 (mission_id,), fetchall=True) or []
    clips   = _q("SELECT sscc, id FROM clips WHERE mission_id=%s AND deleted=false",
                 (mission_id,), fetchall=True) or []

    # Build signed URL map { sscc → url }
    clip_urls = {}
    for c in clips:
        clip_urls[c["sscc"]] = _make_signed_url(str(c["id"]))

    try:
        from reports import generate_pdf
        pdf_bytes = generate_pdf(dict(mission), [dict(p) for p in pallets], clip_urls)
    except Exception as e:
        log.error(f"[pdf] {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

    _audit("report.downloaded", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="mission", target_id=mission_id,
           details={"format": "pdf"})

    filename = f"digiload_report_{mission['name'].replace(' ','_')}.pdf"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


@app.route("/api/missions/<mission_id>/report/excel")
@login_required
def mission_report_excel(mission_id):
    """Generate and serve Excel export."""
    mission = _q("SELECT * FROM missions WHERE id=%s", (mission_id,), fetchone=True)
    if not mission:
        return jsonify({"ok": False, "error": "Not found"}), 404
    if not _can_access_gate(request.user, mission["gate_id"]):
        return jsonify({"ok": False, "error": "Forbidden"}), 403

    pallets = _q("SELECT * FROM pallets WHERE mission_id=%s ORDER BY id",
                 (mission_id,), fetchall=True) or []

    try:
        from reports import generate_excel
        xlsx_bytes = generate_excel(dict(mission), [dict(p) for p in pallets])
    except Exception as e:
        log.error(f"[excel] {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

    _audit("report.downloaded", user_id=request.user.get("sub"),
           user_email=request.user.get("email"),
           target_type="mission", target_id=mission_id,
           details={"format": "excel"})

    filename = f"digiload_{mission['name'].replace(' ','_')}.xlsx"
    return Response(
        xlsx_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ─────────────────────────────────────────────────────────────────────────────
# ── AUTO-DELETE BACKGROUND JOB (retention policy) ────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
def _auto_delete_loop():
    """
    Nightly job — deletes clips older than RETENTION_DAYS.
    Marks clip as deleted in DB, removes file from disk.
    Runs every 24 hours at ~2am.
    """
    while True:
        # Sleep until 2am
        now     = datetime.utcnow()
        target  = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if target <= now:
            target = target.replace(day=target.day + 1)
        time.sleep((target - now).total_seconds())

        log.info("[retention] Starting nightly clip cleanup")
        try:
            expired = _q("""SELECT id, filename FROM clips
                            WHERE expires_at < now() AND deleted=false""",
                         fetchall=True) or []
            deleted = 0
            for clip in expired:
                filepath = os.path.join(CLIPS_DIR, clip["filename"])
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                    _q("UPDATE clips SET deleted=true WHERE id=%s", (clip["id"],))
                    deleted += 1
                except Exception as e:
                    log.warning(f"[retention] Cannot delete {clip['filename']}: {e}")
            log.info(f"[retention] Deleted {deleted} expired clips")
        except Exception as e:
            log.error(f"[retention] {e}")

threading.Thread(target=_auto_delete_loop, daemon=True).start()
threading.Thread(target=_sftp_watcher_loop, daemon=True).start()


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
