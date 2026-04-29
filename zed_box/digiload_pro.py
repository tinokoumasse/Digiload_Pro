"""
Digiload Pro — ZED Edge Application v2.0
Phase 0: gate_id, module system, logging, disk manager, wms_connector hook

Architecture:
- Autonomous operation — works without Central VM
- gate_id tags every DB write and WMS event
- Module system: video_tracking and multi_angle licensed separately
- wms_connector.py called on every state transition (stub in Phase 0)
- Rotating log files — never fills disk with logs
- Disk manager — auto-deletes old clips per retention policy
- ERROR_FORKLIFT clips recorded (same as VALIDATED)
- Minimal Flask status endpoint on port 5001 (read-only, no auth)
"""

import pyzed.sl as sl
import cv2
import requests
import json
import os
import sys
import threading
import time
import sqlite3
import logging
import shutil
import numpy as np
import subprocess
import hmac
import hashlib
import base64
from collections import deque
from datetime import datetime, timedelta
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from plugin_loader import PluginLoader

# ─────────────────────────────────────────────────────────────────────────────
# SOUND FEEDBACK — Core feature (DL-026)
# Generates tones via numpy, plays via aplay (no extra dependencies)
# ─────────────────────────────────────────────────────────────────────────────
import wave, struct, io

def _make_wav(freq: float, duration: float, volume: float = 0.5,
              sample_rate: int = 44100) -> bytes:
    """Generate a sine-wave WAV in memory."""
    n_samples = int(sample_rate * duration)
    buf = io.BytesIO()
    with wave.open(buf, 'wb') as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)   # 16-bit
        wf.setframerate(sample_rate)
        for i in range(n_samples):
            t      = i / sample_rate
            sample = int(volume * 32767 * np.sin(2 * np.pi * freq * t))
            wf.writeframes(struct.pack('<h', sample))
    return buf.getvalue()

# Pre-generate sound clips at startup
_SOUNDS: dict = {}

def _init_sounds():
    global _SOUNDS
    try:
        _SOUNDS = {
            "validated": _make_wav(880,  0.12) + _make_wav(1100, 0.15),   # rising double beep
            "error":     _make_wav(220,  0.40),                            # low buzz
            "standby":   _make_wav(660,  0.08),                            # soft tick
            "complete":  (_make_wav(660, 0.10) + _make_wav(880, 0.10)
                         + _make_wav(1100, 0.20)),                         # triple rising
        }
        log.info("[sound] Initialized — 4 cues ready")
    except Exception as e:
        log.warning(f"[sound] Init failed: {e}")

def play_sound(name: str):
    """Play a named sound cue in a background thread. Never blocks."""
    if not st.feature_sound:
        return
    wav = _SOUNDS.get(name)
    if not wav:
        return
    def _play():
        try:
            proc = subprocess.Popen(
                ["aplay", "-q", "-"],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            proc.communicate(wav, timeout=3)
        except Exception as e:
            log.debug(f"[sound] {name} failed: {e}")
    threading.Thread(target=_play, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs("/var/log/digiload", exist_ok=True)

log = logging.getLogger("digiload")
log.setLevel(logging.DEBUG)

_fh = RotatingFileHandler(
    "/var/log/digiload/app.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
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
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
CONFIG_FILE  = "/etc/digiload/config.json"
CONFIG_LOCAL = "config.json"
DB_FILE      = "/opt/digiload/digiload.db"
DB_LOCAL     = "digiload.db"
WINDOW_NAME  = "Digiload Pro"
ARUCO_DICT   = cv2.aruco.DICT_APRILTAG_36h11
WLED_EFFECTS = {"static": 0, "blink": 1, "breath": 2, "strobe": 51}
BUFFER_RES   = (960, 540)
BUFFER_QUALITY = 75

_CONFIG_FILE = CONFIG_FILE if os.path.exists(os.path.dirname(CONFIG_FILE)) else CONFIG_LOCAL
_DB_FILE     = DB_FILE     if os.path.exists(os.path.dirname(DB_FILE))     else DB_LOCAL

# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "gate_id":   1,
    "gate_name": "Gate 1",
    "ip_mode":   "dhcp",
    "ip":        "",

    # ── Layer 1: Core — hardcoded, never toggleable ────────────────────────
    # aruco_detection, state_machine, sqlite_db, wms_connector are always on

    # ── Layer 2: Key Features — enabled by default, toggleable per site ───
    "features": {
        "led_control":      True,   # WLED LED strip control
        "disk_manager":     True,   # auto-delete clips by age/size
        "hud":              True,   # OpenCV HUD overlay
        "sound":            True,   # audio feedback cues (aplay)
    },

    # ── Layer 3: Plugins — disabled by default, licensed or per-customer ──
    "modules": {
        "video_tracking": {"enabled": False, "license_key": ""},
        "multi_angle":    {"enabled": False, "license_key": ""},
        "sound_feedback": {"enabled": False},   # audio cues (Phase 8)
        "driver_notify":  {"enabled": False},   # push to tablet (DL-024)
        "sftp_watcher":   {"enabled": False},   # SFTP file drop (Phase 6)
        "mqtt_input":     {"enabled": False},   # MQTT input (Phase 10)
    },

    "camera": {
        "primary":   {"serial": 0, "resolution": "HD1080", "fps": 60},
        "secondary": {"enabled": False, "serial": 0, "resolution": "HD720", "fps": 30},
        "auto_exposure": False, "exposure": 30, "gain": 85
    },
    "recording": {
        "output_dir": "clips", "pre_seconds": 10, "post_seconds": 5,
        "buffer_fps": 30, "retention_days": 30, "max_disk_gb": 50
    },
    "gate": {"rect": [100, 100, 400, 400]},
    "led": {
        "ip": "192.168.1.100",
        "presets": {
            "standby":   {"r": 0,   "g": 0,   "b": 255, "effect": "breath",  "brightness": 128, "speed": 100},
            "armed":     {"r": 0,   "g": 255, "b": 0,   "effect": "static",  "brightness": 200, "speed": 128},
            "error":     {"r": 255, "g": 0,   "b": 0,   "effect": "blink",   "brightness": 255, "speed": 240},
            "confirmed": {"r": 255, "g": 255, "b": 255, "effect": "static",  "brightness": 255, "speed": 128},
            "off":       {"r": 0,   "g": 0,   "b": 0,   "effect": "static",  "brightness": 0,   "speed": 128}
        }
    },
    "system": {"target_id": 0, "forklift_ids": [], "lock_duration": 5.0},
    "wms":    {"webhook_url": "", "api_key": "", "retry_interval_s": 30, "max_retries": 288},
    "vm":     {"sync_url": "", "enabled": False, "required": False},
    "theme": {
        "accent_hex": "#2f7df6", "sidebar_hex": "#05080f",
        "text_title_hex": "#4a6080", "text_main_hex": "#e8f0ff",
        "logo_path": "logo.png", "logo_size": 0.15, "opacity": 0.85
    },
    "ui_text": {
        "title": "DIGILOAD PRO", "idle": "AWAITING MISSION",
        "standby": "SCAN BARCODE", "armed": "LOADING AUTHORIZED",
        "wrong_forklift": "WRONG FORKLIFT", "wrong_sscc": "INVALID BARCODE",
        "validated": "PALLET LOADED"
    }
}

# ─────────────────────────────────────────────────────────────────────────────
# LICENSE VALIDATION
# ─────────────────────────────────────────────────────────────────────────────
_LICENSE_SECRET = "DIGILOAD_LICENSE_SECRET_REPLACE_IN_PRODUCTION"

def validate_license(key, gate_id, module):
    if not key or not key.strip():
        return False
    try:
        parts = key.rsplit(".", 1)
        if len(parts) != 2:
            return False
        payload_b64, sig = parts
        expected = hmac.new(
            _LICENSE_SECRET.encode(), payload_b64.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            log.warning(f"[license] Invalid signature module={module} gate={gate_id}")
            return False
        payload = json.loads(base64.b64decode(payload_b64 + "=="))
        if payload.get("gate_id") != gate_id:   return False
        if payload.get("module")  != module:     return False
        expires = payload.get("expires", "")
        if expires and expires < datetime.now().isoformat():
            log.warning(f"[license] Expired module={module}")
            return False
        log.info(f"[license] OK — {module} gate={gate_id} expires={expires or 'never'}")
        return True
    except Exception as e:
        log.error(f"[license] Error: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# LED ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def build_led_payload(r, g, b, effect="static", brightness=200, speed=128, on=True):
    return {
        "on": on, "bri": int(np.clip(brightness,0,255)),
        "seg": [{"col": [[int(r),int(g),int(b)]], "fx": WLED_EFFECTS.get(effect,0),
                 "sx": int(np.clip(speed,0,255))}]
    }

def send_led(ip, payload, callback=None):
    def _do():
        try:
            requests.post(f"http://{ip}/json/state", json=payload, timeout=1.5)
            if callback: callback(True)
        except Exception as e:
            log.debug(f"[led] {e}")
            if callback: callback(False)
    threading.Thread(target=_do, daemon=True).start()

def hex_to_bgr(hex_str, default=(255,255,255)):
    try:
        h = hex_str.lstrip("#")
        r,g,b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        return (b,g,r)
    except: return default

# ─────────────────────────────────────────────────────────────────────────────
# ROLLING FRAME BUFFER
# ─────────────────────────────────────────────────────────────────────────────
class RollingBuffer:
    def __init__(self, pre_seconds=10, fps=30):
        self._buf       = deque(maxlen=pre_seconds * fps)
        self._lock      = threading.Lock()
        self._last_push = 0.0
        self.interval   = 1.0 / max(fps, 1)

    def push(self, frame):
        now = time.time()
        if now - self._last_push < self.interval: return
        self._last_push = now
        small = cv2.resize(frame, BUFFER_RES)
        ok, buf = cv2.imencode(".jpg", small, [cv2.IMWRITE_JPEG_QUALITY, BUFFER_QUALITY])
        if ok:
            with self._lock: self._buf.append(bytes(buf))

    def snapshot(self):
        with self._lock: raw = list(self._buf)
        frames = []
        for b in raw:
            f = cv2.imdecode(np.frombuffer(b, np.uint8), cv2.IMREAD_COLOR)
            if f is not None: frames.append(f)
        return frames

    def clear(self):
        with self._lock: self._buf.clear()

# ─────────────────────────────────────────────────────────────────────────────
# CLIP RECORDER
# ─────────────────────────────────────────────────────────────────────────────
class ClipRecorder:
    def __init__(self, output_dir="clips", post_seconds=5, fps=30):
        self.output_dir   = output_dir
        self.post_seconds = post_seconds
        self.fps          = fps
        self.interval     = 1.0 / max(fps, 1)
        os.makedirs(output_dir, exist_ok=True)
        self._lock        = threading.Lock()
        self._active      = False
        self._post_target = 0
        self._pre_pri=[]; self._pre_sec=[]
        self._post_pri=[]; self._post_sec=[]
        self._sscc=""; self._tour_id=None
        self._event_type="VALIDATED"; self._last_push=0.0

    @property
    def is_recording(self):
        with self._lock: return self._active

    def trigger(self, pri_buffer, sec_buffer, sscc, tour_id, event_type="VALIDATED"):
        with self._lock:
            if self._active: return
            self._pre_pri     = pri_buffer.snapshot()
            self._pre_sec     = sec_buffer.snapshot() if sec_buffer else []
            self._post_pri    = []; self._post_sec = []
            self._post_target = self.post_seconds * self.fps
            self._sscc        = sscc or ""
            self._tour_id     = tour_id
            self._event_type  = event_type
            self._active      = True
            self._last_push   = 0.0
            log.info(f"[recorder] {event_type} sscc={sscc} pre={len(self._pre_pri)}f")

    def push(self, frame_pri, frame_sec=None):
        now = time.time()
        if now - self._last_push < self.interval: return
        self._last_push = now
        save_args = None
        with self._lock:
            if not self._active: return
            self._post_pri.append(cv2.resize(frame_pri, BUFFER_RES))
            if frame_sec is not None:
                self._post_sec.append(cv2.resize(frame_sec, BUFFER_RES))
            if len(self._post_pri) >= self._post_target:
                self._active = False
                save_args = (self._pre_pri[:], self._pre_sec[:],
                             self._post_pri[:], self._post_sec[:],
                             self._sscc, self._tour_id, self._event_type)
        if save_args:
            threading.Thread(target=self._save, args=save_args, daemon=True).start()

    def _save(self, pre_pri, pre_sec, post_pri, post_sec, sscc, tour_id, event_type):
        all_pri = pre_pri + post_pri
        all_sec = pre_sec + post_sec
        has_sec = len(all_sec) > 0
        if not all_pri: return
        w,h   = BUFFER_RES
        out_w = w*2 if has_sec else w
        ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = "clip" if event_type == "VALIDATED" else "err"
        safe   = "".join(c for c in sscc if c.isalnum())[:20]
        filename = f"{prefix}_{safe}_{ts}.mp4"
        filepath = os.path.join(self.output_dir, filename)
        tmp_path = filepath.replace(".mp4","_raw.mp4")
        fourcc = cv2.VideoWriter_fourcc(*"mp4v")
        writer = cv2.VideoWriter(tmp_path, fourcc, float(self.fps), (out_w,h))
        blank  = np.zeros((h,w,3), np.uint8)
        for i,frame in enumerate(all_pri):
            frame = cv2.resize(frame,(w,h))
            if has_sec:
                sec = cv2.resize(all_sec[i],(w,h)) if i<len(all_sec) else blank
                out = np.hstack([frame,sec])
            else: out = frame
            writer.write(out)
        writer.release()
        try:
            result = subprocess.run([
                "ffmpeg","-y","-i",tmp_path,
                "-vcodec","libx264","-preset","fast",
                "-crf","23","-movflags","+faststart",filepath
            ], capture_output=True, timeout=120)
            if result.returncode == 0:
                os.remove(tmp_path)
                log.info(f"[recorder] Saved: {filename} ({len(all_pri)}f {len(all_pri)/self.fps:.1f}s)")
            else:
                os.rename(tmp_path, filepath)
                log.warning(f"[recorder] ffmpeg failed, keeping raw")
        except Exception as e:
            if os.path.exists(tmp_path): os.rename(tmp_path, filepath)
            log.error(f"[recorder] Transcode error: {e}")
        try:
            with sqlite3.connect(_DB_FILE, timeout=5) as conn:
                conn.execute("""
                    INSERT INTO clips (gate_id,tour_id,sscc,event_type,filename,created_at)
                    VALUES (?,?,?,?,?,datetime('now'))
                """, (st.gate_id, tour_id, sscc, event_type, filename))
                conn.commit()
        except Exception as e:
            log.error(f"[recorder] DB error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# DISK MANAGER
# ─────────────────────────────────────────────────────────────────────────────
def disk_manager_loop(clips_dir, retention_days, max_disk_gb):
    log.info(f"[disk] retention={retention_days}d max={max_disk_gb}GB")
    while True:
        try:
            _cleanup_by_age(clips_dir, retention_days)
            _cleanup_by_size(clips_dir, max_disk_gb)
        except Exception as e:
            log.error(f"[disk] {e}")
        time.sleep(3600)

def _cleanup_by_age(clips_dir, retention_days):
    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = 0
    for f in os.listdir(clips_dir):
        fp = os.path.join(clips_dir, f)
        if not os.path.isfile(fp): continue
        if datetime.fromtimestamp(os.path.getmtime(fp)) < cutoff:
            try:
                os.remove(fp)
                deleted += 1
                with sqlite3.connect(_DB_FILE, timeout=5) as conn:
                    conn.execute("UPDATE clips SET deleted=1 WHERE filename=?", (f,))
            except Exception as e:
                log.warning(f"[disk] Cannot delete {f}: {e}")
    if deleted: log.info(f"[disk] Age: deleted {deleted} clips")

def _cleanup_by_size(clips_dir, max_disk_gb):
    max_bytes = max_disk_gb * 1024**3
    files=[]; total=0
    for f in os.listdir(clips_dir):
        fp = os.path.join(clips_dir,f)
        if not os.path.isfile(fp): continue
        size=os.path.getsize(fp); mtime=os.path.getmtime(fp)
        files.append((mtime,size,fp,f)); total+=size
    if total <= max_bytes: return
    files.sort(key=lambda x:x[0])
    deleted=0
    for mtime,size,fp,fname in files:
        if total<=max_bytes: break
        try:
            os.remove(fp); total-=size; deleted+=1
            with sqlite3.connect(_DB_FILE, timeout=5) as conn:
                conn.execute("UPDATE clips SET deleted=1 WHERE filename=?", (fname,))
        except Exception as e:
            log.warning(f"[disk] Cannot delete {fp}: {e}")
    if deleted: log.info(f"[disk] Size: deleted {deleted} clips (over {max_disk_gb}GB)")

# ─────────────────────────────────────────────────────────────────────────────
# CAMERA MANAGER
# ─────────────────────────────────────────────────────────────────────────────
class CameraManager:
    _RES = {
        "HD2K": sl.RESOLUTION.HD2K, "HD1080": sl.RESOLUTION.HD1080,
        "HD720": sl.RESOLUTION.HD720, "VGA": sl.RESOLUTION.VGA,
    }
    def __init__(self):
        self._pri=sl.Camera(); self._sec=None
        self._pri_mat=sl.Mat(); self._sec_mat=sl.Mat()
        self._sec_frame=None; self._sec_lock=threading.Lock()
        self._sec_running=False; self._sec_buf=None
        self.has_secondary=False

    def _make_init_params(self, cfg):
        p=sl.InitParameters()
        p.camera_resolution=self._RES.get(cfg.get("resolution","HD1080"),sl.RESOLUTION.HD1080)
        p.camera_fps=cfg.get("fps",30)
        serial=cfg.get("serial",0)
        if serial: p.set_from_serial_number(serial)
        return p

    def open_primary(self, cfg):
        err=self._pri.open(self._make_init_params(cfg))
        if err!=sl.ERROR_CODE.SUCCESS:
            log.error(f"[camera] Primary failed: {err}"); return False
        info=self._pri.get_camera_information()
        log.info(f"[camera] Primary: {info.camera_model} SN:{info.serial_number}")
        return True

    def open_secondary(self, cfg, sec_buffer):
        if not cfg.get("enabled",False): return True
        self._sec=sl.Camera()
        err=self._sec.open(self._make_init_params(cfg))
        if err!=sl.ERROR_CODE.SUCCESS:
            log.warning(f"[camera] Secondary failed: {err}"); self._sec=None; return False
        info=self._sec.get_camera_information()
        log.info(f"[camera] Secondary: {info.camera_model} SN:{info.serial_number}")
        self._sec_buf=sec_buffer; self.has_secondary=True; self._sec_running=True
        threading.Thread(target=self._sec_loop,daemon=True).start()
        return True

    def apply_settings(self, auto_exp, exposure, gain):
        try:
            exp=-1 if auto_exp else int(exposure)
            g  =-1 if auto_exp else int(gain)
            self._pri.set_camera_settings(sl.VIDEO_SETTINGS.EXPOSURE,exp)
            self._pri.set_camera_settings(sl.VIDEO_SETTINGS.GAIN,g)
        except Exception as e: log.debug(f"[camera] Settings: {e}")

    def grab(self): return self._pri.grab()==sl.ERROR_CODE.SUCCESS
    def get_primary_frame(self):
        self._pri.retrieve_image(self._pri_mat,sl.VIEW.LEFT)
        return np.copy(self._pri_mat.get_data()[:,:,:3])
    def get_secondary_frame(self):
        with self._sec_lock:
            return self._sec_frame.copy() if self._sec_frame is not None else None
    def _sec_loop(self):
        while self._sec_running:
            if self._sec and self._sec.grab()==sl.ERROR_CODE.SUCCESS:
                self._sec.retrieve_image(self._sec_mat,sl.VIEW.LEFT)
                frame=np.copy(self._sec_mat.get_data()[:,:,:3])
                with self._sec_lock: self._sec_frame=frame
                if self._sec_buf: self._sec_buf.push(frame)
    def close(self):
        self._sec_running=False; self._pri.close()
        if self._sec: self._sec.close()
        log.info("[camera] Closed")

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(_DB_FILE) as conn:
        # WAL mode — allows safe concurrent reads/writes from multiple processes
        # (digiload_pro.py + wms_connector.py + agent.py + future modules)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")   # safe + faster than FULL
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS tours (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_id        INTEGER NOT NULL DEFAULT 1,
            wms_mission_id TEXT,
            name           TEXT NOT NULL,
            date_added     TEXT DEFAULT (datetime('now')),
            status         TEXT DEFAULT 'WAITING',
            total_pallets  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS pallets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_id     INTEGER NOT NULL DEFAULT 1,
            tour_id     INTEGER NOT NULL,
            sscc        TEXT NOT NULL,
            sku         TEXT,
            status      TEXT DEFAULT 'WAITING',
            scan_time   TEXT,
            loaded_at   TEXT,
            forklift_id INTEGER,
            FOREIGN KEY(tour_id) REFERENCES tours(id)
        );
        CREATE TABLE IF NOT EXISTS clips (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_id     INTEGER NOT NULL DEFAULT 1,
            tour_id     INTEGER,
            sscc        TEXT,
            event_type  TEXT DEFAULT 'VALIDATED',
            filename    TEXT NOT NULL,
            deleted     INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS wms_queue (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            gate_id     INTEGER NOT NULL DEFAULT 1,
            event_type  TEXT NOT NULL,
            payload     TEXT NOT NULL,
            retry_count INTEGER DEFAULT 0,
            delivered   INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now')),
            last_attempt TEXT
        );
        CREATE TABLE IF NOT EXISTS system_state (
            id             INTEGER PRIMARY KEY DEFAULT 1,
            gate_id        INTEGER DEFAULT 1,
            active_tour_id INTEGER DEFAULT NULL,
            app_mode       TEXT DEFAULT 'IDLE',
            current_sscc   TEXT DEFAULT NULL,
            last_updated   TEXT DEFAULT (datetime('now'))
        );
        INSERT OR IGNORE INTO system_state (id) VALUES (1);
        """)
    log.info(f"[db] {_DB_FILE}")

@contextmanager
def get_db():
    conn=sqlite3.connect(_DB_FILE,timeout=10)
    conn.row_factory=sqlite3.Row
    try: yield conn; conn.commit()
    except: conn.rollback(); raise
    finally: conn.close()

def db_read_state():
    with get_db() as conn:
        row=conn.execute("SELECT * FROM system_state WHERE id=1").fetchone()
        return dict(row) if row else {}

def db_write_state(mode, sscc=None, tour_id=None):
    with get_db() as conn:
        if mode=="IDLE":
            conn.execute("""UPDATE system_state SET app_mode='IDLE',current_sscc=NULL,
                           active_tour_id=NULL,last_updated=datetime('now') WHERE id=1""")
        else:
            conn.execute("""UPDATE system_state SET app_mode=?,current_sscc=?,
                           active_tour_id=COALESCE(?,active_tour_id),
                           last_updated=datetime('now') WHERE id=1""", (mode,sscc,tour_id))

def db_check_sscc(sscc,tour_id):
    with get_db() as conn:
        row=conn.execute(
            "SELECT id FROM pallets WHERE sscc=? AND tour_id=? AND status='WAITING'",
            (sscc,tour_id)
        ).fetchone()
        return row is not None

def db_validate_pallet(sscc,forklift_id,tour_id):
    with get_db() as conn:
        conn.execute("""UPDATE pallets SET status='LOADED',scan_time=datetime('now'),
                       loaded_at=datetime('now'),forklift_id=?
                       WHERE sscc=? AND tour_id=? AND status='WAITING'""",
                     (forklift_id,sscc,tour_id))
        remaining=conn.execute(
            "SELECT COUNT(*) FROM pallets WHERE tour_id=? AND status='WAITING'",(tour_id,)
        ).fetchone()[0]
        return remaining==0

def db_flag_sscc(sscc,tour_id):
    with get_db() as conn:
        conn.execute("""INSERT OR IGNORE INTO pallets (gate_id,tour_id,sscc,status,scan_time)
                       VALUES (?,?,?,'FLAGGED',datetime('now'))""",(st.gate_id,tour_id,sscc))

def db_complete_tour(tour_id):
    with get_db() as conn:
        conn.execute("UPDATE tours SET status='COMPLETED' WHERE id=?",(tour_id,))
    log.info(f"[db] Tour {tour_id} COMPLETED")

def db_get_progress(tour_id):
    try:
        with get_db() as conn:
            total =conn.execute("SELECT total_pallets FROM tours WHERE id=?",(tour_id,)).fetchone()
            loaded=conn.execute("SELECT COUNT(*) FROM pallets WHERE tour_id=? AND status='LOADED'",(tour_id,)).fetchone()
            if total and loaded: return loaded[0],total[0]
    except Exception as e: log.error(f"[db] progress: {e}")
    return 0,0

# ─────────────────────────────────────────────────────────────────────────────
# APPLICATION STATE
# ─────────────────────────────────────────────────────────────────────────────
class AppState:
    def __init__(self):
        self.gate_id=1; self.gate_name="Gate 1"
        self.module_video_tracking=False; self.module_multi_angle=False
        # Layer 2 feature flags
        self.feature_led_control  = True
        self.feature_disk_manager = True
        self.feature_hud          = True
        self.feature_sound        = True
        # Plugin system
        self.plugins: PluginLoader | None = None
        self.ui_mode="MAIN"
        self.app_mode="IDLE"; self.active_tour_id=None
        self.current_sscc=None; self.last_action_time=0.0
        self.lock_duration=5.0; self.target_id=0; self.forklift_ids=[]
        self.gate_rect=(100,100,400,400); self.drawing=False; self.ix=self.iy=-1
        self.auto_exposure=False; self.exposure_val=30; self.gain_val=85
        self.rec_dir="clips"; self.rec_pre=10; self.rec_post=5
        self.rec_fps=30; self.ret_days=30; self.max_disk=50
        self.pri_buffer=None; self.sec_buffer=None; self.recorder=None
        self.accent_color=(246,125,47); self.sidebar_color=(15,8,5)
        self.text_title_color=(128,96,74); self.text_main_color=(255,240,232)
        self.sidebar_opacity=0.85; self.logo_img=None; self.logo_size_ratio=0.15
        self.ui_text={}; self.led_ip="192.168.1.100"; self.led_presets={}
        self.net_status="—"; self.scan_buffer=""; self.last_scan_display=""
        self.hud_expanded=False; self.last_w=1280; self.last_h=720
        self.anim_start=0.0; self.anim_sscc=""; self.anim_loaded=0
        self.anim_total=0; self.anim_particles=[]
        self.raw_config={}; self.settings_temp={}; self.active_field=None
        self.settings_fields=[
            ("ip",           "LED CONTROLLER IP",  380,150),
            ("target_id",    "TARGET FORKLIFT ID", 380,225),
            ("lock_duration","LOCK DURATION (s)",  380,300),
            ("exposure",     "EXPOSURE (cam)",     380,375),
            ("gain",         "GAIN (cam)",         380,450),
            ("rec_pre",      "CLIP PRE-SECONDS",   380,525),
            ("rec_post",     "CLIP POST-SECONDS",  380,600),
            ("th_accent",    "ACCENT COLOR (#)",   780,150),
            ("th_side",      "SIDEBAR COLOR (#)",  780,225),
            ("th_opac",      "SIDEBAR OPACITY",    780,300),
            ("th_logo",      "LOGO SIZE",          780,375),
        ]

st=AppState()

# Global state lock — all state machine transitions must acquire this
# Prevents race conditions when multiple threads (poll_db, ArUco, timeout)
# try to transition simultaneously
_state_lock = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
def load_config():
    if not os.path.exists(_CONFIG_FILE):
        with open(_CONFIG_FILE,"w",encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG,f,indent=4)
        log.info(f"[config] Generated: {_CONFIG_FILE}")
    with open(_CONFIG_FILE,"r",encoding="utf-8") as f:
        d=json.load(f)
    st.raw_config=d
    st.gate_id=d.get("gate_id",1); st.gate_name=d.get("gate_name","Gate 1")
    mods=d.get("modules",{})
    vt=mods.get("video_tracking",{}); ma=mods.get("multi_angle",{})
    st.module_video_tracking=(vt.get("enabled",False) and
        validate_license(vt.get("license_key",""),st.gate_id,"video_tracking"))
    st.module_multi_angle=(ma.get("enabled",False) and st.module_video_tracking and
        validate_license(ma.get("license_key",""),st.gate_id,"multi_angle"))
    log.info(f"[config] Gate {st.gate_id} — {st.gate_name} | VT={st.module_video_tracking} MA={st.module_multi_angle}")
    cam=d.get("camera",{})
    st.auto_exposure=cam.get("auto_exposure",False)
    st.exposure_val=cam.get("exposure",30); st.gain_val=cam.get("gain",85)
    rec=d.get("recording",{})
    st.rec_dir=rec.get("output_dir","clips"); st.rec_pre=int(rec.get("pre_seconds",10))
    st.rec_post=int(rec.get("post_seconds",5)); st.rec_fps=int(rec.get("buffer_fps",30))
    st.ret_days=int(rec.get("retention_days",30)); st.max_disk=int(rec.get("max_disk_gb",50))
    st.gate_rect=tuple(d.get("gate",{}).get("rect",[100,100,400,400]))
    led=d.get("led",{}); st.led_ip=led.get("ip","192.168.1.100")
    st.led_presets=led.get("presets",DEFAULT_CONFIG["led"]["presets"])
    sys_cfg=d.get("system",{})
    st.target_id=sys_cfg.get("target_id",0)
    st.forklift_ids=sys_cfg.get("forklift_ids",[st.target_id]) or [st.target_id]
    st.lock_duration=float(sys_cfg.get("lock_duration",5.0))
    t=d.get("theme",{})
    st.accent_color    =hex_to_bgr(t.get("accent_hex",    "#2f7df6"))
    st.sidebar_color   =hex_to_bgr(t.get("sidebar_hex",   "#05080f"))
    st.text_title_color=hex_to_bgr(t.get("text_title_hex","#4a6080"))
    st.text_main_color =hex_to_bgr(t.get("text_main_hex", "#e8f0ff"))
    st.sidebar_opacity =float(t.get("opacity",0.85))
    st.logo_size_ratio =float(t.get("logo_size",0.15))
    logo_path=t.get("logo_path","logo.png")
    if os.path.exists(logo_path): st.logo_img=cv2.imread(logo_path,cv2.IMREAD_UNCHANGED)
    st.ui_text=d.get("ui_text",DEFAULT_CONFIG["ui_text"])

    # ── Feature flags (Layer 2) ──────────────────────────────────────────────
    feats = d.get("features", DEFAULT_CONFIG["features"])
    st.feature_led_control  = feats.get("led_control",  True)
    st.feature_disk_manager = feats.get("disk_manager", True)
    st.feature_hud          = feats.get("hud",          True)
    st.feature_sound        = feats.get("sound",        True)

    log.info(f"[features] led={st.feature_led_control} disk={st.feature_disk_manager} hud={st.feature_hud} sound={st.feature_sound}")

    with get_db() as conn:
        conn.execute("UPDATE system_state SET gate_id=? WHERE id=1",(st.gate_id,))

def save_config():
    d=st.raw_config
    d.setdefault("led",{})["ip"]=st.settings_temp.get("ip",st.led_ip)
    d.setdefault("system",{}).update({
        "target_id":   _si(st.settings_temp.get("target_id",st.target_id),0),
        "lock_duration":_sf(st.settings_temp.get("lock_duration",st.lock_duration),5.0),
    })
    d.setdefault("camera",{}).update({
        "exposure":_si(st.settings_temp.get("exposure",st.exposure_val),30),
        "gain":    _si(st.settings_temp.get("gain",st.gain_val),85),
    })
    d.setdefault("recording",{}).update({
        "pre_seconds": _si(st.settings_temp.get("rec_pre",st.rec_pre),10),
        "post_seconds":_si(st.settings_temp.get("rec_post",st.rec_post),5),
    })
    d.setdefault("theme",{}).update({
        "accent_hex":  st.settings_temp.get("th_accent","#2f7df6"),
        "sidebar_hex": st.settings_temp.get("th_side",  "#05080f"),
        "opacity":     _sf(st.settings_temp.get("th_opac",st.sidebar_opacity),0.85),
        "logo_size":   _sf(st.settings_temp.get("th_logo",st.logo_size_ratio),0.15),
    })
    with open(_CONFIG_FILE,"w",encoding="utf-8") as f: json.dump(d,f,indent=4)
    log.info("[config] Saved"); load_config(); _reinit_recording()

def _reinit_recording():
    if st.module_video_tracking:
        st.pri_buffer=RollingBuffer(st.rec_pre,st.rec_fps)
        st.recorder  =ClipRecorder(st.rec_dir,st.rec_post,st.rec_fps)
        st.sec_buffer=RollingBuffer(st.rec_pre,st.rec_fps) if st.module_multi_angle else None
    else:
        st.pri_buffer=None; st.sec_buffer=None; st.recorder=None

def init_settings_temp():
    st.settings_temp={
        "ip":st.led_ip,"target_id":str(st.target_id),
        "lock_duration":str(st.lock_duration),"exposure":str(st.exposure_val),
        "gain":str(st.gain_val),"rec_pre":str(st.rec_pre),"rec_post":str(st.rec_post),
        "th_accent":st.raw_config.get("theme",{}).get("accent_hex","#2f7df6"),
        "th_side":  st.raw_config.get("theme",{}).get("sidebar_hex","#05080f"),
        "th_opac":str(st.sidebar_opacity),"th_logo":str(st.logo_size_ratio),
    }
    st.active_field=None

def _si(v,d=0):
    try: return int(v)
    except: return d
def _sf(v,d=0.0):
    try: return float(v)
    except: return d

# ─────────────────────────────────────────────────────────────────────────────
# LED
# ─────────────────────────────────────────────────────────────────────────────
def _led_cb(ok):
    # Legacy callback — LED status now comes from led_control plugin
    st.net_status = "OK" if ok else "ERR"

def send_preset(name):
    # Delegated to led_control plugin via on_state_change
    # Kept as direct call for backward compatibility during transition
    if not st.feature_led_control:
        return
    if st.plugins:
        # Find led_control plugin and call directly if needed outside transition
        import importlib
        for p in st.plugins._plugins:
            if hasattr(p, '_send_preset'):
                p._send_preset(name)
                return
    # Fallback: direct send (used before plugins are loaded)
    _send_led_direct(name)

def _send_led_direct(name):
    """Fallback direct LED send — used only before plugin system is ready."""
    p = st.led_presets.get(name, st.led_presets.get("off", {}))
    if not p or not st.led_ip: return
    import requests, numpy as np
    WLED_EFFECTS = {"static":0,"blink":1,"breath":2,"strobe":51}
    payload = {
        "on": (name != "off"),
        "bri": int(np.clip(p.get("brightness",200),0,255)),
        "seg": [{"col":[[p.get("r",0),p.get("g",0),p.get("b",0)]],
                 "fx": WLED_EFFECTS.get(p.get("effect","static"),0),
                 "sx": int(np.clip(p.get("speed",128),0,255))}]
    }
    def _do():
        try: requests.post(f"http://{st.led_ip}/json/state",json=payload,timeout=1.5)
        except: pass
    import threading
    threading.Thread(target=_do,daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────────
# WMS STUB (Phase 0 — queues events, Phase 1 wms_connector.py processes)
# ─────────────────────────────────────────────────────────────────────────────
def _wms_notify(event_type, payload):
    try:
        with get_db() as conn:
            conn.execute("""INSERT INTO wms_queue (gate_id,event_type,payload,created_at)
                           VALUES (?,?,?,datetime('now'))""",
                         (st.gate_id,event_type,json.dumps(payload)))
        log.info(f"[wms] Queued: {event_type} sscc={payload.get('sscc','')}")
    except Exception as e: log.error(f"[wms] Queue error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# STATE MACHINE
# ─────────────────────────────────────────────────────────────────────────────
_PRESET_FOR_MODE={
    "IDLE":"off","STANDBY":"standby","ARMED":"armed",
    "ERROR_SSCC":"error","ERROR_FORKLIFT":"error","VALIDATED":"confirmed",
}

def transition(mode, sscc=None, tour_id=None):
    with _state_lock:
        prev=st.app_mode; st.app_mode=mode; st.last_action_time=time.time()
        if sscc    is not None: st.current_sscc  =sscc
        if tour_id is not None: st.active_tour_id=tour_id
        db_write_state(mode,st.current_sscc,st.active_tour_id)
        send_preset(_PRESET_FOR_MODE.get(mode,"off"))
        log.info(f"[state] {prev}→{mode}"+(f" sscc={sscc}" if sscc else "")+(f" tour={tour_id}" if tour_id else ""))
        do_validated   = (mode=="VALIDATED"      and st.active_tour_id)
        do_err_fork    = (mode=="ERROR_FORKLIFT")
        tour_snap      = st.active_tour_id
        sscc_snap      = st.current_sscc

    # ── Side effects outside the lock (avoid deadlocks from nested calls) ──
    # Build context dict for plugins
    ctx = {
        "gate_id":    st.gate_id,
        "gate_name":  st.gate_name,
        "sscc":       sscc_snap,
        "tour_id":    tour_snap,
        "forklift_id":st.target_id,
    }
    # Fire plugin on_state_change
    if st.plugins:
        st.plugins.on_state_change(mode, ctx)

    # Sound feedback (core feature)
    _sound_for_mode = {
        "STANDBY":        "standby",
        "VALIDATED":      "validated",
        "ERROR_SSCC":     "error",
        "ERROR_FORKLIFT": "error",
    }
    if mode in _sound_for_mode:
        play_sound(_sound_for_mode[mode])

    if do_validated:
        loaded,total=db_get_progress(tour_snap)
        trigger_validated_anim(sscc_snap,loaded,total)
        if st.module_video_tracking and st.recorder and st.pri_buffer:
            st.recorder.trigger(st.pri_buffer,st.sec_buffer,sscc_snap,tour_snap,"VALIDATED")
        _wms_notify("pallet.loaded",{
            "gate_id":st.gate_id,"gate_name":st.gate_name,
            "sscc":sscc_snap,"forklift_id":st.target_id,
            "loaded_at":datetime.now().isoformat(),"tour_id":tour_snap,
        })
        if st.plugins:
            st.plugins.on_validated(sscc_snap, {
                **ctx, "loaded": loaded, "total": total
            })

    if do_err_fork:
        if st.module_video_tracking and st.recorder and st.pri_buffer:
            st.recorder.trigger(st.pri_buffer,st.sec_buffer,sscc_snap,tour_snap,"ERROR_FORKLIFT")
        _wms_notify("gate.error",{
            "gate_id":st.gate_id,"gate_name":st.gate_name,"error_type":"WRONG_FORKLIFT",
            "sscc":sscc_snap,"timestamp":datetime.now().isoformat(),
        })
        if st.plugins:
            st.plugins.on_error("WRONG_FORKLIFT", ctx)

def poll_db_activation():
    while True:
        try:
            state=db_read_state(); db_mode=state.get("app_mode","IDLE"); db_tour=state.get("active_tour_id")
            if db_mode=="STANDBY" and st.app_mode=="IDLE" and db_tour:
                st.active_tour_id=db_tour; transition("STANDBY",tour_id=db_tour)
            elif db_mode=="IDLE" and st.app_mode!="IDLE":
                st.current_sscc=None; st.active_tour_id=None; transition("IDLE")
        except Exception as e: log.error(f"[poll_db] {e}")
        time.sleep(1.0)

def handle_sscc_scan(sscc):
    if st.app_mode!="STANDBY" or not st.active_tour_id:
        st.last_scan_display=f"OUT OF SESSION: {sscc}"; log.warning(f"[scan] OOS: {sscc}"); return
    if db_check_sscc(sscc,st.active_tour_id):
        transition("ARMED",sscc=sscc); st.last_scan_display=f"OK: {sscc}"
    else:
        db_flag_sscc(sscc,st.active_tour_id); transition("ERROR_SSCC")
        st.last_scan_display=f"INVALID: {sscc}"; log.warning(f"[scan] Invalid: {sscc}")
        if st.plugins:
            st.plugins.on_error("WRONG_SSCC", {
                "gate_id": st.gate_id, "sscc": sscc, "tour_id": st.active_tour_id
            })

def handle_aruco_detected(ids_array):
    flat=ids_array.flatten().tolist()
    if st.app_mode=="ARMED" and st.current_sscc and st.active_tour_id:
        authorized=set(st.forklift_ids); match=authorized&set(flat)
        if match:
            matched_id=list(match)[0]; sscc_done=st.current_sscc
            st.current_sscc=None; st.target_id=matched_id
            is_last=db_validate_pallet(sscc_done,matched_id,st.active_tour_id)
            transition("VALIDATED",sscc=sscc_done)
            if is_last:
                play_sound("complete")
                db_complete_tour(st.active_tour_id)
                _wms_notify("mission.completed",{
                    "gate_id":st.gate_id,"gate_name":st.gate_name,
                    "tour_id":st.active_tour_id,"completed_at":datetime.now().isoformat(),
                })
                def _finish():
                    time.sleep(2.0); st.active_tour_id=None; transition("IDLE")
                threading.Thread(target=_finish,daemon=True).start()
            else:
                def _continue():
                    time.sleep(2.0); transition("STANDBY")
                threading.Thread(target=_continue,daemon=True).start()
        else:
            log.warning(f"[aruco] Wrong: detected={flat} authorized={list(authorized)}")
            transition("ERROR_FORKLIFT")
            check_wrong_gate(flat[0] if flat else -1)

def tick_timeouts():
    if st.app_mode not in ("ERROR_SSCC","ERROR_FORKLIFT"): return
    if time.time()-st.last_action_time<st.lock_duration: return
    if   st.app_mode=="ERROR_SSCC":     transition("STANDBY")
    elif st.app_mode=="ERROR_FORKLIFT": transition("ARMED",sscc=st.current_sscc)

# ─────────────────────────────────────────────────────────────────────────────
# DRAWING
# ─────────────────────────────────────────────────────────────────────────────
BLUE=(246,125,47); WHITE=(255,255,255); GREY=(90,90,90)
DARK=(18,10,5);    GREEN=(80,209,48);   RED=(58,69,255)

def overlay_transparent(bg,overlay,x,y):
    if overlay is None or overlay.ndim<3 or overlay.shape[2]<4: return bg
    oh,ow=overlay.shape[:2]
    if y+oh>bg.shape[0] or x+ow>bg.shape[1]: return bg
    mask=overlay[...,3:]/255.0
    bg[y:y+oh,x:x+ow]=(1-mask)*bg[y:y+oh,x:x+ow]+mask*overlay[...,:3]
    return bg

def txt(img,text,pos,size=0.5,color=(255,255,255),thick=1):
    cv2.putText(img,str(text).upper(),pos,cv2.FONT_HERSHEY_SIMPLEX,size,(0,0,0),thick+2,cv2.LINE_AA)
    cv2.putText(img,str(text).upper(),pos,cv2.FONT_HERSHEY_SIMPLEX,size,color,thick,cv2.LINE_AA)

def txt_sm(img,text,pos,size=0.4,color=(150,150,150)):
    cv2.putText(img,str(text).upper(),pos,cv2.FONT_HERSHEY_SIMPLEX,size,color,1,cv2.LINE_AA)

def draw_rrect(img,pt1,pt2,color,radius,thick=-1,alpha=1.0):
    if alpha<1.0:
        ov=img.copy(); _rrect(ov,pt1,pt2,color,radius,thick)
        cv2.addWeighted(ov,alpha,img,1-alpha,0,img)
    else: _rrect(img,pt1,pt2,color,radius,thick)

def _rrect(img,pt1,pt2,color,radius,thick):
    x1,y1=pt1; x2,y2=pt2; r=min(radius,(x2-x1)//2,(y2-y1)//2)
    if thick==-1:
        cv2.rectangle(img,(x1+r,y1),(x2-r,y2),color,-1)
        cv2.rectangle(img,(x1,y1+r),(x2,y2-r),color,-1)
        for cx2,cy2 in [(x1+r,y1+r),(x2-r,y1+r),(x1+r,y2-r),(x2-r,y2-r)]:
            cv2.circle(img,(cx2,cy2),r,color,-1,cv2.LINE_AA)
    else:
        cv2.line(img,(x1+r,y1),(x2-r,y1),color,thick,cv2.LINE_AA)
        cv2.line(img,(x1+r,y2),(x2-r,y2),color,thick,cv2.LINE_AA)
        cv2.line(img,(x1,y1+r),(x1,y2-r),color,thick,cv2.LINE_AA)
        cv2.line(img,(x2,y1+r),(x2,y2-r),color,thick,cv2.LINE_AA)
        cv2.ellipse(img,(x1+r,y1+r),(r,r),180,0,90,color,thick,cv2.LINE_AA)
        cv2.ellipse(img,(x2-r,y1+r),(r,r),270,0,90,color,thick,cv2.LINE_AA)
        cv2.ellipse(img,(x1+r,y2-r),(r,r), 90,0,90,color,thick,cv2.LINE_AA)
        cv2.ellipse(img,(x2-r,y2-r),(r,r),  0,0,90,color,thick,cv2.LINE_AA)

def draw_gate(frame,h,w):
    rx,ry,rw,rh=st.gate_rect
    rx=max(0,min(rx,w-2)); ry=max(0,min(ry,h-2))
    rw=max(20,min(rw,w-rx)); rh=max(20,min(rh,h-ry))
    now=time.time(); mode=st.app_mode
    if mode=="IDLE": color=GREY; glow=0.0
    elif mode=="STANDBY": pulse=0.5+0.5*np.sin(now*2.5); color=BLUE; glow=0.10+0.08*pulse
    elif mode=="ARMED": color=GREEN; glow=0.12
    elif mode=="VALIDATED":
        elapsed=now-st.last_action_time; pct=max(0.0,1.0-elapsed/st.lock_duration)
        color=GREEN; glow=0.08+0.10*pct
    elif mode in ("ERROR_SSCC","ERROR_FORKLIFT"):
        blink=int(now*3)%2; color=RED if blink else (20,20,60); glow=0.10*blink
    else: color=GREY; glow=0.0
    if glow>0:
        for expand,a_mult in [(18,0.4),(10,0.6),(4,0.9)]:
            ov=frame.copy()
            _rrect(ov,(rx-expand,ry-expand),(rx+rw+expand,ry+rh+expand),color,28,2)
            cv2.addWeighted(ov,glow*a_mult,frame,1-glow*a_mult,0,frame)
    _rrect(frame,(rx,ry),(rx+rw,ry+rh),color,20,2)
    arm=22; thick=2
    for cx2,cy2,sx,sy in [(rx,ry,1,1),(rx+rw,ry,-1,1),(rx,ry+rh,1,-1),(rx+rw,ry+rh,-1,-1)]:
        cv2.line(frame,(cx2,cy2),(cx2+sx*arm,cy2),color,thick,cv2.LINE_AA)
        cv2.line(frame,(cx2,cy2),(cx2,cy2+sy*arm),color,thick,cv2.LINE_AA)

def draw_hud(frame,h,w):
    t=st.ui_text; mode=st.app_mode; now=time.time()
    HUD_H=200 if st.hud_expanded else 52; PAD=10; hud_w=w-2*PAD
    ov=frame.copy(); _rrect(ov,(PAD,PAD),(PAD+hud_w,PAD+HUD_H),DARK,14,-1)
    cv2.addWeighted(ov,0.82,frame,0.18,0,frame)
    bc=BLUE if mode!="IDLE" else (40,42,50)
    _rrect(frame,(PAD,PAD),(PAD+hud_w,PAD+HUD_H),bc,14,1)
    state_color={"IDLE":GREY,"STANDBY":BLUE,"ARMED":GREEN,"VALIDATED":WHITE,
                 "ERROR_SSCC":RED,"ERROR_FORKLIFT":RED}.get(mode,GREY)
    state_label={
        "IDLE":t.get("idle","AWAITING MISSION"),"STANDBY":t.get("standby","SCAN BARCODE"),
        "ARMED":t.get("armed","LOADING AUTHORIZED"),"ERROR_SSCC":t.get("wrong_sscc","INVALID BARCODE"),
        "ERROR_FORKLIFT":t.get("wrong_forklift","WRONG FORKLIFT"),"VALIDATED":t.get("validated","PALLET LOADED"),
    }.get(mode,mode)
    dot_x,dot_y=PAD+22,PAD+26
    if mode in ("STANDBY","ARMED") and int(now*2)%2:
        cv2.circle(frame,(dot_x,dot_y),9,state_color,-1,cv2.LINE_AA)
    cv2.circle(frame,(dot_x,dot_y),6,state_color,-1,cv2.LINE_AA)
    chip_x=dot_x+16; chip_w=len(state_label)*8+20
    _rrect(frame,(chip_x,PAD+14),(chip_x+chip_w,PAD+38),tuple(max(0,c//6) for c in state_color),10,-1)
    txt(frame,state_label,(chip_x+10,PAD+32),0.45,state_color,1)
    cx=w//2
    gate_lbl=f"{t.get('title','DIGILOAD PRO')}  |  {st.gate_name.upper()}"
    txt(frame,gate_lbl,(cx-120,PAD+33),0.45,BLUE,2)
    rx_base=w-PAD-20
    if st.module_video_tracking:
        mod_col=RED if (st.recorder and st.recorder.is_recording and int(now*2)%2) else (60,60,200)
        cv2.circle(frame,(rx_base,PAD+20),5,mod_col,-1,cv2.LINE_AA)
        lbl="REC" if (st.recorder and st.recorder.is_recording) else "VT"
        txt_sm(frame,lbl,(rx_base-30,PAD+25),0.32,mod_col); rx_base-=50
    nc=GREEN if st.net_status=="OK" else RED if st.net_status=="ERR" else GREY
    txt_sm(frame,f"LED:{st.net_status}",(rx_base-70,PAD+25),0.35,nc); rx_base-=85
    fids="+".join(str(f) for f in st.forklift_ids[:3])
    txt_sm(frame,f"FORK #{fids}",(rx_base-80,PAD+25),0.35,(160,160,160)); rx_base-=90
    if st.active_tour_id:
        loaded,total=db_get_progress(st.active_tour_id); pct=loaded/total if total>0 else 0.0
        bx=rx_base-110
        cv2.rectangle(frame,(bx,PAD+18),(bx+100,PAD+24),(30,30,30),-1)
        cv2.rectangle(frame,(bx,PAD+18),(bx+int(100*pct),PAD+24),GREEN,-1)
        txt_sm(frame,f"{loaded}/{total}",(bx,PAD+36),0.32,(140,140,140))
    arrow="v  MORE" if not st.hud_expanded else "^  LESS"
    txt_sm(frame,arrow,(w//2-22,PAD+HUD_H-4),0.3,(60,70,90))
    if not st.hud_expanded: return
    ey=PAD+52+6; cv2.line(frame,(PAD+20,ey),(PAD+hud_w-20,ey),(30,35,50),1); ey+=12
    col1=PAD+20
    if st.current_sscc:
        txt_sm(frame,"ARMED BARCODE",(col1,ey),0.32,GREY)
        txt(frame,st.current_sscc[:24],(col1,ey+20),0.5,GREEN,1); ey2=ey+38
    else: ey2=ey
    fi_w=340; _rrect(frame,(col1,ey2),(col1+fi_w,ey2+36),(15,18,28),8,-1)
    bc=BLUE if int(now*2)%2 else (40,45,60)
    _rrect(frame,(col1,ey2),(col1+fi_w,ey2+36),bc,8,1)
    buf_d=(st.scan_buffer+"_")[:28]
    txt(frame,buf_d if buf_d.strip("_ ") else "type or scan barcode_",
        (col1+10,ey2+24),0.45,st.text_main_color if st.scan_buffer else (50,55,70),1)
    if st.last_scan_display:
        c=GREEN if "OK" in st.last_scan_display else RED
        txt_sm(frame,st.last_scan_display[:40],(col1,ey2+52),0.35,c)
    col2=col1+fi_w+40
    if st.active_tour_id:
        loaded,total=db_get_progress(st.active_tour_id); pct=loaded/total if total>0 else 0.0
        txt_sm(frame,"MISSION PROGRESS",(col2,ey),0.32,GREY)
        bar_w=320; seg_n=min(total,50)
        if seg_n>0:
            sw=(bar_w-seg_n)//seg_n
            for i in range(seg_n):
                sx=col2+i*(sw+1); loaded_seg=(i/seg_n)<pct
                sc=GREEN if loaded_seg else (25,28,40)
                if loaded_seg and (i+1)/seg_n>pct-1.0/seg_n: sc=tuple(min(255,c+60) for c in GREEN)
                cv2.rectangle(frame,(sx,ey+10),(sx+sw,ey+22),sc,-1)
        else:
            cv2.rectangle(frame,(col2,ey+10),(col2+bar_w,ey+22),(25,28,40),-1)
            cv2.rectangle(frame,(col2,ey+10),(col2+int(bar_w*pct),ey+22),GREEN,-1)
        txt(frame,f"{loaded}  /  {total}  PALLETS",(col2,ey+42),0.55,WHITE,1)
        txt_sm(frame,f"{int(pct*100)}% COMPLETE",(col2,ey+60),0.35,GREY)
    col3=col2+380
    if col3+120<w:
        txt_sm(frame,"[S] SETTINGS",(col3,ey+10),0.32,GREY)
        txt_sm(frame,"[A] AUTO-EXP", (col3,ey+28),0.32,GREY)
        txt_sm(frame,"[ESC] QUIT",   (col3,ey+46),0.32,GREY)

def draw_settings_panel(frame,h,w):
    ov=frame.copy(); cv2.rectangle(ov,(0,0),(w,h),(5,8,15),-1)
    cv2.addWeighted(ov,0.95,frame,0.05,0,frame)
    PX,PY,PW,PH=280,50,w-560,h-100
    draw_rrect(frame,(PX,PY),(PX+PW,PY+PH),(12,15,25),18,-1)
    draw_rrect(frame,(PX,PY),(PX+PW,PY+PH),BLUE,18,2)
    txt(frame,"DIGILOAD PRO  —  SYSTEM CONFIGURATION",(PX+30,PY+42),0.62,BLUE,2)
    cv2.line(frame,(PX+30,PY+52),(PX+PW-30,PY+52),(30,40,70),1)
    txt_sm(frame,f"GATE {st.gate_id}  —  {st.gate_name}",(PX+30,PY+72),0.38,GREY)
    txt_sm(frame,"SYSTEM & RECORDING",(PX+30,PY+90),0.35,GREY)
    txt_sm(frame,"THEME",(PX+PW//2+30,PY+90),0.35,GREY)
    for key,label,px,py in st.settings_fields:
        txt_sm(frame,label,(px,py-6),0.32,GREY)
        active=st.active_field==key; fc=BLUE if active else (35,40,60)
        draw_rrect(frame,(px,py),(px+340,py+36),(14,17,30),8,-1)
        draw_rrect(frame,(px,py),(px+340,py+36),fc,8,1)
        val=st.settings_temp.get(key,"")
        if active and int(time.time()*2)%2: val+="_"
        txt(frame,val[:30],(px+10,py+24),0.45,WHITE,1)
    draw_rrect(frame,(PX+PW//2-130,PY+PH-60),(PX+PW//2-10,PY+PH-22),(0,100,40),10,-1)
    txt(frame,"SAVE",(PX+PW//2-105,PY+PH-32),0.5,WHITE,2)
    draw_rrect(frame,(PX+PW//2+10,PY+PH-60),(PX+PW//2+150,PY+PH-22),(80,20,20),10,-1)
    txt(frame,"CANCEL",(PX+PW//2+22,PY+PH-32),0.5,WHITE,2)

# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION ANIMATION
# ─────────────────────────────────────────────────────────────────────────────
def trigger_validated_anim(sscc,loaded,total):
    st.anim_start=time.time(); st.anim_sscc=sscc or ""
    st.anim_loaded=loaded; st.anim_total=total
    rng=np.random.default_rng()
    angles=rng.uniform(0,2*np.pi,80); speeds=rng.uniform(3,12,80)
    st.anim_particles=[{
        "x":0.0,"y":0.0,"vx":float(np.cos(a)*s),"vy":float(np.sin(a)*s)-5,
        "size":int(rng.integers(3,9)),"color":(int(c[0]),int(c[1]),int(c[2])),"alive":True,
    } for a,s,c in zip(angles,speeds,rng.choice(
        [[0,232,122],[0,255,160],[255,255,255],[232,160,32],[0,200,100]],size=80))]

def draw_validated_overlay(frame,h,w):
    if st.anim_start==0.0: return frame
    elapsed=time.time()-st.anim_start
    if elapsed>2.8: st.anim_start=0.0; st.anim_particles.clear(); return frame
    alpha=(elapsed/0.2) if elapsed<0.2 else (1.0 if elapsed<2.2 else 1.0-(elapsed-2.2)/0.6)
    alpha=max(0.0,min(1.0,alpha))
    tint=frame.copy(); cv2.rectangle(tint,(0,0),(w,h),(0,180,80),-1)
    cv2.addWeighted(tint,0.18*alpha,frame,1.0-0.18*alpha,0,frame)
    cx,cy=w//2,h//2
    for p in st.anim_particles:
        if not p["alive"]: continue
        if p["x"]==0.0 and p["y"]==0.0: p["x"],p["y"]=float(cx),float(cy)
        p["x"]+=p["vx"]; p["y"]+=p["vy"]; p["vy"]+=0.35; p["vx"]*=0.98
        fade=max(0.0,1.0-elapsed/2.0)
        if fade<=0 or not (0<=int(p["x"])<w and 0<=int(p["y"])<h):
            p["alive"]=False; continue
        bright=tuple(int(c*fade) for c in p["color"])
        cv2.circle(frame,(int(p["x"]),int(p["y"])),p["size"]//2,bright,-1,cv2.LINE_AA)
    rl=np.zeros_like(frame)
    cv2.circle(rl,(cx,cy),90,(int(0*alpha),int(232*alpha),int(100*alpha)),3,cv2.LINE_AA)
    cv2.addWeighted(rl,1.0,frame,1.0,0,frame)
    cp=min(1.0,max(0.0,(elapsed-0.15)/0.25))
    if cp>0:
        p1=(cx-50,cy+10); pm=(cx-15,cy+40); p2=(cx+50,cy-30)
        if cp<=0.5:
            t2=cp/0.5; ep=(int(p1[0]+(pm[0]-p1[0])*t2),int(p1[1]+(pm[1]-p1[1])*t2))
            cv2.line(frame,p1,ep,(0,255,120),5,cv2.LINE_AA)
        else:
            cv2.line(frame,p1,pm,(0,255,120),5,cv2.LINE_AA)
            t2=(cp-0.5)/0.5; ep=(int(pm[0]+(p2[0]-pm[0])*t2),int(pm[1]+(p2[1]-pm[1])*t2))
            cv2.line(frame,pm,ep,(0,255,120),5,cv2.LINE_AA)
    if elapsed>0.4:
        ta=min(1.0,(elapsed-0.4)/0.2)*alpha
        g=(int(0*ta),int(255*ta),int(120*ta)); m=(int(180*ta),int(180*ta),int(180*ta))
        txt(frame,"PALLET LOADED",(cx-160,cy+130),1.0,g,3)
        if st.anim_sscc: txt(frame,f"BARCODE  {st.anim_sscc}",(cx-180,cy+170),0.55,m,1)
        if st.anim_total>0: txt(frame,f"{st.anim_loaded} / {st.anim_total}  PALLETS",(cx-100,cy+205),0.55,m,1)
    return frame

# ─────────────────────────────────────────────────────────────────────────────
# INPUT
# ─────────────────────────────────────────────────────────────────────────────
def mouse_cb(event,x,y,flags,param):
    if st.ui_mode=="MAIN":
        if event==cv2.EVENT_LBUTTONDOWN:
            hud_h=200 if st.hud_expanded else 52
            if 10<=x<=st.last_w-10 and 10<=y<=10+hud_h:
                st.hud_expanded=not st.hud_expanded; return
            st.drawing=True; st.ix,st.iy=x,y
        elif event==cv2.EVENT_LBUTTONUP and st.drawing:
            st.drawing=False; bw=abs(x-st.ix); bh=abs(y-st.iy)
            if bw>20:
                st.gate_rect=(min(st.ix,x),min(st.iy,y),bw,bh)
                st.raw_config.setdefault("gate",{})["rect"]=list(st.gate_rect)
                with open(_CONFIG_FILE,"w",encoding="utf-8") as f: json.dump(st.raw_config,f,indent=4)
                log.info(f"[gate] Zone: {st.gate_rect}")
    elif st.ui_mode=="SETTINGS":
        if event==cv2.EVENT_LBUTTONDOWN:
            st.active_field=None
            for key,_,px,py in st.settings_fields:
                if px<=x<=px+340 and py<=y<=py+36: st.active_field=key
            h2,w2=st.last_h,st.last_w; PW=w2-560; PX=280; PY=50; PH=h2-100
            if PX+PW//2-130<=x<=PX+PW//2-10 and PY+PH-60<=y<=PY+PH-22:
                save_config(); st.ui_mode="MAIN"
            if PX+PW//2+10<=x<=PX+PW//2+150 and PY+PH-60<=y<=PY+PH-22:
                st.ui_mode="MAIN"

def handle_key(key,cam):
    if key==27: return False
    if st.ui_mode=="MAIN":
        if key in (ord("s"),ord("S")): init_settings_temp(); st.ui_mode="SETTINGS"
        elif key in (13,10):
            if st.scan_buffer.strip(): handle_sscc_scan(st.scan_buffer.strip()); st.scan_buffer=""
        elif key==8: st.scan_buffer=st.scan_buffer[:-1]
        elif key==82: st.forklift_ids=[st.forklift_ids[0]+1] if st.forklift_ids else [1]
        elif key==84: st.forklift_ids=[max(0,st.forklift_ids[0]-1)] if st.forklift_ids else [0]
        elif key in (ord("a"),ord("A")):
            st.auto_exposure=not st.auto_exposure
            cam.apply_settings(st.auto_exposure,st.exposure_val,st.gain_val)
        elif 32<=key<=126: st.scan_buffer+=chr(key)
    elif st.ui_mode=="SETTINGS":
        if st.active_field is not None:
            if key==8: st.settings_temp[st.active_field]=st.settings_temp[st.active_field][:-1]
            elif 32<=key<=126: st.settings_temp[st.active_field]+=chr(key)
        if key in (13,10): save_config(); st.ui_mode="MAIN"
    return True

# Status endpoint removed — served by agent.py on port 5002 (DL-020)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# CORE ALERTS — mission reminder, incomplete mission, wrong gate (DL-026)
# ─────────────────────────────────────────────────────────────────────────────

# Configurable thresholds (seconds)
MISSION_REMINDER_IDLE_S  = 5 * 60   # 5 min idle → reminder
MISSION_REMINDER_REPEAT_S= 3 * 60   # repeat every 3 min

_reminder_last_alert = 0.0
_reminder_notified   = False

def _alert_loop():
    """
    Background thread — checks alerts every 30s.
    1. Mission reminder: gate idle too long during active mission
    2. Incomplete mission: mission closed with pallets still pending
    """
    global _reminder_last_alert, _reminder_notified
    while True:
        time.sleep(30)
        try:
            _check_mission_reminder()
            _check_incomplete_mission()
        except Exception as e:
            log.debug(f"[alerts] {e}")

def _check_mission_reminder():
    """Alert if gate has been in STANDBY/ARMED with no activity for too long."""
    global _reminder_last_alert, _reminder_notified

    if st.app_mode not in ("STANDBY", "ARMED"):
        _reminder_notified = False
        _reminder_last_alert = 0.0
        return

    idle_s = time.time() - st.last_action_time
    if idle_s < MISSION_REMINDER_IDLE_S:
        _reminder_notified = False
        return

    now = time.time()
    if now - _reminder_last_alert < MISSION_REMINDER_REPEAT_S:
        return   # already alerted recently

    _reminder_last_alert = now
    mins = int(idle_s / 60)
    log.warning(f"[alert] Mission reminder — gate {st.gate_id} idle {mins}m in {st.app_mode}")
    play_sound("error")

    # Notify VM (best-effort)
    _wms_notify("gate.alert", {
        "gate_id":    st.gate_id,
        "gate_name":  st.gate_name,
        "alert_type": "MISSION_IDLE",
        "idle_min":   mins,
        "app_mode":   st.app_mode,
        "timestamp":  datetime.now().isoformat(),
    })

def _check_incomplete_mission():
    """
    Detect if a mission was closed (IDLE) but pallets are still WAITING.
    Logs a warning and notifies VM.
    """
    if st.app_mode != "IDLE" or not st.active_tour_id:
        return

    # Check if the last completed tour has unloaded pallets
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM pallets WHERE tour_id=? AND status='WAITING'",
                (st.active_tour_id,)
            ).fetchone()
            pending = row[0] if row else 0

        if pending > 0:
            log.warning(f"[alert] Incomplete mission — tour {st.active_tour_id} "
                        f"has {pending} unloaded pallet(s)")
            play_sound("error")
            _wms_notify("gate.alert", {
                "gate_id":    st.gate_id,
                "gate_name":  st.gate_name,
                "alert_type": "INCOMPLETE_MISSION",
                "tour_id":    st.active_tour_id,
                "pending":    pending,
                "timestamp":  datetime.now().isoformat(),
            })
    except Exception as e:
        log.debug(f"[alerts] incomplete check: {e}")

def check_wrong_gate(detected_id: int) -> bool:
    """
    Called when an ArUco marker is detected that doesn't match current gate.
    Returns True if it's a known forklift from another gate (wrong gate).
    Fires sound + WMS alert.
    """
    # Check if this forklift is configured on another gate
    # For now: log and alert — cross-gate lookup requires VM comms
    log.warning(f"[alert] Wrong gate — forklift {detected_id} not authorized "
                f"for gate {st.gate_id}")
    play_sound("error")
    _wms_notify("gate.alert", {
        "gate_id":           st.gate_id,
        "gate_name":         st.gate_name,
        "alert_type":        "WRONG_FORKLIFT_AT_GATE",
        "detected_forklift": detected_id,
        "timestamp":         datetime.now().isoformat(),
    })
    return True


    log.info("="*60)
    log.info("Digiload Pro v2.0 — Starting")
    log.info("="*60)
    init_db(); load_config()
    _init_sounds()
    state=db_read_state()
    st.app_mode      =state.get("app_mode","IDLE")
    st.active_tour_id=state.get("active_tour_id",None)
    st.current_sscc  =state.get("current_sscc",None)
    log.info(f"[state] Restored: {st.app_mode} tour={st.active_tour_id}")
    _reinit_recording()
    if st.module_video_tracking: log.info(f"[modules] VideoTracking ACTIVE pre={st.rec_pre}s post={st.rec_post}s")
    if st.module_multi_angle:    log.info(f"[modules] MultiAngle ACTIVE")

    # ── Load plugins ──────────────────────────────────────────────────────────
    plugins_dir = os.path.join(os.path.dirname(_CONFIG_FILE or CONFIG_LOCAL), "plugins") \
                  if os.path.exists(os.path.dirname(CONFIG_FILE)) \
                  else "plugins"
    st.plugins = PluginLoader(plugins_dir, st.raw_config)
    st.plugins.on_start(st.raw_config)
    log.info(f"[plugins] Active: {st.plugins.loaded_names()}")
    threading.Thread(target=poll_db_activation, daemon=True).start()
    threading.Thread(target=_alert_loop,         daemon=True).start()
    if st.module_video_tracking:
        if st.feature_disk_manager:
            threading.Thread(target=disk_manager_loop,args=(st.rec_dir,st.ret_days,st.max_disk),daemon=True).start()
        else:
            log.info("[features] Disk manager disabled for this installation")
    cam=CameraManager(); cfg=st.raw_config.get("camera",{})
    if not cam.open_primary(cfg.get("primary",DEFAULT_CONFIG["camera"]["primary"])):
        log.error("Cannot open primary camera — exit"); return
    if st.module_multi_angle:
        cam.open_secondary(cfg.get("secondary",DEFAULT_CONFIG["camera"]["secondary"]),st.sec_buffer)
    cam.apply_settings(st.auto_exposure,st.exposure_val,st.gain_val)
    detector=cv2.aruco.ArucoDetector(cv2.aruco.getPredefinedDictionary(ARUCO_DICT))
    cv2.namedWindow(WINDOW_NAME,cv2.WINDOW_NORMAL|cv2.WINDOW_GUI_NORMAL)
    cv2.resizeWindow(WINDOW_NAME,1280,720)
    cv2.setMouseCallback(WINDOW_NAME,mouse_cb)
    log.info(f"[app] Gate {st.gate_id}: {st.gate_name} — [S] Settings [A] AutoExp [ESC] Quit")
    while True:
        if not cam.grab(): continue
        frame=cam.get_primary_frame()
        sec_frame=cam.get_secondary_frame() if cam.has_secondary else None
        h,w=frame.shape[:2]; st.last_w=w; st.last_h=h
        if st.module_video_tracking and st.pri_buffer:
            st.pri_buffer.push(frame)
            if sec_frame is not None and st.sec_buffer: st.sec_buffer.push(sec_frame)
        if st.module_video_tracking and st.recorder and st.recorder.is_recording:
            st.recorder.push(frame,sec_frame)
        tick_timeouts()
        if st.app_mode in ("ARMED","STANDBY"):
            rx,ry,rw,rh=st.gate_rect
            roi=frame[max(0,ry):min(h,ry+rh),max(0,rx):min(w,rx+rw)]
            if roi.size>100:
                gray=cv2.cvtColor(roi,cv2.COLOR_BGR2GRAY)
                _,ids,_=detector.detectMarkers(gray)
                if ids is not None: handle_aruco_detected(ids)
        if st.ui_mode=="MAIN":
            draw_gate(frame,h,w); draw_hud(frame,h,w)
        elif st.ui_mode=="SETTINGS":
            draw_settings_panel(frame,h,w)
        frame=draw_validated_overlay(frame,h,w)
        cv2.imshow(WINDOW_NAME,frame)
        while True:
            key=cv2.waitKey(1)&0xFF
            if key==255: break
            if not handle_key(key,cam):
                log.info("[app] Shutdown")
                if st.plugins: st.plugins.on_stop()
                cam.close(); cv2.destroyAllWindows(); return

if __name__=="__main__":
    run()
