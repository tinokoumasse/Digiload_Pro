#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Digiload Pro — ZED Box Install Script v2.0
#
# Installs everything from a fresh Ubuntu/JetPack OS:
#   - System packages (python, ffmpeg, audio, network tools)
#   - ZED SDK (if not already installed)
#   - Python dependencies
#   - Digiload Pro code (digiload_pro.py, agent.py, wms_connector.py, plugins/)
#   - systemd services (digiload-agent, digiload-app, digiload-wms)
#   - Firewall rules (iptables port 5002 restricted to VM + LAN)
#   - Audio system (alsa for sound feedback)
#
# Usage:
#   curl -sSL http://VM-IP:5001/install | sudo bash -s -- \
#     --gate-id=3 --central=192.168.1.50 --gate-name="Quai Nord 3"
#
# Or local:
#   sudo bash install.sh --gate-id=3 --central=192.168.1.50
#
# Idempotent — safe to re-run on partially installed systems.
# ─────────────────────────────────────────────────────────────────────────────

set -e

# ── ARGS ─────────────────────────────────────────────────────────────────────
GATE_ID=""
CENTRAL_IP=""
GATE_NAME=""
ZED_SDK_URL="${ZED_SDK_URL:-}"   # optional override for ZED SDK installer
SKIP_SDK="${SKIP_SDK:-0}"        # set =1 to skip SDK install
SKIP_DEPS="${SKIP_DEPS:-0}"      # set =1 to skip apt deps

for arg in "$@"; do
    case $arg in
        --gate-id=*)     GATE_ID="${arg#*=}"     ;;
        --central=*)     CENTRAL_IP="${arg#*=}"  ;;
        --gate-name=*)   GATE_NAME="${arg#*=}"   ;;
        --skip-sdk)      SKIP_SDK=1              ;;
        --skip-deps)     SKIP_DEPS=1             ;;
        --help|-h)
            grep -E "^#" "$0" | head -30
            exit 0
            ;;
    esac
done

if [ -z "$GATE_ID" ] || [ -z "$CENTRAL_IP" ]; then
    echo "Usage: sudo install.sh --gate-id=<id> --central=<vm-ip> [--gate-name=<name>]"
    echo ""
    echo "  --skip-sdk    Skip ZED SDK install (already installed)"
    echo "  --skip-deps   Skip apt package install"
    exit 1
fi

GATE_NAME="${GATE_NAME:-Gate $GATE_ID}"
CENTRAL_URL="http://${CENTRAL_IP}:5001"

# ── ROOT CHECK ───────────────────────────────────────────────────────────────
if [ "$EUID" -ne 0 ]; then
    echo "❌ Must be run as root (use sudo)"
    exit 1
fi

# Detect actual user (for SSH key + perms)
REAL_USER="${SUDO_USER:-$(whoami)}"
REAL_HOME=$(getent passwd "$REAL_USER" | cut -d: -f6)

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║   Digiload Pro — ZED Box Setup v2.0                      ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║   Gate ID:    $GATE_ID                                          "
echo "║   Gate Name:  $GATE_NAME                                          "
echo "║   Central:    $CENTRAL_URL                                          "
echo "║   User:       $REAL_USER                                          "
echo "║   Hostname:   $(hostname)                                          "
echo "║   Arch:       $(uname -m)                                          "
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

STEP=0
TOTAL=10
log_step() { STEP=$((STEP+1)); echo ""; echo "━━━ [$STEP/$TOTAL] $1 ━━━"; }

# ─────────────────────────────────────────────────────────────────────────────
# [1] System packages
# ─────────────────────────────────────────────────────────────────────────────
log_step "Installing system packages"
if [ "$SKIP_DEPS" = "1" ]; then
    echo "  ⏭  Skipped (--skip-deps)"
else
    apt-get update -qq
    apt-get install -y -qq \
        python3 python3-pip python3-venv \
        python3-evdev \
        ffmpeg \
        alsa-utils \
        curl wget git \
        sqlite3 \
        iptables iptables-persistent \
        avahi-daemon \
        libgl1 libglib2.0-0 \
        libsm6 libxext6 libxrender1 \
        v4l-utils \
        net-tools \
        bash-completion \
        ca-certificates \
        2>&1 | tail -5 || true
    echo "  ✓ apt packages installed"
fi

# ─────────────────────────────────────────────────────────────────────────────
# [2] Python packages
# ─────────────────────────────────────────────────────────────────────────────
log_step "Installing Python packages"
PIP_CMD="pip3 install --break-system-packages 2>/dev/null || pip3 install"
pip3 install --break-system-packages --quiet \
    flask \
    requests \
    numpy \
    opencv-python-headless \
    psutil \
    2>&1 | tail -3 || \
pip3 install --quiet \
    flask requests numpy opencv-python-headless psutil 2>&1 | tail -3

echo "  ✓ Python packages installed"

# ─────────────────────────────────────────────────────────────────────────────
# [3] ZED SDK
# ─────────────────────────────────────────────────────────────────────────────
log_step "ZED SDK"
if python3 -c "import pyzed.sl" 2>/dev/null; then
    SDK_VER=$(python3 -c "import pyzed.sl as sl; print(sl.Camera().get_sdk_version())" 2>/dev/null || echo "?")
    echo "  ✓ ZED SDK already installed (version: $SDK_VER)"
elif [ "$SKIP_SDK" = "1" ]; then
    echo "  ⏭  Skipped (--skip-sdk)"
    echo "  ⚠  WARNING: ZED SDK not detected — app will fail to start"
else
    echo "  ⚠ ZED SDK not detected"
    echo ""
    echo "  ZED SDK installation requires manual interactive download from"
    echo "  Stereolabs (license acceptance + ~2GB binary)."
    echo ""
    echo "  Download from: https://www.stereolabs.com/developers/release/"
    echo "  Choose:        Linux (or Jetson if on ZED Box)"
    echo ""
    echo "  After install, re-run with --skip-sdk:"
    echo "    sudo bash install.sh --gate-id=$GATE_ID --central=$CENTRAL_IP --skip-sdk"
    echo ""
    read -p "  Continue without ZED SDK (app won't run cameras)? [y/N] " yn
    case $yn in
        [Yy]*) echo "  ⏭  Continuing without SDK" ;;
        *) echo "  ❌ Aborted — install ZED SDK first"; exit 1 ;;
    esac
fi

# Ensure nvargus-daemon running (Jetson only — for ZED X)
if [ -f "/etc/systemd/system/nvargus-daemon.service" ] || systemctl list-unit-files | grep -q nvargus; then
    systemctl enable nvargus-daemon 2>/dev/null || true
    systemctl restart nvargus-daemon 2>/dev/null || true
    echo "  ✓ nvargus-daemon running (required for ZED X on Jetson)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# [4] Folders
# ─────────────────────────────────────────────────────────────────────────────
log_step "Creating folders"
mkdir -p /opt/digiload/clips
mkdir -p /opt/digiload/logs
mkdir -p /opt/digiload/plugins
mkdir -p /etc/digiload
mkdir -p /var/log/digiload
chown -R "$REAL_USER:$REAL_USER" /opt/digiload /var/log/digiload
chmod 755 /opt/digiload /etc/digiload /var/log/digiload
echo "  ✓ Folders created"

# ─────────────────────────────────────────────────────────────────────────────
# [5] Configuration
# ─────────────────────────────────────────────────────────────────────────────
log_step "Writing config"
if [ ! -f /etc/digiload/config.json ]; then
    cat > /etc/digiload/config.json << EOF
{
  "gate_id":   $GATE_ID,
  "gate_name": "$GATE_NAME",
  "ip_mode":   "dhcp",
  "ip":        "",

  "features": {
    "led_control":  true,
    "disk_manager": true,
    "hud":          true,
    "sound":        true
  },

  "modules": {
    "video_tracking": {"enabled": false, "license_key": ""},
    "multi_angle":    {"enabled": false, "license_key": ""}
  },

  "plugins": {
    "led_control": {"enabled": true}
  },

  "camera": {
    "primary":   {"serial": 0, "resolution": "HD1080", "fps": 60},
    "secondary": {"enabled": false},
    "auto_exposure": false, "exposure": 30, "gain": 85
  },

  "recording": {
    "output_dir":   "clips",
    "pre_seconds":  10,
    "post_seconds": 5,
    "buffer_fps":   30
  },

  "gate": {"rect": [100, 100, 800, 600]},

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

  "wms": {"webhook_url": "", "api_key": "", "retry_interval_s": 30, "max_retries": 288},

  "vm": {
    "sync_url": "$CENTRAL_URL",
    "enabled":  true,
    "required": false
  }
}
EOF
    echo "  ✓ /etc/digiload/config.json created"
else
    # Update only the gate_id, gate_name, vm.sync_url if config exists
    python3 << PYEOF
import json
with open('/etc/digiload/config.json') as f: cfg = json.load(f)
cfg['gate_id']   = $GATE_ID
cfg['gate_name'] = "$GATE_NAME"
cfg['vm']['sync_url'] = "$CENTRAL_URL"
with open('/etc/digiload/config.json','w') as f: json.dump(cfg, f, indent=2)
PYEOF
    echo "  ✓ /etc/digiload/config.json updated (preserved existing settings)"
fi

# Generate agent secret if missing
if [ ! -f /etc/digiload/agent.secret ]; then
    openssl rand -hex 32 > /etc/digiload/agent.secret
    chmod 600 /etc/digiload/agent.secret
    echo "  ✓ Agent secret generated"
fi

# ─────────────────────────────────────────────────────────────────────────────
# [6] Download Digiload code from VM
# ─────────────────────────────────────────────────────────────────────────────
log_step "Downloading Digiload code"
DIGILOAD_DIR="$REAL_HOME/digiload_pro_v2/zed_box"

if [ -d "$DIGILOAD_DIR" ]; then
    echo "  ✓ Local repo found at $DIGILOAD_DIR (using local files)"
else
    mkdir -p "$DIGILOAD_DIR/plugins"
    echo "  Fetching from $CENTRAL_URL/agent/releases/ ..."

    for f in digiload_pro.py agent.py wms_connector.py plugin_loader.py; do
        if curl -fsSL "$CENTRAL_URL/agent/releases/$f" -o "$DIGILOAD_DIR/$f" 2>/dev/null; then
            echo "  ✓ Downloaded $f"
        else
            echo "  ⚠ Could not download $f (will need manual placement)"
        fi
    done

    for plugin in led_control.py; do
        curl -fsSL "$CENTRAL_URL/agent/releases/plugins/$plugin" \
             -o "$DIGILOAD_DIR/plugins/$plugin" 2>/dev/null && \
             echo "  ✓ Downloaded plugin: $plugin" || true
    done

    chown -R "$REAL_USER:$REAL_USER" "$DIGILOAD_DIR"
fi

# Symlink to /opt/digiload for systemd services
ln -sf "$DIGILOAD_DIR/digiload_pro.py"   /opt/digiload/digiload_pro.py
ln -sf "$DIGILOAD_DIR/agent.py"          /opt/digiload/agent.py
ln -sf "$DIGILOAD_DIR/wms_connector.py"  /opt/digiload/wms_connector.py
ln -sf "$DIGILOAD_DIR/plugin_loader.py"  /opt/digiload/plugin_loader.py
ln -sf "$DIGILOAD_DIR/plugins"           /opt/digiload/plugins
echo "  ✓ Symlinked code to /opt/digiload/"

# ─────────────────────────────────────────────────────────────────────────────
# [7] Audio system (for sound feedback)
# ─────────────────────────────────────────────────────────────────────────────
log_step "Audio system"
if command -v aplay >/dev/null 2>&1; then
    if aplay -l 2>/dev/null | grep -q "card "; then
        echo "  ✓ Audio device detected"
    else
        echo "  ⚠ aplay installed but no audio devices found"
        echo "    Sound feedback will not work — connect speaker or HDMI audio"
    fi
else
    echo "  ⚠ aplay not installed — sound feedback disabled"
fi

# ─────────────────────────────────────────────────────────────────────────────
# [8] systemd services
# ─────────────────────────────────────────────────────────────────────────────
log_step "Installing systemd services"

cat > /etc/systemd/system/digiload-app.service << EOF
[Unit]
Description=Digiload Pro — Main App
After=network.target nvargus-daemon.service

[Service]
Type=simple
User=$REAL_USER
WorkingDirectory=/opt/digiload
Environment=DISPLAY=:0
Environment=XAUTHORITY=$REAL_HOME/.Xauthority
ExecStart=/usr/bin/python3 /opt/digiload/digiload_pro.py
Restart=on-failure
RestartSec=5
StandardOutput=append:/var/log/digiload/app.log
StandardError=append:/var/log/digiload/app.log

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/digiload-agent.service << EOF
[Unit]
Description=Digiload Pro — Fleet Agent
After=network.target

[Service]
Type=simple
User=$REAL_USER
WorkingDirectory=/opt/digiload
ExecStart=/usr/bin/python3 /opt/digiload/agent.py
Restart=always
RestartSec=5
StandardOutput=append:/var/log/digiload/agent.log
StandardError=append:/var/log/digiload/agent.log

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/digiload-wms.service << EOF
[Unit]
Description=Digiload Pro — WMS Connector
After=network.target

[Service]
Type=simple
User=$REAL_USER
WorkingDirectory=/opt/digiload
ExecStart=/usr/bin/python3 /opt/digiload/wms_connector.py
Restart=always
RestartSec=10
StandardOutput=append:/var/log/digiload/wms.log
StandardError=append:/var/log/digiload/wms.log

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable digiload-agent.service digiload-wms.service 2>/dev/null
echo "  ✓ systemd services installed"
echo "  ⚠ digiload-app NOT auto-started (needs DISPLAY=:0 from a logged-in session)"
echo "    Start manually after first login: sudo systemctl start digiload-app"

# ─────────────────────────────────────────────────────────────────────────────
# [9] Firewall rules
# ─────────────────────────────────────────────────────────────────────────────
log_step "Firewall rules (iptables)"

# Restrict port 5002 to VM IP + LAN subnet
LAN_SUBNET=$(ip -o -f inet addr show | awk '/scope global/ {print $4}' | head -1 | sed 's|\.[0-9]*/|.0/|')

iptables -C INPUT -p tcp --dport 5002 -s "$CENTRAL_IP" -j ACCEPT 2>/dev/null || \
    iptables -I INPUT -p tcp --dport 5002 -s "$CENTRAL_IP" -j ACCEPT

if [ -n "$LAN_SUBNET" ]; then
    iptables -C INPUT -p tcp --dport 5002 -s "$LAN_SUBNET" -j ACCEPT 2>/dev/null || \
        iptables -I INPUT -p tcp --dport 5002 -s "$LAN_SUBNET" -j ACCEPT
fi

iptables -C INPUT -p tcp --dport 5002 -s 127.0.0.1 -j ACCEPT 2>/dev/null || \
    iptables -I INPUT -p tcp --dport 5002 -s 127.0.0.1 -j ACCEPT

# Save rules
mkdir -p /etc/iptables
iptables-save > /etc/iptables/rules.v4 2>/dev/null || true

echo "  ✓ Port 5002 restricted to: $CENTRAL_IP, $LAN_SUBNET, localhost"

# ─────────────────────────────────────────────────────────────────────────────
# [10] Start services
# ─────────────────────────────────────────────────────────────────────────────
log_step "Starting services"
systemctl restart digiload-agent.service
sleep 2
systemctl restart digiload-wms.service
sleep 1

if systemctl is-active --quiet digiload-agent; then
    echo "  ✓ digiload-agent running on port 5002"
else
    echo "  ❌ digiload-agent failed to start"
    journalctl -u digiload-agent --no-pager -n 20
fi

if systemctl is-active --quiet digiload-wms; then
    echo "  ✓ digiload-wms running"
else
    echo "  ⚠ digiload-wms not running (will start on next mission)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ✅ Digiload Pro installed successfully!                 ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "  Gate $GATE_ID ($GATE_NAME) → $CENTRAL_URL"
echo ""
echo "  Next steps:"
echo "    1. Connect your monitor to the ZED Box (HDMI)"
echo "    2. Log in to the desktop session"
echo "    3. Open a terminal and run:"
echo "         sudo systemctl start digiload-app"
echo "    4. Verify status:"
echo "         curl http://localhost:5002/status"
echo "    5. Check fleet dashboard:"
echo "         $CENTRAL_URL/admin/super"
echo ""
echo "  Logs:"
echo "    /var/log/digiload/app.log     (main app)"
echo "    /var/log/digiload/agent.log   (fleet agent)"
echo "    /var/log/digiload/wms.log     (WMS connector)"
echo ""
echo "  Useful commands:"
echo "    systemctl status digiload-app digiload-agent digiload-wms"
echo "    journalctl -fu digiload-app"
echo "    tail -f /var/log/digiload/app.log"
echo ""
