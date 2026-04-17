#!/bin/bash
# Digiload Pro — ZED Box Install Script v1.0
# Usage: curl -sSL https://get.digiload.io/install | bash -s -- --gate-id=3 --central=192.168.10.50
# Or:    bash install.sh --gate-id=3 --central=192.168.10.50

set -e

# ─────────────────────────────────────────────────────────────────────────────
# ARGS
# ─────────────────────────────────────────────────────────────────────────────
GATE_ID=""
CENTRAL_IP=""
GATE_NAME=""

for arg in "$@"; do
    case $arg in
        --gate-id=*)   GATE_ID="${arg#*=}"   ;;
        --central=*)   CENTRAL_IP="${arg#*=}" ;;
        --gate-name=*) GATE_NAME="${arg#*=}"  ;;
    esac
done

if [ -z "$GATE_ID" ] || [ -z "$CENTRAL_IP" ]; then
    echo "Usage: install.sh --gate-id=<id> --central=<vm-ip> [--gate-name=<name>]"
    exit 1
fi

GATE_NAME="${GATE_NAME:-Gate $GATE_ID}"
CENTRAL_URL="http://${CENTRAL_IP}:5001"

echo "=================================================="
echo "  Digiload Pro — ZED Box Setup"
echo "  Gate ID:   $GATE_ID"
echo "  Gate Name: $GATE_NAME"
echo "  Central:   $CENTRAL_URL"
echo "=================================================="

# ─────────────────────────────────────────────────────────────────────────────
# FOLDERS
# ─────────────────────────────────────────────────────────────────────────────
echo "[1/7] Creating folders..."
mkdir -p /opt/digiload/clips
mkdir -p /opt/digiload/logs
mkdir -p /etc/digiload
mkdir -p /var/log/digiload

# ─────────────────────────────────────────────────────────────────────────────
# BOOTSTRAP CONFIG
# ─────────────────────────────────────────────────────────────────────────────
echo "[2/7] Writing bootstrap config..."
cat > /etc/digiload/config.json << EOF
{
    "gate_id":   $GATE_ID,
    "gate_name": "$GATE_NAME",
    "ip_mode":   "dhcp",
    "ip":        "",
    "modules": {
        "video_tracking": {"enabled": false, "license_key": ""},
        "multi_angle":    {"enabled": false, "license_key": ""}
    },
    "camera": {
        "primary":   {"serial": 0, "resolution": "HD1080", "fps": 60},
        "secondary": {"enabled": false, "serial": 0, "resolution": "HD720", "fps": 30},
        "auto_exposure": false, "exposure": 30, "gain": 85
    },
    "recording": {
        "output_dir": "/opt/digiload/clips",
        "pre_seconds": 10, "post_seconds": 5,
        "buffer_fps": 30, "retention_days": 30, "max_disk_gb": 50
    },
    "gate": {"rect": [100, 100, 400, 400]},
    "led": {
        "ip": "192.168.1.100",
        "presets": {
            "standby":   {"r": 0,   "g": 0,   "b": 255, "effect": "breath",  "brightness": 128, "speed": 100},
            "armed":     {"r": 0,   "g": 255, "b": 0,   "effect": "static",  "brightness": 200, "speed": 128},
            "error":     {"r": 255, "g": 0,   "b": 0,   "effect": "blink",   "brightness": 255, "speed": 100},
            "confirmed": {"r": 255, "g": 255, "b": 255, "effect": "static",  "brightness": 255, "speed": 128},
            "off":       {"r": 0,   "g": 0,   "b": 0,   "effect": "static",  "brightness": 0,   "speed": 128}
        }
    },
    "system": {"target_id": 0, "forklift_ids": [0], "lock_duration": 5.0},
    "wms":    {"webhook_url": "", "api_key": "", "retry_interval_s": 30, "max_retries": 288},
    "vm":     {"sync_url": "${CENTRAL_URL}/api/sync", "enabled": true, "required": false},
    "theme": {
        "accent_hex": "#2f7df6", "sidebar_hex": "#05080f",
        "text_title_hex": "#4a6080", "text_main_hex": "#e8f0ff",
        "logo_path": "/opt/digiload/logo.png", "logo_size": 0.15, "opacity": 0.85
    },
    "ui_text": {
        "title": "DIGILOAD PRO", "idle": "AWAITING MISSION",
        "standby": "SCAN BARCODE", "armed": "LOADING AUTHORIZED",
        "wrong_forklift": "WRONG FORKLIFT", "wrong_sscc": "INVALID BARCODE",
        "validated": "PALLET LOADED"
    }
}
EOF

# ─────────────────────────────────────────────────────────────────────────────
# DOWNLOAD APP FILES FROM CENTRAL VM
# ─────────────────────────────────────────────────────────────────────────────
echo "[3/7] Downloading app files from Central VM..."

download_file() {
    local url="$1"
    local dest="$2"
    local name="$3"
    if curl -sSf --connect-timeout 5 "$url" -o "$dest" 2>/dev/null; then
        echo "  ✅ $name"
    else
        echo "  ⚠️  Could not download $name from VM — using local copy if available"
    fi
}

download_file "${CENTRAL_URL}/agent/releases/digiload_pro.py"  /opt/digiload/digiload_pro.py  "digiload_pro.py"
download_file "${CENTRAL_URL}/agent/releases/wms_connector.py" /opt/digiload/wms_connector.py "wms_connector.py"
download_file "${CENTRAL_URL}/agent/releases/agent.py"         /opt/digiload/agent.py         "agent.py"

# If VM not reachable, try copying from current directory
for f in digiload_pro.py wms_connector.py agent.py; do
    if [ ! -f "/opt/digiload/$f" ] && [ -f "$f" ]; then
        cp "$f" "/opt/digiload/$f"
        echo "  📋 Copied $f from local directory"
    fi
done

# ─────────────────────────────────────────────────────────────────────────────
# PYTHON DEPENDENCIES
# ─────────────────────────────────────────────────────────────────────────────
echo "[4/7] Installing Python dependencies..."
pip install flask requests numpy 2>/dev/null || \
pip install flask requests numpy --break-system-packages 2>/dev/null || \
echo "  ⚠️  pip install failed — dependencies may already be installed"

# ─────────────────────────────────────────────────────────────────────────────
# SYSTEMD SERVICES
# ─────────────────────────────────────────────────────────────────────────────
echo "[5/7] Installing systemd services..."

cat > /etc/systemd/system/digiload-agent.service << 'SVCEOF'
[Unit]
Description=Digiload Fleet Agent
After=network.target
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/digiload
ExecStart=/usr/bin/python3 /opt/digiload/agent.py
Restart=always
RestartSec=5
StandardOutput=append:/var/log/digiload/agent.log
StandardError=append:/var/log/digiload/agent.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
SVCEOF

cat > /etc/systemd/system/digiload-app.service << 'SVCEOF'
[Unit]
Description=Digiload Pro Gate Application
After=network.target digiload-agent.service
Requires=digiload-agent.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/digiload
ExecStart=/usr/bin/python3 /opt/digiload/digiload_pro.py
Restart=always
RestartSec=5
StandardOutput=append:/var/log/digiload/app.log
StandardError=append:/var/log/digiload/app.log
Environment=PYTHONUNBUFFERED=1
Environment=DISPLAY=:0

[Install]
WantedBy=multi-user.target
SVCEOF

cat > /etc/systemd/system/digiload-wms.service << 'SVCEOF'
[Unit]
Description=Digiload WMS Connector
After=network.target digiload-agent.service
Wants=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/digiload
ExecStart=/usr/bin/python3 /opt/digiload/wms_connector.py
Restart=always
RestartSec=5
StandardOutput=append:/var/log/digiload/wms.log
StandardError=append:/var/log/digiload/wms.log
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable digiload-agent digiload-app digiload-wms
echo "  ✅ Services installed and enabled"

# ─────────────────────────────────────────────────────────────────────────────
# IPTABLES — restrict agent port 5002 to VM IP only
# ─────────────────────────────────────────────────────────────────────────────
echo "[6/7] Configuring firewall..."
VM_IP=$(echo "$CENTRAL_IP" | cut -d: -f1)

# Allow VM IP on port 5002
iptables -I INPUT -p tcp --dport 5002 -s "$VM_IP" -j ACCEPT 2>/dev/null && \
# Allow localhost (for local testing)
iptables -I INPUT -p tcp --dport 5002 -s 127.0.0.1 -j ACCEPT 2>/dev/null && \
# Block all other sources on port 5002
iptables -A INPUT -p tcp --dport 5002 -j DROP 2>/dev/null && \
echo "  ✅ Port 5002 restricted to VM ($VM_IP) + localhost" || \
echo "  ⚠️  iptables failed — configure manually if needed"

# ─────────────────────────────────────────────────────────────────────────────
# START SERVICES
# ─────────────────────────────────────────────────────────────────────────────
echo "[7/7] Starting services..."
systemctl start digiload-agent
sleep 2
systemctl start digiload-wms
sleep 1
systemctl start digiload-app

echo ""
echo "=================================================="
echo "  ✅ Digiload Pro installed successfully"
echo ""
echo "  Gate:      $GATE_NAME (ID: $GATE_ID)"
echo "  Status:    curl http://localhost:5002/status"
echo "  Logs:      tail -f /var/log/digiload/agent.log"
echo "  Dashboard: $CENTRAL_URL"
echo "=================================================="
