#!/bin/bash
# Digiload Pro — Central VM Install Script v1.0
# Usage: curl -sSL https://get.digiload.io/install-central | bash
# Or:    bash install_central.sh

set -e

INSTALL_DIR="/opt/digiload-central"
CERTS_DIR="$INSTALL_DIR/certs"

echo "=================================================="
echo "  Digiload Pro — Central VM Setup"
echo "=================================================="

# ── 1. Check Docker ──────────────────────────────────────────────────────────
echo "[1/7] Checking Docker..."
if ! command -v docker &>/dev/null; then
    echo "  Installing Docker..."
    curl -fsSL https://get.docker.com | bash
    usermod -aG docker "$USER"
    echo "  ✅ Docker installed"
else
    echo "  ✅ Docker already installed"
fi

if ! command -v docker compose &>/dev/null && ! docker compose version &>/dev/null 2>&1; then
    echo "  Installing Docker Compose plugin..."
    apt-get install -y docker-compose-plugin 2>/dev/null || \
    pip install docker-compose --break-system-packages 2>/dev/null || true
fi

# ── 2. Create folder structure ───────────────────────────────────────────────
echo "[2/7] Creating folders..."
mkdir -p "$INSTALL_DIR"
mkdir -p "$CERTS_DIR"
mkdir -p "$INSTALL_DIR/clips"
mkdir -p "$INSTALL_DIR/reports"
mkdir -p "$INSTALL_DIR/releases"    # ZED Box app files served from here
mkdir -p "$INSTALL_DIR/templates"

# ── 3. Generate .env ─────────────────────────────────────────────────────────
echo "[3/7] Configuring environment..."

if [ -f "$INSTALL_DIR/.env" ]; then
    echo "  ⚠️  .env already exists — skipping generation"
else
    DB_PASSWORD=$(openssl rand -hex 24)
    SECRET_KEY=$(openssl rand -hex 32)
    LICENSE_SECRET=$(openssl rand -hex 32)

    cat > "$INSTALL_DIR/.env" << EOF
DB_PASSWORD=$DB_PASSWORD
SECRET_KEY=$SECRET_KEY
LICENSE_SECRET=$LICENSE_SECRET
DB_NAME=digiload
DB_USER=digiload
CLIP_RETENTION_DAYS=30
RELEASE_BASE_URL=http://$(hostname -I | awk '{print $1}'):5001
EOF
    chmod 600 "$INSTALL_DIR/.env"
    echo "  ✅ .env generated with random secrets"
fi

# ── 4. Generate self-signed SSL certificate ───────────────────────────────────
echo "[4/7] Generating SSL certificate..."
VM_IP=$(hostname -I | awk '{print $1}')

if [ -f "$CERTS_DIR/digiload.crt" ]; then
    echo "  ⚠️  Certificate already exists — skipping"
else
    openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
        -keyout "$CERTS_DIR/digiload.key" \
        -out    "$CERTS_DIR/digiload.crt" \
        -subj "/CN=digiload-central/O=Digiload Pro/C=FR" \
        -addext "subjectAltName=IP:$VM_IP,DNS:localhost" \
        2>/dev/null
    chmod 600 "$CERTS_DIR/digiload.key"
    echo "  ✅ Self-signed certificate generated (10 years)"
    echo "  ℹ️  To use your own certificate:"
    echo "       Replace $CERTS_DIR/digiload.crt and digiload.key"
    echo "       Then: docker compose restart nginx"
fi

# ── 5. Download docker-compose.yml and config files ──────────────────────────
echo "[5/7] Downloading stack configuration..."

# If running from a local clone, copy files directly
if [ -f "./docker-compose.yml" ]; then
    cp ./docker-compose.yml "$INSTALL_DIR/"
    cp ./nginx.conf          "$INSTALL_DIR/"
    cp ./mosquitto.conf      "$INSTALL_DIR/"
    cp -r ./templates        "$INSTALL_DIR/"
    cp ./central_app.py      "$INSTALL_DIR/"
    cp ./reports.py          "$INSTALL_DIR/"
    cp ./requirements.txt    "$INSTALL_DIR/"
    [ -f "./Dockerfile" ] && cp ./Dockerfile "$INSTALL_DIR/"
    echo "  ✅ Files copied from local directory"
else
    echo "  ⚠️  No local files found — place docker-compose.yml in $INSTALL_DIR manually"
fi

# Copy ZED Box release files if available
for f in digiload_pro.py wms_connector.py agent.py install.sh; do
    if [ -f "../zed_box/$f" ]; then
        cp "../zed_box/$f" "$INSTALL_DIR/releases/"
        echo "  📋 Release: $f"
    fi
done

# ── 6. Start Docker stack ─────────────────────────────────────────────────────
echo "[6/7] Starting Docker services..."
cd "$INSTALL_DIR"

if [ -f "docker-compose.yml" ]; then
    docker compose --env-file .env pull 2>/dev/null || true
    docker compose --env-file .env up -d
    echo "  ✅ Services starting..."
    sleep 5
    docker compose ps
else
    echo "  ⚠️  docker-compose.yml not found in $INSTALL_DIR"
    echo "      Copy it manually and run: docker compose --env-file .env up -d"
fi

# ── 7. Summary ────────────────────────────────────────────────────────────────
echo ""
echo "[7/7] Setup complete"
echo ""
echo "=================================================="
echo "  ✅ Digiload Pro Central VM Ready"
echo ""
echo "  VM IP:       $VM_IP"
echo "  Dashboard:   https://$VM_IP"
echo "              (accept self-signed cert warning)"
echo ""
echo "  Default admin:"
echo "    Email:    admin@digiload.local"
echo "    Password: Digiload2024!"
echo "    ⚠️  CHANGE THIS IMMEDIATELY"
echo ""
echo "  Logs:  docker compose logs -f central-app"
echo "  Stop:  docker compose down"
echo ""
echo "  ZED Box install command:"
echo "    bash install.sh --gate-id=1 --central=$VM_IP"
echo "=================================================="
