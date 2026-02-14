#!/usr/bin/env bash

# ---------------------------------------------
# Docker Remote Auto-Reconnect Script
# Mac → Linux Docker Host
# ---------------------------------------------

USER_NAME="abhi"
SUBNET="192.168.1.0/24"
CONTEXT_NAME="linux-server"

# 1️⃣ Switch to default context

echo "Switching to default Docker context..."
docker context use default >/dev/null 2>&1

# 2️⃣ Remove old context (if exists)

echo "Removing old context (if any)..."
docker context rm -f "$CONTEXT_NAME" >/dev/null 2>&1

# 3️⃣ Detect Linux IP automatically using nmap

if ! command -v nmap &> /dev/null; then
    echo "nmap not found. Installing..."
    brew install nmap
fi

echo "Scanning network for Linux host (SSH port 22)..."
IP=$(nmap -p 22 --open $SUBNET | grep "Nmap scan report" | awk '{print $5}' | head -n 1)

if [ -z "$IP" ]; then
    echo "❌ No SSH host found in subnet $SUBNET"
    exit 1
fi

echo "Found Linux host at: $IP"

# 4️⃣ Test SSH connectivity

echo "Testing SSH connection..."
if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$USER_NAME@$IP" "exit" 2>/dev/null; then
    echo "⚠️ SSH key authentication failed. Trying manual SSH test..."
    ssh "$USER_NAME@$IP"
fi

# 5️⃣ Create new Docker context

echo "Creating new Docker context..."
docker context create "$CONTEXT_NAME" \
  --docker "host=ssh://$USER_NAME@$IP" >/dev/null

# 6️⃣ Use new context

echo "Switching to $CONTEXT_NAME context..."
docker context use "$CONTEXT_NAME" >/dev/null

# 7️⃣ Test Docker

echo "Testing Docker connection..."
if docker ps >/dev/null 2>&1; then
    echo "✅ Docker successfully connected to $IP"
    docker ps
else
    echo "❌ Docker connection failed. Check SSH and Docker service on Linux."
    exit 1
fi

# ---------------------------------------------
# Done
# ---------------------------------------------
