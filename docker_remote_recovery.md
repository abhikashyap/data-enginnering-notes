# 🛠 Docker Remote Recovery Guide (Mac → Linux)

Use this guide if:

- Linux IP changes
- `docker ps` hangs
- SSH stops working
- Router rebooted
- Docker cannot connect to remote host

---

# 🥇 STEP 1 — Find New Linux IP

On the Linux machine:

```bash
ip a
```

Look for something like:

```
inet 192.168.1.xxx/24
```

Example:

```
192.168.1.23
```

That is your new IP.

---

# 🥈 STEP 2 — Test SSH From Mac

On your Mac:

```bash
ssh abhi@NEW_IP
```

Example:

```bash
ssh abhi@192.168.1.23
```

If it logs in without password → good  
If it asks for password → fine  
If it fails → check SSH service on Linux:

```bash
sudo systemctl status ssh
sudo systemctl start ssh
sudo systemctl enable ssh
```

---

# 🥉 STEP 3 — Switch Docker Back to Default Context (Mac)

```bash
docker context use default
```

---

# 🏁 STEP 4 — Remove Old Remote Context

```bash
docker context rm -f linux-server
```

---

# 🚀 STEP 5 — Create New Docker Context

```bash
docker context create linux-server \
  --docker "host=ssh://abhi@NEW_IP"
```

Example:

```bash
docker context create linux-server \
  --docker "host=ssh://abhi@192.168.1.23"
```

---

# 🔄 STEP 6 — Switch to New Context

```bash
docker context use linux-server
```

---

# ✅ STEP 7 — Test Docker Connection

```bash
docker ps
```

You should now see your running containers.

---

# 🔐 If You See "Permission Denied"

Reinstall SSH key:

```bash
ssh-copy-id abhi@NEW_IP
```

Test again:

```bash
ssh abhi@NEW_IP
docker ps
```

---

# 🔍 Diagnostic Commands

Check current Docker contexts:

```bash
docker context ls
```

Inspect remote context:

```bash
docker context inspect linux-server
```

---

# 🧠 Recommended: Make IP Permanent

To avoid repeating this process:

1. Open your router settings
2. Reserve IP for MAC address:

```
b0:a4:60:a0:8e:a8
```

3. Assign permanent IP:

```
192.168.1.11
```

Then your Linux server IP will never change.

---

# ⚡ Quick Recovery (Short Version)

```bash
docker context use default
docker context rm -f l