# Columbia Fruit Analytics — Production Deployment Guide

Deploy the Dash app on a fresh Ubuntu/Debian server with gunicorn, Nginx, and systemd.

---

## Prerequisites

- Ubuntu 22.04 LTS (or similar Debian-based)
- Root/sudo access
- App code in a git repo (or copy files manually)

---

## Step-by-Step Deployment

### 1. Create app directory and user

```bash
sudo mkdir -p /opt/dashapp
sudo chown $USER:www-data /opt/dashapp
# Or if www-data should own everything: sudo chown -R www-data:www-data /opt/dashapp
```

### 2. Clone or copy the app

```bash
cd /opt
sudo git clone <your-repo-url> dashapp
# Or: sudo cp -r /path/to/dashapp/* /opt/dashapp/
cd /opt/dashapp
```

### 3. Create Python virtualenv and install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

### 4. Create log directory

```bash
sudo mkdir -p /var/log/dashapp
sudo chown www-data:www-data /var/log/dashapp
```

### 5. Create `.env` with secrets

```bash
sudo -u www-data nano /opt/dashapp/.env
```

Add your Snowflake credentials (and any other env vars):

```
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_TOKEN=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
```

Restrict permissions:

```bash
sudo chmod 600 /opt/dashapp/.env
sudo chown www-data:www-data /opt/dashapp/.env
```

### 6. Set ownership

```bash
sudo chown -R www-data:www-data /opt/dashapp
```

### 7. Install systemd service

```bash
sudo cp deployment/dashapp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable dashapp
sudo systemctl start dashapp
sudo systemctl status dashapp
```

### 8. Install Nginx config

```bash
sudo cp deployment/nginx-dashapp.conf /etc/nginx/sites-available/dashapp
sudo ln -sf /etc/nginx/sites-available/dashapp /etc/nginx/sites-enabled/
# Remove default site if it conflicts:
# sudo rm /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

### 9. Open firewall (if using ufw)

```bash
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp  # if using HTTPS later
sudo ufw enable
```

### 10. Verify

- Open `http://<server-ip>/` — app should load
- Open `http://<server-ip>/health` — should return `OK`

---

## Useful Commands

| Command | Purpose |
|---------|---------|
| `make prod-test` | Run gunicorn locally on 127.0.0.1:8050 |
| `make deploy` | Git pull + restart (run from /opt/dashapp) |
| `make logs` | Tail dashapp service logs |
| `make restart` | Restart dashapp service |
| `make status` | Check dashapp service status |

---

## Optional: Cloudflare (Later)

When ready to put the app behind Cloudflare:

1. **DNS**: Add an A record for your domain pointing to the server IP
2. **Proxy**: Turn on the orange cloud (proxied)
3. **SSL**: Set SSL mode to Full (or Flexible if Nginx is HTTP only)
4. **Optional**: Add a Page Rule for `/_dash-component-suites/*` to cache aggressively
5. **Optional**: Enable WAF for extra protection

---

## Local Testing Checklist

- [ ] Run `make prod-test`
- [ ] Open http://127.0.0.1:8050 — app loads
- [ ] Open http://127.0.0.1:8050/health — returns OK
- [ ] Navigate TV, PIDK, PFR — all work

## Server Rollout Checklist

- [ ] App runs under www-data
- [ ] `.env` has correct Snowflake credentials
- [ ] `/var/log/dashapp` exists and is writable by www-data
- [ ] systemd service starts and restarts on failure
- [ ] Nginx proxies to unix socket
- [ ] http://server/ and http://server/health work
