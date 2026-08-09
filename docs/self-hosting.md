# Self-hosting agenterr

One binary, one SQLite file. Everything below is optional hardening on
top of the README Quickstart.

## Docker Compose

```yaml
services:
  agenterr:
    image: ghcr.io/agenterr/agenterr:latest
    restart: unless-stopped
    ports:
      - "3617:3617"
    volumes:
      - agenterr-data:/data
    environment:
      # Optional: skip the one-time generated password printed on first boot.
      # AGENTERR_ADMIN_PASSWORD: change-me
      AGENTERR_MAX_DB_BYTES: "10737418240"   # 10 GiB guardrail; 0 = unlimited
    stop_grace_period: 20s   # graceful drain needs up to ~15s

volumes:
  agenterr-data:
```

Two notes:

- The image is `FROM scratch` — there is no shell, curl, or `healthcheck`
  subcommand inside it, so an in-container `healthcheck:` block isn't an
  option. If your orchestrator needs a health probe, point it at
  `http://<host>:3617/healthz` from outside the container instead.
- `AGENTERR_DB=/data/agenterr.db` is baked into the image; the volume is
  the only state.

## Reverse proxy (TLS)

agenterr trusts `X-Forwarded-Proto` for exactly two things: marking the
session cookie `Secure` and rendering copy-paste snippet URLs. Any proxy
that sets the standard header works unmodified.

**Caddy** (automatic TLS):

```
errors.example.com {
    reverse_proxy localhost:3617
}
```

**nginx:**

```nginx
server {
    listen 443 ssl;
    server_name errors.example.com;
    # ssl_certificate / ssl_certificate_key ...
    location / {
        proxy_pass http://127.0.0.1:3617;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

**Traefik** (Docker labels):

```yaml
    labels:
      - traefik.enable=true
      - traefik.http.routers.agenterr.rule=Host(`errors.example.com`)
      - traefik.http.routers.agenterr.entrypoints=websecure
      - traefik.http.routers.agenterr.tls.certresolver=le
      - traefik.http.services.agenterr.loadbalancer.server.port=3617
```

Keep ingest reachable by your apps (`/api/v1/ingest`, `/v1/logs` for
OTLP) — key auth protects them; the web UI is admin-password + session.
If you'd rather not expose the UI publicly at all, bind it to a VPN or
private network and only publish the ingest routes through the proxy.

## systemd (bare binary)

```ini
[Unit]
Description=agenterr error tracker
After=network-online.target
Wants=network-online.target

[Service]
User=agenterr
ExecStart=/usr/local/bin/agenterr --listen :3617 --db /var/lib/agenterr/agenterr.db
Restart=on-failure
TimeoutStopSec=20
StateDirectory=agenterr

[Install]
WantedBy=multi-user.target
```

## Backup

State is one SQLite file (WAL mode). Options, best first:

1. **[Litestream](https://litestream.io/)** — continuous WAL replication
   to S3-compatible storage; restores to the second.
2. `sqlite3 /data/agenterr.db ".backup /backups/agenterr-$(date +%F).db"`
   on a timer — consistent even while the server runs.
3. Raw file copy — only safe if you also copy the `-wal`/`-shm`
   sidecars, or the process is stopped.

## Upgrades and rollback

- **Upgrade:** pull the new image (or binary) and restart. Migrations
  run forward automatically on boot.
- **Stop grace:** give the process ≥ 15 seconds to stop — it drains the
  ingest pipeline to SQLite before exiting, so nothing accepted is lost.
- **Rollback:** keep the previous image tag; because migrations are
  forward-only, restore the pre-upgrade database backup when rolling
  back across a version that migrated the schema.
- Watch `/healthz` after any restart: `200 {"status":"ok",...}` means
  the store answered a ping.
