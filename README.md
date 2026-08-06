[![CI](https://github.com/agenterr/agenterr/actions/workflows/ci.yml/badge.svg)](https://github.com/agenterr/agenterr/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.25-00ADD8)](go.mod)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

# Agenterr

Agenterr is a self-hosted error and log tracker in one Go binary: OTLP and
JSON log ingest, error grouping, full-text search, a REST API, an MCP
server, and a minimal web UI, backed by SQLite — no queues, no Postgres, no
separate services to run. It's built for agents first: seventeen MCP tools and
a shipped Claude Code skill are the primary interface, and the web UI
exists for verification and light management.

![An agent diagnosing a production incident through Agenterr's MCP tools](docs/media/demo.gif)

<sub>Replayed session — the tool outputs are captured verbatim from a live
instance. Rendered from [`docs/demo/demo.tape`](docs/demo/demo.tape).</sub>

## Quickstart

Run it with Docker, mounting a volume for the database:

```
docker run -p 3617:3617 -v agenterr-data:/data ghcr.io/agenterr/agenterr:latest
```

Or grab a binary from the [releases page](https://github.com/agenterr/agenterr/releases)
and run it directly:

```
./agenterr --listen :3617 --db ./agenterr.db
```

Either way, the first run prints a block like this once, to stdout:

```
=== Agenterr: first run ===
Generated admin password: 8f2a1c9d4e6b0173
Admin API key:            agt_admin_9c1e...
Setup URL:                http://localhost:3617/
Save these now: the key is shown only once and cannot be recovered.
============================
```

The password logs you into the web UI at the setup URL. The admin key
authenticates everything else below — it is instance-wide and satisfies any
key check, so keep it out of client code; mint narrower `api`/`ingest` keys
per project instead. Neither is recoverable if lost: mint a fresh admin key
by hand against the database, or delete `agenterr.db` and let the process
re-bootstrap.

Create a project and mint keys for it:

```
curl -X POST http://localhost:3617/api/v1/projects \
  -H "Authorization: Bearer agt_admin_9c1e..." \
  -H "Content-Type: application/json" \
  -d '{"name":"checkout-api","retention_days":30}'
# {"id":1,"name":"checkout-api","slug":"checkout-api","retention_days":30}

curl -X POST http://localhost:3617/api/v1/projects/1/keys \
  -H "Authorization: Bearer agt_admin_9c1e..." \
  -H "Content-Type: application/json" \
  -d '{"kind":"ingest"}'
# {"key":"agt_ingest_..."}
```

Point something at it. A raw JSON batch:

```
curl -X POST http://localhost:3617/api/v1/ingest \
  -H "Authorization: Bearer agt_ingest_..." \
  -H "Content-Type: application/json" \
  -d '[{"severity":"error","service":"checkout-api","environment":"production","body":"panic: nil pointer"}]'
```

Or an OTLP/HTTP log exporter (gzip-encoded bodies are also accepted):

```
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://localhost:3617/v1/logs
OTEL_EXPORTER_OTLP_LOGS_HEADERS=Authorization=Bearer%20agt_ingest_...
```

Or point an OpenTelemetry Collector's `otlphttp` exporter at the same
endpoint:

```yaml
# otel-collector-config.yaml
exporters:
  otlphttp/agenterr:
    logs_endpoint: http://localhost:3617/v1/logs
    headers:
      Authorization: "Bearer agt_ingest_..."

service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp/agenterr]
```

### Structured log bodies

If a record's body is itself a JSON object or a logfmt line — the common
case when tailing container stdout from apps using slog, zerolog, pino,
or logfmt loggers — Agenterr lifts its fields at ingest: `level` becomes
the record's severity, `msg`/`message` becomes the body, `time` the
timestamp (within a ±48h sanity window), and remaining keys become
queryable attributes. A body-level `error` therefore triggers error
detection and grouping even when the shipper sent no severity at all.

Lifting is conservative (a body must carry a message or level key to
qualify), never overwrites an explicitly-set severity or existing
attributes, and can be disabled with `--parse-bodies=false` or
`AGENTERR_PARSE_BODIES=false`.

### Noise rules

Per-project rules that stop chatty, low-value logs from ever being
stored — evaluated at ingest, before a record hits the database. Three
kinds:

- **`severity_floor`** — drop records below a given severity for a
  service (e.g. drop `debug`/`info` noise from a chatty health-check
  loop, keep `warn` and up).
- **`drop_match`** — drop records whose body contains a given substring.
- **`sample`** — keep 1 of every N records at or below a given severity,
  dropping the rest (e.g. keep 1 in 20 `info` lines instead of all of
  them).

Rules are fail-open: an unknown kind, an empty pattern, or an unloaded
rule engine all mean "keep the record" — a misconfigured rule can never
silently black-hole ingest. Every record a rule drops still increments
that rule's counter, so the volume it's cutting stays visible even
though the record itself is gone; counters are persisted periodically
(`--noise-flush-ms`, default 30s) and once more at shutdown.

Manage rules over REST:

| Method & path | Purpose |
|---|---|
| `GET /api/v1/projects/{id}/noise-rules` | List a project's rules |
| `POST /api/v1/projects/{id}/noise-rules` | Create or update a rule (include `id` to update) |
| `DELETE /api/v1/noise-rules/{id}` | Remove a rule |
| `GET /api/v1/noise-report` | Top services by volume plus every rule's drop count |
| `PATCH /api/v1/projects/{id}` | Toggle `parse_bodies` for a project |

The same surface is available as five MCP tools, so an agent can find
and cut noise on its own:

| Tool | Purpose |
|---|---|
| `list_noise_rules` | List the rules configured for a project |
| `upsert_noise_rule` | Create or update a rule |
| `delete_noise_rule` | Remove a rule |
| `get_noise_report` | Top services by volume plus every rule's drop count |
| `set_project_parse` | Toggle structured-body parsing for a project |

### Alerting

Per-project rules that POST a webhook when something worth paging on
happens — evaluated on the pipeline's write path, right alongside issue
grouping. Three kinds:

- **`new_issue`** — fires the first time a fingerprint is seen.
- **`regression`** — fires when a resolved issue reopens on a new event.
- **`threshold`** — fires when at least `n` matching events land within a
  sliding `window_minutes` window.

All three kinds share the same scope fields — `service`, `environment`,
`min_severity` — each optional and each empty meaning "any". Only events
that would already group into an issue (severity `error`/`fatal`, or a
record carrying an `exception.*` attribute) are ever evaluated against a
rule, so a `threshold` rule's `min_severity` narrows within that set
rather than admitting plain `info`/`debug` noise.

A rule fires at most once per `cooldown_seconds` (default 900); a fire
that's suppressed by cooldown is never queued at all, so it costs nothing.
Delivery itself is best-effort: a POST with a 5s timeout, retried up to
3 times with 1s/4s backoff, success being any 2xx. Every attempt's outcome
lands in the rule's `last_fired`/`last_error` — nothing fails silently —
and a full delivery queue drops the notification (logged, counted) rather
than block ingest, since alerting must never be able to slow down or stall
log ingestion.

The webhook body is the same shape for every kind, `event_count` and
`window_minutes` present only for `threshold`:

```json
{
  "rule": {"id": 3, "name": "checkout errors", "kind": "new_issue"},
  "project_id": 1,
  "issue": {
    "id": 42,
    "title": "PoolExhaustedError: no connections available",
    "severity": "ERROR",
    "count": 1,
    "first_seen": "2026-08-06T15:44:39Z",
    "last_seen": "2026-08-06T15:44:39Z"
  },
  "fired_at": "2026-08-06T15:44:39Z"
}
```

`issue.severity` is always rendered UPPERCASE, regardless of the case used
at ingest. Threshold windows are tracked in memory, not persisted — a
restart loses whatever partial window was in progress (a rule mid-count
starts over), though `last_fired` itself is persisted, so cooldowns
survive a restart. This is a deliberate v1 trade-off for a single-process
tracker.

Manage rules over REST:

| Method & path | Purpose |
|---|---|
| `GET /api/v1/projects/{id}/alert-rules` | List a project's rules |
| `POST /api/v1/projects/{id}/alert-rules` | Create or update a rule (include `id` to update) |
| `DELETE /api/v1/alert-rules/{id}` | Remove a rule |
| `POST /api/v1/alert-rules/{id}/test` | Fire a rule immediately with a sample issue (`"test":true` in the payload) and report whether delivery succeeded |

The same surface is available as four MCP tools:

| Tool | Purpose |
|---|---|
| `list_alert_rules` | List the rules configured for a project |
| `upsert_alert_rule` | Create or update a rule |
| `delete_alert_rule` | Remove a rule |
| `test_alert_rule` | Fire a rule immediately with a sample issue and report the outcome |

## Agent setup

Give an agent access to the seventeen MCP tools (`list_projects`,
`list_issues`, `get_issue`, `search_logs`, `get_log_context`,
`resolve_issue`, `ignore_issue`, `get_stats`, `list_noise_rules`,
`upsert_noise_rule`, `delete_noise_rule`, `get_noise_report`,
`set_project_parse`, `list_alert_rules`, `upsert_alert_rule`,
`delete_alert_rule`, `test_alert_rule`) either directly over Streamable
HTTP:

```
claude mcp add --transport http agenterr https://logs.example.com/mcp \
  --header "Authorization: Bearer agt_api_..."
```

or through the `agenterr-mcp` stdio proxy, if the agent only speaks stdio:

```
claude mcp add agenterr -- agenterr-mcp --url https://logs.example.com --key agt_api_...
```

Then install the shipped workflow skill from this repo's `skills/`
directory — `skills/agenterr-debugging/SKILL.md` — which walks an agent
through orientation, finding the top issue, pulling log context, fixing the
code, and resolving the issue once the fix has shipped.

## The web UI

For when you want to look yourself: issues, search, and settings, behind a
single admin login.

![The issues list: grouped errors with severity, counts, and recency](docs/media/ui-issues.png)

## Configuration

| Env var | Flag | Default | Meaning |
|---|---|---|---|
| `AGENTERR_LISTEN` | `--listen` | `:3617` | HTTP listen address |
| `AGENTERR_DB` | `--db` | `./agenterr.db` | SQLite database path |
| `AGENTERR_ADMIN_PASSWORD` | `--admin-password` | (generated) | Admin web UI password; set to skip first-run generation |
| `AGENTERR_BUFFER_SIZE` | `--buffer-size` | `10000` | Pipeline in-memory buffer capacity, in logs |
| `AGENTERR_FLUSH_EVERY_MS` | `--flush-every` | `200` | Pipeline batch-write interval |
| `AGENTERR_MAX_BODY_BYTES` | `--max-body-bytes` | `5242880` (5MB) | Max request body any ingest edge accepts |
| `AGENTERR_MAX_DB_BYTES` | `--max-db-bytes` | `0` (unlimited) | Guardrail: if the DB file exceeds this, prune the oldest day across every project |
| `AGENTERR_NOISE_FLUSH_MS` | `--noise-flush-ms` | `30000` | How often noise-rule drop counters are persisted |

Flags override env vars, which override the defaults above.

## Self-hosting notes

- **Backup is one file.** Everything lives in the SQLite database at
  `AGENTERR_DB` (WAL mode, so also copy the `-wal`/`-shm` files if you're
  doing a raw filesystem copy while the process is running). For continuous
  off-site backup with minimal downtime risk, run it under
  [Litestream](https://litestream.io/).
- **Retention** is set per project (`retention_days` at creation) and
  enforced by an hourly sweep; `AGENTERR_MAX_DB_BYTES` is a coarse
  last-resort guardrail on top of that, not a replacement for it.
- **`/healthz`** is unauthenticated and pings the store on every call —
  point your uptime checker or orchestrator's readiness probe at it.

## License

AGPL-3.0 — self-hosting is free and unlimited, forever.
