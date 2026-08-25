[![CI](https://github.com/agenterr/agenterr/actions/workflows/ci.yml/badge.svg)](https://github.com/agenterr/agenterr/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.25-00ADD8)](go.mod)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue)](LICENSE)

# Agenterr

Agenterr is a self-hosted error and log tracker in one Go binary: OTLP and
JSON log ingest, error grouping, substring search, a REST API, an MCP
server, and a minimal web UI — no queues, no Postgres, no separate services
to run. Log bodies are stored via lossless template extraction in immutable
columnar zstd segments; SQLite holds only metadata (projects, issues,
triage, rules, templates). It's built for agents first: twenty-one MCP
tools and a shipped Claude Code skill are the primary interface, and the
web UI exists for verification and light management.

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

### Storage & search

Log bodies are stored via lossless template extraction — CLP-style: each
log splits into a template (its static structure) and variables, with
byte-for-byte reconstruction verified at ingest — packed into immutable
columnar zstd segments on disk. SQLite holds only metadata: projects,
issues, triage state, rules, templates, and the segment manifest. Measured
on a real 317k-log production day, that's **9.4 bytes/record all-in**.

Search is substring matching over the reconstructed body — there's no
tokenizer or full-text index, so it finds exact substrings, not stemmed or
tokenized matches. On the same reference corpus: a scoped search (one
service, 24h) runs in ~11ms; an unscoped full-day search runs in ~15ms.

Measured head-to-head against OpenObserve on identical data (see
[`docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md`](docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md)
for the full methodology and table), agenterr matches or beats OpenObserve
on storage (9.4 vs 10.9 bytes/record all-in), and is 3.4x faster on scoped
search, 2.3x faster on unscoped search, and 119x faster on aggregation —
while writes are durable (fsync-before-ack), unlike OpenObserve's
async-ack default.

### Severity rules

Per-project rules that lift the severity of plain-text logs which print
errors without a level — closing the common blind spot where, say, a GORM
`record not found` line prints at `info` and never triggers an
issue or an alert. A rule is a service scope plus a Go regexp matched
against the log body plus a target severity; it fires only on logs still
at `info` or below, and it only raises severity — never downgrades (that's
`severity_floor` noise rules' job). The first matching rule wins.

Manage rules over REST:

| Method & path | Purpose |
|---|---|
| `GET /api/v1/projects/{id}/severity-rules` | List a project's rules |
| `POST /api/v1/projects/{id}/severity-rules` | Create or update a rule (include `id` to update) |
| `DELETE /api/v1/severity-rules/{id}` | Remove a rule |

The same surface is available as three MCP tools:

| Tool | Purpose |
|---|---|
| `list_severity_rules` | List the rules configured for a project |
| `upsert_severity_rule` | Create or update a rule |
| `delete_severity_rule` | Remove a rule |

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
    "count": 0,
    "first_seen": "2026-08-06T15:44:39Z",
    "last_seen": "2026-08-06T15:44:39Z"
  },
  "fired_at": "2026-08-06T15:44:39Z"
}
```

`count` is populated only for threshold fires (the in-window event count); for new_issue and regression fires it is 0.
`first_seen` and `last_seen` reflect the triggering event's timestamp, not the issue's historical first sighting — notably, on a regression fire they are the reopening event's time.

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

### Aggregation

`GET /api/v1/aggregate` and the `aggregate_logs` MCP tool group a
project's logs by `service`, `severity`, `hour`, or `day` over an optional
`since`/`until` window, returning log and event counts per group. Both are
served from pre-computed rollups rather than scanning segments, which is
what keeps aggregation fast (0.17ms on the reference corpus — see
[Storage & search](#storage--search) above).

### Shipping logs

`agenterr ship` is a sidecar that tails Docker container logs and/or local
files, cleans and joins them into records, buffers them to disk, and ships
them to an ingest endpoint over HTTP — for when the thing producing logs
can't (or shouldn't) POST to Agenterr directly. It's the same binary as the
server, dispatched as a subcommand, so there's nothing extra to install.

Docker form — tail every non-excluded container on the host:

```
docker run -d --name agenterr-ship \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v agenterr-ship-data:/data \
  ghcr.io/agenterr/agenterr:latest ship \
  --docker --data-dir /data \
  --url https://your-agenterr-host --key agt_ingest_...
```

File form — tail one or more glob patterns, each mapped to a service name:

```
agenterr ship \
  --file '/var/log/myapp/*.log=myapp' \
  --url https://your-agenterr-host --key agt_ingest_...
```

`--file` is repeatable for multiple globs/services in one process; `--docker`
and `--file` can both be set at once.

**Semantics, in brief:**

- **Sources.** `--docker` tails every container's logs unless excluded;
  `--file 'GLOB=SERVICE'` tails matching files, reopening from the start on
  rotation (both truncate-in-place and rename+recreate).
- **Service naming (docker).** The container label
  `com.docker.swarm.service.name`, then `com.docker.compose.service`, then
  the container name — sanitized to `[a-zA-Z0-9_-]`.
- **Selection.** `--exclude svc1,svc2` and `--only svc1,svc2` match against
  the derived service name; the container label `agenterr.ignore=true`
  always excludes regardless of either flag.
- **Line processing.** ANSI color/cursor codes are stripped and multiline
  records are joined — indented continuation lines, Java-style
  `at `/`Caused by:`/`... N more` traces, and a Go panic/fatal-error dump
  (blank lines and non-indented frames included, so a real crash report
  lands as one record instead of fragmenting). A record flushes on a
  non-continuation line, when the join window elapses (`--join-window-ms`,
  default 1000), or at 64KB.
- **Buffering.** Records land in an on-disk spool before they're sent, so a
  server outage doesn't lose anything already tailed — delivery resumes from
  the last acknowledged position on restart. This is at-least-once, not
  exactly-once (docker sources resume with a 1s overlap); Agenterr's
  fingerprint-based grouping absorbs the rare resulting duplicate.
- **Delivery.** Batches are gzip-POSTed to `/api/v1/ingest` and retried
  forever on network errors or 5xx with exponential backoff (1s..30s); an
  oversized batch is split and retried, a 429 backs off 5s, and a 401/403 is
  fatal at startup (bad key, wrong instance) but only logged and retried at
  runtime (so a key rotated out from under a running shipper recovers on its
  own once fixed).
- **Severity.** Ship never derives or sends a severity — the server owns
  that. Structured (JSON/logfmt) bodies get their level lifted at ingest,
  and plain-text Go crash dumps (`panic:` / `fatal error:` prefixes) are
  detected server-side and recorded as FATAL — so a shipped panic arrives
  as one joined record *and* becomes a grouped, alertable issue.

| Env var | Flag | Default | Meaning |
|---|---|---|---|
| `AGENTERR_SHIP_URL` | `--url` | (required) | Agenterr ingest URL |
| `AGENTERR_SHIP_KEY` | `--key` | (required) | Ingest API key |
| `AGENTERR_SHIP_DOCKER` | `--docker` | `false` | Tail all non-excluded containers via the Docker socket |
| `AGENTERR_SHIP_DOCKER_SOCK` | `--docker-sock` | `/var/run/docker.sock` | Docker socket path |
| `AGENTERR_SHIP_FILE` | `--file` | (none) | `GLOB=SERVICE`; comma-separated in the env form, repeatable as a flag |
| `AGENTERR_SHIP_EXCLUDE` | `--exclude` | (none) | Comma-separated service names to exclude |
| `AGENTERR_SHIP_ONLY` | `--only` | (none) | Comma-separated service names to include exclusively |
| `AGENTERR_SHIP_DATA_DIR` | `--data-dir` | `./agenterr-ship-data` | Spool directory for buffered records |
| `AGENTERR_SHIP_MAX_BUFFER_BYTES` | `--max-buffer-bytes` | `536870912` (512MB) | Max on-disk spool bytes before oldest segments are dropped |
| `AGENTERR_SHIP_JOIN_WINDOW_MS` | `--join-window-ms` | `1000` | Multiline join window in milliseconds |

Flags override env vars, which override the defaults above — same
precedence as the server's own configuration. Nothing is ever lost
silently: a dropped batch, an oversized record, or a spool eviction is
always logged and counted, visible in ship's own periodic self-log line
(shipped/buffered/dropped/last-error, once a minute).

Migrating from a Vector + OpenObserve (or similar) stack? See
[docs/replacing-vector-openobserve.md](docs/replacing-vector-openobserve.md).

## Agent setup

### Claude Code: install the plugin

The fastest path. Two commands, then Claude Code asks for your server URL
and an API key — it stores the key in your OS keychain, wires up the MCP
server, and installs the debugging skill alongside it:

```
/plugin marketplace add agenterr/agenterr
/plugin install agenterr@agenterr
```

There's nothing to install locally: the plugin talks to your server over
Streamable HTTP, so the only inputs are the URL (`http://localhost:3617`
for a local instance) and an `agt_api_...` key.

### Any other MCP client

Give an agent access to the twenty-one MCP tools (`list_projects`,
`list_issues`, `get_issue`, `search_logs`, `get_log_context`,
`resolve_issue`, `ignore_issue`, `get_stats`, `aggregate_logs`,
`list_noise_rules`, `upsert_noise_rule`, `delete_noise_rule`,
`get_noise_report`, `set_project_parse`, `list_alert_rules`,
`upsert_alert_rule`, `delete_alert_rule`, `test_alert_rule`,
`list_severity_rules`, `upsert_severity_rule`, `delete_severity_rule`)
either directly over Streamable HTTP:

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
code, and resolving the issue once the fix has shipped. (The Claude Code
plugin above bundles this skill already; this step is only for clients
installed by hand.)

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
| `AGENTERR_MAX_DB_BYTES` | `--max-db-bytes` | `0` (unlimited) | Guardrail: if the SQLite file plus the engine's segment directory together exceed this, prune the oldest day across every project |
| `AGENTERR_NOISE_FLUSH_MS` | `--noise-flush-ms` | `30000` | How often noise-rule drop counters are persisted |

Flags override env vars, which override the defaults above.

## Self-hosting notes

- **Full deployment guide** — compose file, reverse-proxy TLS examples, systemd, backup, upgrades: [docs/self-hosting.md](docs/self-hosting.md).
- **Backup is one directory.** Metadata lives in the SQLite database at
  `AGENTERR_DB` (WAL mode, so also copy the `-wal`/`-shm` files if you're
  doing a raw filesystem copy while the process is running); log bodies
  live in the immutable columnar segments under the sibling `engine/`
  directory next to `AGENTERR_DB`. Back up both together. Segments are
  immutable once written, so a plain filesystem copy of `engine/` is safe
  even while the process is running; only the SQLite file needs WAL-aware
  handling. [Litestream](https://litestream.io/) covers continuous
  replication of the SQLite file itself — pair it with your own sync of
  `engine/` (e.g. `rclone`/`rsync` to the same off-site target) for full
  coverage.
- **Retention** is set per project (`retention_days` at creation) and
  enforced by an hourly sweep; `AGENTERR_MAX_DB_BYTES` is a coarse
  last-resort guardrail on top of that, not a replacement for it.
- **`/healthz`** is unauthenticated and pings the store on every call —
  point your uptime checker or orchestrator's readiness probe at it.
- **Login throttling** is per client IP (5 failed attempts per minute).
  Behind a reverse proxy all clients share the proxy's IP; the limiter
  deliberately ignores `X-Forwarded-For` (trivially forgeable). Keep the
  UI behind your proxy's own protections if you need per-user limits.

## License

AGPL-3.0 — self-hosting is free and unlimited, forever.
