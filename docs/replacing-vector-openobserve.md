# Replacing a Vector + OpenObserve stack

If you run Vector (or Fluent Bit/Promtail) shipping container logs into
OpenObserve (or Loki/Elastic) mainly to *find errors*, agenterr replaces
both halves with one binary: `agenterr ship` is the shipper, the server
is the store, grouper, and alerter. This guide maps each piece.

## What maps to what

| Before | After |
|---|---|
| Vector `docker_logs` source | `agenterr ship --docker` |
| Vector `file` source | `agenterr ship --file '/var/log/app/*.log=myapp'` |
| Vector transforms (multiline join, severity parsing) | server-side: structured-body parsing (JSON/logfmt) + panic-prefix detection, ship-side multiline joining |
| OpenObserve streams + retention | projects with per-project `retention_days` |
| OpenObserve scheduled alerts | alert rules (threshold over sliding window → webhook) |
| SQL/log search UI | web UI + full-text search, or MCP tools from your agent |

## Run both in parallel first

Nothing about agenterr requires cutting over blind. Keep your existing
pipeline running and point a second shipper at agenterr:

```
agenterr ship --docker \
  --url https://errors.example.com \
  --key agt_ingest_xxx
```

Ship buffers to disk and retries, and it can never backpressure your
existing pipeline — it is a separate process reading the same sources.
Compare a week of data before touching the old stack.

## Severity: the usual migration surprise

Level-based alerts in log stores silently match nothing when apps log
plain text without a parsed level field. agenterr closes that gap
server-side, at ingest:

- JSON and logfmt bodies get their `level`/`severity` field lifted
  automatically — no shipper transforms to write.
- Plain-text Go crash dumps (`panic:` / `fatal error:`) are detected,
  joined into one record by ship, and recorded as FATAL grouped issues.
- Everything else defaults to INFO and is searchable, but only
  error/exception records become issues and feed alert rules.

Check your alert assumptions: if an old alert matched on message
*content* rather than level, recreate it as a noise-rule-plus-alert
combination or adjust the producing app to log structured bodies.

## Alerts

OpenObserve-style scheduled alerts ("N error logs in M minutes")
translate directly: create an alert rule with the same threshold and
window, pointing at your existing webhook receiver. The payload is JSON
carrying the rule name, project ID, and an issue object (title,
severity, event count, first/last seen), plus the event count and
window that triggered the rule — enough to route and format a
notification without an extra API call. Rules only see error/exception
events, so there is no "alert on INFO" — use noise rules to drop junk
instead of alerting around it.

## Cutover checklist

1. Deploy agenterr ([self-hosting guide](self-hosting.md)) and create a
   project + ingest key per service or environment.
2. Run `agenterr ship` alongside the old pipeline (above).
3. Recreate each alert as an alert rule; fire a test event end-to-end.
4. Verify a week of parallel running: issue grouping looks right, noisy
   sources are handled by noise rules (not by dropping in the shipper).
5. Point dashboards/bookmarks at agenterr, then retire the old
   shipper config and store. Retention means the old data ages out on
   its own if you keep the store around read-only for a while.

## What you give up

Honest list — agenterr is an error tracker, not a general log platform:

- No long-term log analytics/dashboards or SQL over logs.
- No metrics or traces (OTLP *logs* only, at `POST /v1/logs`).
- Single-node SQLite storage — right-sized for team-scale error
  tracking, not fleet-wide log archival.

If you need those, keep the log platform for analytics and let agenterr
own errors — ship dual-writes happily.
