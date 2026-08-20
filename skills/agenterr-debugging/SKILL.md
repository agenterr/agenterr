---
name: agenterr-debugging
description: Use when the user asks to check production errors, wants to know why prod is failing, says "debug with agenterr", asks what's erroring, or otherwise wants a live production error/log investigation against an Agenterr server. Triggers on phrasing like "check production errors", "why is prod failing", "debug with agenterr", "what's erroring right now".
---

# Agenterr debugging

Agenterr is an error and log tracker reachable through twenty-one MCP
tools: `list_projects`, `list_issues`, `get_issue`, `search_logs`,
`get_log_context`, `resolve_issue`, `ignore_issue`, `get_stats`,
`aggregate_logs`, `list_noise_rules`, `upsert_noise_rule`,
`delete_noise_rule`, `get_noise_report`, `set_project_parse`,
`list_alert_rules`, `upsert_alert_rule`, `delete_alert_rule`,
`test_alert_rule`, `list_severity_rules`, `upsert_severity_rule`,
`delete_severity_rule`. This skill is the workflow for using them to go
from "something's wrong in prod" to a verified fix, for tuning out the
noise that gets in the way of that, and for setting up alerts so you hear
about the next one without having to go looking.

## Workflow

1. **Orient with `get_stats`.** Confirms the server is reachable and shows
   whether there's actually elevated error volume before you go looking.
2. **List open issues with `list_issues`.** Filter by environment and status
   `open`. Issues are sorted by most recent activity, so the top rows are
   what's happening right now.
3. **Pull full detail with `get_issue`** on the top (or user-named) issue —
   severity, seen-range, affected environments/releases, a 7-day histogram,
   and the newest sample event, stack trace included if present.
4. **Get surrounding context with `get_log_context`**, centered on the
   `id` of that newest sample event, to see what happened immediately
   before and after it.
5. **Find the code and fix it.** Use the stack trace, service name, and
   surrounding log lines from steps 3–4 to locate the failing code path in
   this repo. Fix it, then deploy as you normally would — Agenterr doesn't
   deploy anything for you.
6. **Resolve with `resolve_issue`** once the fix has actually shipped. Don't
   resolve before deploying — there's nothing to verify against yet.

If a previously-resolved issue reappears with new events, that is a
**regression**, not a data quirk: Agenterr reopens an issue automatically the
moment a new event lands against it. Treat a reopened issue as "the fix
didn't hold" and go back to step 3.

## Example run

Orientation:

```
get_stats(since="24h")
```

```
logs: 48213  events: 312  open issues: 4
2026-08-05: 24102 logs, 180 events
2026-08-06: 24111 logs, 132 events
```

Find what's open in production:

```
list_issues(environment="production", status="open")
```

```
4 open issues (production):
#118 ×212 [ERROR] panic: runtime error: invalid memory address or nil pointer dereference (last 3m ago, first 2h ago)
#104 ×41 [ERROR] pq: duplicate key value violates unique constraint "orders_pkey" (last 22m ago, first 1d ago)
#97 ×9 [WARN] context deadline exceeded calling billing-service (last 1h ago, first 4d ago)
#61 ×2 [ERROR] unexpected end of JSON input (last 6h ago, first 9d ago)
```

Drill into the top one:

```
get_issue(id=118)
```

```
#118 [ERROR] panic: runtime error: invalid memory address or nil pointer dereference
status: open  fingerprint: 7f3a9c2e1b8d4f60
first seen: 2026-08-06T14:02:11Z  last seen: 2026-08-06T16:58:47Z  count: 212
environments: production
releases: v2.4.1
7d: 0 0 0 0 0 45 167

newest event (2026-08-06T16:58:47Z):
service: checkout-api
panic: runtime error: invalid memory address or nil pointer dereference [recovered]
exception.stacktrace:
goroutine 42 [running]:
main.(*OrderHandler).ServeHTTP(0x0, {0x10, 0x0}, {0x0, 0x0})
	/app/internal/checkout/handler.go:87 +0x1a4
net/http.HandlerFunc.ServeHTTP(...)
	/usr/local/go/src/net/http/server.go:2166
order_id=ord_9f21ac
user_id=u_44210
```

Check what led up to it:

```
get_log_context(log_id=990142, n=10)
```

```
21 logs:
2026-08-06T16:58:44Z [INFO] checkout-api starting checkout for order ord_9f21ac id=990138
2026-08-06T16:58:44Z [INFO] checkout-api payment method lookup miss, falling back to default id=990139
2026-08-06T16:58:45Z [WARN] checkout-api default payment method is nil for user u_44210 id=990140
2026-08-06T16:58:47Z [ERROR] checkout-api panic: runtime error: invalid memory address or nil pointer dereference id=990141
→ 2026-08-06T16:58:47Z [ERROR] checkout-api panic: runtime error: invalid memory address or nil pointer dereference id=990142
2026-08-06T16:58:47Z [INFO] checkout-api request completed with 500 id=990143
```

That's enough to find the bug: `handler.go:87` dereferences the user's
default payment method without checking whether the lookup fell back to
`nil`. Fix it in `internal/checkout/handler.go`, deploy, then:

```
resolve_issue(id=118)
```

```
issue #118 resolved
```

If `#118` reopens later with fresh events, the fix didn't actually address
the nil case — go back to `get_issue(id=118)` and look at the newest sample
again rather than assuming it's a new, unrelated issue.

## Tuning noise

Chatty, low-value logs make `search_logs` and `get_stats` harder to read and
cost tokens on every call. Bring the volume down with a see-noise →
add-rule → verify-drop loop:

1. **See the noise with `get_noise_report`.** Run it when ingest volume
   looks dominated by low-value logs — it shows the top services by log
   volume alongside every existing rule's drop count, so you can tell
   what's noisy and whether a rule already covers it.
2. **Add a rule with `upsert_noise_rule`.** Pick the kind that fits: a
   `severity_floor` to drop everything below a level for a chatty service,
   a `drop_match` to drop lines containing a specific substring (e.g. a
   health-check body), or a `sample` to keep only 1 in N of a repetitive
   but not-worthless service.
3. **Verify the drop with a second `get_noise_report`.** The rule's
   `dropped_count` should be climbing and the noisy service's log volume
   falling. If it isn't, check `list_noise_rules` for a typo in `service`
   or `pattern` before assuming the rule is broken.

### Worked example: severity floor on a chatty infra service

`get_noise_report` shows `traefik` dominating ingest with mostly `INFO`
routing chatter drowning out the handful of `WARN`/`ERROR` lines that
actually matter:

```
get_noise_report(project_id=1)
```

```
noise report (24h): 3 services, 0 rules, 0 total dropped
top services by volume:
traefik: 41800 logs
checkout-api: 1900 logs
worker: 640 logs
rules:
```

Drop everything from `traefik` below `WARN`:

```
upsert_noise_rule(project_id=1, kind="severity_floor", service="traefik", severity="warn", enabled=true)
```

```
rule #4 severity_floor service=traefik severity=warn enabled=true dropped=0
```

Verify it's actually cutting volume:

```
get_noise_report(project_id=1)
```

```
noise report (24h): 3 services, 1 rules, 38900 total dropped
top services by volume:
traefik: 41800 logs
checkout-api: 1900 logs
worker: 640 logs
rules:
#4 severity_floor service=traefik severity=warn enabled=true dropped=38900
```

`top services by volume` reflects everything ingested before filtering, so
`traefik`'s number won't drop by itself — `dropped_count` climbing on rule
`#4` is the signal the rule is working.

### Fail-open, and every drop is counted

Rules are fail-open: anything ambiguous — an unknown kind, an empty
`pattern`, a rule that hasn't loaded yet — is treated as "don't drop" rather
than risk black-holing real records. A misconfigured rule costs you noise,
never data. And no drop is invisible: every record a rule drops increments
that rule's `dropped_count`, visible in both `list_noise_rules` and
`get_noise_report`, so you can always account for what's been filtered out
and undo a rule (`delete_noise_rule`) if it turns out to be too aggressive.

## Alerting

Instead of polling `list_issues`, set up a rule so Agenterr pushes a webhook
when something worth knowing about happens. A rule that's accepted but never
proven to actually deliver is worse than no rule — it looks like coverage
but silently isn't. Use a create → test-fire → trust loop every time:

1. **Create or update the rule with `upsert_alert_rule`.** Pick the kind
   that fits: `new_issue` fires the first time a fingerprint is seen,
   `regression` fires when a resolved issue reopens, `threshold` fires when
   `n` matching events land within `window_minutes`. Scope it with `service`,
   `environment`, and `min_severity` (all optional; omitted means "any").
   Alert rules only evaluate events that group into issues — records with
   severity ≥ error or exception attributes; a rule's `min_severity`
   narrows within that set, so a threshold rule aimed at info-level volume
   will never fire. Note that `test_alert_rule` verifies delivery (webhook
   reachability and payload structure), not that the rule's conditions can
   actually match — don't let a successful test-fire imply the rule is
   live-proven.
2. **Fire it for real with `test_alert_rule`.** This sends the rule's
   webhook immediately with a sample issue and `"test":true` in the payload,
   and reports back whether delivery succeeded — no need to wait for a
   matching event or poll `last_error` on `list_alert_rules`.
3. **Only trust the rule once step 2 reports delivered.** If it fails, the
   error text names the problem (connection refused, a non-2xx status, a
   timeout) — fix the `url` or `headers` with another `upsert_alert_rule`
   call and test again.

### Worked example: threshold alert on a spiking error

Watch for `checkout-api` producing 10 or more `error`-or-worse events inside
any 5-minute window, and notify a webhook:

```
upsert_alert_rule(project_id=1, name="checkout spike", kind="threshold", service="checkout-api", min_severity="error", n=10, window_minutes=5, url="https://hooks.example.com/agenterr")
```

```
rule #6 "checkout spike" threshold service=checkout-api environment=any min_severity=error n=10 window_minutes=5 cooldown=900s url=https://hooks.example.com/agenterr enabled=true last_fired=never fired
```

Prove the webhook actually receives it before walking away:

```
test_alert_rule(id=6)
```

```
alert rule #6 test-fired: delivered
```

If that had failed — wrong URL, receiver down, a required auth header
missing — `test_alert_rule` would have reported the delivery error directly
instead of a bare "ok", and the rule would still be sitting there looking
configured but unable to actually notify anyone.

### Cooldown and best-effort delivery

Every rule has a `cooldown_seconds` (default 900): after a fire — successful
or not, an attempt counts — the rule goes quiet for that long before it can
fire again, so a sustained spike doesn't turn into a webhook flood. Pass
`cooldown_seconds=0` on `upsert_alert_rule` if you genuinely want it to fire
every single time.

Delivery is best-effort and never blocks ingest: a webhook POST gets three
attempts with backoff, and every outcome — success or failure — lands in
the rule's `last_fired`/`last_error`, visible via `list_alert_rules`. If the
delivery queue itself is ever full (a receiver wedged under heavy load), the
fire is dropped rather than stalling log ingestion — alerting can lose a
notification, but it will never lose a log.

## Getting logs into Agenterr

This skill assumes logs are already flowing in. If `get_stats` comes back
empty or suspiciously quiet, the gap is usually collection, not the query:
Agenterr accepts direct OTLP/JSON ingest from an app or collector, or —
when the thing producing logs can't POST to Agenterr itself, e.g. tailing
an existing Docker host or a plain log file — `agenterr ship`, a sidecar
that tails Docker containers and/or files, joins multiline records
(including real panic/stack-trace dumps into one record instead of
fragments), buffers them to disk so a server restart doesn't lose anything,
and ships them to the ingest endpoint. See the README's "Shipping logs"
section for the docker/file quickstart and the full flag/env reference.

## Connecting Agenterr

Two ways to give an agent access to these tools.

**Remote (Streamable HTTP, no local process):**

```
claude mcp add --transport http agenterr https://logs.example.com/mcp \
  --header "Authorization: Bearer agt_api_..."
```

**stdio proxy (`agenterr-mcp` binary, forwards to the remote server):**

```
claude mcp add agenterr -- agenterr-mcp --url https://logs.example.com --key agt_api_...
```

Either an `agt_api_...` key (scoped to one project) or an `agt_admin_...`
key (instance-wide, can pass `project` on any tool that takes it) works.
`agenterr-mcp` also accepts the URL and key via `AGENTERR_URL` and
`AGENTERR_API_KEY` env vars instead of flags.
