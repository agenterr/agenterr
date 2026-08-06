---
name: agenterr-debugging
description: Use when the user asks to check production errors, wants to know why prod is failing, says "debug with agenterr", asks what's erroring, or otherwise wants a live production error/log investigation against an Agenterr server. Triggers on phrasing like "check production errors", "why is prod failing", "debug with agenterr", "what's erroring right now".
---

# Agenterr debugging

Agenterr is an error and log tracker reachable through eight MCP tools:
`list_projects`, `list_issues`, `get_issue`, `search_logs`, `get_log_context`,
`resolve_issue`, `ignore_issue`, `get_stats`. This skill is the workflow for
using them to go from "something's wrong in prod" to a verified fix.

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
4 open issues in production:
#118 ×212 [error] panic: runtime error: invalid memory address or nil pointer dereference (last 3m ago, first 2h ago)
#104 ×41 [error] pq: duplicate key value violates unique constraint "orders_pkey" (last 22m ago, first 1d ago)
#97 ×9 [warn] context deadline exceeded calling billing-service (last 1h ago, first 4d ago)
#61 ×2 [error] unexpected end of JSON input (last 6h ago, first 9d ago)
```

Drill into the top one:

```
get_issue(id=118)
```

```
#118 [error] panic: runtime error: invalid memory address or nil pointer dereference
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
2026-08-06T16:58:44Z [info] checkout-api starting checkout for order ord_9f21ac id=990138
2026-08-06T16:58:44Z [info] checkout-api payment method lookup miss, falling back to default id=990139
2026-08-06T16:58:45Z [warn] checkout-api default payment method is nil for user u_44210 id=990140
2026-08-06T16:58:47Z [error] checkout-api panic: runtime error: invalid memory address or nil pointer dereference id=990141
→ 2026-08-06T16:58:47Z [error] checkout-api panic: runtime error: invalid memory address or nil pointer dereference id=990142
2026-08-06T16:58:47Z [info] checkout-api request completed with 500 id=990143
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
