# Deferred items (from MVP build reviews)

Carried out of the MVP review cycle, 2026-08-06. Each was reviewed and
ruled acceptable to ship; none blocks self-hosted use. Ordered by weight.

## Product improvements

- **Per-issue day counts.** The issue-detail sparkline uses project-wide
  stats as a labeled proxy; a `store` method for per-issue daily counts
  replaces it properly.
- **Regression annotation in `list_issues`.** `core.Issue` cannot express
  resolved→reopened; a `reopened_at` column would enable the "regressed"
  marker the MCP row format reserved.
- **`+N more` counts cap at the 500-row fetch bound.** A count-only store
  query would make truncation notices exact.
- **Light theme.** Dark is the design-system reference; light stays
  stubbed until demanded.
- **MCP proxy capability types.** `agenterr-mcp` proxies tools only; if
  the server ever gains resources/prompts, the proxy needs
  `HasResources`/`HasPrompts` + forwarding cases.

## Small hygiene

- Ingest-key lookup prefix carries ~1 random char (64 buckets) — widen
  stored prefix if key volume ever makes lookups slow.
- Ingest tolerances (unbounded unix-second timestamps; JSON `null` body
  accepted as one empty log; non-scalar attrs rendered via `%v`) are
  documented forgiving-ingest behavior; tighten only with evidence.
- `Content-Encoding: identity` returns 415 instead of being treated as
  no-encoding (RFC 7231 nit).
- `PATCH /issues/{id}` is fetch-then-write; becomes a real TOCTOU only if
  project delete/transfer ever exists.
- `internal/app` e2e-adjacent tests bind a free port with a close-then-
  rebind window; retry-once on bind failure if CI ever flakes there.
- `/healthz` hits the store per call, unauthenticated — cache briefly if
  exposed to high-frequency pollers.
