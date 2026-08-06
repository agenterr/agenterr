# Agenterr

A lightweight, open-source, **agent-first** error and log tracker.

- **One Go binary.** OTLP + JSON log ingest, error grouping, search, REST API,
  MCP server, and a minimal web UI — no queues, no Redis, no Postgres required.
- **Truly lightweight.** SQLite storage, ~25MB idle, happy on a $5 VPS
  alongside the apps it monitors.
- **Agents are the primary interface.** First-class MCP tools and a shipped
  Claude Code skill; the web UI is for verification and light management.
- **Insight only.** Clean, queryable errors and logs for agents and humans.
  Not an APM, not a tracer, not an auto-fixer.

Status: **pre-MVP, under active development.** Design spec lives in the
workspace `docs/superpowers/specs/`.

## Layout

```
cmd/agenterr/        main binary (serve, migrate, admin)
cmd/agenterr-mcp/    stdio→HTTP MCP proxy
internal/ingest/     Ingester seam: OTLP/HTTP + JSON normalizers
internal/pipeline/   bounded buffer, single-writer batching, error detection
internal/grouping/   Grouper seam: fingerprinting
internal/store/      Store seam: SQLite (WAL + FTS5) + migrations
internal/api/        REST handlers (/api/v1)
internal/mcp/        MCP tools + Streamable HTTP transport (/mcp)
internal/web/        embedded htmx UI
internal/auth/       Authenticator seam: admin session + API/ingest keys
internal/config/     flags / env / optional TOML
skills/              shipped agent skills (installable by users)
```

## License

AGPL-3.0 — self-hosting is free and unlimited, forever.
