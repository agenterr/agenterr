# Architecture

Agenterr is one Go binary. This page is the map: how the packages depend on
each other, the five interface seams that keep them replaceable, the lint
rules that enforce both, how to add a new `Store` backend, and how the
release artifact is used when Agenterr runs as a hosted service.

## Dependency graph

```
                        cmd/agenterr (fx wiring only)
                                |
                          internal/app
                                |
                         internal/server
              (composes every edge onto one mux + /healthz)
           /        |         |         |          \
   ingest/jsonapi  ingest/otlp  api    mcp         web
           \        |         |         |          /
            \       |    internal/pipeline         /
             \      |         |                   /
              -------------- internal/store -------
                              |         \
                       store/sqlite   store/storetest
                                |
                          internal/core
                    (imports stdlib only)

   internal/auth --- depended on by: server, api, mcp, ingest/*, web
   internal/config -- depended on by: app, server (cfg values only)
```

`internal/core` sits at the bottom: it defines `Log`, `Event`, `Issue`,
severities, fingerprinting — the vocabulary everything else shares — and
imports nothing but the standard library. Every other package depends on
`core`, directly or transitively; `core` depends on nothing in this repo.
`cmd/agenterr-mcp` is the odd one out: it's a separate binary that only
depends on the `go-sdk/mcp` client and talks to a *running* Agenterr server
over HTTP — it does not import `internal/*` at all.

## The five seams

Agenterr is built around five narrow interfaces. Each is defined near the
package that owns the concept, implemented by exactly one concrete type
today, and exists so that type can be swapped without touching callers.

| Seam | Interface | Defined in | Today's implementation |
|---|---|---|---|
| Ingest | `ingest.Ingester` | `internal/ingest/ingest.go` | `ingest/jsonapi.Handler`, `ingest/otlp.Handler` |
| Grouping | `pipeline.Grouper` | `internal/pipeline/ports.go` | `core.DefaultGrouper` (delegates to `core.Fingerprint`) |
| Storage | `store.Store` (`Reader` + `Writer` + `Admin`) | `internal/store/store.go` | `store/sqlite.DB` |
| Auth | `auth.KeyAuth` / `auth.SessionAuth` | `internal/auth/auth.go` | `auth.Auth` |
| Notification | `pipeline.Notifier` | `internal/pipeline/ports.go` | `pipeline.NopNotifier` |

Every edge (`ingest/jsonapi`, `ingest/otlp`, `api`, `mcp`, `web`) depends on
these interfaces, never on the concrete SQLite/fx types — that's what makes
`internal/core` and `internal/pipeline` unit-testable without a database,
and `store/sqlite` swappable in principle without touching any edge.

## depguard rules

Two rules in `.golangci.yml` make the dependency graph above a build-time
guarantee rather than a convention:

```yaml
depguard:
  rules:
    core-is-pure:
      files: ["**/internal/core/**"]
      allow: ["$gostd", "github.com/agenterr/agenterr/internal/core"]
    fx-only-in-app:
      files: ["!**/internal/app/**", "!**/cmd/agenterr/**"]
      deny:
        - pkg: "go.uber.org/fx"
          desc: "fx is contained to internal/app"
```

- **`core-is-pure`** — `internal/core` may import the standard library and
  itself, nothing else. This is what keeps the shared vocabulary
  (`core.Log`, `core.Issue`, fingerprinting) free of framework and
  I/O concerns, and keeps every other package's dependency on it cheap.
- **`fx-only-in-app`** — `go.uber.org/fx` may only appear in
  `internal/app` and `cmd/agenterr`. Every other package is wired by
  plain constructor functions, so it can be constructed and tested without
  pulling in the DI framework or its container.

## Adding a `Store` implementation

`store.Store` is `Reader` + `Writer` + `Admin` (see `internal/store/store.go`).
To add a new backend (Postgres, for example):

1. Implement the three interfaces against your backend, satisfying the
   contracts documented on each method — in particular `WriteBatch`'s
   upsert-by-`(project, fingerprint)` semantics (open on first sight,
   increment count and reopen a resolved issue on a new event, cap sample
   events at 50 per issue) and `MintKey`/`LookupKey`'s three key kinds
   (`ingest`, `api`, `admin`).
2. Wire it into `internal/app` in place of `store/sqlite.DB` wherever the
   `store.Store` is constructed.
3. Prove it's behaviorally identical to the existing backend with one line,
   the same way `store/sqlite/sqlite_test.go` does:

   ```go
   func TestPostgresStore(t *testing.T) {
       storetest.Run(t, func(t *testing.T) store.Store {
           db, err := postgres.Open(testDSN(t))
           if err != nil {
               t.Fatal(err)
           }
           t.Cleanup(func() { db.Close() })
           return db
       })
   }
   ```

`storetest.Run` (`internal/store/storetest/suite.go`) is the single shared
behavioral contract for `store.Store` — every implementation runs the same
suite, so `internal/pipeline`, `internal/api`, and `internal/mcp` can rely
on identical semantics regardless of which `Store` is behind them.

## How the release artifact is used by cloud

The `.goreleaser.yml` Docker image (`ghcr.io/agenterr/agenterr`, built
`FROM scratch` around the `agenterr` binary and a CA bundle, see
`Dockerfile`) is not a convenience extra — under the planned hosted offering
(Model 3: one container per customer, not a shared multi-tenant process) it
is literally the unit the control plane provisions, starts, stops, and
upgrades. It reads its listen address and SQLite path the same way a
self-hosted operator would (`AGENTERR_LISTEN`, `AGENTERR_DB`, or their flag
equivalents — see `internal/config/config.go`), exposes `/healthz` for the
provisioner's readiness checks, and persists everything to the single
SQLite file at `AGENTERR_DB`, which is what the control plane snapshots or
Litestream-replicates for backup. Nothing about the image assumes it's
running standalone versus provisioned — self-hosting and the hosted product
run the exact same artifact.
