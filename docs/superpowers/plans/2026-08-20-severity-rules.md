# Severity Rules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-project `regex → severity` rules applied at ingest (spec §1 lift-chain slot 4), closing the trial's plain-text severity blind spot (GORM printing `record not found` at default INFO).

**Architecture:** Mirror the noise-rules stack end to end: a `severity_rules` table (migration 0011), `core.SeverityRule` + `store.SeverityRules` CRUD in sqlite, caching + compiled-regex matching + match accounting in `rules.Engine`, application in `pipeline.process` after panic detection and before noise `Decide`, and CRUD surfaces in MCP (3 new tools, 18→21) and the JSON API — all write-through the engine so the pipeline's cache stays fresh.

**Tech Stack:** Go 1.26, stdlib `regexp`. No new dependencies.

**Spec:** docs/superpowers/specs/2026-08-12-template-storage-engine-design.md — §1 severity-lift chain item 4 ("per-project severity rules: user-defined regex → severity, same storage/CRUD/MCP pattern as noise rules"), §6 ("severity-rule CRUD (mirror noise-rule tools)").

## Global Constraints

- **Lift semantics (plan ruling, recorded here because the spec's "first match wins" chain has no wire-level "unset" marker — absent severity already defaults to INFO at ingest):** a severity rule fires only when the log's severity is still ≤ `core.SeverityInfo` after the earlier chain slots (structured-body lift, panic detection), and it only LIFTS — the log's severity becomes the rule's severity (which validation requires to be > INFO). Rules never downgrade; downgrading is the existing `severity_floor` noise rule's domain. First matching rule (ascending ID) wins.
- Rules are per-project; `Service` scoping like noise rules ("" = any). Pattern is a Go regexp, validated with `regexp.Compile` at upsert time on every edge (sqlite, MCP, API) — a stored-but-broken pattern must be impossible.
- Fail-open everywhere: an unloaded engine, a project with no rules, or (belt-and-braces) a cached rule whose regex failed to compile lifts nothing. Ingest must never error because of severity rules.
- Match accounting mirrors drop accounting: in-memory pending counts, flushed by the existing lifecycle tickers right next to `FlushDrops`, folded back on flush error.
- The pipeline applies rules AFTER `core.DetectPanicSeverity` and BEFORE `p.d.Decide` (noise `severity_floor` rules must see the lifted value).
- All tests -race. Before every commit: `gofmt -l .` empty, `go vet ./...` clean, `$(go env GOPATH)/bin/golangci-lint run` 0 issues (gocyclo ≤15, NO nolint escapes; doc comments on all exported symbols).
- Migration immutability: new migration file 0011, never edit 0001–0010.
- MCP tool count goes 18 → 21; `cmd/agenterr-mcp/main_test.go` pins the count — update it in the task that adds the tools.

## File Structure

- `internal/store/sqlite/migrations/0011_severity_rules.sql` (new)
- `internal/core/severityrule.go` (new): `SeverityRule` + `Matches`
- `internal/store/store.go` (modify): `SeverityRules` interface + `SeverityRuleRow`, wired into the `Store` composite
- `internal/store/sqlite/severityrules.go` (new, mirrors noise.go)
- `internal/store/sqlite/severityrules_test.go` (new)
- `internal/rules/engine.go` (modify): severity cache, `Lift`, `FlushLifts`, `UpsertSeverity`, `DeleteSeverity`
- `internal/rules/engine_test.go` (modify): Lift/accounting tests
- `internal/rules/fake_store_test.go` (modify): fake gains severity-rule methods
- `internal/pipeline/pipeline.go` (modify): apply Lift in `process`
- `internal/app/lifecycle.go` (modify): flush lifts beside `FlushDrops`
- `internal/mcp/severityrules.go` (new, mirrors noise.go) + `render.go` + `tools.go` comment + `cmd/agenterr-mcp/main_test.go`
- `internal/api/handlers/severityrules.go` (new, mirrors noiserules.go) + router wiring
- Fakes used by mcp/api/pipeline tests gain the new store methods (grep for types implementing `store.NoiseRules` in *_test.go fakes).

---

### Task 1: Core type, migration, store interface, sqlite CRUD

**Files:**
- Create: `internal/store/sqlite/migrations/0011_severity_rules.sql`
- Create: `internal/core/severityrule.go`
- Modify: `internal/store/store.go`
- Create: `internal/store/sqlite/severityrules.go`
- Create: `internal/store/sqlite/severityrules_test.go`

**Interfaces (produced, consumed by Tasks 2–4):**

```go
// core
type SeverityRule struct {
	ID        int64
	ProjectID int64
	Service   string // "" = any service
	Pattern   string // Go regexp matched against the log body
	Severity  Severity // the lifted-to severity; validation requires > SeverityInfo
	Enabled   bool
}

// store
type SeverityRules interface {
	SeverityRules(ctx context.Context, projectID int64) ([]SeverityRuleRow, error) // 0 = all, ordered by ascending ID
	UpsertSeverityRule(ctx context.Context, r core.SeverityRule) (SeverityRuleRow, error)
	DeleteSeverityRule(ctx context.Context, id int64) error // missing → ErrNotFound
	AddSeverityLifts(ctx context.Context, counts map[int64]int64) error
}
type SeverityRuleRow struct {
	core.SeverityRule
	LiftedCount int64
	CreatedAt   time.Time
}
```

`SeverityRules` joins the `store.Store` composite interface exactly like `NoiseRules` does.

- [ ] **Step 1: Migration**

```sql
-- 0011_severity_rules.sql
-- Per-project severity-lift rules (spec §1 lift-chain slot 4): a Go
-- regexp over the log body that lifts still-default-severity logs to
-- the rule's severity. Mirrors noise_rules' shape; lifted_count needs
-- atomic increments like dropped_count.
CREATE TABLE severity_rules (
    id           INTEGER PRIMARY KEY,
    project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    service      TEXT    NOT NULL DEFAULT '',
    pattern      TEXT    NOT NULL,
    severity     TEXT    NOT NULL,
    enabled      INTEGER NOT NULL DEFAULT 1,
    lifted_count INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);
CREATE INDEX idx_severity_rules_project ON severity_rules(project_id);
```

Register it wherever migrations are embedded/listed (grep how 0010 is picked up — likely an embed glob, in which case the file alone suffices; verify by opening a fresh store in a test).

- [ ] **Step 2: core.SeverityRule** (`internal/core/severityrule.go`)

```go
package core

// SeverityRule lifts the severity of logs whose body matches Pattern
// (a Go regexp) and whose severity is still at-or-below the ingest
// default (SeverityInfo) after the earlier lift-chain slots (spec §1
// item 4). Rules only lift — downgrading is the severity_floor noise
// rule's domain — and the first matching rule by ascending ID wins.
type SeverityRule struct {
	ID        int64
	ProjectID int64
	Service   string   // "" = any service
	Pattern   string   // Go regexp matched against the log body
	Severity  Severity // lifted-to severity; must be > SeverityInfo
	Enabled   bool
}
```

(Matching itself lives in rules.Engine, which caches the COMPILED regexp — core deliberately stores only the source string.)

- [ ] **Step 3: sqlite CRUD** — write `internal/store/sqlite/severityrules.go` mirroring `noise.go` structure exactly (column consts, scan helper, ByID helper, RowsAffected → ErrNotFound). Differences from noise.go:
  - `UpsertSeverityRule` validates BEFORE touching the DB: `regexp.Compile(r.Pattern)` must succeed (error `"sqlite: invalid severity rule pattern: %v"`), `r.Pattern != ""`, and `r.Severity > core.SeverityInfo` (error `"sqlite: severity rule must lift above info"`).
  - Severity stored lowercase via `r.Severity.String()`, parsed back with `core.ParseSeverity` (same as noise).
  - `AddSeverityLifts` mirrors `AddNoiseDrops` verbatim (one tx, missing IDs silently skipped, empty map no-op).

- [ ] **Step 4: Tests** (`severityrules_test.go`, mirror the noise CRUD tests' setup): CRUD round-trip incl. LiftedCount/CreatedAt; update of missing ID → ErrNotFound; delete missing → ErrNotFound; invalid regex rejected; severity ≤ info rejected; empty pattern rejected; AddSeverityLifts accumulates and skips unknown IDs; projectID 0 returns all projects' rules ordered by ID.

- [ ] **Step 5:** `go test ./internal/store/... -race`, lint, commit.

---

### Task 2: rules.Engine — cache, Lift, accounting, write-through

**Files:**
- Modify: `internal/rules/engine.go`
- Modify: `internal/rules/engine_test.go`, `internal/rules/fake_store_test.go`

**Interfaces (produced):**

```go
func New(nr store.NoiseRules, sr store.SeverityRules, adm store.Admin) *Engine // signature change! update all callers (grep rules.New — app wiring + tests)
func (e *Engine) Lift(l core.Log) (core.Log, int64) // returns possibly-lifted log + winning rule ID (0 = none)
func (e *Engine) FlushLifts(ctx context.Context) error
func (e *Engine) UpsertSeverity(ctx context.Context, r core.SeverityRule) (store.SeverityRuleRow, error)
func (e *Engine) DeleteSeverity(ctx context.Context, id int64) error
```

Implementation contract (mirror the noise halves of the same file):

- Engine gains `sr store.SeverityRules`, `sevByProject map[int64][]compiledSevRule`, `pendingLifts map[int64]int64`. `compiledSevRule` = `{row store.SeverityRuleRow; re *regexp.Regexp}`.
- `Load` additionally fetches `e.sr.SeverityRules(ctx, 0)` and compiles each enabled rule's pattern; a pattern that fails to compile (corrupt row predating upsert validation) is SKIPPED with `slog.Warn`, never an error — fail-open, and one bad row must not disable the rest.
- `Lift(l)`:
  - RLock snapshot like `Decide`; unloaded or no rules → `(l, 0)`.
  - If `l.Severity > core.SeverityInfo` → `(l, 0)` (already meaningful).
  - First rule (ascending ID, preserved from store order) with `Enabled`, service match (`r.Service == "" || r.Service == l.Service`), and `re.MatchString(l.Body)` → set `l.Severity = r.Severity`, record pending lift (write lock, like `recordDrop`), return `(l, r.ID)`.
- `FlushLifts` mirrors `FlushDrops` exactly (swap map, `AddSeverityLifts`, fold back on error).
- `UpsertSeverity`/`DeleteSeverity` mirror `Upsert`/`Delete` (write-through + `Load`).
- Tests: lift fires on matching info-severity body; does NOT fire on already-error log; does NOT fire on disabled rule / wrong service / non-matching body; first-match-wins across two rules; corrupt-pattern row skipped without disabling others (seed the fake store directly); FlushLifts persists and resets, folds back on store error; fake store gains the four methods.

- [ ] Steps: failing tests → implement → `go test ./internal/rules/ -race` → update `rules.New` callers (`grep -rn "rules.New(" --include="*.go"`) so the repo builds → full `go test ./... -race` → lint → commit.

---

### Task 3: Pipeline + lifecycle wiring

**Files:**
- Modify: `internal/pipeline/pipeline.go` (process), its test file
- Modify: `internal/app/lifecycle.go` (both FlushDrops call sites)

- [ ] In `pipeline.process`, immediately after `l = core.DetectPanicSeverity(l)` and before `p.d.Decide(l)`:

```go
	l, _ = p.d.Lift(l)
```

(`p.d` is the decider interface the pipeline holds — it currently exposes `Decide`/`ParseBodies`; add `Lift(core.Log) (core.Log, int64)` to that interface and to any pipeline-test fake. The comment above `process` about ordering gains one sentence: severity rules lift before Decide so `severity_floor` noise rules key on the lifted value.)

- [ ] In `internal/app/lifecycle.go`, at BOTH existing `engine.FlushDrops(ctx)` sites, add the mirrored call + error log for `engine.FlushLifts(ctx)`.
- [ ] Pipeline test: a log with body matching a seeded severity rule at INFO arrives → stored entry has the lifted severity AND becomes an event if the lifted severity crosses the event threshold (check `core.IsEvent`'s threshold and assert accordingly — this is the whole point of the feature: lifted logs must alert).
- [ ] Full suite, lint, commit.

---

### Task 4: MCP tools + API handlers

**Files:**
- Create: `internal/mcp/severityrules.go`; modify `internal/mcp/render.go`, `internal/mcp/tools.go` (registration call + count comment), `internal/mcp/server.go` (if the Server needs nothing new — mutations go through the existing `s.engine`; list reads need `store.SeverityRules`, so add an `sr` field to Server and thread it through `mcp.New` and its callers)
- Modify: `cmd/agenterr-mcp/main_test.go` (tool count 18 → 21)
- Create: `internal/api/handlers/severityrules.go`; modify the API router where noiserules routes are registered
- Modify: any api/mcp test fakes implementing the store composite

Three MCP tools, registered from a `registerSeverityTools()` called in `registerTools`:
- `list_severity_rules` — mirrors `list_noise_rules` scoping (admin sees all/any project; project keys their own), renders ID, service, pattern, severity, enabled, lifted count.
- `upsert_severity_rule` — input `{id?, project_id?, service?, pattern, severity, enabled?}`; validates severity via `core.ParseSeverityStrict` AND `> info` (message: `"severity: must be above info — severity rules only lift"`), pattern non-empty and `regexp.Compile`-valid at this edge too; ownership check on update mirrors `upsertNoiseRule`; mutation via `s.engine.UpsertSeverity`. Description must tell the agent WHEN to use it: "Lift the severity of plain-text logs that print errors without a level (e.g. a GORM 'record not found' at info). Regex over the body; fires only on logs still at info or below."
- `delete_severity_rule` — mirrors `delete_noise_rule`, via `s.engine.DeleteSeverity`.

API: `severityrules.go` mirrors `noiserules.go` (list/upsert/delete endpoints, same scoping), mutations through the engine (check how noiserules.go reaches it — mirror exactly), plus router lines.

- [ ] Steps: failing MCP test (tool count + one handler test mirroring an existing noise-tool test) → implement → API handlers + router → fakes → full suite -race → lint → commit.

---

## Self-Review Notes

- Spec coverage: §1 item 4 (rules at slot 4, after structured/panic, before the never-shipped red-ANSI heuristic — slot 5 remains unshipped and untouched); §6 CRUD mirror. The lift-only, ≤INFO-gated semantics are a documented plan ruling (no wire-level unset marker exists).
- Regex-vs-substring: spec says regex; noise drop_match's substring precedent noted but spec governs.
- Signature change `rules.New` is called out with a grep instruction (app wiring + tests).
- Type consistency: `SeverityRuleRow` naming follows `NoiseRuleRow`; `AddSeverityLifts`/`FlushLifts`/`LiftedCount` naming is consistent across tasks.
- Tool-count pin (18→21) is updated in the same task that changes it.
