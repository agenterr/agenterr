# Template Engine Assembly Implementation Plan (Engine Plan B of B)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Assemble Plan A's libraries (internal/template, internal/segment, internal/engine) into `internal/store/enginestore` — a full `store.Store` passing `storetest` — wire the app to it, and delete the old `logs` table + FTS5.

**Architecture:** `enginestore.Store` **embeds `*sqlite.DB`** (which keeps issues, triage, rules, keys, settings, and gains templates/manifest/rollups/issue_events tables) and overrides every log-path method with the template engine: per-project WAL+memtable (spec decisions log), flush to immutable segments, substring search over reconstructed bodies. Old sqlite log paths stay intact until the final cutover task so every commit is green.

**Tech Stack:** Go, existing deps only (modernc.org/sqlite, klauspost/compress).

**Spec:** `docs/superpowers/specs/2026-08-12-template-storage-engine-design.md` §2–§5 + its **decisions log** (per-project engine state; flush sequencing `segment.Write → manifest insert → WAL.Reset → Memtable.Reset`; replay dedupe on LogID vs manifest MaxLogID; rollups outlive retention).

## Global Constraints

- Pure Go, no cgo. No new dependencies.
- Per-project `{WAL, memtable}`; WAL at `<dir(DBPath)>/engine/wal/<projectID>.wal`; segments at `<dir(DBPath)>/engine/segments/<projectID>/`.
- Flush sequencing exactly: `segment.Write` → manifest insert → rollups add → `WAL.Reset` → `Memtable.Reset`. Recovery dedupes replayed rows on `LogID >` the project's manifest MaxLogID.
- Durability: `WAL.Append` + `WAL.Sync` complete before `WriteBatch` returns (one fsync per batch — stronger than spec's 100 ms window; batches arrive at ~200 ms cadence).
- Rollups outlive segment retention (spec decision) — `Prune` never touches `log_rollups`.
- Memtable rows are **immutable once appended** — never mutate `Vars` in place (Plan A convention; Snapshot is shallow w.r.t. Vars).
- Reader semantics MUST mirror the sqlite implementations exactly (storetest enforces): SearchLogs most-recent-first, Limit 0 → 50; Issues env filter matches issues having an event with that environment; LogContext n-before (inclusive) + n-after, ascending; Stats PerDay ascending by day; ServiceCounts top 20, `logs DESC, service ASC`.
- Every exported symbol gets a doc comment (golangci-lint `revive` runs in CI only). gofmt clean, gocyclo ≤ 15 — keep functions small.
- Branch: `git checkout -b feat/engine-assembly` before Task 1.

## File Structure

```
internal/store/sqlite/migrations/0007_engine_metadata.sql   templates, segment_manifest, log_rollups, issue_events
internal/store/sqlite/engine_meta.go                        template.Store impl, segments CRUD, rollups, UpsertIssues, IssueRefs, helpers
internal/store/sqlite/engine_meta_test.go
internal/template/template_race_test.go                     deferred -race test from Plan A
internal/store/enginestore/enginestore.go                   Store type, Open, recovery, Close
internal/store/enginestore/write.go                         WriteBatch, flush
internal/store/enginestore/read.go                          SearchLogs, LogContext, Stats, ServiceCounts, Issue, Prune
internal/store/enginestore/enginestore_test.go              unit tests (write/flush/recovery)
internal/store/enginestore/read_test.go                     reader unit tests
internal/store/enginestore/storetest_test.go                storetest.Run gate
internal/store/sqlite/migrations/0008_drop_logs.sql         cutover: drop logs, logs_fts, events
```

---

### Task 1: SQLite engine metadata — migration 0007 + methods

**Files:**
- Create: `internal/store/sqlite/migrations/0007_engine_metadata.sql`, `internal/store/sqlite/engine_meta.go`
- Create: `internal/template/template_race_test.go` (clears a Plan-A deferred item)
- Test: `internal/store/sqlite/engine_meta_test.go`

**Interfaces:**
- Consumes: existing `sqlite.DB`, `template.Row`/`template.Store` (Plan A), `store.Entry`/`store.IssueOutcome`.
- Produces (Tasks 2–4 depend on these exact signatures on `*sqlite.DB`):
  - `InsertTemplate(ctx context.Context, projectID int64, text string) (int64, error)` and `LoadTemplates(ctx context.Context, projectID int64) ([]template.Row, error)` — satisfies `template.Store` (text stored in a BLOB column: it contains NUL wildcard bytes).
  - `type SegmentMeta struct { ID, ProjectID int64; Path string; MinTs, MaxTs, MinLogID, MaxLogID, Count, Events int64; Services []string }`
  - `InsertSegment(ctx context.Context, m SegmentMeta) (int64, error)`; `Segments(ctx context.Context, projectID int64) ([]SegmentMeta, error)` (projectID 0 = all, ordered by MinTs asc); `DeleteSegment(ctx context.Context, id int64) error`
  - `type RollupKey struct { ProjectID int64; Service string; Severity int; Hour string }` (Hour format `"2006-01-02T15"` UTC); `type RollupAdd struct { Logs, Events int64 }`; `AddRollups(ctx context.Context, counts map[RollupKey]RollupAdd) error` (upsert-accumulate)
  - `RollupStats(ctx context.Context, projectID int64, since time.Time) (logs, events int64, perDay map[string]store.DayCount, err error)` (hours with `hour >= since` truncated-to-hour; day = hour[:10])
  - `RollupServiceCounts(ctx context.Context, projectID int64, since time.Time) (map[string]int64, error)`
  - `UpsertIssues(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error)` — the issue half of the old `WriteBatch`: same upsert/reopen/count semantics and outcome ordering (one per IsEvent entry, in order), but event samples go to `issue_events (issue_id, log_id, project_id, environment, ts)` with the same 50-newest trim. `entries[i].Log.ID` must already be set by the caller.
  - `IssueRefs(ctx context.Context, id int64) (core.Issue, []EventRef, error)` with `type EventRef struct { LogID, IssueID int64; Ts time.Time }` — refs ordered `ts DESC, id DESC`; missing issue → `store.ErrNotFound`.
  - `OpenIssueCount(ctx context.Context, projectID int64) (int64, error)`
  - `DeleteIssueEventsBefore(ctx context.Context, projectID int64, before time.Time) error`

- [ ] **Step 1: Write the migration**

`internal/store/sqlite/migrations/0007_engine_metadata.sql`:

```sql
CREATE TABLE templates (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  text BLOB NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX templates_project ON templates(project_id, id);
CREATE TABLE segment_manifest (
  id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL REFERENCES projects(id),
  path TEXT NOT NULL UNIQUE,
  min_ts INTEGER NOT NULL, max_ts INTEGER NOT NULL,
  min_log_id INTEGER NOT NULL, max_log_id INTEGER NOT NULL,
  count INTEGER NOT NULL, events INTEGER NOT NULL,
  services TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL);
CREATE INDEX segment_manifest_project ON segment_manifest(project_id, min_ts);
CREATE TABLE log_rollups (
  project_id INTEGER NOT NULL, service TEXT NOT NULL,
  severity INTEGER NOT NULL, hour TEXT NOT NULL,
  logs INTEGER NOT NULL DEFAULT 0, events INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (project_id, service, severity, hour));
CREATE TABLE issue_events (
  id INTEGER PRIMARY KEY, issue_id INTEGER NOT NULL REFERENCES issues(id),
  log_id INTEGER NOT NULL, project_id INTEGER NOT NULL,
  environment TEXT NOT NULL DEFAULT '', ts TEXT NOT NULL);
CREATE INDEX issue_events_issue ON issue_events(issue_id, ts DESC);
CREATE INDEX issue_events_project_ts ON issue_events(project_id, ts);
```

Note `issue_events.log_id` has **no** foreign key to logs — log bodies live in the engine after cutover. `environment` is denormalized onto the event row because the Issues environment filter loses its `logs` join at cutover (Task 5 rewrites that query against `issue_events`).

- [ ] **Step 2: Write the failing tests**

`internal/store/sqlite/engine_meta_test.go` (this package's test convention: `Open(filepath.Join(t.TempDir(), "t.db"))`; look at `settings_test.go` for the pattern):

```go
package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/template"
)

func openTest(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustProj(ctx context.Context, t *testing.T, db *DB) core.Project {
	t.Helper()
	p, err := db.CreateProject(ctx, "p", 30)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTemplateStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	var _ template.Store = db // compile-time interface check

	text := "req \x00 done in \x00ms" // NUL wildcards must survive BLOB storage
	id1, err := db.InsertTemplate(ctx, p.ID, text)
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := db.InsertTemplate(ctx, p.ID, "other template")
	if id2 <= id1 {
		t.Errorf("ids not increasing: %d then %d", id1, id2)
	}
	rows, err := db.LoadTemplates(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != id1 || rows[0].Text != text {
		t.Errorf("rows = %+v", rows)
	}
}

func TestSegmentManifestCRUD(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	m := SegmentMeta{ProjectID: p.ID, Path: "engine/segments/1/000001.seg",
		MinTs: 100, MaxTs: 200, MinLogID: 1, MaxLogID: 50, Count: 50, Events: 3,
		Services: []string{"api", "web"}}
	id, err := db.InsertSegment(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.Segments(ctx, p.ID)
	if err != nil || len(got) != 1 {
		t.Fatalf("segments: %v %v", got, err)
	}
	m.ID = id
	if got[0].Path != m.Path || got[0].MaxLogID != 50 || len(got[0].Services) != 2 {
		t.Errorf("got %+v", got[0])
	}
	all, _ := db.Segments(ctx, 0)
	if len(all) != 1 {
		t.Errorf("projectID 0 should list all, got %d", len(all))
	}
	if err := db.DeleteSegment(ctx, id); err != nil {
		t.Fatal(err)
	}
	if got, _ := db.Segments(ctx, p.ID); len(got) != 0 {
		t.Error("segment not deleted")
	}
}

func TestRollupsAccumulateAndQuery(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	k1 := RollupKey{ProjectID: p.ID, Service: "api", Severity: 9, Hour: "2026-01-01T12"}
	k2 := RollupKey{ProjectID: p.ID, Service: "web", Severity: 17, Hour: "2026-01-02T03"}
	if err := db.AddRollups(ctx, map[RollupKey]RollupAdd{k1: {Logs: 2, Events: 0}, k2: {Logs: 1, Events: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := db.AddRollups(ctx, map[RollupKey]RollupAdd{k1: {Logs: 3, Events: 1}}); err != nil {
		t.Fatal(err)
	}
	logs, events, perDay, err := db.RollupStats(ctx, p.ID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if logs != 6 || events != 2 {
		t.Errorf("logs=%d events=%d, want 6, 2", logs, events)
	}
	if d := perDay["2026-01-01"]; d.Logs != 5 || d.Events != 1 {
		t.Errorf("day1 = %+v", d)
	}
	// since filters out earlier hours
	logs2, _, _, _ := db.RollupStats(ctx, p.ID, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if logs2 != 1 {
		t.Errorf("since-filtered logs = %d, want 1", logs2)
	}
	svc, err := db.RollupServiceCounts(ctx, p.ID, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || svc["api"] != 5 || svc["web"] != 1 {
		t.Errorf("svc = %v err %v", svc, err)
	}
}

func TestUpsertIssuesSemantics(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ev := func(logID int64, fp string, ts time.Time) store.Entry {
		return store.Entry{
			Log:         core.Log{ID: logID, ProjectID: p.ID, Time: ts, Severity: core.SeverityError, Body: "boom", Environment: "production"},
			IsEvent:     true, Fingerprint: fp, Title: "boom",
		}
	}
	plain := store.Entry{Log: core.Log{ID: 99, ProjectID: p.ID, Time: at, Severity: core.SeverityInfo, Body: "ok"}}

	out, err := db.UpsertIssues(ctx, []store.Entry{plain, ev(1, "fp1", at)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || !out[0].New {
		t.Fatalf("outcomes = %+v, want one New", out)
	}
	issueID := out[0].IssueID

	out2, _ := db.UpsertIssues(ctx, []store.Entry{ev(2, "fp1", at.Add(time.Minute))})
	if len(out2) != 1 || out2[0].New || out2[0].Reopened {
		t.Errorf("repeat outcome = %+v", out2)
	}

	iss, refs, err := db.IssueRefs(ctx, issueID)
	if err != nil {
		t.Fatal(err)
	}
	if iss.Count != 2 || len(refs) != 2 {
		t.Fatalf("count=%d refs=%d", iss.Count, len(refs))
	}
	if refs[0].LogID != 2 { // ts DESC: newest first
		t.Errorf("refs[0].LogID = %d, want 2", refs[0].LogID)
	}

	if err := db.SetIssueStatus(ctx, issueID, core.StatusResolved); err != nil {
		t.Fatal(err)
	}
	out3, _ := db.UpsertIssues(ctx, []store.Entry{ev(3, "fp1", at.Add(2 * time.Minute))})
	if !out3[0].Reopened {
		t.Errorf("want Reopened after resolve, got %+v", out3[0])
	}

	n, err := db.OpenIssueCount(ctx, p.ID)
	if err != nil || n != 1 {
		t.Errorf("open issues = %d err %v", n, err)
	}

	if _, _, err := db.IssueRefs(ctx, 9999); err != store.ErrNotFound {
		t.Errorf("missing issue err = %v", err)
	}

	if err := db.DeleteIssueEventsBefore(ctx, p.ID, at.Add(90*time.Second)); err != nil {
		t.Fatal(err)
	}
	_, refs2, _ := db.IssueRefs(ctx, issueID)
	if len(refs2) != 1 || refs2[0].LogID != 3 {
		t.Errorf("after delete-before: refs = %+v, want only logID 3", refs2)
	}
}

func TestIssueEventsTrimTo50(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 55; i++ {
		e := store.Entry{
			Log:     core.Log{ID: int64(i + 1), ProjectID: p.ID, Time: at.Add(time.Duration(i) * time.Second), Severity: core.SeverityError, Body: "x"},
			IsEvent: true, Fingerprint: "fp", Title: "x",
		}
		if _, err := db.UpsertIssues(ctx, []store.Entry{e}); err != nil {
			t.Fatal(err)
		}
	}
	iss, refs, err := db.IssueRefs(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if iss.Count != 55 {
		t.Errorf("count = %d", iss.Count)
	}
	if len(refs) != 50 || refs[0].LogID != 55 {
		t.Errorf("refs len=%d first=%d, want 50 newest (first logID 55)", len(refs), refs[0].LogID)
	}
}
```

`internal/template/template_race_test.go` (Plan A deferred item — concurrent Extract under -race):

```go
package template

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestExtractConcurrent(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				body := fmt.Sprintf("worker %d iteration %d done", g, i)
				id, vars, ok, err := e.Extract(context.Background(), int64(g%3+1), body)
				if err != nil || !ok {
					t.Errorf("extract: ok=%v err=%v", ok, err)
					return
				}
				if got, ok2 := e.Reconstruct(int64(g%3+1), id, vars); !ok2 || got != body {
					t.Errorf("round trip: %q", got)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/store/sqlite/ -run 'Template|Segment|Rollup|UpsertIssues|IssueEvents' -v`
Expected: FAIL (methods undefined). And `go test ./internal/template/ -race -run Concurrent -v` should PASS already (it tests existing code — if it fails, that is a real Plan A bug: stop and report).

- [ ] **Step 4: Implement `engine_meta.go`**

```go
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/template"
)

// This file holds the metadata surface the template storage engine
// (internal/store/enginestore) needs from SQLite: template persistence,
// the segment manifest, hourly rollups, and issue upserts decoupled from
// the legacy logs table (event samples live in issue_events, which has no
// FK to logs — log bodies are the engine's).

// InsertTemplate persists one template text for a project and returns its
// id. Text is stored as a BLOB because it embeds NUL wildcard bytes.
func (db *DB) InsertTemplate(ctx context.Context, projectID int64, text string) (int64, error) {
	res, err := db.sql.ExecContext(ctx,
		`INSERT INTO templates (project_id, text, created_at) VALUES (?, ?, ?)`,
		projectID, []byte(text), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("sqlite: insert template: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlite: template id: %w", err)
	}
	return id, nil
}

// LoadTemplates returns a project's templates ordered by ascending id.
func (db *DB) LoadTemplates(ctx context.Context, projectID int64) ([]template.Row, error) {
	rows, err := db.sql.QueryContext(ctx,
		`SELECT id, text FROM templates WHERE project_id = ? ORDER BY id`, projectID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load templates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []template.Row
	for rows.Next() {
		var r template.Row
		var text []byte
		if err := rows.Scan(&r.ID, &text); err != nil {
			return nil, fmt.Errorf("sqlite: scan template: %w", err)
		}
		r.Text = string(text)
		out = append(out, r)
	}
	return out, rows.Err()
}

// SegmentMeta is one row of the segment manifest — the durable record of
// an on-disk segment file and its pruning metadata.
type SegmentMeta struct {
	ID        int64
	ProjectID int64
	Path      string // relative to the engine data dir
	MinTs     int64  // epoch micros
	MaxTs     int64
	MinLogID  int64
	MaxLogID  int64
	Count     int64
	Events    int64
	Services  []string
}

// InsertSegment records a freshly written segment and returns its
// manifest id.
func (db *DB) InsertSegment(ctx context.Context, m SegmentMeta) (int64, error) {
	svc, err := json.Marshal(m.Services)
	if err != nil {
		return 0, fmt.Errorf("sqlite: marshal services: %w", err)
	}
	res, err := db.sql.ExecContext(ctx, `
INSERT INTO segment_manifest (project_id, path, min_ts, max_ts, min_log_id, max_log_id, count, events, services, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ProjectID, m.Path, m.MinTs, m.MaxTs, m.MinLogID, m.MaxLogID, m.Count, m.Events,
		string(svc), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("sqlite: insert segment: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlite: segment id: %w", err)
	}
	return id, nil
}

// Segments lists the manifest for one project (0 = all projects),
// ordered by ascending MinTs.
func (db *DB) Segments(ctx context.Context, projectID int64) ([]SegmentMeta, error) {
	q := `SELECT id, project_id, path, min_ts, max_ts, min_log_id, max_log_id, count, events, services
FROM segment_manifest`
	var args []any
	if projectID != 0 {
		q += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	q += ` ORDER BY min_ts ASC`
	rows, err := db.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: segments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SegmentMeta
	for rows.Next() {
		var m SegmentMeta
		var svc string
		if err := rows.Scan(&m.ID, &m.ProjectID, &m.Path, &m.MinTs, &m.MaxTs,
			&m.MinLogID, &m.MaxLogID, &m.Count, &m.Events, &svc); err != nil {
			return nil, fmt.Errorf("sqlite: scan segment: %w", err)
		}
		if err := json.Unmarshal([]byte(svc), &m.Services); err != nil {
			return nil, fmt.Errorf("sqlite: segment services: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteSegment removes one manifest row (after its file is deleted or
// replaced by Prune).
func (db *DB) DeleteSegment(ctx context.Context, id int64) error {
	if _, err := db.sql.ExecContext(ctx, `DELETE FROM segment_manifest WHERE id = ?`, id); err != nil {
		return fmt.Errorf("sqlite: delete segment: %w", err)
	}
	return nil
}

// RollupKey identifies one hourly rollup bucket. Hour is UTC,
// formatted "2006-01-02T15".
type RollupKey struct {
	ProjectID int64
	Service   string
	Severity  int
	Hour      string
}

// RollupAdd is the increment AddRollups applies to a bucket.
type RollupAdd struct {
	Logs   int64
	Events int64
}

// AddRollups accumulates hourly log/event counts, upserting buckets.
// Applied in one transaction at segment-flush time.
func (db *DB) AddRollups(ctx context.Context, counts map[RollupKey]RollupAdd) error {
	if len(counts) == 0 {
		return nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: rollups begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for k, v := range counts {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO log_rollups (project_id, service, severity, hour, logs, events)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id, service, severity, hour) DO UPDATE SET
  logs = logs + excluded.logs, events = events + excluded.events`,
			k.ProjectID, k.Service, k.Severity, k.Hour, v.Logs, v.Events); err != nil {
			return fmt.Errorf("sqlite: rollup upsert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: rollups commit: %w", err)
	}
	return nil
}

// RollupStats sums flushed log/event counts for a project since the
// given time, plus per-day buckets keyed "YYYY-MM-DD".
func (db *DB) RollupStats(ctx context.Context, projectID int64, since time.Time) (int64, int64, map[string]store.DayCount, error) {
	hour := since.UTC().Truncate(time.Hour).Format("2006-01-02T15")
	rows, err := db.sql.QueryContext(ctx, `
SELECT substr(hour, 1, 10) AS day, SUM(logs), SUM(events)
FROM log_rollups WHERE project_id = ? AND hour >= ?
GROUP BY day`, projectID, hour)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("sqlite: rollup stats: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var logs, events int64
	perDay := map[string]store.DayCount{}
	for rows.Next() {
		var d store.DayCount
		if err := rows.Scan(&d.Day, &d.Logs, &d.Events); err != nil {
			return 0, 0, nil, fmt.Errorf("sqlite: scan rollup: %w", err)
		}
		perDay[d.Day] = d
		logs += d.Logs
		events += d.Events
	}
	return logs, events, perDay, rows.Err()
}

// RollupServiceCounts sums flushed per-service log counts since the
// given time.
func (db *DB) RollupServiceCounts(ctx context.Context, projectID int64, since time.Time) (map[string]int64, error) {
	hour := since.UTC().Truncate(time.Hour).Format("2006-01-02T15")
	rows, err := db.sql.QueryContext(ctx, `
SELECT service, SUM(logs) FROM log_rollups
WHERE project_id = ? AND hour >= ? GROUP BY service`, projectID, hour)
	if err != nil {
		return nil, fmt.Errorf("sqlite: rollup services: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int64{}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, fmt.Errorf("sqlite: scan rollup service: %w", err)
		}
		out[s] = n
	}
	return out, rows.Err()
}

// UpsertIssues is the issue half of the legacy WriteBatch, decoupled from
// the logs table: same upsert/reopen/count/outcome semantics (see
// store.Writer), but event samples are recorded in issue_events keyed by
// the engine-assigned Log.ID, trimmed to the 50 newest per issue.
func (db *DB) UpsertIssues(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error) {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: upsert issues begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var outcomes []store.IssueOutcome
	for _, e := range entries {
		if !e.IsEvent {
			continue
		}
		o, err := upsertIssueEvent(ctx, tx, e)
		if err != nil {
			return nil, err
		}
		outcomes = append(outcomes, o)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: upsert issues commit: %w", err)
	}
	return outcomes, nil
}

func upsertIssueEvent(ctx context.Context, tx *sql.Tx, e store.Entry) (store.IssueOutcome, error) {
	ts := e.Log.Time.UTC().Format(time.RFC3339Nano)

	var prevStatus string
	err := tx.QueryRowContext(ctx, selectIssueStatusByFingerprint, e.Log.ProjectID, e.Fingerprint).Scan(&prevStatus)
	existed := true
	switch {
	case errors.Is(err, sql.ErrNoRows):
		existed = false
	case err != nil:
		return store.IssueOutcome{}, fmt.Errorf("sqlite: select issue status: %w", err)
	}

	if _, err := tx.ExecContext(ctx, upsertIssue,
		e.Log.ProjectID, e.Fingerprint, e.Title, int(e.Log.Severity), ts, ts); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: upsert issue: %w", err)
	}
	var issueID int64
	if err := tx.QueryRowContext(ctx, selectIssueIDByFingerprint, e.Log.ProjectID, e.Fingerprint).Scan(&issueID); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: select issue id: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO issue_events (issue_id, log_id, project_id, environment, ts) VALUES (?, ?, ?, ?, ?)`,
		issueID, e.Log.ID, e.Log.ProjectID, e.Log.Environment, ts); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: insert issue event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM issue_events WHERE issue_id = ? AND id NOT IN (
  SELECT id FROM issue_events WHERE issue_id = ? ORDER BY ts DESC, id DESC LIMIT 50)`,
		issueID, issueID); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: trim issue events: %w", err)
	}

	return store.IssueOutcome{
		IssueID:  issueID,
		New:      !existed,
		Reopened: existed && prevStatus == string(core.StatusResolved),
	}, nil
}

// EventRef is one retained event sample: the issue/log linkage without
// the log body (the engine resolves bodies by LogID).
type EventRef struct {
	LogID   int64
	IssueID int64
	Ts      time.Time
}

// IssueRefs returns an issue plus its retained event refs, newest first,
// or store.ErrNotFound.
func (db *DB) IssueRefs(ctx context.Context, id int64) (core.Issue, []EventRef, error) {
	row := db.sql.QueryRowContext(ctx, selectIssueByID, id)
	iss, err := scanIssue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return core.Issue{}, nil, store.ErrNotFound
	}
	if err != nil {
		return core.Issue{}, nil, err
	}
	rows, err := db.sql.QueryContext(ctx,
		`SELECT log_id, issue_id, ts FROM issue_events WHERE issue_id = ? ORDER BY ts DESC, id DESC`, id)
	if err != nil {
		return core.Issue{}, nil, fmt.Errorf("sqlite: issue refs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var refs []EventRef
	for rows.Next() {
		var r EventRef
		var ts string
		if err := rows.Scan(&r.LogID, &r.IssueID, &ts); err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: scan issue ref: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: issue ref ts: %w", err)
		}
		r.Ts = t
		refs = append(refs, r)
	}
	return iss, refs, rows.Err()
}

// OpenIssueCount counts a project's open issues.
func (db *DB) OpenIssueCount(ctx context.Context, projectID int64) (int64, error) {
	var n int64
	err := db.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM issues WHERE project_id = ? AND status = 'open'`, projectID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sqlite: open issue count: %w", err)
	}
	return n, nil
}

// DeleteIssueEventsBefore removes event refs older than before for a
// project — the retention companion to the engine's segment pruning.
func (db *DB) DeleteIssueEventsBefore(ctx context.Context, projectID int64, before time.Time) error {
	if _, err := db.sql.ExecContext(ctx,
		`DELETE FROM issue_events WHERE project_id = ? AND ts < ?`,
		projectID, before.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("sqlite: delete issue events: %w", err)
	}
	return nil
}
```

Note: `scanIssue` must accept both `*sql.Row` and `*sql.Rows` — it already does if it takes a scanner interface; check its signature in `read.go` and adapt the call if it is `*sql.Rows`-only (in that case add a tiny `scanIssueRow(*sql.Row)` beside it rather than changing existing callers).

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/store/sqlite/ -v && go test ./internal/template/ -race -v`
Expected: PASS (all new + all pre-existing sqlite tests — the migration is additive).

- [ ] **Step 6: gofmt, vet, full suite, commit**

Run: `gofmt -l . && go vet ./... && go test ./...`

```bash
git add internal/store/sqlite/ internal/template/
git commit -m "feat(sqlite): engine metadata tables and methods (templates, manifest, rollups, issue_events)"
```

---

### Task 2: `internal/store/enginestore` — Open, recovery, WriteBatch, flush, Close

**Files:**
- Create: `internal/store/enginestore/enginestore.go`, `internal/store/enginestore/write.go`
- Test: `internal/store/enginestore/enginestore_test.go`

**Interfaces:**
- Consumes: `sqlite.DB` + Task 1 methods; `template.Extractor`; `segment.Row`/`Write`; `engine.WAL`/`Memtable`/`ReplayWAL`.
- Produces (Task 3–4 build on):
  - `type Options struct { FlushRows int; FlushEvery time.Duration }` (defaults 64_000 rows, 5 min)
  - `func Open(dbPath string, opts Options) (*Store, error)` — engine data under `filepath.Dir(dbPath)/engine/{wal,segments}`.
  - `type Store struct { *sqlite.DB; ... }` — embeds sqlite so Admin/NoiseRules/AlertRules/Issues/SetIssueStatus/Settings promote unchanged.
  - `func (s *Store) WriteBatch(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error)` (overrides sqlite's)
  - `func (s *Store) Close() error` (flushes everything first)
  - Internal, used by Task 3: `func (s *Store) projectRows(projectID int64) []segment.Row` is NOT provided — Task 3 reads memtable+segments itself via `s.proj()` and `s.DB.Segments`; exported here for Task 3: `func (s *Store) FlushAll() error` and `func (s *Store) flushProject(projectID int64) error` (unexported; Task 4's Prune calls it — same package).
  - Row conventions: `Row.Attrs` = `json.Marshal(log.Attrs)` verbatim (nil map → `"null"`, matching the legacy sqlite behavior so reads round-trip identically); `Row.TsMicros = log.Time.UTC().UnixMicro()`; extraction failure (`ok=false`) → `TemplateID 0` + `Raw = body`.

- [ ] **Step 1: Write the failing tests**

```go
package enginestore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

var ctx = context.Background()

func openStore(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	s, err := Open(filepath.Join(dir, "agenterr.db"), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func logEntry(pid int64, body, service string, at time.Time) store.Entry {
	return store.Entry{Log: core.Log{ProjectID: pid, Time: at, Severity: core.SeverityInfo,
		Body: body, Service: service, Attrs: map[string]string{"k": "v"}}}
}

func TestWriteBatchAssignsIDsAndUpsertsIssues(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, err := s.CreateProject(ctx, "p", 30)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	entries := []store.Entry{
		logEntry(p.ID, "hello world one", "api", at),
		{Log: core.Log{ProjectID: p.ID, Time: at.Add(time.Second), Severity: core.SeverityError, Body: "boom now", Service: "api"},
			IsEvent: true, Fingerprint: "fp1", Title: "boom now"},
	}
	out, err := s.WriteBatch(ctx, entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || !out[0].New {
		t.Fatalf("outcomes = %+v", out)
	}
	// Issue is queryable through the embedded sqlite immediately.
	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID})
	if err != nil || len(issues) != 1 {
		t.Fatalf("issues = %v err %v", issues, err)
	}
}

func TestFlushThresholdWritesSegment(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{FlushRows: 3})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var entries []store.Entry
	for i := 0; i < 3; i++ {
		entries = append(entries, logEntry(p.ID, "row number x", "api", at.Add(time.Duration(i)*time.Second)))
	}
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}
	segs, err := s.DB.Segments(ctx, p.ID)
	if err != nil || len(segs) != 1 {
		t.Fatalf("segments = %v err %v", segs, err)
	}
	if segs[0].Count != 3 {
		t.Errorf("segment count = %d", segs[0].Count)
	}
	// Rollups recorded at flush.
	logs, _, _, err := s.DB.RollupStats(ctx, p.ID, at.Add(-time.Hour))
	if err != nil || logs != 3 {
		t.Errorf("rollup logs = %d err %v", logs, err)
	}
}

func TestRecoveryReplaysWALAndDedupes(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{FlushRows: 2})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	// Batch of 2 → flushed to a segment; batch of 1 → memtable+WAL only.
	if _, err := s.WriteBatch(ctx, []store.Entry{
		logEntry(p.ID, "flushed a", "api", at),
		logEntry(p.ID, "flushed b", "api", at.Add(time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(ctx, []store.Entry{
		logEntry(p.ID, "unflushed c", "api", at.Add(2*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate crash: close WITHOUT flushing the tail is not possible via
	// Close (it flushes), so reopen against a copy... instead: kill by
	// abandoning the store (no Close) and opening a second instance on the
	// same dir. The first store's WAL was synced by WriteBatch, so the
	// second instance must recover row "unflushed c".
	s2, err := Open(filepath.Join(dir, "agenterr.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	logs, err := s2.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 3 {
		t.Fatalf("after recovery: %d logs, want 3", len(logs))
	}
	// New writes must not collide with recovered ids.
	if _, err := s2.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "post recovery", "api", at.Add(3*time.Second))}); err != nil {
		t.Fatal(err)
	}
	logs2, _ := s2.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
	seen := map[int64]bool{}
	for _, l := range logs2 {
		if seen[l.ID] {
			t.Fatalf("duplicate log id %d after recovery", l.ID)
		}
		seen[l.ID] = true
	}
}
```

Note: `TestRecoveryReplaysWALAndDedupes` and `TestWriteBatchAssignsIDsAndUpsertsIssues` call `SearchLogs`, implemented in Task 3. To keep this task self-contained and compiling, add a MINIMAL `SearchLogs` to `write.go` in this task — memtable+segments scan without query/severity filters (Task 3 replaces it with the full version):

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/enginestore/ -v`
Expected: FAIL (package does not exist).

- [ ] **Step 3: Implement**

`internal/store/enginestore/enginestore.go`:

```go
// Package enginestore assembles the template storage engine into a full
// store.Store: SQLite (embedded) keeps issues, triage, rules, keys,
// settings, templates, the segment manifest, and rollups; log bodies live
// in per-project WAL + memtable + immutable columnar segments
// (spec §2–§4 and the decisions log in
// docs/superpowers/specs/2026-08-12-template-storage-engine-design.md).
package enginestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agenterr/agenterr/internal/engine"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/template"
)

// Options tunes the flush policy. Zero values select the defaults.
type Options struct {
	FlushRows  int           // segment flush threshold; default 64_000
	FlushEvery time.Duration // background flush interval; default 5m
}

// Store is the engine-backed store.Store. The embedded *sqlite.DB serves
// every metadata method unchanged; log-path methods are overridden here.
type Store struct {
	*sqlite.DB
	dir  string // <dir(dbPath)>/engine
	ex   *template.Extractor
	opts Options

	mu       sync.Mutex
	projects map[int64]*projState

	nextLogID atomic.Int64

	stop chan struct{}
	wg   sync.WaitGroup
}

type projState struct {
	mu  sync.Mutex
	wal *engine.WAL
	mem *engine.Memtable
	seq int64
}

// Open opens the SQLite metadata store at dbPath, prepares the engine
// data directory beside it, replays per-project WALs (deduping rows the
// manifest already covers), and starts the background flush ticker.
func Open(dbPath string, opts Options) (*Store, error) {
	if opts.FlushRows <= 0 {
		opts.FlushRows = 64_000
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 5 * time.Minute
	}
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(filepath.Dir(dbPath), "engine")
	for _, d := range []string{filepath.Join(dir, "wal"), filepath.Join(dir, "segments")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("enginestore: mkdir %s: %w", d, err)
		}
	}
	s := &Store{DB: db, dir: dir, opts: opts, projects: map[int64]*projState{}, stop: make(chan struct{})}
	s.ex = template.NewExtractor(db, 0)

	if err := s.recover(context.Background()); err != nil {
		return nil, err
	}

	s.wg.Add(1)
	go s.flushLoop()
	return s, nil
}

// recover seeds nextLogID from the manifest and replays every WAL file
// (listed from the directory, never the manifest, so an orphaned WAL is
// never skipped), deduping rows whose LogID the manifest already covers.
func (s *Store) recover(ctx context.Context) error {
	segs, err := s.DB.Segments(ctx, 0)
	if err != nil {
		return err
	}
	maxByProject := map[int64]int64{}
	for _, m := range segs {
		if m.MaxLogID > maxByProject[m.ProjectID] {
			maxByProject[m.ProjectID] = m.MaxLogID
		}
		if m.MaxLogID > s.nextLogID.Load() {
			s.nextLogID.Store(m.MaxLogID)
		}
	}
	walFiles, err := filepath.Glob(filepath.Join(s.dir, "wal", "*.wal"))
	if err != nil {
		return fmt.Errorf("enginestore: list wals: %w", err)
	}
	for _, wf := range walFiles {
		base := strings.TrimSuffix(filepath.Base(wf), ".wal")
		pid, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			return fmt.Errorf("enginestore: unexpected wal file %s", wf)
		}
		rows, err := engine.ReplayWAL(wf)
		if err != nil {
			return fmt.Errorf("enginestore: replay %s: %w", wf, err)
		}
		var keep []segment.Row
		for _, r := range rows {
			if r.LogID > maxByProject[pid] {
				keep = append(keep, r)
			}
			if r.LogID > s.nextLogID.Load() {
				s.nextLogID.Store(r.LogID)
			}
		}
		ps, err := s.proj(pid)
		if err != nil {
			return err
		}
		ps.mem.Append(keep)
	}
	return nil
}

// proj returns (creating on first use) the per-project engine state.
func (s *Store) proj(projectID int64) (*projState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ps, ok := s.projects[projectID]; ok {
		return ps, nil
	}
	w, err := engine.OpenWAL(filepath.Join(s.dir, "wal", fmt.Sprintf("%d.wal", projectID)))
	if err != nil {
		return nil, fmt.Errorf("enginestore: open wal: %w", err)
	}
	ps := &projState{wal: w, mem: engine.NewMemtable()}
	s.projects[projectID] = ps
	return ps, nil
}

func (s *Store) flushLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.FlushEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			_ = s.FlushAll() // errors are logged inside flushProject
		}
	}
}

// FlushAll flushes every project's memtable to a segment.
func (s *Store) FlushAll() error {
	s.mu.Lock()
	pids := make([]int64, 0, len(s.projects))
	for pid := range s.projects {
		pids = append(pids, pid)
	}
	s.mu.Unlock()
	var firstErr error
	for _, pid := range pids {
		if err := s.flushProject(pid); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close stops the flush loop, flushes everything, closes WALs, and
// closes the metadata DB.
func (s *Store) Close() error {
	close(s.stop)
	s.wg.Wait()
	err := s.FlushAll()
	s.mu.Lock()
	for _, ps := range s.projects {
		if cerr := ps.wal.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	s.mu.Unlock()
	if cerr := s.DB.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}
```

`internal/store/enginestore/write.go`:

```go
package enginestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/sqlitehelp" // does not exist — see note below
	"github.com/agenterr/agenterr/internal/store"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
)

// NOTE TO IMPLEMENTER: the sqlitehelp import above is a plan artifact —
// remove it; everything needed is on the embedded sqlite.DB.

// WriteBatch converts entries to engine rows (assigning monotonic log
// ids and extracting templates), makes them durable in the per-project
// WAL, serves them from the memtable, and upserts issues in SQLite.
// Rows are durable (WAL fsync'd) before this returns — the pipeline's
// ack. Issue upserts happen after log durability; a failure there
// surfaces as the batch error (the pipeline logs and drops).
func (s *Store) WriteBatch(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error) {
	byProject := map[int64][]segment.Row{}
	for i := range entries {
		e := &entries[i]
		id := s.nextLogID.Add(1)
		e.Log.ID = id
		row, err := s.toRow(ctx, e)
		if err != nil {
			return nil, err
		}
		byProject[e.Log.ProjectID] = append(byProject[e.Log.ProjectID], row)
	}

	var flushPids []int64
	for pid, rows := range byProject {
		ps, err := s.proj(pid)
		if err != nil {
			return nil, err
		}
		ps.mu.Lock()
		if err := ps.wal.Append(rows); err == nil {
			err = ps.wal.Sync()
		} else {
			ps.mu.Unlock()
			return nil, fmt.Errorf("enginestore: wal append: %w", err)
		}
		ps.mem.Append(rows)
		need := ps.mem.Len() >= s.opts.FlushRows
		ps.mu.Unlock()
		if need {
			flushPids = append(flushPids, pid)
		}
	}

	outcomes, err := s.DB.UpsertIssues(ctx, entries)
	if err != nil {
		return nil, err
	}
	for _, pid := range flushPids {
		if err := s.flushProject(pid); err != nil {
			slog.Error("enginestore: threshold flush failed", "project", pid, "error", err)
		}
	}
	return outcomes, nil
}

// toRow converts one entry to its engine row, extracting the template
// (raw fallback on ok=false) and canonicalizing attrs exactly as the
// legacy sqlite store did (json.Marshal; nil map → "null").
func (s *Store) toRow(ctx context.Context, e *store.Entry) (segment.Row, error) {
	attrs, err := json.Marshal(e.Log.Attrs)
	if err != nil {
		return segment.Row{}, fmt.Errorf("enginestore: marshal attrs: %w", err)
	}
	row := segment.Row{
		LogID: e.Log.ID, TsMicros: e.Log.Time.UTC().UnixMicro(),
		Severity: int(e.Log.Severity),
		Service:  e.Log.Service, Environment: e.Log.Environment,
		Release: e.Log.Release, TraceID: e.Log.TraceID,
		Attrs: string(attrs), IsEvent: e.IsEvent,
	}
	tid, vars, ok, err := s.ex.Extract(ctx, e.Log.ProjectID, e.Log.Body)
	if err != nil {
		return segment.Row{}, err
	}
	if ok {
		row.TemplateID, row.Vars = tid, vars
	} else {
		row.TemplateID, row.Raw = 0, e.Log.Body
	}
	return row, nil
}

// flushProject writes the project's memtable to a new immutable segment
// following the spec's sequencing: segment.Write → manifest insert →
// rollups → WAL.Reset → Memtable.Reset. An empty memtable is a no-op.
func (s *Store) flushProject(projectID int64) error {
	ps, err := s.proj(projectID)
	if err != nil {
		return err
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	rows := ps.mem.Snapshot()
	if len(rows) == 0 {
		return nil
	}
	ps.seq++
	rel := filepath.Join("segments", fmt.Sprintf("%d", projectID), fmt.Sprintf("%06d-%d.seg", ps.seq, rows[0].LogID))
	abs := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("enginestore: mkdir segment dir: %w", err)
	}
	foot, err := segment.Write(abs, rows)
	if err != nil {
		return fmt.Errorf("enginestore: write segment: %w", err)
	}
	meta := sqlitestore.SegmentMeta{
		ProjectID: projectID, Path: rel,
		MinTs: foot.MinTs, MaxTs: foot.MaxTs,
		MinLogID: foot.MinLogID, MaxLogID: foot.MaxLogID,
		Count: int64(foot.Count), Events: foot.Events, Services: foot.Services,
	}
	if _, err := s.DB.InsertSegment(context.Background(), meta); err != nil {
		// The file exists but the manifest doesn't know it: remove the
		// orphan so a retry doesn't collide, keep memtable+WAL intact.
		_ = os.Remove(abs)
		return err
	}
	if err := s.DB.AddRollups(context.Background(), rollupsFrom(rows)); err != nil {
		slog.Error("enginestore: rollup update failed (counts will undercount)", "error", err)
	}
	if err := ps.wal.Reset(); err != nil {
		return fmt.Errorf("enginestore: wal reset: %w", err)
	}
	ps.mem.Reset()
	return nil
}

// rollupsFrom aggregates rows into hourly rollup increments.
func rollupsFrom(rows []segment.Row) map[sqlitestore.RollupKey]sqlitestore.RollupAdd {
	out := map[sqlitestore.RollupKey]sqlitestore.RollupAdd{}
	for _, r := range rows {
		hour := time.UnixMicro(r.TsMicros).UTC().Format("2006-01-02T15")
		k := sqlitestore.RollupKey{ProjectID: projectOf(r), Service: r.Service, Severity: r.Severity, Hour: hour}
		a := out[k]
		a.Logs++
		if r.IsEvent {
			a.Events++
		}
		out[k] = a
	}
	return out
}
```

**Two deliberate gaps the implementer must resolve (they are the task's judgment content):**
1. `projectOf(r)` does not exist — `segment.Row` has no ProjectID (spec decision). `rollupsFrom` is only ever called from `flushProject(projectID)`, so change its signature to `rollupsFrom(projectID int64, rows []segment.Row)` and use that.
2. The stray `sqlitehelp` import — delete it. `core` may also end up unused in write.go depending on your minimal SearchLogs; keep imports tidy.

Minimal `SearchLogs` for this task (replaced in Task 3), in `write.go`:

```go
// SearchLogs (minimal, Task-2 scope): project + time filters only —
// Task 3 replaces this with the full implementation.
func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	rows, err := s.collectRows(ctx, f.ProjectID, f.Since, f.Until, "")
	if err != nil {
		return nil, err
	}
	out := make([]core.Log, 0, len(rows))
	for _, r := range rows {
		l, err := s.rowToLog(f.ProjectID, r)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}
```

…which requires the two real helpers (also in this task, in `write.go` or a small `rows.go`; Task 3 moves/keeps them):

```go
// collectRows returns all rows for a project across the memtable and
// every manifest segment overlapping [since, until] (zero times = no
// bound), optionally filtered by service via segment footers.
func (s *Store) collectRows(ctx context.Context, projectID int64, since, until time.Time, service string) ([]segment.Row, error) {
	var out []segment.Row
	sinceM, untilM := int64(-1<<62), int64(1<<62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	if !until.IsZero() {
		untilM = until.UTC().UnixMicro()
	}
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		return nil, err
	}
	for _, m := range segs {
		if m.MaxTs < sinceM || m.MinTs > untilM {
			continue
		}
		if service != "" && !contains(m.Services, service) {
			continue
		}
		_, rows, err := segment.Read(filepath.Join(s.dir, m.Path))
		if err != nil {
			return nil, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
		}
		for _, r := range rows {
			if r.TsMicros >= sinceM && r.TsMicros <= untilM {
				out = append(out, r)
			}
		}
	}
	ps, err := s.proj(projectID)
	if err != nil {
		return nil, err
	}
	for _, r := range ps.mem.Snapshot() {
		if r.TsMicros >= sinceM && r.TsMicros <= untilM {
			out = append(out, r)
		}
	}
	return out, nil
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// rowToLog reconstructs the core.Log a row represents.
func (s *Store) rowToLog(projectID int64, r segment.Row) (core.Log, error) {
	body := r.Raw
	if r.TemplateID != 0 {
		var ok bool
		body, ok = s.ex.Reconstruct(projectID, r.TemplateID, r.Vars)
		if !ok {
			return core.Log{}, fmt.Errorf("enginestore: template %d missing for log %d", r.TemplateID, r.LogID)
		}
	}
	var attrs map[string]string
	if err := json.Unmarshal([]byte(r.Attrs), &attrs); err != nil {
		return core.Log{}, fmt.Errorf("enginestore: attrs for log %d: %w", r.LogID, err)
	}
	return core.Log{
		ID: r.LogID, ProjectID: projectID, Time: time.UnixMicro(r.TsMicros).UTC(),
		Severity: core.Severity(r.Severity), Body: body,
		Service: r.Service, Environment: r.Environment,
		Release: r.Release, TraceID: r.TraceID, Attrs: attrs,
	}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/enginestore/ -v -race`
Expected: PASS all three tests.

- [ ] **Step 5: gofmt, vet, full suite, commit**

Run: `gofmt -l . && go vet ./... && go test ./...`

```bash
git add internal/store/enginestore/
git commit -m "feat(enginestore): write path — per-project WAL/memtable, flush, recovery"
```

---

### Task 3: enginestore readers — SearchLogs, LogContext, Stats, ServiceCounts, Issue

**Files:**
- Create: `internal/store/enginestore/read.go` (move the minimal SearchLogs + helpers here, replacing the minimal version)
- Test: `internal/store/enginestore/read_test.go`

**Interfaces:**
- Consumes: Task 2's `collectRows`/`rowToLog`/`proj`; Task 1's `RollupStats`/`RollupServiceCounts`/`IssueRefs`/`OpenIssueCount`.
- Produces: the full `store.Reader` on `*Store`. Exact semantics to mirror (consult `internal/store/sqlite/read.go` for the reference implementation of each):
  - `SearchLogs`: filters project/query(substring on reconstructed body)/minSeverity/service/environment/since/until; **most recent first (ts DESC, ties by LogID DESC)**; Limit 0 → 50.
  - `LogContext(logID, n)`: same project+service; n rows with `ts <= target` (target inclusive, nearest first — i.e. take the n newest of them) plus n rows with `ts > target` (oldest first); returned merged in ascending time order. Missing id → `store.ErrNotFound`.
  - `Stats`: `Logs`/`Events` = rollups(since) + memtable rows(ts ≥ since); `OpenIssues` via `OpenIssueCount`; `PerDay` merged from rollup days + memtable days, ascending by day.
  - `ServiceCounts`: rollups + memtable merged; top 20 ordered `logs DESC, service ASC`.
  - `Issue(id)`: `IssueRefs` + resolve each ref's log via the engine (`logByID`); events keep ref order (newest first); a ref whose log is missing (pruned segment) yields the event with `Log: core.Log{ID: ref.LogID}` — metadata without body — rather than an error.
  - Internal: `logByID(ctx, projectID... )` — NOTE: refs carry no projectID on the surface, but `issue_events` rows do; extend `EventRef` in Task 1 if you reach this point and find it missing — it is NOT missing: `IssueRefs` returns the issue, and `core.Issue.ProjectID` provides it. Locate the row via memtable scan, else manifest `MinLogID ≤ id ≤ MaxLogID` segments.

- [ ] **Step 1: Write the failing tests**

```go
package enginestore

import (
	"strings"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

func seed(t *testing.T, s *Store, pid int64) time.Time {
	t.Helper()
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	entries := []store.Entry{
		logEntry(pid, "connection refused by host", "api", at),
		logEntry(pid, "disk full on device", "api", at.Add(time.Minute)),
		logEntry(pid, "connection refused by host", "web", at.Add(2*time.Minute)),
		{Log: core.Log{ProjectID: pid, Time: at.Add(3 * time.Minute), Severity: core.SeverityError,
			Body: "kaboom happened", Service: "api"}, IsEvent: true, Fingerprint: "fp-k", Title: "kaboom happened"},
	}
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}
	return at
}

func TestSearchLogsSubstringSeverityOrderLimit(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := seed(t, s, p.ID)

	// Substring, not token: "onnection ref" matches mid-word.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: "onnection ref"})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("substring matches = %d, want 2", len(logs))
	}
	if !logs[0].Time.After(logs[1].Time) {
		t.Error("not most-recent-first")
	}
	// Severity + service + time filters.
	logs, _ = s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, MinSeverity: core.SeverityError})
	if len(logs) != 1 || !strings.Contains(logs[0].Body, "kaboom") {
		t.Fatalf("minseverity: %+v", logs)
	}
	logs, _ = s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Service: "web"})
	if len(logs) != 1 {
		t.Fatalf("service filter: %d", len(logs))
	}
	logs, _ = s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Since: at.Add(90 * time.Second)})
	if len(logs) != 2 {
		t.Fatalf("since filter: %d", len(logs))
	}
	logs, _ = s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Limit: 1})
	if len(logs) != 1 {
		t.Fatalf("limit: %d", len(logs))
	}
	// Search spans flushed segments too.
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	logs, _ = s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: "disk full"})
	if len(logs) != 1 {
		t.Fatalf("post-flush search: %d", len(logs))
	}
}

func TestLogContextAndIssueResolution(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	seed(t, s, p.ID)

	all, _ := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Service: "api"})
	// all is ts DESC; the middle api log is "disk full on device".
	var target core.Log
	for _, l := range all {
		if strings.Contains(l.Body, "disk full") {
			target = l
		}
	}
	nbrs, err := s.LogContext(ctx, target.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(nbrs) != 3 { // 1 before + target + 1 after (api service only)
		t.Fatalf("context len = %d: %+v", len(nbrs), nbrs)
	}
	for i := 1; i < len(nbrs); i++ {
		if nbrs[i].Time.Before(nbrs[i-1].Time) {
			t.Error("context not ascending")
		}
	}
	if _, err := s.LogContext(ctx, 999999, 2); err != store.ErrNotFound {
		t.Errorf("missing id: %v", err)
	}

	issues, _ := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID})
	iss, events, err := s.Issue(ctx, issues[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if iss.Count != 1 || len(events) != 1 {
		t.Fatalf("issue = %+v events = %d", iss, len(events))
	}
	if events[0].Log.Body != "kaboom happened" {
		t.Errorf("event body = %q", events[0].Log.Body)
	}
}

func TestStatsAndServiceCounts(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := seed(t, s, p.ID)

	// Split across flushed and unflushed: flush now, then add one more.
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "late arrival", "api", at.Add(25*time.Hour))}); err != nil {
		t.Fatal(err)
	}

	st, err := s.Stats(ctx, store.StatsFilter{ProjectID: p.ID, Since: at.Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if st.Logs != 5 || st.Events != 1 || st.OpenIssues != 1 {
		t.Fatalf("stats = %+v", st)
	}
	if len(st.PerDay) != 2 {
		t.Fatalf("perday = %+v", st.PerDay)
	}
	if st.PerDay[0].Day > st.PerDay[1].Day {
		t.Error("perday not ascending")
	}

	sc, err := s.ServiceCounts(ctx, p.ID, at.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(sc) != 2 || sc[0].Service != "api" || sc[0].Logs != 4 {
		t.Fatalf("service counts = %+v", sc)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/enginestore/ -run 'SearchLogsSubstring|LogContextAndIssue|StatsAndService' -v`
Expected: FAIL (full SearchLogs filters, LogContext, Stats, ServiceCounts, Issue undefined or minimal).

- [ ] **Step 3: Implement `read.go`**

```go
package enginestore

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store"
)

// SearchLogs returns logs matching f, most recent first (ties broken by
// descending id, matching the legacy store), capped at f.Limit (0 → 50).
// Query is a SUBSTRING match on the reconstructed body — no tokenizer
// exists anywhere in this engine (spec §5).
func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}
	rows, err := s.collectRows(ctx, f.ProjectID, f.Since, f.Until, f.Service)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].TsMicros != rows[j].TsMicros {
			return rows[i].TsMicros > rows[j].TsMicros
		}
		return rows[i].LogID > rows[j].LogID
	})
	out := make([]core.Log, 0, limit)
	for _, r := range rows {
		if f.Service != "" && r.Service != f.Service {
			continue
		}
		if f.Environment != "" && r.Environment != f.Environment {
			continue
		}
		if r.Severity < int(f.MinSeverity) {
			continue
		}
		l, err := s.rowToLog(f.ProjectID, r)
		if err != nil {
			return nil, err
		}
		if f.Query != "" && !strings.Contains(l.Body, f.Query) {
			continue
		}
		out = append(out, l)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// logByID locates one row by log id within a project (memtable first,
// then manifest segments whose id range covers it).
func (s *Store) logByID(ctx context.Context, projectID, logID int64) (segment.Row, bool, error) {
	ps, err := s.proj(projectID)
	if err != nil {
		return segment.Row{}, false, err
	}
	for _, r := range ps.mem.Snapshot() {
		if r.LogID == logID {
			return r, true, nil
		}
	}
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		return segment.Row{}, false, err
	}
	for _, m := range segs {
		if logID < m.MinLogID || logID > m.MaxLogID {
			continue
		}
		_, rows, err := segment.Read(s.segPath(m.Path))
		if err != nil {
			return segment.Row{}, false, err
		}
		for _, r := range rows {
			if r.LogID == logID {
				return r, true, nil
			}
		}
	}
	return segment.Row{}, false, nil
}

// LogContext returns up to n logs at-or-before the target (inclusive)
// and n after it, same project and service, ascending in time.
func (s *Store) LogContext(ctx context.Context, logID int64, n int) ([]core.Log, error) {
	target, projectID, err := s.findLog(ctx, logID)
	if err != nil {
		return nil, err
	}
	rows, err := s.collectRows(ctx, projectID, time.Time{}, time.Time{}, target.Service)
	if err != nil {
		return nil, err
	}
	var before, after []segment.Row
	for _, r := range rows {
		if r.Service != target.Service {
			continue
		}
		if r.TsMicros <= target.TsMicros {
			before = append(before, r)
		} else {
			after = append(after, r)
		}
	}
	sort.Slice(before, func(i, j int) bool { return before[i].TsMicros > before[j].TsMicros }) // newest first
	sort.Slice(after, func(i, j int) bool { return after[i].TsMicros < after[j].TsMicros })   // oldest first
	if len(before) > n {
		before = before[:n]
	}
	if len(after) > n {
		after = after[:n]
	}
	// Merge ascending: reversed(before) then after.
	out := make([]core.Log, 0, len(before)+len(after))
	for i := len(before) - 1; i >= 0; i-- {
		l, err := s.rowToLog(projectID, before[i])
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	for _, r := range after {
		l, err := s.rowToLog(projectID, r)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// findLog resolves a bare log id to its row and project by consulting
// every known project (memtables) and the manifest.
func (s *Store) findLog(ctx context.Context, logID int64) (segment.Row, int64, error) {
	s.mu.Lock()
	pids := make([]int64, 0, len(s.projects))
	for pid := range s.projects {
		pids = append(pids, pid)
	}
	s.mu.Unlock()
	for _, pid := range pids {
		if r, ok, err := s.logByID(ctx, pid, logID); err != nil {
			return segment.Row{}, 0, err
		} else if ok {
			return r, pid, nil
		}
	}
	segs, err := s.DB.Segments(ctx, 0)
	if err != nil {
		return segment.Row{}, 0, err
	}
	for _, m := range segs {
		if logID >= m.MinLogID && logID <= m.MaxLogID {
			if r, ok, err := s.logByID(ctx, m.ProjectID, logID); err != nil {
				return segment.Row{}, 0, err
			} else if ok {
				return r, m.ProjectID, nil
			}
		}
	}
	return segment.Row{}, 0, store.ErrNotFound
}

// Stats merges flushed rollups with unflushed memtable rows, so counts
// are exact and immediate. OpenIssues comes from the metadata DB.
func (s *Store) Stats(ctx context.Context, f store.StatsFilter) (store.Stats, error) {
	logs, events, perDay, err := s.DB.RollupStats(ctx, f.ProjectID, f.Since)
	if err != nil {
		return store.Stats{}, err
	}
	sinceM := int64(-1 << 62)
	if !f.Since.IsZero() {
		sinceM = f.Since.UTC().UnixMicro()
	}
	ps, err := s.proj(f.ProjectID)
	if err != nil {
		return store.Stats{}, err
	}
	for _, r := range ps.mem.Snapshot() {
		if r.TsMicros < sinceM {
			continue
		}
		logs++
		day := time.UnixMicro(r.TsMicros).UTC().Format("2006-01-02")
		d := perDay[day]
		d.Day = day
		d.Logs++
		if r.IsEvent {
			events++
			d.Events++
		}
		perDay[day] = d
	}
	open, err := s.DB.OpenIssueCount(ctx, f.ProjectID)
	if err != nil {
		return store.Stats{}, err
	}
	days := make([]store.DayCount, 0, len(perDay))
	for _, d := range perDay {
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Day < days[j].Day })
	return store.Stats{Logs: logs, Events: events, OpenIssues: open, PerDay: days}, nil
}

// ServiceCounts merges rollups with the memtable and returns the top 20
// services by log count (descending, ties by ascending name).
func (s *Store) ServiceCounts(ctx context.Context, projectID int64, since time.Time) ([]store.ServiceCount, error) {
	counts, err := s.DB.RollupServiceCounts(ctx, projectID, since)
	if err != nil {
		return nil, err
	}
	sinceM := int64(-1 << 62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	ps, err := s.proj(projectID)
	if err != nil {
		return nil, err
	}
	for _, r := range ps.mem.Snapshot() {
		if r.TsMicros >= sinceM {
			counts[r.Service]++
		}
	}
	out := make([]store.ServiceCount, 0, len(counts))
	for svc, n := range counts {
		out = append(out, store.ServiceCount{Service: svc, Logs: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Logs != out[j].Logs {
			return out[i].Logs > out[j].Logs
		}
		return out[i].Service < out[j].Service
	})
	if len(out) > 20 {
		out = out[:20]
	}
	return out, nil
}

// Issue returns the issue plus its retained events, newest first, with
// each event's log resolved from the engine. A ref whose log has been
// pruned yields the event with only its LogID populated.
func (s *Store) Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error) {
	iss, refs, err := s.DB.IssueRefs(ctx, id)
	if err != nil {
		return core.Issue{}, nil, err
	}
	events := make([]core.Event, 0, len(refs))
	for _, ref := range refs {
		ev := core.Event{LogID: ref.LogID, IssueID: ref.IssueID, Time: ref.Ts, Log: core.Log{ID: ref.LogID}}
		if r, ok, err := s.logByID(ctx, iss.ProjectID, ref.LogID); err != nil {
			return core.Issue{}, nil, err
		} else if ok {
			l, err := s.rowToLog(iss.ProjectID, r)
			if err != nil {
				return core.Issue{}, nil, err
			}
			ev.Log = l
		}
		events = append(events, ev)
	}
	return iss, events, nil
}
```

Move `collectRows`/`rowToLog`/`contains` from write.go into read.go if that leaves each file more cohesive; add `func (s *Store) segPath(rel string) string { return filepath.Join(s.dir, rel) }` and use it everywhere a manifest path is opened. Delete the Task-2 minimal SearchLogs.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/store/enginestore/ -v -race`
Expected: PASS all tests including Task 2's.

- [ ] **Step 5: gofmt, vet, full suite, commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/store/enginestore/
git commit -m "feat(enginestore): readers — substring search, context, stats, service counts, issue resolution"
```

---

### Task 4: Prune + the storetest gate

**Files:**
- Modify: `internal/store/enginestore/read.go` (add Prune) or a new `prune.go`
- Create: `internal/store/enginestore/storetest_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces: `func (s *Store) Prune(ctx context.Context, projectID int64, before time.Time) (int64, error)` — row-precision: flush the project first, then delete whole segments with `MaxTs < cutoff`; segments straddling the cutoff are read, filtered to `ts >= cutoff`, rewritten (new file + manifest row), old file+row deleted. Companion `DeleteIssueEventsBefore` cleans event refs. Returns the number of removed logs. Rollups untouched (spec: they outlive retention).

- [ ] **Step 1: Write the storetest gate + a prune unit test**

`internal/store/enginestore/storetest_test.go`:

```go
package enginestore_test

import (
	"path/filepath"
	"testing"

	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/enginestore"
	"github.com/agenterr/agenterr/internal/store/storetest"
)

func TestEngineStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		s, err := enginestore.Open(filepath.Join(t.TempDir(), "agenterr.db"), enginestore.Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
```

Add to `read_test.go` (package `enginestore`, straddling-segment prune):

```go
func TestPruneStraddlingSegment(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cutoff := old.Add(24 * time.Hour)
	fresh := old.Add(48 * time.Hour)
	if _, err := s.WriteBatch(ctx, []store.Entry{
		logEntry(p.ID, "old one", "api", old),
		logEntry(p.ID, "old two", "api", old.Add(time.Hour)),
		logEntry(p.ID, "new one", "api", fresh),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil { // one segment straddles the cutoff
		t.Fatal(err)
	}
	n, err := s.Prune(ctx, p.ID, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("pruned %d, want 2", n)
	}
	logs, _ := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
	if len(logs) != 1 || logs[0].Body != "new one" {
		t.Fatalf("after prune: %+v", logs)
	}
	segs, _ := s.DB.Segments(ctx, p.ID)
	if len(segs) != 1 || segs[0].Count != 1 {
		t.Fatalf("manifest after prune: %+v", segs)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/store/enginestore/ -run 'Prune|Contract' -v`
Expected: Prune test FAILS (method resolves to embedded sqlite Prune, which targets the legacy logs table — wrong counts); some Contract subtests fail for the same reason.

- [ ] **Step 3: Implement Prune**

```go
// Prune removes a project's logs older than before, with row precision:
// the project is flushed first (so the WAL and memtable cannot
// resurrect pruned rows), whole-old segments are deleted outright, and
// a segment straddling the cutoff is rewritten without its old rows.
// Event refs are cleaned alongside. Rollups are intentionally retained
// (spec: trend history outlives bodies). Returns removed log count.
func (s *Store) Prune(ctx context.Context, projectID int64, before time.Time) (int64, error) {
	if err := s.flushProject(projectID); err != nil {
		return 0, err
	}
	cutoff := before.UTC().UnixMicro()
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		return 0, err
	}
	var removed int64
	for _, m := range segs {
		switch {
		case m.MaxTs < cutoff: // entirely old
			if err := s.dropSegment(ctx, m); err != nil {
				return removed, err
			}
			removed += m.Count
		case m.MinTs < cutoff: // straddles: rewrite without old rows
			n, err := s.rewriteSegment(ctx, m, cutoff)
			if err != nil {
				return removed, err
			}
			removed += n
		}
	}
	if err := s.DB.DeleteIssueEventsBefore(ctx, projectID, before); err != nil {
		return removed, err
	}
	return removed, nil
}

// dropSegment deletes a segment's manifest row then its file. Manifest
// first: a file with no manifest row is an ignorable orphan, while a
// manifest row with no file would fail reads.
func (s *Store) dropSegment(ctx context.Context, m sqlitestore.SegmentMeta) error {
	if err := s.DB.DeleteSegment(ctx, m.ID); err != nil {
		return err
	}
	if err := os.Remove(s.segPath(m.Path)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("enginestore: remove segment: %w", err)
	}
	return nil
}

// rewriteSegment replaces m with a copy holding only rows at/after
// cutoff, returning how many rows were dropped. If nothing survives the
// filter the segment is simply dropped.
func (s *Store) rewriteSegment(ctx context.Context, m sqlitestore.SegmentMeta, cutoff int64) (int64, error) {
	_, rows, err := segment.Read(s.segPath(m.Path))
	if err != nil {
		return 0, err
	}
	keep := rows[:0]
	for _, r := range rows {
		if r.TsMicros >= cutoff {
			keep = append(keep, r)
		}
	}
	dropped := int64(len(rows) - len(keep))
	if len(keep) == 0 {
		return dropped, s.dropSegment(ctx, m)
	}
	rel := strings.TrimSuffix(m.Path, ".seg") + "-pruned.seg"
	foot, err := segment.Write(s.segPath(rel), keep)
	if err != nil {
		return 0, err
	}
	meta := sqlitestore.SegmentMeta{
		ProjectID: m.ProjectID, Path: rel,
		MinTs: foot.MinTs, MaxTs: foot.MaxTs,
		MinLogID: foot.MinLogID, MaxLogID: foot.MaxLogID,
		Count: int64(foot.Count), Events: foot.Events, Services: foot.Services,
	}
	if _, err := s.DB.InsertSegment(ctx, meta); err != nil {
		_ = os.Remove(s.segPath(rel))
		return 0, err
	}
	return dropped, s.dropSegment(ctx, m)
}
```

(Adjust imports; `sqlitestore` is the existing alias for the sqlite package used in write.go.)

- [ ] **Step 4: Run the gate**

Run: `go test ./internal/store/enginestore/ -v -race`
Expected: `TestPruneStraddlingSegment` PASSES, and `TestEngineStoreContract` runs all ~47 storetest subtests. **Expect divergences on first run** — this step is the point of the task. For each failure, fix `enginestore` to match the contract (consult the corresponding sqlite implementation as the reference). Do NOT modify `storetest` itself; if a subtest seems to demand legacy-FTS-specific behavior, note it in your report instead of changing the suite — the controller decides.

- [ ] **Step 5: gofmt, vet, full suite, commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/store/enginestore/
git commit -m "feat(enginestore): row-precision prune; storetest contract green"
```

---

### Task 5: Cutover — drop logs+FTS, rewire the app

**Files:**
- Create: `internal/store/sqlite/migrations/0008_drop_logs.sql`
- Modify: `internal/store/sqlite/read.go`, `internal/store/sqlite/write.go`, `internal/store/sqlite/prune.go` (delete log-path code), `internal/store/sqlite/sqlite_test.go` (drop storetest.Run), `internal/store/sqlite/storetest` — NO: `internal/store/storetest/suite.go` is NOT modified.
- Modify: `internal/app/app.go` (wire enginestore), `internal/store/store.go:34` (comment only)
- Test: existing suites.

**Interfaces:**
- Consumes: everything; after this task `sqlite.DB` no longer implements `store.Store` (it loses SearchLogs/LogContext/WriteBatch/Prune/Stats/ServiceCounts/Issue) — `enginestore.Store` is the only full Store.
- Produces: the running binary on the engine.

- [ ] **Step 1: Migration**

`internal/store/sqlite/migrations/0008_drop_logs.sql` — first read `0002_fts.sql` for the exact trigger names, then:

```sql
DROP TRIGGER IF EXISTS logs_ai;
DROP TRIGGER IF EXISTS logs_ad;
DROP TABLE IF EXISTS logs_fts;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS logs;
```

(If 0002's triggers have different names, use those. `events` is the legacy table — `issue_events` from 0007 replaces it.)

- [ ] **Step 2: Excise sqlite log paths**

Delete from the sqlite package (and their query constants): `WriteBatch`/`writeEntry` (write.go — keep `MintKey`/`LookupKey`), `Prune` (prune.go — delete the file), `SearchLogs`, `LogContext`, `Stats`, `ServiceCounts`, `Issue` (read.go — keep `Issues`, `IssueRefs`, `scanIssue`, issue constants), `quoteFTS`, `logColumns`/`logColumnsWithPrefix` and every `SELECT ... FROM logs` constant. In `Issues` (read.go:50-53) replace the environment filter subquery:

```go
b.WriteString(` AND id IN (SELECT issue_id FROM issue_events WHERE environment = ?)`)
```

In `internal/store/sqlite/sqlite_test.go`, delete the `storetest.Run` block (the contract now runs against enginestore only — Task 4's gate). Keep any non-contract sqlite tests.

- [ ] **Step 3: Rewire the app**

In `internal/app/app.go`:
- `openDB` becomes `func openDB(cfg config.Config) (*enginestore.Store, error) { return enginestore.Open(cfg.DBPath, enginestore.Options{}) }`
- Every `*sqlite.DB` parameter in providers becomes `*enginestore.Store` (`asStore`, `asReader`, `asWriter`, `asAdmin`, `asNoiseRules`, `asAlertRules`, `newAuth`) — the embedded sqlite promotes `Setting`/`SetSetting` so `newAuth` compiles unchanged apart from its parameter type.
- Import `enginestore`, drop the now-unused `sqlite` import.
- Check `internal/app/lifecycle.go` for other `*sqlite.DB` references (the shutdown hook calls `Close` — enginestore.Close flushes first, which is exactly right) and update types accordingly.

- [ ] **Step 4: Update the stale comment**

`internal/store/store.go:34`: change `Query string // FTS match on body; "" = all` to `Query string // substring match on body; "" = all`.

- [ ] **Step 5: Full verification**

Run: `gofmt -l . && go vet ./... && go test ./... -race 2>&1 | tail -20`
Expected: everything green — enginestore contract suite, app tests (they boot the fx graph), pipeline, api, web, mcp. Any test that constructed `*sqlite.DB` as a full Store needs updating to `enginestore.Open` — do so, preserving what the test asserts.

Then a smoke boot: `go run ./cmd/agenterr --db /tmp/agenterr-smoke/agenterr.db` (Ctrl-C after "listening"); verify `/tmp/agenterr-smoke/engine/wal/` exists.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat!: cut the server over to the template storage engine; drop logs table and FTS5"
```

---

## Self-review notes

- **Spec coverage:** §2 (extractor already landed; wired here via sqlite-backed template.Store), §3 (segments + manifest with services pruning metadata), §4 (per-project WAL/memtable, flush sequencing verbatim from the decisions log, recovery with LogID dedupe, ack-after-fsync), §5 (substring search over memtable+segments, rollup-backed Stats/ServiceCounts; the `aggregate_logs` MCP tool + severity rules remain the NEXT plan — this plan stops at store parity). Cutover per "no migrator" decision: 0008 drops legacy tables; existing DBs lose log history by design (fresh-store decision; the runbook's cold backup covers the trial box).
- **Type consistency check performed:** `SegmentMeta`/`RollupKey`/`RollupAdd`/`EventRef` names match across Tasks 1–4; `Options`/`Open`/`FlushAll` match between Tasks 2–4; `collectRows`/`rowToLog`/`segPath`/`proj` shared within the package.
- **Known judgment points (flagged, not hidden):** Task 2's `projectOf` gap + import cleanup are called out explicitly; Task 4 Step 4 expects storetest divergences and defines the fix protocol; Task 5 expects type-update fallout in app tests.
- **Placeholder scan:** clean — the only deliberate gaps are the two explicitly-flagged implementer decisions in Task 2.
