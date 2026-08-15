# Query Layer & Engine Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Segment compaction (bounding read amplification), the `aggregate_logs` capability (MCP tool + API endpoint), engine metrics in `get_stats`, and the read-path hygiene fixes deferred from the engine-assembly reviews.

**Architecture:** Compaction merges flushed segments per project on a schedule (hour-groups for today, day-groups for prior days) via a crash-atomic multi-row manifest swap. Aggregation generalizes the rollup+memtable merge already proven in Stats/ServiceCounts into a `store.Reader.Aggregate` method grouped by service/severity/hour/day. Engine metrics (templating rate, bytes/record, segment counts) come from two new manifest columns filled at flush time. Hygiene: reads stop materializing WAL files for untouched projects, and template-reconstruction failures distinguish "missing" from "store error".

**Tech Stack:** Go, existing deps only.

**Spec:** `docs/superpowers/specs/2026-08-12-template-storage-engine-design.md` §5–§6 + decisions log; Plan-B ledger deferrals (compaction priority, WAL-file litter, Reconstruct transparency).

## Global Constraints

- Pure Go, no new dependencies.
- Rollups are the aggregation source of truth for flushed data; memtable rows fill the unflushed tail — counts stay exact and immediate (same discipline as Stats/ServiceCounts).
- All manifest mutations that replace segments (compaction, prune rewrite) are crash-atomic: one SQLite transaction deletes old rows and inserts the new row; files are removed only after commit (orphan files are harmless, manifest-rows-without-files are not).
- Segment mutations happen under the project's `ps.mu` (the established coherence discipline vs readers).
- Reads must NEVER create engine state (no WAL files, no projStates) for projects they merely query.
- storetest/suite.go is never modified.
- Every exported symbol gets a doc comment (CI revive); gofmt clean; gocyclo ≤ 15; watch staticcheck QF1008 (use promoted selectors `s.X`, not `s.DB.X`) and revive redefines-builtin-id (never name a variable `max`/`min`).
- Branch: `git checkout -b feat/query-layer` before Task 1.

## File Structure

```
internal/store/sqlite/migrations/0009_manifest_metrics.sql   raw_rows, size_bytes columns
internal/store/sqlite/engine_meta.go                         ReplaceSegments, RollupAggregate, EngineTotals (append)
internal/template/template.go                                Reconstruct signature gains error
internal/store/store.go                                      AggregateFilter/AggregateRow, Reader.Aggregate
internal/store/enginestore/read.go                           readProj hygiene, Aggregate, rowToLog error plumb
internal/store/enginestore/compact.go                        compaction loop + CompactAll
internal/store/enginestore/stats.go                          EngineStats
internal/mcp/tools.go                                        aggregate_logs tool, get_stats enrichment
internal/api/api.go (+ handlers file it points to)           GET /api/v1/aggregate, stats fields
```

---

### Task 1: Manifest metrics columns + sqlite methods (ReplaceSegments, RollupAggregate, EngineTotals)

**Files:**
- Create: `internal/store/sqlite/migrations/0009_manifest_metrics.sql`
- Modify: `internal/store/sqlite/engine_meta.go` (append; also extend `SegmentMeta` and rewrite `SwapSegment` as a wrapper)
- Test: `internal/store/sqlite/engine_meta_test.go` (append)

**Interfaces:**
- Consumes: existing `SegmentMeta`, `RollupKey` conventions (`Hour` = `"2006-01-02T15"` UTC).
- Produces (Tasks 3–5 depend on):
  - `SegmentMeta` gains `RawRows int64` and `SizeBytes int64` (persisted; `Segments` scans them; `InsertSegment` writes them).
  - `func (db *DB) ReplaceSegments(ctx context.Context, oldIDs []int64, m SegmentMeta) (int64, error)` — deletes every oldID and inserts m in ONE transaction. Empty oldIDs → plain insert. A missing oldID is not an error (idempotent retry). `SwapSegment(ctx, oldID, m)` becomes `return db.ReplaceSegments(ctx, []int64{oldID}, m)` (keep the exported wrapper — prune calls it).
  - `func (db *DB) RollupAggregate(ctx context.Context, projectID int64, since, until time.Time, groupBy string) (map[string]store.AggregateRow, error)` — groupBy ∈ `"service"|"severity"|"hour"|"day"`; keys: service name / severity integer as decimal string / `"2006-01-02T15"` / `"2006-01-02"`. `since` truncates down to the hour (documented rollup granularity); zero `until` = no upper bound, else `hour <= until-truncated-to-hour`. Unknown groupBy → error.
  - `func (db *DB) EngineTotals(ctx context.Context, projectID int64) (segments, rows, rawRows, sizeBytes int64, err error)` — SUM over segment_manifest (projectID 0 = all).
- Note: `store.AggregateRow` is defined in Task 3; to keep this task self-contained and compiling, define the map value locally as `RollupAgg struct { Logs, Events int64 }` and have Task 3's enginestore adapt it. (RollupAggregate returns `map[string]RollupAgg`.)

- [ ] **Step 1: Migration**

`internal/store/sqlite/migrations/0009_manifest_metrics.sql`:

```sql
ALTER TABLE segment_manifest ADD COLUMN raw_rows INTEGER NOT NULL DEFAULT 0;
ALTER TABLE segment_manifest ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0;
```

- [ ] **Step 2: Write the failing tests** (append to `engine_meta_test.go`; reuse `openTest`/`mustProj`)

```go
func TestReplaceSegmentsAtomicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	mk := func(path string, minLog, maxLog int64) int64 {
		id, err := db.InsertSegment(ctx, SegmentMeta{ProjectID: p.ID, Path: path,
			MinTs: 1, MaxTs: 2, MinLogID: minLog, MaxLogID: maxLog, Count: maxLog - minLog + 1,
			RawRows: 1, SizeBytes: 100, Services: []string{"api"}})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	a, b := mk("a.seg", 1, 10), mk("b.seg", 11, 20)
	merged := SegmentMeta{ProjectID: p.ID, Path: "ab.seg", MinTs: 1, MaxTs: 2,
		MinLogID: 1, MaxLogID: 20, Count: 20, RawRows: 2, SizeBytes: 180, Services: []string{"api"}}
	if _, err := db.ReplaceSegments(ctx, []int64{a, b}, merged); err != nil {
		t.Fatal(err)
	}
	segs, _ := db.Segments(ctx, p.ID)
	if len(segs) != 1 || segs[0].Path != "ab.seg" || segs[0].RawRows != 2 || segs[0].SizeBytes != 180 {
		t.Fatalf("after replace: %+v", segs)
	}
	// Idempotent retry: old ids already gone, unique path collision must not occur on same meta re-insert…
	// (retry semantics: missing oldIDs tolerated; re-inserting the same path errors on UNIQUE — callers
	// only retry after a crash BEFORE commit, when neither delete nor insert happened.)
	if _, err := db.ReplaceSegments(ctx, []int64{a, b}, SegmentMeta{ProjectID: p.ID, Path: "ab2.seg",
		MinTs: 1, MaxTs: 2, MinLogID: 1, MaxLogID: 20, Count: 20, Services: []string{"api"}}); err != nil {
		t.Fatalf("missing oldIDs must be tolerated: %v", err)
	}
}

func TestRollupAggregate(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	add := map[RollupKey]RollupAdd{
		{ProjectID: p.ID, Service: "api", Severity: 9, Hour: "2026-01-01T10"}:  {Logs: 5, Events: 0},
		{ProjectID: p.ID, Service: "api", Severity: 17, Hour: "2026-01-01T11"}: {Logs: 2, Events: 2},
		{ProjectID: p.ID, Service: "web", Severity: 9, Hour: "2026-01-02T10"}:  {Logs: 3, Events: 0},
	}
	if err := db.AddRollups(ctx, add); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	bySvc, err := db.RollupAggregate(ctx, p.ID, since, time.Time{}, "service")
	if err != nil || bySvc["api"].Logs != 7 || bySvc["api"].Events != 2 || bySvc["web"].Logs != 3 {
		t.Fatalf("service: %+v err %v", bySvc, err)
	}
	bySev, _ := db.RollupAggregate(ctx, p.ID, since, time.Time{}, "severity")
	if bySev["17"].Logs != 2 || bySev["9"].Logs != 8 {
		t.Fatalf("severity: %+v", bySev)
	}
	byDay, _ := db.RollupAggregate(ctx, p.ID, since, time.Time{}, "day")
	if byDay["2026-01-01"].Logs != 7 || byDay["2026-01-02"].Logs != 3 {
		t.Fatalf("day: %+v", byDay)
	}
	byHour, _ := db.RollupAggregate(ctx, p.ID, since,
		time.Date(2026, 1, 1, 23, 0, 0, 0, time.UTC), "hour")
	if len(byHour) != 2 || byHour["2026-01-01T10"].Logs != 5 {
		t.Fatalf("hour with until: %+v", byHour)
	}
	if _, err := db.RollupAggregate(ctx, p.ID, since, time.Time{}, "bogus"); err == nil {
		t.Error("unknown groupBy must error")
	}
}

func TestEngineTotals(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	for i, m := range []SegmentMeta{
		{ProjectID: p.ID, Path: "t1.seg", MinTs: 1, MaxTs: 2, MinLogID: 1, MaxLogID: 5, Count: 5, RawRows: 1, SizeBytes: 50, Services: []string{"a"}},
		{ProjectID: p.ID, Path: "t2.seg", MinTs: 3, MaxTs: 4, MinLogID: 6, MaxLogID: 8, Count: 3, RawRows: 0, SizeBytes: 30, Services: []string{"a"}},
	} {
		if _, err := db.InsertSegment(ctx, m); err != nil {
			t.Fatalf("seg %d: %v", i, err)
		}
	}
	segs, rows, raw, size, err := db.EngineTotals(ctx, p.ID)
	if err != nil || segs != 2 || rows != 8 || raw != 1 || size != 80 {
		t.Fatalf("totals: %d %d %d %d err %v", segs, rows, raw, size, err)
	}
	if segs, _, _, _, _ := db.EngineTotals(ctx, 0); segs != 2 {
		t.Errorf("projectID 0 = all")
	}
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./internal/store/sqlite/ -run 'ReplaceSegments|RollupAggregate|EngineTotals' -v` → FAIL (undefined).

- [ ] **Step 4: Implement** (append to `engine_meta.go`; also add `RawRows`/`SizeBytes` to `SegmentMeta`, thread them through `InsertSegment`'s INSERT and `Segments`' SELECT/scan)

```go
// RollupAgg is one aggregation bucket's totals (sqlite-side shape;
// enginestore adapts it to store.AggregateRow).
type RollupAgg struct {
	Logs   int64
	Events int64
}

// ReplaceSegments atomically deletes every manifest row in oldIDs and
// inserts m, in one transaction — the crash-atomic primitive behind
// prune rewrites and compaction. Missing oldIDs are tolerated
// (idempotent retry after a pre-commit crash). Returns the new row id.
func (db *DB) ReplaceSegments(ctx context.Context, oldIDs []int64, m SegmentMeta) (int64, error) {
	svc, err := json.Marshal(m.Services)
	if err != nil {
		return 0, fmt.Errorf("sqlite: marshal services: %w", err)
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: replace segments begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, id := range oldIDs {
		if _, err := tx.ExecContext(ctx, `DELETE FROM segment_manifest WHERE id = ?`, id); err != nil {
			return 0, fmt.Errorf("sqlite: replace segments delete %d: %w", id, err)
		}
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO segment_manifest (project_id, path, min_ts, max_ts, min_log_id, max_log_id, count, events, services, raw_rows, size_bytes, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ProjectID, m.Path, m.MinTs, m.MaxTs, m.MinLogID, m.MaxLogID, m.Count, m.Events,
		string(svc), m.RawRows, m.SizeBytes, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("sqlite: replace segments insert: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlite: replace segments id: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: replace segments commit: %w", err)
	}
	return id, nil
}

// RollupAggregate groups flushed rollups for a project by service,
// severity (decimal string), hour ("2006-01-02T15"), or day
// ("2006-01-02"), between since (truncated down to the hour — rollup
// granularity) and until (zero = unbounded, else truncated to the hour,
// inclusive).
func (db *DB) RollupAggregate(ctx context.Context, projectID int64, since, until time.Time, groupBy string) (map[string]RollupAgg, error) {
	var keyExpr string
	switch groupBy {
	case "service":
		keyExpr = "service"
	case "severity":
		keyExpr = "CAST(severity AS TEXT)"
	case "hour":
		keyExpr = "hour"
	case "day":
		keyExpr = "substr(hour, 1, 10)"
	default:
		return nil, fmt.Errorf("sqlite: unknown aggregate groupBy %q", groupBy)
	}
	q := `SELECT ` + keyExpr + ` AS k, SUM(logs), SUM(events) FROM log_rollups WHERE project_id = ? AND hour >= ?`
	args := []any{projectID, since.UTC().Truncate(time.Hour).Format("2006-01-02T15")}
	if !until.IsZero() {
		q += ` AND hour <= ?`
		args = append(args, until.UTC().Truncate(time.Hour).Format("2006-01-02T15"))
	}
	q += ` GROUP BY k`
	rows, err := db.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: rollup aggregate: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]RollupAgg{}
	for rows.Next() {
		var k string
		var a RollupAgg
		if err := rows.Scan(&k, &a.Logs, &a.Events); err != nil {
			return nil, fmt.Errorf("sqlite: scan aggregate: %w", err)
		}
		out[k] = a
	}
	return out, rows.Err()
}

// EngineTotals sums manifest-level engine metrics for a project
// (0 = all projects): segment count, stored rows, raw-fallback rows,
// and on-disk segment bytes.
func (db *DB) EngineTotals(ctx context.Context, projectID int64) (segments, rows, rawRows, sizeBytes int64, err error) {
	q := `SELECT COUNT(*), COALESCE(SUM(count),0), COALESCE(SUM(raw_rows),0), COALESCE(SUM(size_bytes),0) FROM segment_manifest`
	var args []any
	if projectID != 0 {
		q += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	err = db.sql.QueryRowContext(ctx, q, args...).Scan(&segments, &rows, &rawRows, &sizeBytes)
	if err != nil {
		err = fmt.Errorf("sqlite: engine totals: %w", err)
	}
	return segments, rows, rawRows, sizeBytes, err
}
```

Also: `SwapSegment` body becomes `return db.ReplaceSegments(ctx, []int64{oldID}, m)` (delete its old transaction body; keep its doc comment, adjusted).

- [ ] **Step 5: Run to verify pass** — `go test ./internal/store/sqlite/ -v` → PASS all (including prune-related callers of SwapSegment via enginestore suite: run `go test ./internal/store/... -race`).
- [ ] **Step 6: gofmt, vet, full suite, commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/store/sqlite/
git commit -m "feat(sqlite): crash-atomic ReplaceSegments, rollup aggregation, engine totals"
```

---

### Task 2: Read-path hygiene — non-creating project lookup + Reconstruct error transparency

**Files:**
- Modify: `internal/template/template.go` (Reconstruct signature), `internal/template/template_test.go` (update callers; add load-error test)
- Modify: `internal/store/enginestore/read.go` (readProj + rowToLog), `internal/store/enginestore/write.go`/`enginestore.go` if they call Reconstruct (they don't) — update ALL Reconstruct call sites repo-wide (`grep -rn "\.Reconstruct(" --include="*.go"`).
- Test: `internal/store/enginestore/read_test.go` (append)

**Interfaces:**
- Produces:
  - `func (e *Extractor) Reconstruct(projectID, id int64, vars []string) (string, bool, error)` — `("", false, nil)` = template genuinely absent; `("", false, err)` = the lazy LoadTemplates failed (transient store error, NOT "missing"). All existing callers updated.
  - enginestore: `func (s *Store) readProj(projectID int64) *projState` — returns the existing projState or nil; NEVER creates one (no WAL file side effects). Every read path (`collectRows`, `collectRowsAllProjects`, `logByID`, `Stats`, `ServiceCounts`) uses it; `proj()` (creating) remains for WriteBatch/flush/Prune only. A nil readProj means "no memtable contribution" — segments are still consulted via the manifest.

- [ ] **Step 1: Failing tests**

Append to `internal/template/template_test.go`:

```go
func TestReconstructSurfacesLoadError(t *testing.T) {
	fs := newFakeStore()
	e1 := NewExtractor(fs, 0)
	id, vars, ok, _ := e1.Extract(ctx, 1, "alpha beta gamma")
	if !ok {
		t.Fatal("should template")
	}
	// Fresh extractor whose store now fails loads: error, not "missing".
	fs.failLoad = errors.New("disk exploded")
	e2 := NewExtractor(fs, 0)
	if _, ok, err := e2.Reconstruct(1, id, vars); ok || err == nil {
		t.Errorf("want load error surfaced, got ok=%v err=%v", ok, err)
	}
	fs.failLoad = nil
	if got, ok, err := e2.Reconstruct(1, id, vars); !ok || err != nil || got != "alpha beta gamma" {
		t.Errorf("after recovery: %q ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := e2.Reconstruct(1, 9999, nil); ok || err != nil {
		t.Errorf("genuinely missing: want (false, nil), got ok=%v err=%v", ok, err)
	}
}
```

(Add `failLoad error` to the test fake's fields and return it from `LoadTemplates` when set.)

Append to `internal/store/enginestore/read_test.go`:

```go
func TestReadsCreateNoEngineState(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{})
	// Reads against never-written project ids must not mint WAL files.
	_, _ = s.SearchLogs(ctx, store.LogFilter{ProjectID: 424242})
	_, _ = s.Stats(ctx, store.StatsFilter{ProjectID: 424242})
	_, _ = s.ServiceCounts(ctx, 424242, time.Time{})
	if _, err := os.Stat(filepath.Join(dir, "engine", "wal", "424242.wal")); !os.IsNotExist(err) {
		t.Fatalf("read created engine state: stat err = %v", err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "engine", "wal"))
	if len(entries) != 0 {
		t.Fatalf("wal dir not empty after pure reads: %v", entries)
	}
}
```

- [ ] **Step 2: Verify failure** — template test FAILS (signature), enginestore test FAILS (wal file created).
- [ ] **Step 3: Implement.** Template: change `Reconstruct` to return `(string, bool, error)` — the lazy `load` error becomes the third return instead of `("", false)`; genuinely-missing id stays `("", false, nil)`; success `(body, true, nil)`. Update every caller: `rowToLog` distinguishes — load error → wrap and return it; missing → keep today's `"template %d missing for log %d"` error. enginestore: add

```go
// readProj returns the project's engine state if it exists, or nil.
// Reads must never create WAL files or projStates for projects they
// merely query — segments are still served via the manifest.
func (s *Store) readProj(projectID int64) *projState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.projects[projectID]
}
```

and switch every read path to it (nil → skip memtable snapshot / skip ps.mu and read segments without it — segments-only reads for unknown projects need no coherence lock because no flush can be running for a project with no state). `findLog`'s all-projects sweep keeps using known projects + manifest fallback (which uses `logByID` — make `logByID` readProj-based too).
- [ ] **Step 4: Verify pass** — `go test ./internal/template/ ./internal/store/enginestore/ -race -v` → PASS (all, including the storetest contract).
- [ ] **Step 5: gofmt, vet, full suite, commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/template/ internal/store/enginestore/
git commit -m "feat(engine): non-creating read lookups; Reconstruct surfaces load errors"
```

---

### Task 3: `store.Reader.Aggregate` + enginestore implementation

**Files:**
- Modify: `internal/store/store.go` (types + interface), `internal/store/enginestore/read.go` (implementation)
- Modify: any test fake implementing `store.Reader` (grep `SearchLogs(ctx` in `_test.go` files — mcp/api/web fakes need a stub)
- Test: `internal/store/enginestore/read_test.go` (append)

**Interfaces:**
- Produces:
  - In `store`: `type AggregateFilter struct { ProjectID int64; Since, Until time.Time; GroupBy string }` (GroupBy ∈ `"service"|"severity"|"hour"|"day"`); `type AggregateRow struct { Key string; Logs, Events int64 }`; `Reader` gains `Aggregate(ctx context.Context, f AggregateFilter) ([]AggregateRow, error)`.
  - enginestore `Aggregate`: rollups via `RollupAggregate` + memtable rows (ts ≥ since, ≤ until when set) bucketed in Go with IDENTICAL keys (severity as decimal string; hour/day formatted from `time.UnixMicro(r.TsMicros).UTC()`). Ordering: `service` → Logs DESC, ties Key ASC; `severity` → numeric Key DESC (most severe first); `hour`/`day` → Key ASC. Unknown GroupBy → error (propagated from sqlite or checked first). ProjectID 0: aggregate across all projects (rollup query already supports it via projectID 0? — NO: `RollupAggregate` filters `project_id = ?` unconditionally; treat ProjectID 0 by summing per-known-project memtables AND change the sqlite query? Simpler ruling, do this: in Task 1 the SQL is per-project only; here, for ProjectID 0, error with "aggregate requires a project" — MCP/API callers always resolve a concrete project (project-scoped keys) or pass one (admin). Document in the interface comment.)

- [ ] **Step 1: Failing test** (append to `read_test.go`)

```go
func TestAggregate(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	if _, err := s.WriteBatch(ctx, []store.Entry{
		logEntry(p.ID, "a one", "api", at),
		logEntry(p.ID, "a two", "api", at.Add(30*time.Minute)),
		logEntry(p.ID, "b one", "web", at.Add(2*time.Hour)),
		{Log: core.Log{ProjectID: p.ID, Time: at.Add(3 * time.Hour), Severity: core.SeverityError,
			Body: "boom", Service: "api"}, IsEvent: true, Fingerprint: "f", Title: "boom"},
	}); err != nil {
		t.Fatal(err)
	}
	// Split flushed/unflushed to prove the merge.
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "late", "api", at.Add(4*time.Hour))}); err != nil {
		t.Fatal(err)
	}

	rows, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, Since: at.Add(-time.Hour), GroupBy: "service"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Key != "api" || rows[0].Logs != 4 || rows[0].Events != 1 || rows[1].Key != "web" {
		t.Fatalf("service: %+v", rows)
	}
	byHour, _ := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, Since: at.Add(-time.Hour), GroupBy: "hour"})
	if len(byHour) != 4 || byHour[0].Key != "2026-01-01T10" || byHour[0].Logs != 2 {
		t.Fatalf("hour: %+v", byHour)
	}
	bySev, _ := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, Since: at.Add(-time.Hour), GroupBy: "severity"})
	if bySev[0].Key != "17" || bySev[0].Logs != 1 {
		t.Fatalf("severity ordering: %+v", bySev)
	}
	if _, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, GroupBy: "nope"}); err == nil {
		t.Error("unknown groupBy must error")
	}
	if _, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: 0, GroupBy: "service"}); err == nil {
		t.Error("ProjectID 0 must error (documented)")
	}
}
```

- [ ] **Step 2: Verify failure** — undefined `Aggregate`.
- [ ] **Step 3: Implement.** In `store.go` add the types + interface method (doc comment: substring of Reader's contract, note ProjectID must be non-zero). In enginestore:

```go
// Aggregate groups a project's log volume by service, severity, hour,
// or day — flushed rollups plus the unflushed memtable, so results are
// exact and immediate. ProjectID must be non-zero.
func (s *Store) Aggregate(ctx context.Context, f store.AggregateFilter) ([]store.AggregateRow, error) {
	if f.ProjectID == 0 {
		return nil, fmt.Errorf("enginestore: aggregate requires a project")
	}
	agg, err := s.RollupAggregate(ctx, f.ProjectID, f.Since, f.Until, f.GroupBy)
	if err != nil {
		return nil, err
	}
	buckets := map[string]sqlitestore.RollupAgg{}
	for k, v := range agg {
		buckets[k] = v
	}
	if ps := s.readProj(f.ProjectID); ps != nil {
		sinceM, untilM := boundsMicros(f.Since, f.Until)
		for _, r := range ps.mem.Snapshot() {
			if r.TsMicros < sinceM || r.TsMicros > untilM {
				continue
			}
			k, err := memKey(f.GroupBy, r)
			if err != nil {
				return nil, err
			}
			b := buckets[k]
			b.Logs++
			if r.IsEvent {
				b.Events++
			}
			buckets[k] = b
		}
	}
	return orderAggregate(f.GroupBy, buckets), nil
}
```

with helpers `memKey(groupBy, row)` (service / `strconv.Itoa(r.Severity)` / hour / day from `time.UnixMicro(r.TsMicros).UTC()`) and `orderAggregate` implementing the stated orderings (severity compares `strconv.Atoi` of keys, DESC). Reuse/adjust `boundsMicros` from Task 2's refactored code. Add the one-line stub to every test fake that implements `store.Reader` (`return nil, nil` with a doc comment "unused in these tests").
- [ ] **Step 4: Verify pass** — `go test ./internal/store/enginestore/ -race && go test ./...` (fakes compile).
- [ ] **Step 5: Commit**

```bash
git add internal/store/ internal/mcp/ internal/api/ internal/web/
git commit -m "feat(store): Aggregate — rollup+memtable group-by for service/severity/hour/day"
```

---

### Task 4: Segment compaction

**Files:**
- Create: `internal/store/enginestore/compact.go`
- Modify: `internal/store/enginestore/enginestore.go` (Options.CompactEvery, loop wiring)
- Test: `internal/store/enginestore/compact_test.go`

**Interfaces:**
- Consumes: `ReplaceSegments` (Task 1), `segment.Read`/`Write`, `ps.mu` discipline, `segPath`.
- Produces:
  - `Options` gains `CompactEvery time.Duration` (0 → 1 h; negative → disabled, for tests).
  - `func (s *Store) CompactAll(ctx context.Context) error` — for every project WITH manifest segments: group flushed segments by bucket key — `"2006-01-02T15"` (hour) for segments whose MinTs falls on the CURRENT UTC day, `"2006-01-02"` (day) otherwise — excluding the CURRENT hour's bucket (still filling). Any bucket with ≥ 2 segments merges: read all rows, `segment.Write` to `segments/<pid>/c-<bucket>-<newMinLogID>.seg`, `ReplaceSegments(oldIDs, newMeta)` (RawRows = sum, SizeBytes = stat of new file), remove old files post-commit. The whole per-bucket swap runs under the project's `ps.mu`; reading the old segments happens OUTSIDE the lock (they are immutable), only the manifest swap + file removal inside.
  - Compaction loop: same goroutine pattern as flushLoop (`stop` channel, wg), ticker at CompactEvery, logs errors via slog. Prune's `"-pruned.seg"` outputs participate in later compaction naturally (they are just segments).
  - Bucketing uses each segment's **MinTs**; merged output preserves all rows (counts identical before/after — assert in tests).

- [ ] **Step 1: Failing tests**

```go
package enginestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/store"
)

// yesterdayBatches writes 3 separate flushed segments on a past day.
func yesterdayBatches(t *testing.T, s *Store, pid int64) {
	t.Helper()
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour).Add(6 * time.Hour)
	for i := 0; i < 3; i++ {
		if _, err := s.WriteBatch(context.Background(), []store.Entry{
			logEntry(pid, "past row", "api", base.Add(time.Duration(i)*time.Minute)),
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.FlushAll(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactMergesPastDayBuckets(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	yesterdayBatches(t, s, p.ID)
	segsBefore, _ := s.Segments(ctx, p.ID)
	if len(segsBefore) != 3 {
		t.Fatalf("precondition: %d segments", len(segsBefore))
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	segs, _ := s.Segments(ctx, p.ID)
	if len(segs) != 1 || segs[0].Count != 3 {
		t.Fatalf("after compact: %+v", segs)
	}
	// Old files removed, new file present.
	for _, m := range segsBefore {
		if _, err := os.Stat(s.segPath(m.Path)); !os.IsNotExist(err) {
			t.Errorf("old segment file survives: %s", m.Path)
		}
	}
	if _, err := os.Stat(s.segPath(segs[0].Path)); err != nil {
		t.Errorf("merged file missing: %v", err)
	}
	// All rows still readable.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
	if err != nil || len(logs) != 3 {
		t.Fatalf("post-compact read: %d err %v", len(logs), err)
	}
	// Idempotent: nothing left to merge.
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if segs2, _ := s.Segments(ctx, p.ID); len(segs2) != 1 {
		t.Errorf("second compact changed things: %+v", segs2)
	}
}

func TestCompactSkipsCurrentHour(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	now := time.Now().UTC()
	for i := 0; i < 2; i++ {
		if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "now row", "api", now)}); err != nil {
			t.Fatal(err)
		}
		if err := s.FlushAll(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if segs, _ := s.Segments(ctx, p.ID); len(segs) != 2 {
		t.Errorf("current-hour segments must not compact: %+v", segs)
	}
}

func TestCompactConcurrentWithReadsNoDuplicates(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	yesterdayBatches(t, s, p.ID)
	stopc := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopc:
				return
			default:
				_ = s.CompactAll(ctx)
			}
		}
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
		if err != nil {
			t.Fatalf("read during compact: %v", err)
		}
		seen := map[int64]bool{}
		for _, l := range logs {
			if seen[l.ID] {
				t.Fatalf("duplicate log %d during compaction", l.ID)
			}
			seen[l.ID] = true
		}
		if len(logs) != 3 {
			t.Fatalf("row count changed during compaction: %d", len(logs))
		}
	}
	close(stopc)
}
```

- [ ] **Step 2: Verify failure** — `CompactAll` undefined.
- [ ] **Step 3: Implement `compact.go`** — structure:

```go
// CompactAll merges small flushed segments to bound read amplification:
// per project, segments are bucketed by MinTs — hourly buckets for the
// current UTC day, daily buckets for prior days — and any bucket with
// two or more segments (excluding the still-filling current hour) is
// merged into one. The manifest swap is crash-atomic (ReplaceSegments);
// old files are removed only after commit. Reads stay coherent: the
// swap and removals run under the project's ps.mu, mirroring flush and
// prune. Reading the old (immutable) segments happens outside the lock.
func (s *Store) CompactAll(ctx context.Context) error
```

Implementation outline the implementer fills with the established helpers: list projects via `s.Segments(ctx, 0)` grouped by ProjectID; for each project compute buckets (`bucketKey(minTs, now)`); for each mergeable bucket: read rows of each member via `segment.Read` (outside lock), concatenate, `segment.Write` to the new path, stat for SizeBytes, sum RawRows from member metas; then `ps := s.proj(projectID)` (creating is fine here — compaction is a write-side actor), `ps.mu.Lock()`, re-verify the member manifest rows still exist (`s.Segments(ctx, pid)` — a concurrent prune may have removed one; if changed, skip bucket and clean up the orphan new file), `ReplaceSegments`, remove old files, unlock. Wire `CompactEvery` default (0 → `time.Hour`, < 0 → disabled) into a `compactLoop` goroutine beside `flushLoop`; `Close` stops it. Keep every function under gocyclo 15 (bucket computation, merge-one-bucket, and the loop as separate functions).
- [ ] **Step 4: Verify pass** — `go test ./internal/store/enginestore/ -race -v` (all, incl. storetest contract).
- [ ] **Step 5: gofmt, vet, full suite, commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/store/enginestore/
git commit -m "feat(enginestore): scheduled segment compaction with crash-atomic swaps"
```

---

### Task 5: Surface it — `aggregate_logs` MCP tool, `GET /api/v1/aggregate`, engine metrics in stats

**Files:**
- Modify: `internal/mcp/tools.go` (register + implement `aggregate_logs`; enrich `get_stats`), `internal/mcp/render.go` (renderers)
- Modify: `internal/api/api.go` + the stats/logs handler file it references (add `GET /api/v1/aggregate`; extend stats response)
- Modify: `internal/store/enginestore/stats.go` (new file): `EngineStats`
- Test: `internal/mcp/tools_test.go`, `internal/api/api_test.go` (append, following each file's existing table patterns)

**Interfaces:**
- Produces:
  - enginestore: `type EngineStatsRow struct { Segments, Rows, RawRows, SizeBytes, MemRows int64 }`; `func (s *Store) EngineStats(ctx context.Context, projectID int64) (EngineStatsRow, error)` — `EngineTotals` + memtable Len (readProj; 0 when absent). Templating rate and B/rec are DERIVED at the presentation layer: rate = 1 − RawRows/Rows (flushed rows only; guard divide-by-zero), bytesPerRecord = SizeBytes/Rows.
  - MCP `aggregate_logs`: input `{group_by string (required: service|severity|hour|day), since string (parseSince format, default "24h"), until string (optional), project int64 (admin only)}`; project resolution identical to `get_stats` (`callerScope`); renders a compact table (`renderAggregate` in render.go: KEY, LOGS, EVENTS columns; severity keys rendered via `core.Severity` name). Tool description: "Aggregate log volume grouped by service, severity, hour, or day — counts of logs and error events per bucket."
  - MCP `get_stats` gains an engine block appended to `renderStats` output when `EngineStats` succeeds: segments, stored rows, raw fallback %, avg bytes/record, unflushed rows. Reader interface note: `EngineStats` is enginestore-specific — the MCP server holds `store.Reader`; do a checked type assertion `if es, ok := s.reader.(interface{ EngineStats(context.Context, int64) (enginestore.EngineStatsRow, error) }); ok { ... }`... NO: mcp must not import enginestore (layering). Instead define the metrics struct in `store`: `type EngineStats struct { Segments, Rows, RawRows, SizeBytes, MemRows int64 }` and OPTIONAL interface `type EngineMetrics interface { EngineStats(ctx context.Context, projectID int64) (EngineStats, error) }` in the store package; enginestore implements it; MCP/API type-assert `store.EngineMetrics` and degrade gracefully (omit the block) when absent. This keeps fakes untouched.
  - API: `GET /api/v1/aggregate?group_by=service&since=...&until=...` (project from key scope, same as `/api/v1/logs`'s pattern) → JSON `[{"key":..., "logs":..., "events":...}]`; `GET /api/v1/stats` response gains `"engine": {"segments":..., "rows":..., "raw_rows":..., "size_bytes":..., "mem_rows":...}` when the store implements `store.EngineMetrics`.

- [ ] **Step 1: Failing tests** — follow the existing table-driven patterns: in `tools_test.go`, add `TestAggregateLogsTool` (seed via the test harness's store, call the tool, assert rendered keys/counts and severity NAME rendering; assert non-admin key scoping) and extend the stats test to assert the engine block appears. In `api_test.go`, add `TestAggregateEndpoint` (200 + JSON shape + group_by validation 400) and extend the stats test for the `engine` field. Write real assertions against the fixtures those files already build — read the neighboring tests first and mirror their setup exactly.
- [ ] **Step 2: Verify failures.**
- [ ] **Step 3: Implement** per the Produces block: `store.EngineStats` + `store.EngineMetrics` (doc comments), `enginestore.EngineStats` (stats.go), MCP tool + renderer + get_stats enrichment, API route + handler + stats field. Severity name rendering: `core.Severity(atoi(key)).String()` with the numeric fallback if parse fails.
- [ ] **Step 4: Verify pass** — `go test ./internal/mcp/ ./internal/api/ ./internal/store/... -race && go test ./...`.
- [ ] **Step 5: gofmt, vet, commit**

```bash
gofmt -l . && go vet ./... && go test ./...
git add internal/store/ internal/mcp/ internal/api/ internal/store/enginestore/
git commit -m "feat(mcp,api): aggregate_logs tool and endpoint; engine metrics in stats"
```

---

## After this plan

The bench-suite plan (next): committed corpus generator, `test/bench` CI latency/size gates asserting spec §7 targets, and the `make bench-vs-o2` docker-compose head-to-head harness whose output table backs the public "beats OpenObserve" claim and goes in the v0.2.0 release notes.

## Self-review notes

- **Spec coverage:** §5 aggregations (Task 3 + 5 close the "aggregation is o2-only" trial shortcoming with the exact `aggregate_logs` tool named in §6); §6 get_stats additions (templating rate, raw %, B/rec, segment counts — Task 5); compaction is the Plan-B-ledger top deferral (Task 4); hygiene items (Task 2). NOT in this plan (still pending after it): severity rules (§1 item 4 — own feature plan), bench suite (§7), bad-segment quarantine (ledgered; needs operator-surface design).
- **Type consistency:** `RollupAgg` (sqlite) vs `store.AggregateRow` (interface) adaptation is explicit in Task 3; `ReplaceSegments` consumed by Task 4 and by `SwapSegment` wrapper; `readProj` produced in Task 2, consumed in Tasks 3–4; `store.EngineStats`/`EngineMetrics` defined once in Task 5.
- **Known judgment points flagged:** Task 3's ProjectID-0 ruling (error, documented); Task 4's implementation outline names its helpers but leaves function-body assembly to the implementer with the locking/crash rules stated; Task 5's optional-interface pattern avoids fake churn and an mcp→enginestore import.
- **Placeholder scan:** Task 4 Step 3 and Task 5 Steps 1/3 are outline-plus-contract rather than verbatim code — deliberate for these integration tasks (the patterns exist in neighboring files the tasks point to); all constraints, signatures, orderings, and edge cases are stated exactly.
