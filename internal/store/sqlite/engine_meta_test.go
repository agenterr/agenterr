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

// TestSwapSegmentAtomicReplace guards the crash-atomicity SwapSegment
// exists for: the old manifest row must be gone and the new one present
// after a successful swap (both effects of the same commit), and
// swapping an already-missing oldID must not error — it's the retry path
// after a crash that landed post-commit — instead it just inserts the new
// row (documented, chosen over erroring so a retried swap stays
// idempotent).
func TestSwapSegmentAtomicReplace(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)

	old := SegmentMeta{ProjectID: p.ID, Path: "segments/1/000001.seg",
		MinTs: 100, MaxTs: 200, MinLogID: 1, MaxLogID: 50, Count: 50, Services: []string{"api"}}
	oldID, err := db.InsertSegment(ctx, old)
	if err != nil {
		t.Fatal(err)
	}

	replacement := SegmentMeta{ProjectID: p.ID, Path: "segments/1/000001-pruned.seg",
		MinTs: 150, MaxTs: 200, MinLogID: 30, MaxLogID: 50, Count: 20, Services: []string{"api"}}
	// Note: SQLite may legitimately reuse oldID's rowid for the
	// replacement here — segment_manifest's id is a plain rowid alias
	// (no AUTOINCREMENT), so once the table's only row is deleted within
	// the same transaction, the next insert's default rowid selection can
	// land back on 1. That is not a correctness problem for the swap: the
	// assertion that matters is the row's content (Path/Count below), not
	// whether the id number happens to be reused.
	newID, err := db.SwapSegment(ctx, oldID, replacement)
	if err != nil {
		t.Fatal(err)
	}

	got, err := db.Segments(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("segments after swap = %+v, want exactly the replacement", got)
	}
	if got[0].ID != newID || got[0].Path != replacement.Path || got[0].Count != 20 {
		t.Errorf("got %+v", got[0])
	}

	// Swapping a missing oldID (simulating a retry after a crash that
	// landed post-commit) does not error — it still inserts the new row.
	another := SegmentMeta{ProjectID: p.ID, Path: "segments/1/000002.seg",
		MinTs: 300, MaxTs: 400, MinLogID: 51, MaxLogID: 60, Count: 10, Services: []string{"api"}}
	if _, err := db.SwapSegment(ctx, 999999, another); err != nil {
		t.Fatalf("SwapSegment with missing oldID errored: %v", err)
	}
	got2, err := db.Segments(ctx, p.ID)
	if err != nil || len(got2) != 2 {
		t.Fatalf("segments after missing-oldID swap = %+v, err %v", got2, err)
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
			Log:     core.Log{ID: logID, ProjectID: p.ID, Time: ts, Severity: core.SeverityError, Body: "boom", Environment: "production"},
			IsEvent: true, Fingerprint: fp, Title: "boom",
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
	out3, _ := db.UpsertIssues(ctx, []store.Entry{ev(3, "fp1", at.Add(2*time.Minute))})
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

// TestMaxIssueEventLogID guards enginestore's recover, which uses this to
// seed nextLogID alongside the manifest and WALs: after a full prune, a
// project's manifest and WAL can both go empty while issue_events still
// references older LogIDs (event refs deliberately outlive bodies), and
// nextLogID must not restart low enough to reissue one of those ids.
func TestMaxIssueEventLogID(t *testing.T) {
	ctx := context.Background()
	db := openTest(t)
	p := mustProj(ctx, t, db)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	gotMax, err := db.MaxIssueEventLogID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gotMax != 0 {
		t.Fatalf("empty issue_events: max = %d, want 0", gotMax)
	}

	ev := func(logID int64, fp string) store.Entry {
		return store.Entry{
			Log:     core.Log{ID: logID, ProjectID: p.ID, Time: at, Severity: core.SeverityError, Body: "boom"},
			IsEvent: true, Fingerprint: fp, Title: "boom",
		}
	}
	if _, err := db.UpsertIssues(ctx, []store.Entry{ev(7, "fp-a"), ev(3, "fp-b"), ev(42, "fp-c")}); err != nil {
		t.Fatal(err)
	}

	wantMax, err := db.MaxIssueEventLogID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if wantMax != 42 {
		t.Fatalf("max = %d, want 42", wantMax)
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
