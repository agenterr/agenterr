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
