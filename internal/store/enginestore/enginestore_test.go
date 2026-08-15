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
