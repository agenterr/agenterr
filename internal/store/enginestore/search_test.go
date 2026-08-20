package enginestore

import (
	"context"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// searchFixture writes a mixed corpus (templated repeats, a raw
// oddball, two services, two severities), flushes so rows live in a
// real segment, then writes two more unflushed rows so the memtable
// path is exercised too.
func searchFixture(t *testing.T) (*Store, int64) {
	t.Helper()
	s := openStore(t, t.TempDir(), Options{})
	ctx := context.Background()
	p, err := s.CreateProject(ctx, "search", 30)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var entries []store.Entry
	add := func(off int, sev core.Severity, svc, body string) {
		entries = append(entries, store.Entry{Log: core.Log{
			ProjectID: p.ID, Time: base.Add(time.Duration(off) * time.Second),
			Severity: sev, Service: svc, Body: body,
		}})
	}
	for i := 0; i < 50; i++ {
		add(i, core.SeverityError, "api", "record not found for user 42")
	}
	add(60, core.SeverityInfo, "api", "user 99 logged in ok")
	add(61, core.SeverityError, "web", "record not found for user 7")
	add(62, core.SeverityError, "api", "!!raw@@line##with War saw inside") // ANSI-free but non-templatable? if it templates, fine — the assertions below don't depend on it
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	// Unflushed memtable rows.
	var late []store.Entry
	late = append(late, store.Entry{Log: core.Log{ProjectID: p.ID, Time: base.Add(2 * time.Minute), Severity: core.SeverityError, Service: "api", Body: "record not found for user 555"}})
	late = append(late, store.Entry{Log: core.Log{ProjectID: p.ID, Time: base.Add(3 * time.Minute), Severity: core.SeverityWarn, Service: "web", Body: "cache warm done"}})
	if _, err := s.WriteBatch(ctx, late); err != nil {
		t.Fatal(err)
	}
	return s, p.ID
}

func TestSearchSubstringAcrossSegmentAndMemtable(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Query: "record not found"})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 50 { // default limit caps 52 matches at 50
		t.Fatalf("got %d, want 50", len(logs))
	}
	// Most recent first: the memtable row leads.
	if logs[0].Body != "record not found for user 555" {
		t.Errorf("first = %q", logs[0].Body)
	}
	for i := 1; i < len(logs); i++ {
		a, b := logs[i-1], logs[i]
		if a.Time.Before(b.Time) || (a.Time.Equal(b.Time) && a.ID < b.ID) {
			t.Fatalf("order violated at %d: %v/%d then %v/%d", i, a.Time, a.ID, b.Time, b.ID)
		}
	}
}

func TestSearchQueryStraddlesVarBoundary(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	// "user 42" spans static text and a variable — the always-match
	// classification must NOT claim it, and reconstruction must find it.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Query: "for user 42", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 50 {
		t.Fatalf("got %d, want 50", len(logs))
	}
	for _, l := range logs {
		if l.Body != "record not found for user 42" {
			t.Errorf("unexpected body %q", l.Body)
		}
	}
}

func TestSearchFiltersComposeWithQuery(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	logs, err := s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "record not found", Service: "web", Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].Body != "record not found for user 7" {
		t.Fatalf("service+query: got %+v", logs)
	}
	logs, err = s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "logged in", MinSeverity: core.SeverityError, Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("severity should exclude the info row, got %d", len(logs))
	}
	logs, err = s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "zzz-not-present", Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("no-hit query returned %d", len(logs))
	}
}

func TestSearchTimeWindow(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	logs, err := s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "record not found",
		Since: base.Add(30 * time.Second), Until: base.Add(70 * time.Second),
		Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 21 { // offsets 30..49 templated + offset 61 web row
		t.Fatalf("got %d, want 21", len(logs))
	}
}
