package enginestore

import (
	"strings"
	"sync"
	"sync/atomic"
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

// TestLogContextTiesIncludeTarget guards against a non-total-order sort
// (TsMicros only) silently truncating the target out of `before` when
// several rows share its exact timestamp.
func TestLogContextTiesIncludeTarget(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	entries := []store.Entry{
		logEntry(p.ID, "tie a", "svc", at),
		logEntry(p.ID, "tie b", "svc", at),
		logEntry(p.ID, "tie c", "svc", at),
		logEntry(p.ID, "tie d", "svc", at),
	}
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}

	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: "tie c"})
	if err != nil || len(logs) != 1 {
		t.Fatalf("seed lookup: err=%v logs=%+v", err, logs)
	}
	target := logs[0]

	nbrs, err := s.LogContext(ctx, target.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, l := range nbrs {
		if l.ID == target.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("target id %d missing from context result %+v", target.ID, nbrs)
	}
}

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

// TestIssuesEnvironmentFilterAllProjects guards ProjectID == 0 ("all
// projects", per store.IssueFilter's documented convention) against the
// Environment filter silently scoping to a single project: before the
// fix, IssueIDsInEnvironment always constrained project_id, so an
// all-projects environment query returned nothing.
func TestIssuesEnvironmentFilterAllProjects(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p1, _ := s.CreateProject(ctx, "p1", 30)
	p2, _ := s.CreateProject(ctx, "p2", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	mk := func(pid int64, env, fp string) store.Entry {
		return store.Entry{
			Log: core.Log{ProjectID: pid, Time: at, Severity: core.SeverityError,
				Body: "boom " + fp, Service: "api", Environment: env},
			IsEvent: true, Fingerprint: fp, Title: "boom " + fp,
		}
	}
	if _, err := s.WriteBatch(ctx, []store.Entry{
		mk(p1.ID, "production", "fp1"),
		mk(p2.ID, "production", "fp2"),
		mk(p1.ID, "staging", "fp3"),
	}); err != nil {
		t.Fatal(err)
	}

	issues, err := s.Issues(ctx, store.IssueFilter{Environment: "production"})
	if err != nil {
		t.Fatal(err)
	}
	if len(issues) != 2 {
		t.Fatalf("issues = %+v, want 2 (one per project)", issues)
	}
	seenProjects := map[int64]bool{}
	for _, iss := range issues {
		seenProjects[iss.ProjectID] = true
	}
	if !seenProjects[p1.ID] || !seenProjects[p2.ID] {
		t.Fatalf("expected issues from both projects, got: %+v", issues)
	}
}

// TestSearchLogsNoDuplicatesDuringConcurrentPrune guards the Prune
// manifest-swap locking fix: a segment rewrite (insert "-pruned" replacement
// then delete the original) concurrent with a read must never let
// collectRows observe both the old segment and its replacement in the same
// Segments() snapshot — that would double-count the rows that survived the
// rewrite.
func TestSearchLogsNoDuplicatesDuringConcurrentPrune(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cutoff := base.Add(24 * time.Hour)

	stop := make(chan struct{})
	var writeErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			entries := []store.Entry{
				logEntry(p.ID, "old row", "svc", base.Add(time.Duration(i)*time.Microsecond)),     // before cutoff
				logEntry(p.ID, "fresh row", "svc", cutoff.Add(time.Duration(i)*time.Microsecond)), // after cutoff: straddles with the row above once flushed together
			}
			if _, err := s.WriteBatch(ctx, entries); err != nil {
				writeErr.Store(err)
				return
			}
			if err := s.FlushAll(); err != nil {
				writeErr.Store(err)
				return
			}
			if _, err := s.Prune(ctx, p.ID, cutoff); err != nil {
				writeErr.Store(err)
				return
			}
			i++
		}
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Limit: 100000})
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[int64]bool, len(logs))
		for _, l := range logs {
			if seen[l.ID] {
				t.Fatalf("duplicate log id %d in result set of %d logs", l.ID, len(logs))
			}
			seen[l.ID] = true
		}
	}
	close(stop)
	wg.Wait()
	if v := writeErr.Load(); v != nil {
		t.Fatalf("writer error: %v", v)
	}
}

// TestSearchLogsNoDuplicatesDuringConcurrentFlush guards the
// collectRows/logByID coherence fix: a flush concurrent with a read must
// never produce a result set where the same LogID appears twice (seen
// once pre-flush via the memtable, once post-flush via the new
// segment), nor drop rows.
func TestSearchLogsNoDuplicatesDuringConcurrentFlush(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	stop := make(chan struct{})
	var writeErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			entries := []store.Entry{
				logEntry(p.ID, "race row", "svc", at.Add(time.Duration(i)*time.Microsecond)),
				logEntry(p.ID, "race row", "svc", at.Add(time.Duration(i+1)*time.Microsecond)),
			}
			if _, err := s.WriteBatch(ctx, entries); err != nil {
				writeErr.Store(err)
				return
			}
			if err := s.FlushAll(); err != nil {
				writeErr.Store(err)
				return
			}
			i += 2
		}
	}()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Limit: 100000})
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[int64]bool, len(logs))
		for _, l := range logs {
			if seen[l.ID] {
				t.Fatalf("duplicate log id %d in result set of %d logs", l.ID, len(logs))
			}
			seen[l.ID] = true
		}
	}
	close(stop)
	wg.Wait()
	if v := writeErr.Load(); v != nil {
		t.Fatalf("writer error: %v", v)
	}
}
