package enginestore

import (
	"os"
	"path/filepath"
	"strconv"
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
	segs, _ := s.Segments(ctx, p.ID)
	if len(segs) != 1 || segs[0].Count != 1 {
		t.Fatalf("manifest after prune: %+v", segs)
	}
}

// TestPruneStraddlingSegmentRawRowsAndSizeBytesGroundTruth is I1's
// regression case: rewriteSegment's manifest row must report the true
// RawRows/SizeBytes of the rows it actually kept, not zero values — a
// straddling segment whose survivors include a raw-fallback row (body
// with a newline, per template.Extract) previously produced a rewritten
// manifest row with RawRows=0/SizeBytes=0 regardless of what was kept.
// Compacting that rewritten segment with another one must then still
// report the true merged RawRows, since buildMergedSegment used to sum
// member RawRows (poisoned by the zero) instead of recomputing it.
func TestPruneStraddlingSegmentRawRowsAndSizeBytesGroundTruth(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)

	day := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour).Add(6 * time.Hour)
	cutoff := day.Add(30 * time.Minute)
	dropped := day                          // before cutoff: pruned away
	rawKept := day.Add(45 * time.Minute)    // after cutoff: multiline -> raw fallback, survives
	normalKept := day.Add(50 * time.Minute) // after cutoff: templatable, survives

	if _, err := s.WriteBatch(ctx, []store.Entry{
		logEntry(p.ID, "dropped row", "api", dropped),
		logEntry(p.ID, "raw kept\nsecond line", "api", rawKept),
		logEntry(p.ID, "normal kept row", "api", normalKept),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil { // one segment straddling cutoff
		t.Fatal(err)
	}

	n, err := s.Prune(ctx, p.ID, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned %d rows, want 1", n)
	}

	segs, err := s.Segments(ctx, p.ID)
	if err != nil || len(segs) != 1 {
		t.Fatalf("manifest after prune: err=%v segs=%+v", err, segs)
	}
	rewritten := segs[0]
	if rewritten.Count != 2 {
		t.Fatalf("rewritten count = %d, want 2", rewritten.Count)
	}
	if rewritten.RawRows != 1 {
		t.Errorf("rewritten RawRows = %d, want 1 (ground truth from kept rows, not zeroed member metadata)", rewritten.RawRows)
	}
	if rewritten.SizeBytes <= 0 {
		t.Errorf("rewritten SizeBytes = %d, want > 0 (os.Stat on the written file)", rewritten.SizeBytes)
	}

	// Compact the rewritten segment together with a second, freshly
	// flushed segment in the same past-day bucket.
	other := day.Add(2 * time.Hour)
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "other segment row", "api", other)}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if segs, _ := s.Segments(ctx, p.ID); len(segs) != 2 {
		t.Fatalf("precondition before compact: %+v", segs)
	}

	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	merged, err := s.Segments(ctx, p.ID)
	if err != nil || len(merged) != 1 {
		t.Fatalf("manifest after compact: err=%v segs=%+v", err, merged)
	}
	if merged[0].Count != 3 {
		t.Fatalf("merged count = %d, want 3", merged[0].Count)
	}
	if merged[0].RawRows != 1 {
		t.Errorf("merged RawRows = %d, want 1 (recomputed over the concatenated rows, not summed from members)", merged[0].RawRows)
	}
	if merged[0].SizeBytes <= 0 {
		t.Errorf("merged SizeBytes = %d, want > 0", merged[0].SizeBytes)
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

// TestSearchLogsAllProjectsMergesEveryProject guards the ProjectID == 0
// ("all projects", the convention used by the web /search page, the
// admin-key /api/v1/logs endpoint, and the MCP search_logs tool) read
// path: before the fix, collectRows(0, ...) read every project's manifest
// segments but reconstructed each row via Reconstruct(projectID=0, ...),
// which always failed with "template missing" once any row had gone
// through a flush (and never saw unflushed rows at all, since project 0
// has no memtable of its own). This must work identically before AND
// after FlushAll: pre-flush it exercises the memtable-only path, post-
// flush it exercises the segment/Reconstruct path this bug specifically
// broke.
func TestSearchLogsAllProjectsMergesEveryProject(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p1, err := s.CreateProject(ctx, "p1", 30)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := s.CreateProject(ctx, "p2", 30)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if _, err := s.WriteBatch(ctx, []store.Entry{
		logEntry(p1.ID, "connection refused by peer one", "api", at),
		logEntry(p2.ID, "connection refused by peer two", "web", at.Add(time.Second)),
	}); err != nil {
		t.Fatal(err)
	}

	checkBoth := func(when string) {
		t.Helper()
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: 0, Query: "connection refused"})
		if err != nil {
			t.Fatalf("%s: SearchLogs err = %v", when, err)
		}
		if len(logs) != 2 {
			t.Fatalf("%s: got %d logs, want 2: %+v", when, len(logs), logs)
		}
		seenProjects := map[int64]bool{}
		for _, l := range logs {
			seenProjects[l.ProjectID] = true
		}
		if !seenProjects[p1.ID] || !seenProjects[p2.ID] {
			t.Fatalf("%s: expected logs from both projects, got: %+v", when, logs)
		}
	}

	checkBoth("before flush")

	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	checkBoth("after flush")
}

// TestReadsCreateNoEngineState guards readProj's non-creating contract:
// pure reads against a project id that was never written must not mint a
// WAL file (or any other engine-state side effect) for it.
// TestRetryReadSegmentRowsSkipsWhenReplaced covers I2's normal case: a
// caller snapshotted a segment's manifest row before a concurrent
// compaction/prune dropped it (row and file together, via dropSegment).
// Retrying against a fresh manifest finds the row gone too, so this is a
// legitimate replacement, not corruption — ok=false, err=nil, and the
// caller (collectRows/logByID) simply skips the segment.
func TestRetryReadSegmentRowsSkipsWhenReplaced(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "row one", "api", at)}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	segs, err := s.Segments(ctx, p.ID)
	if err != nil || len(segs) != 1 {
		t.Fatalf("precondition: err=%v segs=%+v", err, segs)
	}
	stale := segs[0] // as an earlier caller would have snapshotted it

	// Simulate the race: manifest row AND file both go away (dropSegment,
	// same call prune/compact use) after `stale` was captured.
	if err := s.dropSegment(ctx, stale); err != nil {
		t.Fatal(err)
	}

	sinceM, untilM := boundsMicros(time.Time{}, time.Time{})
	rows, ok, err := s.retryReadSegmentRows(ctx, p.ID, stale, sinceM, untilM, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false (segment legitimately gone), got rows=%+v", rows)
	}

	// The read path as a whole must still succeed and return no rows
	// (nothing else was written) rather than erroring.
	got, err := s.collectRows(ctx, p.ID, time.Time{}, time.Time{}, "")
	if err != nil {
		t.Fatalf("collectRows after drop: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("collectRows after drop: got %d rows, want 0", len(got))
	}
}

// TestRetryReadSegmentRowsErrorsOnRealCorruption covers I2's corruption
// case: the file is gone but the manifest row is still there — a
// combination neither compaction nor prune can produce (both remove the
// manifest row before the file), so it must propagate as an error naming
// the path rather than being silently skipped.
func TestRetryReadSegmentRowsErrorsOnRealCorruption(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "row one", "api", at)}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	segs, err := s.Segments(ctx, p.ID)
	if err != nil || len(segs) != 1 {
		t.Fatalf("precondition: err=%v segs=%+v", err, segs)
	}
	m := segs[0]
	if err := os.Remove(s.segPath(m.Path)); err != nil {
		t.Fatal(err)
	}

	sinceM, untilM := boundsMicros(time.Time{}, time.Time{})
	_, ok, err := s.retryReadSegmentRows(ctx, p.ID, m, sinceM, untilM, "")
	if ok {
		t.Fatal("expected ok=false on corruption")
	}
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), m.Path) {
		t.Fatalf("error does not mention the missing path %q: %v", m.Path, err)
	}
}

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
	// core.Severity is an internal int8 enum (SeverityError = 4), not the
	// OTLP wire encoding — the most-severe bucket sorts first.
	if bySev[0].Key != strconv.Itoa(int(core.SeverityError)) || bySev[0].Logs != 1 {
		t.Fatalf("severity ordering: %+v", bySev)
	}
	if _, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, GroupBy: "nope"}); err == nil {
		t.Error("unknown groupBy must error")
	}
	if _, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: 0, GroupBy: "service"}); err == nil {
		t.Error("ProjectID 0 must error (documented)")
	}
}

// TestAggregateHourGranularWindowsCongruentAcrossFlush pins that
// non-hour-aligned Since/Until produce the same result whether the
// underlying rows are still in the memtable or have been flushed to
// rollups — the two paths must agree on the same (truncated-hour)
// window, not just on data that happens to align to the hour.
func TestAggregateHourGranularWindowsCongruentAcrossFlush(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})
	p, _ := s.CreateProject(ctx, "p", 30)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Since=10:30 truncates down to hour 10; Until=12:10 truncates down
	// to hour 12 and extends to that hour's end — so the effective
	// window covers hours [10, 13), i.e. up to but not including 13:00.
	since := base.Add(10*time.Hour + 30*time.Minute)
	until := base.Add(12*time.Hour + 10*time.Minute)
	entries := []store.Entry{
		logEntry(p.ID, "r1", "svc", base.Add(10*time.Hour+15*time.Minute)), // hour 10: in window
		logEntry(p.ID, "r2", "svc", base.Add(10*time.Hour+45*time.Minute)), // hour 10: in window
		logEntry(p.ID, "r3", "svc", base.Add(12*time.Hour+30*time.Minute)), // hour 12: in window (until's hour is inclusive)
		logEntry(p.ID, "r4", "svc", base.Add(13*time.Hour+5*time.Minute)),  // hour 13: outside window
	}
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}

	assertLogs := func(t *testing.T, want int64) {
		t.Helper()
		rows, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, Since: since, Until: until, GroupBy: "service"})
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, r := range rows {
			total += r.Logs
		}
		if total != want {
			t.Fatalf("logs = %d, want %d (%+v)", total, want, rows)
		}
	}

	assertLogs(t, 3) // all four rows still unflushed (memtable path)
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	assertLogs(t, 3) // same rows now flushed (rollup path) — must agree
}
