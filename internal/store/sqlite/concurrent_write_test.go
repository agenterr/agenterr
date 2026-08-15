package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/sqlite"
)

// TestConcurrentWritersDoNotBusyFail is a regression test for the SQLITE_BUSY
// batch drop documented in
// .superpowers/sdd/2026-08-09-oss-hardening-and-release/busy-rootcause.md:
// two goroutines opening deferred write transactions on the same *sql.DB at
// roughly the same time can collide such that the loser's first write
// statement fails immediately with SQLITE_BUSY, without ever waiting on
// busy_timeout(5000) — because a deferred tx only invokes the busy handler
// while still TRANS_NONE, and by the time the writer-lock attempt happens
// the tx has already entered TRANS_READ.
//
// This mirrors the two real colliding writers: (*DB).UpsertIssues (engine
// batch flush's issue accounting) and (*DB).AddNoiseDrops (noise
// drop-counter flush), both hammered concurrently from tight loops for
// ~2s, plus a third writer (DeleteIssueEventsBefore) to widen the
// collision window. It must fail deterministically pre-fix (bare deferred
// BEGIN) and pass deterministically post-fix (_txlock=immediate in the
// DSN).
func TestConcurrentWritersDoNotBusyFail(t *testing.T) {
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()

	project, err := db.CreateProject(ctx, "concurrent-writers", 30)
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	rule, err := db.UpsertNoiseRule(ctx, core.NoiseRule{
		ProjectID: project.ID,
		Kind:      core.NoiseSeverityFloor,
		Severity:  core.SeverityDebug,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("upsert noise rule: %v", err)
	}

	const duration = 2 * time.Second
	deadline := time.Now().Add(duration)

	var errCount int64
	var wg sync.WaitGroup
	var firstErr atomic.Value // string

	recordErr := func(msg string) {
		atomic.AddInt64(&errCount, 1)
		firstErr.CompareAndSwap(nil, msg)
	}

	// Writer 1: engine UpsertIssues shape — a batch with one event entry
	// (issue upsert + issue_events insert + trim). Log.ID is set directly,
	// standing in for the monotonic id the engine assigns before calling
	// UpsertIssues (see enginestore.Store.WriteBatch).
	var nextLogID int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; time.Now().Before(deadline); i++ {
			logID := atomic.AddInt64(&nextLogID, 1)
			entries := []store.Entry{
				{
					Log: core.Log{
						ID:          logID,
						ProjectID:   project.ID,
						Time:        time.Now().UTC(),
						Severity:    core.SeverityWarn,
						Body:        fmt.Sprintf("writebatch warn %d", i),
						Service:     "svc-a",
						Environment: "test",
					},
					IsEvent:     true,
					Fingerprint: "concurrent-fp",
					Title:       "concurrent issue",
				},
			}
			if _, err := db.UpsertIssues(ctx, entries); err != nil {
				recordErr(fmt.Sprintf("UpsertIssues: %v", err))
			}
		}
	}()

	// Writer 2: noise drop-counter flush shape.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			counts := map[int64]int64{rule.ID: 1}
			if err := db.AddNoiseDrops(ctx, counts); err != nil {
				recordErr(fmt.Sprintf("AddNoiseDrops: %v", err))
			}
		}
	}()

	// Writer 3: retention prune shape — widens the collision window per the
	// task instructions ("you may also add a third writer goroutine").
	wg.Add(1)
	go func() {
		defer wg.Done()
		cutoff := time.Now().Add(-time.Hour)
		for time.Now().Before(deadline) {
			if err := db.DeleteIssueEventsBefore(ctx, project.ID, cutoff); err != nil {
				recordErr(fmt.Sprintf("DeleteIssueEventsBefore: %v", err))
			}
		}
	}()

	wg.Wait()

	if errCount > 0 {
		msg, _ := firstErr.Load().(string)
		t.Fatalf("concurrent writers produced %d errors; first: %s", errCount, msg)
	}
}
