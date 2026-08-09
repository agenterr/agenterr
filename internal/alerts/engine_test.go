package alerts

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

func mustLoad(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func entry(projectID int64, service, env string, sev core.Severity, ts time.Time, title string) store.Entry {
	return store.Entry{
		Log: core.Log{
			ProjectID:   projectID,
			Service:     service,
			Environment: env,
			Severity:    sev,
			Time:        ts,
		},
		IsEvent: true,
		Title:   title,
	}
}

// recvJob waits briefly for a job to land on e.queue, failing the test if
// none arrives (or, when want is false, failing if one does).
func recvJob(t *testing.T, e *Engine, want bool) fireJob {
	t.Helper()
	select {
	case job := <-e.queue:
		if !want {
			t.Fatalf("unexpected enqueue: rule %d", job.rule.ID)
		}
		return job
	case <-time.After(100 * time.Millisecond):
		if want {
			t.Fatalf("expected an enqueue, got none")
		}
		return fireJob{}
	}
}

// 1. new_issue rule fires exactly on o.New within scope; not on subsequent events.
func TestIssueEvent_NewIssueFiresOnlyOnNew(t *testing.T) {
	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: "https://example.com/hook", Enabled: true})
	e := New(fs, nil)
	mustLoad(t, e)

	en := entry(1, "api", "prod", core.SeverityError, time.Now(), "boom")
	e.IssueEvent(en, store.IssueOutcome{IssueID: 42, New: true})
	job := recvJob(t, e, true)
	if job.rule.ID != rule.ID || job.issueID != 42 {
		t.Errorf("job = %+v, want rule %d issue 42", job, rule.ID)
	}

	// Subsequent (non-new) event on the same issue must not fire.
	e.IssueEvent(en, store.IssueOutcome{IssueID: 42, New: false})
	recvJob(t, e, false)
}

// 2. regression fires exactly on o.Reopened.
func TestIssueEvent_RegressionFiresOnlyOnReopened(t *testing.T) {
	fs := newFakeStore()
	fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertRegression, URL: "https://example.com/hook", Enabled: true})
	e := New(fs, nil)
	mustLoad(t, e)

	en := entry(1, "api", "prod", core.SeverityError, time.Now(), "boom")

	// New (not reopened) must not fire a regression rule.
	e.IssueEvent(en, store.IssueOutcome{IssueID: 1, New: true})
	recvJob(t, e, false)

	e.IssueEvent(en, store.IssueOutcome{IssueID: 1, Reopened: true})
	job := recvJob(t, e, true)
	if job.rule.Kind != core.AlertRegression {
		t.Errorf("job.rule.Kind = %v, want regression", job.rule.Kind)
	}
}

// 3. threshold: N=3 in 1min — 2 events no fire, 3rd fires; window slides
// (old buckets expire); scope filters (service/env/min_severity) apply.
func TestIssueEvent_ThresholdWindowAndScope(t *testing.T) {
	fs := newFakeStore()
	fs.seedRule(core.AlertRule{
		ProjectID: 1, Kind: core.AlertThreshold, Service: "api", Environment: "prod",
		MinSeverity: core.SeverityWarn, N: 3, WindowMinutes: 1, URL: "https://example.com/hook", Enabled: true,
	})
	e := New(fs, nil)
	mustLoad(t, e)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Out of scope: wrong service, wrong env, below min severity — none count.
	e.IssueEvent(entry(1, "web", "prod", core.SeverityError, base, "x"), store.IssueOutcome{})
	e.IssueEvent(entry(1, "api", "staging", core.SeverityError, base, "x"), store.IssueOutcome{})
	e.IssueEvent(entry(1, "api", "prod", core.SeverityInfo, base, "x"), store.IssueOutcome{})
	recvJob(t, e, false)

	// Two in-scope events: no fire yet.
	e.IssueEvent(entry(1, "api", "prod", core.SeverityError, base, "x"), store.IssueOutcome{})
	e.IssueEvent(entry(1, "api", "prod", core.SeverityError, base.Add(10*time.Second), "x"), store.IssueOutcome{})
	recvJob(t, e, false)

	// Third in-scope event within the window fires.
	e.IssueEvent(entry(1, "api", "prod", core.SeverityError, base.Add(20*time.Second), "x"), store.IssueOutcome{})
	job := recvJob(t, e, true)
	if job.eventCount != 3 {
		t.Errorf("eventCount = %d, want 3", job.eventCount)
	}

	// Window slides: two more events far enough in the future that the
	// first three buckets have aged out must not immediately refire (only
	// 2 in-window events so far after the slide).
	future := base.Add(5 * time.Minute)
	e.IssueEvent(entry(1, "api", "prod", core.SeverityError, future, "x"), store.IssueOutcome{})
	e.IssueEvent(entry(1, "api", "prod", core.SeverityError, future.Add(5*time.Second), "x"), store.IssueOutcome{})
	recvJob(t, e, false)
}

// 4. Cooldown: after a fire, matching events within cooldown are
// suppressed (no queue entry); fires again after. Seeded from LastFired
// on Load (restart survivability).
func TestIssueEvent_Cooldown(t *testing.T) {
	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, CooldownSeconds: 60, URL: "https://example.com/hook", Enabled: true})
	e := New(fs, nil)
	mustLoad(t, e)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	en := func(ts time.Time) store.Entry { return entry(1, "api", "prod", core.SeverityError, ts, "x") }

	e.IssueEvent(en(base), store.IssueOutcome{IssueID: 1, New: true})
	recvJob(t, e, true)

	// Within cooldown: suppressed, not queued.
	e.IssueEvent(en(base.Add(30*time.Second)), store.IssueOutcome{IssueID: 2, New: true})
	recvJob(t, e, false)

	// After cooldown: fires again.
	e.IssueEvent(en(base.Add(61*time.Second)), store.IssueOutcome{IssueID: 3, New: true})
	recvJob(t, e, true)

	// Restart survivability: a fresh engine loading a rule whose stored
	// LastFired is recent must suppress an immediate matching event.
	fs2 := newFakeStore()
	fs2.seedRuleWithLastFired(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, CooldownSeconds: 3600, URL: "https://example.com/hook", Enabled: true}, base)
	e2 := New(fs2, nil)
	mustLoad(t, e2)
	e2.IssueEvent(en(base.Add(time.Second)), store.IssueOutcome{IssueID: 9, New: true})
	recvJob(t, e2, false)

	_ = rule
}

// 6. Full queue: with a stuffed channel, IssueEvent does not block
// (returns immediately) and increments a drop counter.
func TestIssueEvent_FullQueueDropsWithoutBlocking(t *testing.T) {
	fs := newFakeStore()
	fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: "https://example.com/hook", Enabled: true})
	e := New(fs, nil)
	mustLoad(t, e)

	// Fill the queue to capacity directly.
	for i := 0; i < queueCap; i++ {
		e.queue <- fireJob{}
	}

	done := make(chan struct{})
	go func() {
		e.IssueEvent(entry(1, "api", "prod", core.SeverityError, time.Now(), "x"), store.IssueOutcome{IssueID: 1, New: true})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("IssueEvent blocked on a full queue")
	}

	if got := e.Dropped(); got != 1 {
		t.Errorf("Dropped() = %d, want 1", got)
	}
}

// 7. Fan-out: two matching rules both fire for one event.
func TestIssueEvent_FanOutTwoRulesBothFire(t *testing.T) {
	fs := newFakeStore()
	r1 := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: "https://example.com/1", Enabled: true})
	r2 := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: "https://example.com/2", Enabled: true})
	e := New(fs, nil)
	mustLoad(t, e)

	e.IssueEvent(entry(1, "api", "prod", core.SeverityError, time.Now(), "x"), store.IssueOutcome{IssueID: 1, New: true})

	first := recvJob(t, e, true)
	second := recvJob(t, e, true)
	gotIDs := map[int64]bool{first.rule.ID: true, second.rule.ID: true}
	if !gotIDs[r1.ID] || !gotIDs[r2.ID] {
		t.Errorf("got fires for rules %v, want both %d and %d", gotIDs, r1.ID, r2.ID)
	}
	// Determinism: rules evaluate in ascending-ID order, so the first
	// enqueued job must be the lower-ID rule.
	if first.rule.ID >= second.rule.ID {
		t.Errorf("enqueue order = [%d, %d], want ascending-ID order [%d, %d]", first.rule.ID, second.rule.ID, r1.ID, r2.ID)
	}
}

// 8. Concurrency: -race hammer — IssueEvent from one goroutine,
// Upsert/Load from another, e.Run(ctx) actually running as the worker; no
// races, and every enqueued fire is delivered (and recorded) exactly once
// (no lost/duplicate fires). Exercises Run's real queue-consume path
// (not a hand-rolled drain), including its shutdown-drain branch. Per
// Run's documented precondition, ctx is canceled only after every
// IssueEvent-calling goroutine has finished.
func TestConcurrency_NoRacesNoLostOrDuplicateFires(t *testing.T) {
	var delivered atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fs := newFakeStore()
	// Scoped to service "api" so the rules concurrently upserted below
	// (scoped to "other") never match the hammered events — otherwise
	// fan-out (a correct, separate behavior) would make the expected
	// delivered count depend on upsert/event interleaving.
	fs.seedRule(core.AlertRule{ProjectID: 1, Service: "api", Kind: core.AlertNewIssue, URL: srv.URL, Enabled: true})
	e := New(fs, nil)
	e.queue = make(chan fireJob, 4096) // avoid dropped-by-design fires muddying the count
	mustLoad(t, e)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		e.Run(ctx)
	}()

	var wg sync.WaitGroup

	const events = 200
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < events; i++ {
			e.IssueEvent(entry(1, "api", "prod", core.SeverityError, time.Now(), "x"), store.IssueOutcome{IssueID: int64(i), New: true})
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = e.Upsert(context.Background(), core.AlertRule{ProjectID: 1, Service: "other", Kind: core.AlertNewIssue, URL: fmt.Sprintf("https://example.com/%d", i), Enabled: true})
			_ = e.Load(context.Background())
		}
	}()

	// Wait for every IssueEvent caller to finish before canceling — Run's
	// drain-on-cancel is a single best-effort pass and can lose a fire
	// that races with cancellation (see Run's doc comment).
	wg.Wait()
	cancel()
	<-runDone

	if got := delivered.Load(); got != events {
		t.Errorf("delivered = %d, want %d (no lost/duplicate fires)", got, events)
	}
}
