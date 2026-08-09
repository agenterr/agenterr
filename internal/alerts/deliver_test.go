package alerts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
)

// shortBackoff replaces an Engine's retry backoff with sub-millisecond
// delays so retry tests don't sleep for real seconds.
func shortBackoff(e *Engine) {
	e.backoff = []time.Duration{time.Millisecond, time.Millisecond}
}

// 5a. Delivery: 2xx success records last_fired + empty last_error, and
// the payload shape (incl. headers applied) is exact.
func TestDeliver_SuccessPayloadShapeAndHeaders(t *testing.T) {
	var gotBody map[string]any
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Token")
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{
		ProjectID: 7, Name: "threshold rule", Kind: core.AlertThreshold,
		N: 3, WindowMinutes: 1, URL: srv.URL, Headers: map[string]string{"X-Token": "secret"}, Enabled: true,
	})
	e := New(fs, nil)
	shortBackoff(e)
	mustLoad(t, e)

	firedAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	e.deliver(fireJob{
		rule: rule, issueID: 99, title: "boom", severity: core.SeverityError,
		eventTime: firedAt, eventCount: 3, windowMinutes: 1,
	})

	if gotHeader != "secret" {
		t.Errorf("X-Token header = %q, want secret", gotHeader)
	}

	ruleField, _ := gotBody["rule"].(map[string]any)
	if ruleField["id"] != float64(rule.ID) || ruleField["name"] != "threshold rule" || ruleField["kind"] != "threshold" {
		t.Errorf("rule field = %v, want id=%d name=threshold rule kind=threshold", ruleField, rule.ID)
	}
	if gotBody["project_id"] != float64(7) {
		t.Errorf("project_id = %v, want 7", gotBody["project_id"])
	}
	issue, _ := gotBody["issue"].(map[string]any)
	if issue["id"] != float64(99) || issue["title"] != "boom" || issue["severity"] != "ERROR" || issue["count"] != float64(3) {
		t.Errorf("issue field = %v, want id=99 title=boom severity=ERROR count=3", issue)
	}
	if gotBody["event_count"] != float64(3) || gotBody["window_minutes"] != float64(1) {
		t.Errorf("event_count/window_minutes = %v/%v, want 3/1", gotBody["event_count"], gotBody["window_minutes"])
	}
	if _, ok := gotBody["fired_at"]; !ok {
		t.Errorf("fired_at missing from payload")
	}
	if _, ok := gotBody["test"]; ok {
		t.Errorf("test field present on a non-test fire, want omitted")
	}

	call, ok := fs.lastRecordCall()
	if !ok {
		t.Fatalf("expected a RecordAlertResult call")
	}
	if call.id != rule.ID || call.lastErr != "" {
		t.Errorf("record call = %+v, want id=%d lastErr=empty", call, rule.ID)
	}
}

// 5b. Delivery: 500x3 gives up recording last_error, and hits the
// receiver exactly 3 times (1 attempt + 2 retries), proving the retry
// backoff path runs without ever sleeping real seconds.
func TestDeliver_RetriesThenRecordsError(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: srv.URL, Enabled: true})
	e := New(fs, nil)
	shortBackoff(e)
	mustLoad(t, e)

	start := time.Now()
	e.deliver(fireJob{rule: rule, issueID: 1, title: "x", severity: core.SeverityError, eventTime: time.Now()})
	elapsed := time.Since(start)

	if got := atomic.LoadInt32(&hits); got != 3 {
		t.Errorf("receiver hits = %d, want 3", got)
	}
	if elapsed > time.Second {
		t.Errorf("deliver took %v, want well under 1s (backoff must be injected short, not the real 1s/4s)", elapsed)
	}

	call, ok := fs.lastRecordCall()
	if !ok {
		t.Fatalf("expected a RecordAlertResult call")
	}
	if call.id != rule.ID || call.lastErr == "" {
		t.Errorf("record call = %+v, want id=%d with a non-empty lastErr", call, rule.ID)
	}
}

// 9. TestFire delivers the "test":true payload synchronously with a
// sample issue and records the result, returning the delivery error.
func TestTestFire_DeliversSampleSynchronouslyAndRecords(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: srv.URL, Enabled: true})
	e := New(fs, nil)
	shortBackoff(e)
	mustLoad(t, e)

	if err := e.TestFire(context.Background(), rule.ID); err != nil {
		t.Fatalf("TestFire: %v", err)
	}

	if gotBody["test"] != true {
		t.Errorf("test field = %v, want true", gotBody["test"])
	}
	issue, _ := gotBody["issue"].(map[string]any)
	if issue == nil {
		t.Fatalf("issue field missing from test-fire payload")
	}

	call, ok := fs.lastRecordCall()
	if !ok || call.id != rule.ID || call.lastErr != "" {
		t.Errorf("record call = %+v, ok=%v, want id=%d lastErr=empty", call, ok, rule.ID)
	}
}

// TestFire on a failing webhook returns the delivery error (nil only on
// 2xx) and still records the failure.
func TestTestFire_FailureReturnsErrorAndRecords(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: srv.URL, Enabled: true})
	e := New(fs, nil)
	mustLoad(t, e)

	err := e.TestFire(context.Background(), rule.ID)
	if err == nil {
		t.Fatalf("TestFire err = nil, want non-nil on a 500 response")
	}

	call, ok := fs.lastRecordCall()
	if !ok || call.lastErr == "" {
		t.Errorf("record call = %+v, ok=%v, want a non-empty lastErr", call, ok)
	}
}

// Run drains the queue on shutdown: items already enqueued still get
// delivered (and recorded) after ctx is canceled.
func TestRun_DrainsQueueOnShutdown(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fs := newFakeStore()
	rule := fs.seedRule(core.AlertRule{ProjectID: 1, Kind: core.AlertNewIssue, URL: srv.URL, Enabled: true})
	e := New(fs, nil)
	shortBackoff(e)
	mustLoad(t, e)

	ctx, cancel := context.WithCancel(context.Background())

	// Queue several jobs before the worker ever starts, then cancel
	// immediately — Run must still drain and deliver every one of them.
	const n = 5
	for i := 0; i < n; i++ {
		e.queue <- fireJob{rule: rule, issueID: int64(i), title: "x", severity: core.SeverityError, eventTime: time.Now()}
	}
	cancel()

	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after ctx cancellation")
	}

	if got := atomic.LoadInt32(&hits); got != n {
		t.Errorf("receiver hits = %d, want %d (drain must deliver every queued item)", got, n)
	}
	if got := fs.recordCallCount(); got != n {
		t.Errorf("RecordAlertResult calls = %d, want %d", got, n)
	}
}
