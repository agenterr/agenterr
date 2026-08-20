package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// fakeWriter records every batch it is asked to write. Safe for concurrent
// use since the pipeline's writer loop and the test goroutine both touch it.
type fakeWriter struct {
	mu      sync.Mutex
	batches [][]store.Entry
	failN   int // if > 0, the next failN WriteBatch calls return errWrite
}

var errWrite = errors.New("fake writer: forced failure")

// WriteBatch returns one IssueOutcome per event entry, in entry order,
// tagged New=true with a distinguishing IssueID (1-based position in the
// batch) so tests can assert the pipeline routes each outcome to the
// matching event entry and skips non-event entries — the real values
// (New/Reopened semantics) are storetest's job, not fakeWriter's.
func (f *fakeWriter) WriteBatch(_ context.Context, e []store.Entry) ([]store.IssueOutcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return nil, errWrite
	}
	batch := make([]store.Entry, len(e))
	copy(batch, e)
	f.batches = append(f.batches, batch)

	var outcomes []store.IssueOutcome
	for i, entry := range e {
		if entry.IsEvent {
			outcomes = append(outcomes, store.IssueOutcome{IssueID: int64(i + 1), New: true})
		}
	}
	return outcomes, nil
}

func (f *fakeWriter) Prune(context.Context, int64, time.Time) (int64, error) { return 0, nil }

func (f *fakeWriter) snapshot() [][]store.Entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]store.Entry, len(f.batches))
	copy(out, f.batches)
	return out
}

func (f *fakeWriter) totalEntries() int {
	n := 0
	for _, b := range f.snapshot() {
		n += len(b)
	}
	return n
}

// fakeDropper is a scriptable Dropper for tests: decide is consulted per
// log (nil means "never drop"), parseBodies is a per-project map (missing
// entries default to true, matching rules.Engine's fail-open default),
// and lift is consulted per log (nil means "never lift").
type fakeDropper struct {
	decide      func(l core.Log) (bool, int64)
	parseBodies map[int64]bool
	lift        func(l core.Log) (core.Log, int64)
}

func (f fakeDropper) Decide(l core.Log) (bool, int64) {
	if f.decide == nil {
		return false, 0
	}
	return f.decide(l)
}

func (f fakeDropper) ParseBodies(projectID int64) bool {
	if f.parseBodies == nil {
		return true
	}
	on, ok := f.parseBodies[projectID]
	if !ok {
		return true
	}
	return on
}

func (f fakeDropper) Lift(l core.Log) (core.Log, int64) {
	if f.lift == nil {
		return l, 0
	}
	return f.lift(l)
}

// eventually polls cond until it returns true or the deadline passes,
// failing the test if the deadline is reached first. No sleeps as
// assertions: every wait is bounded and polled.
func eventually(t *testing.T, deadline time.Duration, cond func() bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	for {
		if cond() {
			return
		}
		if time.Now().After(end) {
			t.Fatalf("condition not met within %s", deadline)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func testLog(i int, severity core.Severity) core.Log {
	return core.Log{
		ID:       int64(i),
		Time:     time.Now(),
		Severity: severity,
		Body:     "test log body",
	}
}

func errorLog(i int) core.Log {
	return core.Log{
		ID:       int64(i),
		Time:     time.Now(),
		Severity: core.SeverityError,
		Body:     "boom: something failed",
		Attrs: map[string]string{
			"exception.type":    "BoomError",
			"exception.message": "something failed",
		},
	}
}

func TestBatchesByCountAndAnnotates(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 1000, FlushEvery: time.Hour, MaxBatch: 500})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	logs := make([]core.Log, 0, 500)
	for i := 0; i < 500; i++ {
		if i%10 == 0 {
			logs = append(logs, errorLog(i))
		} else {
			logs = append(logs, testLog(i, core.SeverityInfo))
		}
	}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	eventually(t, 2*time.Second, func() bool { return fw.totalEntries() == 500 })

	batches := fw.snapshot()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch (MaxBatch=500 hit exactly), got %d", len(batches))
	}
	if len(batches[0]) != 500 {
		t.Fatalf("expected batch of 500, got %d", len(batches[0]))
	}

	errCount := 0
	for _, e := range batches[0] {
		if e.Log.Severity == core.SeverityError {
			errCount++
			if !e.IsEvent {
				t.Errorf("error log %d: expected IsEvent=true", e.Log.ID)
			}
			if e.Fingerprint == "" {
				t.Errorf("error log %d: expected non-empty Fingerprint", e.Log.ID)
			}
			if e.Title == "" {
				t.Errorf("error log %d: expected non-empty Title", e.Log.ID)
			}
		} else {
			if e.IsEvent {
				t.Errorf("info log %d: expected IsEvent=false", e.Log.ID)
			}
		}
	}
	if errCount != 50 {
		t.Fatalf("expected 50 error entries, got %d", errCount)
	}
}

func TestFlushesByTimer(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 100, FlushEvery: 30 * time.Millisecond, MaxBatch: 500})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	logs := []core.Log{testLog(1, core.SeverityInfo), testLog(2, core.SeverityInfo), testLog(3, core.SeverityInfo)}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	eventually(t, 2*time.Second, func() bool { return fw.totalEntries() == 3 })

	batches := fw.snapshot()
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch flushed by timer, got %d", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Fatalf("expected batch of 3, got %d", len(batches[0]))
	}
}

func TestBackpressureErrFull(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 10, FlushEvery: time.Hour, MaxBatch: 500})
	// Run is intentionally NOT started: nothing drains the buffer.

	logs := make([]core.Log, 11)
	for i := range logs {
		logs[i] = testLog(i, core.SeverityInfo)
	}

	err := p.Enqueue(logs)
	if !errors.Is(err, ErrFull) {
		t.Fatalf("expected ErrFull enqueuing 11 logs into a 10-capacity buffer, got %v", err)
	}

	// All-or-nothing: the rejected slice must not have partially landed.
	if err := p.Enqueue(make([]core.Log, 10)); err != nil {
		t.Fatalf("expected exactly-fitting enqueue to succeed after prior rejection, got %v", err)
	}
}

func TestDrainFlushesEverything(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 1000, FlushEvery: time.Hour, MaxBatch: 500})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	logs := make([]core.Log, 200)
	for i := range logs {
		logs[i] = testLog(i, core.SeverityInfo)
	}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	cancel() // shut down Run; FlushEvery is an hour so only ctx-cancel + Drain can flush this

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := fw.totalEntries(); got != 200 {
		t.Fatalf("expected all 200 entries written exactly once, got %d", got)
	}
	batches := fw.snapshot()
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	if total != 200 {
		t.Fatalf("entries written more than once or lost: total %d across %d batches", total, len(batches))
	}
}

func TestEnqueueAfterShutdownErrFull(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 1000, FlushEvery: 10 * time.Millisecond, MaxBatch: 500})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{testLog(1, core.SeverityInfo)}); err != nil {
		t.Fatalf("Enqueue before shutdown: %v", err)
	}

	cancel()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Give Run's ctx.Done branch a moment to flip stopped, then confirm
	// Enqueue consistently refuses new work — no panics on the closed path.
	eventually(t, 2*time.Second, func() bool {
		return errors.Is(p.Enqueue([]core.Log{testLog(2, core.SeverityInfo)}), ErrFull)
	})
}

func TestWriteErrorDropsButLoopContinues(t *testing.T) {
	fw := &fakeWriter{failN: 1}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 100, FlushEvery: time.Hour, MaxBatch: 5})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	first := make([]core.Log, 5)
	for i := range first {
		first[i] = testLog(i, core.SeverityInfo)
	}
	if err := p.Enqueue(first); err != nil {
		t.Fatalf("Enqueue first batch: %v", err)
	}

	// The first batch of 5 hits MaxBatch and triggers a flush that the
	// fakeWriter is set up to fail. Wait for the writer to have been
	// invoked (unflushed drops back to 0) even though nothing landed.
	eventually(t, 2*time.Second, func() bool {
		return atomic.LoadInt64(&p.unflushed) == 0
	})
	if got := fw.totalEntries(); got != 0 {
		t.Fatalf("expected failed batch to be dropped (0 entries recorded), got %d", got)
	}

	second := make([]core.Log, 5)
	for i := range second {
		second[i] = testLog(i+5, core.SeverityInfo)
	}
	if err := p.Enqueue(second); err != nil {
		t.Fatalf("Enqueue second batch: %v", err)
	}

	eventually(t, 2*time.Second, func() bool { return fw.totalEntries() == 5 })

	batches := fw.snapshot()
	if len(batches) != 1 {
		t.Fatalf("expected exactly 1 recorded batch (the second, successful one), got %d", len(batches))
	}
	if len(batches[0]) != 5 {
		t.Fatalf("expected recorded batch of 5, got %d", len(batches[0]))
	}
}

// TestDrainNoTOCTOUWithConcurrentEnqueue reproduces the Drain/Enqueue TOCTOU:
// pre-fix, Enqueue pushed logs into buf and only incremented p.unflushed
// afterward, so Drain — which reads p.unflushed without taking p.mu — could
// observe unflushed==0 (and return "done") while a log had already been
// pushed into buf (or handed to Run's pending batch) but not yet counted.
//
// A single large burst is enqueued while Run is actively flushing small
// batches (MaxBatch well below the burst size), which widens the window
// between "log visible to Run" and "log counted in unflushed" enough for
// two concurrent watchers to have a realistic chance of catching it:
//   - one watches p.unflushed for a negative reading, which can only happen
//     if Run flushed (and decremented for) entries whose corresponding
//     increment hadn't landed yet;
//   - one repeatedly calls Drain and checks that it never reports "done"
//     while entries are still sitting unconsumed in the channel.
func TestDrainNoTOCTOUWithConcurrentEnqueue(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 20000, FlushEvery: time.Hour, MaxBatch: 10})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	const n = 8000
	logs := make([]core.Log, n)
	for i := range logs {
		logs[i] = testLog(i, core.SeverityInfo)
	}

	var minUnflushed int64 = 1 << 62
	var mismatch int32
	stop := make(chan struct{})
	var watchers sync.WaitGroup

	watchers.Add(1)
	go func() {
		defer watchers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if v := atomic.LoadInt64(&p.unflushed); v < minUnflushed {
				minUnflushed = v
			}
		}
	}()

	watchers.Add(1)
	go func() {
		defer watchers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			dctx, dcancel := context.WithTimeout(context.Background(), 3*time.Millisecond)
			err := p.Drain(dctx)
			dcancel()
			if err == nil && len(p.buf) > 0 {
				atomic.StoreInt32(&mismatch, 1)
			}
		}
	}()

	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	eventually(t, 2*time.Second, func() bool { return fw.totalEntries() == n })
	close(stop)
	watchers.Wait()

	if minUnflushed < 0 {
		t.Fatalf("p.unflushed observed negative (%d): a flush decremented for entries whose Enqueue increment had not landed yet", minUnflushed)
	}
	if atomic.LoadInt32(&mismatch) != 0 {
		t.Fatalf("Drain reported done while entries were still sitting unconsumed in the buffer")
	}
}

// TestNew_Defaults exercises the zero-value fallback branches in New: every
// other test in this file passes explicit Options, so without this test
// the "field <= 0 -> default" path for BufferSize/FlushEvery/MaxBatch is
// never actually run.
func TestNew_Defaults(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{})

	if cap(p.buf) != defaultBufferSize {
		t.Errorf("BufferSize default = %d, want %d", cap(p.buf), defaultBufferSize)
	}
	if p.o.FlushEvery != defaultFlushEvery {
		t.Errorf("FlushEvery default = %v, want %v", p.o.FlushEvery, defaultFlushEvery)
	}
	if p.o.MaxBatch != defaultMaxBatch {
		t.Errorf("MaxBatch default = %d, want %d", p.o.MaxBatch, defaultMaxBatch)
	}
}

// TestPending_InitiallyZero covers Pending(), which every other test in
// this file only ever exercises indirectly via Drain's polling loop.
func TestPending_InitiallyZero(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{BufferSize: 10, FlushEvery: time.Hour, MaxBatch: 5})
	if got := p.Pending(); got != 0 {
		t.Errorf("Pending() on a fresh pipeline = %d, want 0", got)
	}
}

// TestNopNotifier_IssueEvent pins the no-panic contract of the v1 no-op
// Notifier — it has no other observable behavior, but a future change that
// makes it dereference something on Entry must not silently panic in
// production.
func TestNopNotifier_IssueEvent(_ *testing.T) {
	NopNotifier{}.IssueEvent(store.Entry{}, store.IssueOutcome{})
}

// spyNotifier records every IssueEvent call it receives, for tests that
// need to assert the pipeline routed the right (Entry, IssueOutcome) pair
// rather than merely that a call happened.
type spyNotifier struct {
	mu    sync.Mutex
	calls []notifierCall
}

type notifierCall struct {
	entry   store.Entry
	outcome store.IssueOutcome
}

func (s *spyNotifier) IssueEvent(e store.Entry, o store.IssueOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, notifierCall{entry: e, outcome: o})
}

func (s *spyNotifier) snapshot() []notifierCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]notifierCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// TestFlush_NotifierGetsMatchingOutcomePerEventEntry pins the flush routing
// contract: outcomes returned by WriteBatch align with event entries in
// entry order (store.Writer's contract), and non-event entries in the same
// batch must not produce a notifier call at all.
func TestFlush_NotifierGetsMatchingOutcomePerEventEntry(t *testing.T) {
	fw := &fakeWriter{}
	sn := &spyNotifier{}
	p := New(fw, core.DefaultGrouper{}, sn, NopDropper{}, Options{BufferSize: 100, FlushEvery: time.Hour, MaxBatch: 4})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	// info, error (event), info, error (event) — MaxBatch=4 forces exactly
	// this batch through flush in one shot.
	logs := []core.Log{
		testLog(1, core.SeverityInfo),
		errorLog(2),
		testLog(3, core.SeverityInfo),
		errorLog(4),
	}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	eventually(t, 2*time.Second, func() bool { return len(sn.snapshot()) == 2 })

	calls := sn.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected exactly 2 notifier calls (one per event entry, skipping 2 plain logs), got %d", len(calls))
	}
	for i, c := range calls {
		if !c.entry.IsEvent {
			t.Errorf("call %d: entry.IsEvent = false, want true (non-event entries must not reach the notifier)", i)
		}
	}
	// fakeWriter tags outcomes with IssueID = 1-based position in the
	// batch: entry 2 (errorLog(2)) is at batch index 1 -> IssueID 2; entry
	// 4 (errorLog(4)) is at batch index 3 -> IssueID 4.
	if calls[0].outcome.IssueID != 2 {
		t.Errorf("calls[0].outcome.IssueID = %d, want 2 (first event entry's outcome)", calls[0].outcome.IssueID)
	}
	if calls[1].outcome.IssueID != 4 {
		t.Errorf("calls[1].outcome.IssueID = %d, want 4 (second event entry's outcome)", calls[1].outcome.IssueID)
	}
	if !calls[0].outcome.New || !calls[1].outcome.New {
		t.Errorf("calls = %+v, want New=true on both outcomes", calls)
	}
}

// TestFlush_FailedWriteCallsNoNotifier pins that a batch dropped on a
// WriteBatch error must not fire the notifier at all — the batch never
// landed, so there is nothing to report.
func TestFlush_FailedWriteCallsNoNotifier(t *testing.T) {
	fw := &fakeWriter{failN: 1}
	sn := &spyNotifier{}
	p := New(fw, core.DefaultGrouper{}, sn, NopDropper{}, Options{BufferSize: 100, FlushEvery: time.Hour, MaxBatch: 2})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{errorLog(1), errorLog(2)}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	eventually(t, 2*time.Second, func() bool {
		return atomic.LoadInt64(&p.unflushed) == 0
	})

	if calls := sn.snapshot(); len(calls) != 0 {
		t.Fatalf("expected 0 notifier calls after a failed write, got %d: %+v", len(calls), calls)
	}
}

// TestRun_ParsesStructuredBodies confirms annotate lifts a structured body
// before event detection runs: the lifted level=error must be what triggers
// IsEvent, not something already true of the raw record.
func TestRun_ParsesStructuredBodies(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	// Severity is explicitly Info (the value every real ingest path assigns
	// to a record with no severity of its own, per core.ParseSeverity's own
	// zero-value behavior) so the body-derived level is eligible to lift,
	// matching how logs actually arrive at the pipeline in production.
	err := p.Enqueue([]core.Log{{
		Time:     time.Now(),
		Severity: core.SeverityInfo,
		Body:     `{"level":"error","msg":"payment declined","request_id":"r1"}`,
	}})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	cancel()

	batches := fw.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("got %v batches, want 1 batch of 1 entry", batches)
	}
	e := batches[0][0]
	if !e.IsEvent {
		t.Fatal("lifted ERROR severity did not trigger event detection")
	}
	if e.Log.Body != "payment declined" {
		t.Errorf("body = %q, want lifted msg", e.Log.Body)
	}
	if e.Log.Attrs["request_id"] != "r1" {
		t.Errorf("request_id = %q, want r1", e.Log.Attrs["request_id"])
	}
}

// TestRun_DisableBodyParse confirms the escape hatch: with DisableBodyParse
// set, annotate must leave the raw body and severity untouched, so a
// structured body carrying level=error does not trigger event detection.
func TestRun_DisableBodyParse(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{DisableBodyParse: true})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{{
		Time: time.Now(),
		Body: `{"level":"error","msg":"payment declined"}`,
	}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("drain: %v", err)
	}
	cancel()

	batches := fw.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("got %v batches, want 1 batch of 1 entry", batches)
	}
	e := batches[0][0]
	if e.IsEvent {
		t.Error("parsing disabled but body was still lifted into an event")
	}
	if e.Log.Body == "payment declined" {
		t.Error("parsing disabled but msg was lifted")
	}
}

// TestDrop_NeverWrittenButDrainAccounts confirms a dropped log never
// reaches the writer, and that Drain still returns — the trap this
// guards against is the drop path forgetting to decrement unflushed,
// which would hang Drain forever waiting for a count that never reaches
// zero.
func TestDrop_NeverWrittenButDrainAccounts(t *testing.T) {
	fw := &fakeWriter{}
	d := fakeDropper{decide: func(core.Log) (bool, int64) { return true, 1 }}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, d, Options{BufferSize: 100, FlushEvery: time.Hour, MaxBatch: 500})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{testLog(1, core.SeverityInfo)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	cancel()

	if got := fw.totalEntries(); got != 0 {
		t.Fatalf("dropped log reached the writer: %d entries written", got)
	}
}

// TestDrop_OnShutdownDrainPath forces the drop decision through Run's
// shutdown drain branch specifically (cancel before any log is consumed
// by the live loop, mirroring TestDrainFlushesEverything's pattern) —
// process() is shared by both branches, but this pins that the drain
// branch's call site actually honors a drop rather than unconditionally
// appending to pending.
func TestDrop_OnShutdownDrainPath(t *testing.T) {
	fw := &fakeWriter{}
	d := fakeDropper{decide: func(core.Log) (bool, int64) { return true, 1 }}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, d, Options{BufferSize: 1000, FlushEvery: time.Hour, MaxBatch: 500})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	logs := make([]core.Log, 50)
	for i := range logs {
		logs[i] = testLog(i, core.SeverityInfo)
	}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	cancel() // shut down immediately; FlushEvery is an hour so only the drain path can process these

	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := fw.totalEntries(); got != 0 {
		t.Fatalf("dropped logs reached the writer via the drain path: %d entries written", got)
	}
}

// TestDrop_ParseThenDecideOrdering proves rules key on the lifted body:
// a fakeDropper that drops anything below SeverityInfo must see the
// body's level=debug lifted into Severity before Decide runs, since the
// raw record carries no severity at all (zero value = SeverityInfo,
// which would NOT be dropped) — only after parsing does the drop fire.
func TestDrop_ParseThenDecideOrdering(t *testing.T) {
	fw := &fakeWriter{}
	d := fakeDropper{decide: func(l core.Log) (bool, int64) {
		if l.Severity < core.SeverityInfo {
			return true, 1
		}
		return false, 0
	}}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, d, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{{
		Time: time.Now(),
		Body: `{"level":"debug","msg":"noise"}`,
	}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	cancel()

	if got := fw.totalEntries(); got != 0 {
		t.Fatalf("expected lifted debug severity to be dropped, got %d entries written", got)
	}
}

// TestPerProjectParseBodiesFalse confirms the per-project toggle refines
// the global flag: with DisableBodyParse left off (global on) but this
// project's ParseBodies false, the body must stay raw — no lift, and
// nothing for a severity-keyed dropper to catch.
func TestPerProjectParseBodiesFalse(t *testing.T) {
	fw := &fakeWriter{}
	const projectID = int64(42)
	d := fakeDropper{parseBodies: map[int64]bool{projectID: false}}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, d, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{{
		ProjectID: projectID,
		Time:      time.Now(),
		Body:      `{"level":"error","msg":"payment declined"}`,
	}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	cancel()

	batches := fw.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("got %v batches, want 1 batch of 1 entry", batches)
	}
	e := batches[0][0]
	if e.Log.Body != `{"level":"error","msg":"payment declined"}` {
		t.Errorf("body = %q, want raw (unparsed) body", e.Log.Body)
	}
	if e.IsEvent {
		t.Error("parse-bodies off for this project but body was still lifted into an event")
	}
}

// TestLift_MatchingBodyBecomesEvent proves severity rules run before
// Decide and actually change stored/event outcomes: a fakeDropper.lift
// that raises any body containing "OOM" from its ingest severity to
// SeverityError (mirroring a seeded severity_floor-style rule) must land
// a stored entry carrying the lifted severity, and that entry must be
// flagged IsEvent — core.IsEvent's threshold is SeverityError, so a lift
// that stopped short of it would prove nothing about the feature's whole
// point: lifted logs must alert. A second, non-matching log is asserted
// to stay at its original INFO severity and non-event, so the fake isn't
// lifting unconditionally.
func TestLift_MatchingBodyBecomesEvent(t *testing.T) {
	fw := &fakeWriter{}
	d := fakeDropper{lift: func(l core.Log) (core.Log, int64) {
		if l.Severity <= core.SeverityInfo && strings.Contains(l.Body, "OOM") {
			l.Severity = core.SeverityError
			return l, 7
		}
		return l, 0
	}}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, d, Options{})

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	if err := p.Enqueue([]core.Log{
		{ProjectID: 1, Time: time.Now(), Severity: core.SeverityInfo, Body: "worker killed: OOM"},
		{ProjectID: 1, Time: time.Now(), Severity: core.SeverityInfo, Body: "worker started normally"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	drainCtx, dcancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dcancel()
	if err := p.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	cancel()

	batches := fw.snapshot()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("got %v batches, want 1 batch of 2 entries", batches)
	}

	lifted := batches[0][0]
	if lifted.Log.Severity != core.SeverityError {
		t.Errorf("lifted entry severity = %v, want SeverityError", lifted.Log.Severity)
	}
	if !lifted.IsEvent {
		t.Error("lifted entry not flagged IsEvent — lifted logs must alert")
	}

	unmatched := batches[0][1]
	if unmatched.Log.Severity != core.SeverityInfo {
		t.Errorf("non-matching entry severity = %v, want SeverityInfo (unchanged)", unmatched.Log.Severity)
	}
	if unmatched.IsEvent {
		t.Error("non-matching entry unexpectedly flagged IsEvent")
	}
}

// TestProcess_PanicBodyBecomesEvent confirms panic-prefix severity
// detection runs even with body-parse disabled: it must not be gated by
// the structured-parsing toggles, since ParseStructuredBody handles JSON/
// logfmt bodies while a raw panic dump is neither.
func TestProcess_PanicBodyBecomesEvent(t *testing.T) {
	fw := &fakeWriter{}
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{DisableBodyParse: true})

	entry, keep := p.process(core.Log{
		ProjectID: 1,
		Severity:  core.SeverityInfo,
		Body:      "panic: runtime error: invalid memory address\n\ngoroutine 1 [running]:\nmain.main()",
	})
	if !keep {
		t.Fatal("record dropped")
	}
	if !entry.IsEvent {
		t.Fatal("panic record should be an event")
	}
	if entry.Fingerprint == "" || entry.Title == "" {
		t.Errorf("event not annotated: fingerprint=%q title=%q", entry.Fingerprint, entry.Title)
	}
	if entry.Log.Severity != core.SeverityFatal {
		t.Errorf("severity = %v, want fatal", entry.Log.Severity)
	}
}

// TestNopDropper_PreservesPlan1Behavior pins that NopDropper never drops
// and always parses — every pre-noise-controls pipeline test uses it via
// New's call sites above, so this is a direct, explicit pin of the
// contract those tests otherwise only exercise indirectly.
func TestNopDropper_PreservesPlan1Behavior(t *testing.T) {
	if drop, ruleID := (NopDropper{}).Decide(core.Log{}); drop || ruleID != 0 {
		t.Errorf("NopDropper.Decide = (%v, %d), want (false, 0)", drop, ruleID)
	}
	if !(NopDropper{}).ParseBodies(1) {
		t.Error("NopDropper.ParseBodies = false, want true")
	}
}

func TestProcessStripsANSI(t *testing.T) {
	w := &fakeWriter{}
	p := New(w, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{FlushEvery: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	logs := []core.Log{
		{ProjectID: 1, Time: time.Now(), Severity: core.SeverityInfo,
			Body: "2026/08/08 22:18:20 \x1b[31;1mrepo.go:22 \x1b[35;1mrecord not found"},
		// Panic lift must fire even when ANSI precedes the prefix —
		// this is the case DetectPanicSeverity missed before normalize.
		{ProjectID: 1, Time: time.Now(), Severity: core.SeverityInfo,
			Body: "\x1b[31mpanic: boom"},
	}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	eventually(t, time.Second, func() bool { return w.totalEntries() == 2 })

	var entries []store.Entry
	for _, b := range w.snapshot() {
		entries = append(entries, b...)
	}
	if got := entries[0].Log.Body; got != "2026/08/08 22:18:20 repo.go:22 record not found" {
		t.Errorf("body not stripped: %q", got)
	}
	if entries[0].Log.Attrs["ansi.red"] != "true" {
		t.Errorf("ansi.red hint missing, attrs = %v", entries[0].Log.Attrs)
	}
	if entries[1].Log.Body != "panic: boom" {
		t.Errorf("panic body not stripped: %q", entries[1].Log.Body)
	}
	if entries[1].Log.Severity != core.SeverityFatal {
		t.Errorf("panic behind ANSI not lifted to FATAL, got %v", entries[1].Log.Severity)
	}
	if !entries[1].IsEvent {
		t.Error("stripped panic should be an event")
	}
}

// TestProcessStripsANSI_DoesNotMutateCallerAttrs proves that when a log with
// a non-nil Attrs map is stripped of ANSI and marked with ansi.red, the
// original caller's map is not mutated — the pipeline clones it first.
func TestProcessStripsANSI_DoesNotMutateCallerAttrs(t *testing.T) {
	w := &fakeWriter{}
	p := New(w, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{FlushEvery: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	// Create a log with non-nil Attrs map and red ANSI in body.
	originalAttrs := map[string]string{
		"user_id":  "42",
		"trace_id": "abc123",
	}
	log := core.Log{
		ProjectID: 1,
		Time:      time.Now(),
		Severity:  core.SeverityInfo,
		Body:      "\x1b[31mError happened\x1b[0m",
		Attrs:     originalAttrs,
	}

	if err := p.Enqueue([]core.Log{log}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	eventually(t, time.Second, func() bool { return w.totalEntries() == 1 })

	batches := w.snapshot()
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Fatalf("expected 1 entry, got %d", w.totalEntries())
	}
	storedEntry := batches[0][0]

	// The stored entry's Attrs should have ansi.red and the original attrs.
	if storedEntry.Log.Attrs["ansi.red"] != "true" {
		t.Errorf("stored entry missing ansi.red, attrs = %v", storedEntry.Log.Attrs)
	}
	if storedEntry.Log.Attrs["user_id"] != "42" {
		t.Errorf("stored entry user_id = %q, want 42", storedEntry.Log.Attrs["user_id"])
	}
	if storedEntry.Log.Attrs["trace_id"] != "abc123" {
		t.Errorf("stored entry trace_id = %q, want abc123", storedEntry.Log.Attrs["trace_id"])
	}

	// The original caller's map must NOT have been mutated.
	if _, ok := originalAttrs["ansi.red"]; ok {
		t.Errorf("caller's original map was mutated: has ansi.red key")
	}
	if len(originalAttrs) != 2 {
		t.Errorf("caller's original map length changed: got %d, want 2", len(originalAttrs))
	}
	if originalAttrs["user_id"] != "42" || originalAttrs["trace_id"] != "abc123" {
		t.Errorf("caller's original map contents were modified")
	}
}
