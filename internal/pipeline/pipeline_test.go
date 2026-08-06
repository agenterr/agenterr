package pipeline

import (
	"context"
	"errors"
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

func (f *fakeWriter) WriteBatch(_ context.Context, e []store.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failN > 0 {
		f.failN--
		return errWrite
	}
	batch := make([]store.Entry, len(e))
	copy(batch, e)
	f.batches = append(f.batches, batch)
	return nil
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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 1000, FlushEvery: time.Hour, MaxBatch: 500})

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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 100, FlushEvery: 30 * time.Millisecond, MaxBatch: 500})

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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 10, FlushEvery: time.Hour, MaxBatch: 500})
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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 1000, FlushEvery: time.Hour, MaxBatch: 500})

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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 1000, FlushEvery: 10 * time.Millisecond, MaxBatch: 500})

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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 100, FlushEvery: time.Hour, MaxBatch: 5})

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
	p := New(fw, core.DefaultGrouper{}, NopNotifier{}, Options{BufferSize: 20000, FlushEvery: time.Hour, MaxBatch: 10})

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

