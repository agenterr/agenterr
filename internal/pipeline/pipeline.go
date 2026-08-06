package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// ErrFull is returned by Enqueue when the buffer lacks room for the whole
// slice being enqueued. Callers (edges) map this to HTTP 429.
var ErrFull = errors.New("pipeline: buffer full")

// Options configures a Pipeline. Zero values are replaced with defaults by
// New.
type Options struct {
	BufferSize int           // capacity of the internal log buffer; default 10_000
	FlushEvery time.Duration // max time pending entries wait before a flush; default 200ms
	MaxBatch   int           // batch size that triggers an immediate flush; default 500

	// DisableBodyParse turns off structured-body lifting in annotate.
	// The zero value keeps it on: parsing is the default behavior, and
	// the flag exists as an escape hatch.
	DisableBodyParse bool
}

const (
	defaultBufferSize = 10_000
	defaultFlushEvery = 200 * time.Millisecond
	defaultMaxBatch   = 500
)

// Pipeline is the single path to disk for log writes: edges call Enqueue,
// and a single writer goroutine (Run) drains the buffer, annotates each log
// (event detection, fingerprint, title) and batches writes to the store.
type Pipeline struct {
	w store.Writer
	g Grouper
	n Notifier
	o Options

	buf chan core.Log

	// mu guards stopped and serializes the capacity check + send in Enqueue
	// against the shutdown transition in Run, so the all-or-nothing
	// reservation below is race-free: len(buf) can only grow while mu is
	// held (by another Enqueue) or shrink (Run only ever removes), so the
	// capacity check taken under mu remains valid for the sends that follow
	// it in the same critical section.
	mu      sync.Mutex
	stopped bool

	// unflushed counts logs accepted by Enqueue but not yet durably
	// written or dropped by Run. Drain polls this to know when it is safe
	// to return.
	unflushed int64
}

// New constructs a Pipeline. Zero-valued Options fields fall back to
// defaults (BufferSize 10_000, FlushEvery 200ms, MaxBatch 500).
func New(w store.Writer, g Grouper, n Notifier, o Options) *Pipeline {
	if o.BufferSize <= 0 {
		o.BufferSize = defaultBufferSize
	}
	if o.FlushEvery <= 0 {
		o.FlushEvery = defaultFlushEvery
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = defaultMaxBatch
	}
	return &Pipeline{
		w:   w,
		g:   g,
		n:   n,
		o:   o,
		buf: make(chan core.Log, o.BufferSize),
	}
}

// Enqueue accepts logs into the buffer, never blocking. It is all-or-
// nothing: either every log in the slice fits and is accepted, or none of
// them are (ErrFull), so callers get a clean "the whole batch was
// rejected" story instead of having to figure out which logs made it in.
// After Run's context has been canceled, Enqueue always returns ErrFull —
// the pipeline is shutting down and cannot accept more work.
func (p *Pipeline) Enqueue(logs []core.Log) error {
	if len(logs) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return ErrFull
	}
	if len(p.buf)+len(logs) > cap(p.buf) {
		return ErrFull
	}
	// Increment before sending: Drain reads unflushed without taking mu,
	// so if the count were bumped after the sends, a concurrent Drain
	// could observe unflushed==0 (and report "done") while these logs are
	// already visible in buf — or even already picked up and flushed by
	// Run, whose decrement would then race ahead of this increment and
	// briefly (and wrongly) suggest a deficit. Incrementing first, still
	// under mu, guarantees unflushed accounts for a log before it can
	// possibly become visible to Run.
	atomic.AddInt64(&p.unflushed, int64(len(logs)))
	// Space is reserved: no other Enqueue can race in (mu held) and Run
	// only ever removes from buf, so these sends cannot block.
	for _, l := range logs {
		p.buf <- l
	}
	return nil
}

// Run is the single writer loop: it drains the buffer, annotates each log,
// and flushes batches to the store either when MaxBatch entries have
// accumulated or when FlushEvery elapses with entries pending. It returns
// once ctx is canceled, after draining any logs still in the buffer and
// performing a final flush.
func (p *Pipeline) Run(ctx context.Context) {
	ticker := time.NewTicker(p.o.FlushEvery)
	defer ticker.Stop()

	pending := make([]store.Entry, 0, p.o.MaxBatch)

	for {
		select {
		case <-ctx.Done():
			p.mu.Lock()
			p.stopped = true
			p.mu.Unlock()

			// Drain whatever already made it into buf before stopped was
			// observed by Enqueue (see the mu comment above for why this
			// is race-free) — those logs were promised a write.
		drain:
			for {
				select {
				case l := <-p.buf:
					pending = append(pending, p.annotate(l))
					if len(pending) >= p.o.MaxBatch {
						p.flush(pending)
						pending = pending[:0]
					}
				default:
					break drain
				}
			}
			p.flush(pending)
			return

		case l := <-p.buf:
			pending = append(pending, p.annotate(l))
			if len(pending) >= p.o.MaxBatch {
				p.flush(pending)
				pending = pending[:0]
				// A MaxBatch-triggered flush just emptied pending, so an
				// already in-flight FlushEvery tick would otherwise fire
				// on a near-empty (or empty) batch shortly after. Reset
				// the window so the timer only fires FlushEvery after
				// this flush, not after whatever was left of the
				// previous window.
				ticker.Reset(p.o.FlushEvery)
			}

		case <-ticker.C:
			if len(pending) > 0 {
				p.flush(pending)
				pending = pending[:0]
			}
		}
	}
}

// Drain blocks until the buffer is empty and any in-flight batch has been
// flushed (written or dropped), or until ctx's deadline passes. It is
// intended for use at shutdown, after canceling Run's context, or in tests
// that want to observe a specific batch land.
func (p *Pipeline) Drain(ctx context.Context) error {
	if atomic.LoadInt64(&p.unflushed) == 0 {
		return nil
	}

	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if atomic.LoadInt64(&p.unflushed) == 0 {
				return nil
			}
		}
	}
}

// Pending reports the number of logs accepted by Enqueue but not yet
// durably written or dropped by Run — i.e. the sum of what's sitting in
// buf and whatever batch Run currently has in flight. Intended for
// lightweight health/diagnostic reporting (see internal/server's healthz),
// not for correctness decisions — Drain is the synchronization primitive
// for that.
func (p *Pipeline) Pending() int {
	return int(atomic.LoadInt64(&p.unflushed))
}

// annotate runs event detection, fingerprinting, and titling for a single
// log, producing the store.Entry the writer persists.
func (p *Pipeline) annotate(l core.Log) store.Entry {
	if !p.o.DisableBodyParse {
		l = core.ParseStructuredBody(l)
	}
	e := store.Entry{Log: l}
	if core.IsEvent(l) {
		e.IsEvent = true
		e.Fingerprint = p.g.Fingerprint(l)
		e.Title = core.Title(l)
	}
	return e
}

// flush writes one batch via a single WriteBatch call and always accounts
// for it in unflushed, whether the write succeeded or not.
//
// On error, the batch is logged and dropped rather than retried: a log
// tracker exists to observe an application's failures, and it must not
// crash-loop (or wedge the single writer goroutine retrying forever) on
// its own storage failures — that would turn a storage hiccup into total
// ingest loss instead of partial loss.
//
// Uses context.Background() rather than Run's ctx so the final flush after
// shutdown (ctx already canceled) still gets a chance to land.
func (p *Pipeline) flush(pending []store.Entry) {
	if len(pending) == 0 {
		return
	}

	if err := p.w.WriteBatch(context.Background(), pending); err != nil {
		slog.Error("pipeline: write batch failed, dropping batch", "error", err, "batch_size", len(pending))
	} else {
		for _, e := range pending {
			if e.IsEvent {
				// Fire-and-forget: kept as a synchronous call because
				// NopNotifier is a no-op in v1, but real Notifier
				// implementations MUST NOT block here — this runs on the
				// single writer goroutine, and a slow notifier would stall
				// every subsequent flush.
				p.n.IssueEvent(e)
			}
		}
	}

	atomic.AddInt64(&p.unflushed, -int64(len(pending)))
}
