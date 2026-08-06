// Package sender implements the agenterr-ship HTTP sender: it drains wire
// records from a buffer.Spool in batches, gzips and POSTs them to the
// ingest endpoint, and Acks the spool once the server confirms delivery.
// Retry/backoff, batch splitting on 413, and the startup auth preflight all
// live here — see the ship semantics doc for the exact table this
// implements.
package sender

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agenterr/agenterr/internal/ship/buffer"
)

const (
	maxBatchRecords = 500
	maxBatchBytes   = 1 << 20 // 1MB pre-gzip
)

// Config configures a Sender. Only URL and Key are required; everything
// else has a production default and exists mainly so tests can inject fast
// fakes for the timing knobs (backoffs, sleeps, request timeout) without
// making the retry-forever behaviors actually take minutes to run.
type Config struct {
	URL string
	Key string

	// HTTPClient is the client used for POSTs. Defaults to http.DefaultClient.
	HTTPClient *http.Client

	// Sleep is called to wait out backoffs; it must return early if ctx is
	// done. Defaults to a context-aware time.Sleep. Tests inject a no-op (or
	// duration-recording) stand-in so retry-forever tests run instantly.
	Sleep func(ctx context.Context, d time.Duration)

	// BackoffBase/BackoffMax bound the exponential backoff applied to
	// 5xx/network errors and to runtime 401/403 (which retry the same
	// batch forever rather than failing it). Defaults: 1s / 30s.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// RateLimitBackoff is the fixed backoff applied on a 429. Default: 5s.
	RateLimitBackoff time.Duration

	// EmptyPollInterval is how long the Run loop sleeps after finding
	// nothing to send before checking the spool again. Default: 200ms.
	EmptyPollInterval time.Duration

	// RequestTimeout bounds a single POST attempt. It is deliberately NOT
	// derived from the ctx passed to Run/Preflight: on shutdown, Run stops
	// starting new attempts once ctx is done, but an attempt already in
	// flight is allowed to finish (or hit this timeout) on its own rather
	// than being yanked mid-request. Default: 30s.
	RequestTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.HTTPClient == nil {
		c.HTTPClient = http.DefaultClient
	}
	if c.Sleep == nil {
		c.Sleep = defaultSleep
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = 30 * time.Second
	}
	if c.RateLimitBackoff <= 0 {
		c.RateLimitBackoff = 5 * time.Second
	}
	if c.EmptyPollInterval <= 0 {
		c.EmptyPollInterval = 200 * time.Millisecond
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	return c
}

// defaultSleep waits d or returns early if ctx is done.
func defaultSleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Sender drains a buffer.Spool and ships batches to the ingest endpoint.
// One Sender per spool; Run is meant to be called once from its own
// goroutine.
type Sender struct {
	cfg Config

	shipped   int64
	oversized int64
	lastErr   atomic.Value // string

	mu sync.Mutex // guards nothing concurrent today, but Stats reads under it for a consistent snapshot
}

// New returns a Sender configured by cfg (zero-value fields take their
// documented defaults).
func New(cfg Config) *Sender {
	return &Sender{cfg: cfg.withDefaults()}
}

// Stats returns cumulative counters for the periodic self-log line: records
// successfully shipped, records dropped as un-splittable oversized single
// records, and the last error message seen (empty if none yet).
func (s *Sender) Stats() (shipped, oversizedDropped int64, lastErr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	le, _ := s.lastErr.Load().(string)
	return atomic.LoadInt64(&s.shipped), atomic.LoadInt64(&s.oversized), le
}

func (s *Sender) recordErr(err error) {
	log.Printf("ship: ERROR sender: %v", err)
	s.lastErr.Store(err.Error())
}

// Preflight proves the configured key is accepted by the server before the
// orchestrator starts spending effort: it POSTs an empty batch. A 2xx or a
// 400 (still proves auth — the server parsed and rejected an empty array on
// its own merits, not on the Authorization header) is success. A 401/403 is
// returned as an error for main to report and exit non-zero on; any other
// response or a network error is also returned as an error (transient
// network trouble at startup is still worth failing loudly rather than
// silently limping into the retry-forever runtime path).
func (s *Sender) Preflight(ctx context.Context) error {
	resp, err := s.post(ctx, nil)
	if err != nil {
		return fmt.Errorf("ship: preflight request to %s failed: %w", s.cfg.URL, err)
	}
	defer drain(resp)

	switch {
	case resp.StatusCode/100 == 2, resp.StatusCode == http.StatusBadRequest:
		return nil
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("ship: %s rejected key (HTTP %d) — check --key/AGENTERR_SHIP_KEY", s.cfg.URL, resp.StatusCode)
	default:
		return fmt.Errorf("ship: preflight to %s: unexpected HTTP %d", s.cfg.URL, resp.StatusCode)
	}
}

// Run drains spool and ships batches until ctx is done. It never returns an
// error for ordinary operation — send failures are retried per the backoff
// table, forever, since the buffer is the durability layer and this loop's
// only job is to keep trying. It returns once ctx is done and any batch
// in-flight selection has settled.
func (s *Sender) Run(ctx context.Context, spool *buffer.Spool) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := s.nextBatchSize(spool)
		if err != nil {
			s.recordErr(fmt.Errorf("reading spool: %w", err))
			s.cfg.Sleep(ctx, s.cfg.EmptyPollInterval)
			continue
		}
		if n == 0 {
			s.cfg.Sleep(ctx, s.cfg.EmptyPollInterval)
			continue
		}
		s.sendBatch(ctx, spool, n)
	}
}

// nextBatchSize peeks at the spool's unacked head and returns how many
// records the next POST should include: up to maxBatchRecords, further
// capped so the pre-gzip body stays at or under maxBatchBytes (a lone
// record already over the cap is still sent alone — the server's 413 path
// handles it from there).
func (s *Sender) nextBatchSize(spool *buffer.Spool) (int, error) {
	all, _, err := spool.Next(maxBatchRecords)
	if err != nil {
		return 0, err
	}
	if len(all) == 0 {
		return 0, nil
	}
	size := 0
	for i, r := range all {
		size += len(r) + 1 // +1 for the joining comma/bracket
		if size > maxBatchBytes && i > 0 {
			return i, nil
		}
	}
	return len(all), nil
}

// sendBatch sends exactly the n oldest unacked records, retrying per the
// backoff table until they're delivered (2xx), split-and-retried (413,
// multi-record), dropped (413, single record), or ctx is cancelled. It
// always re-reads the batch from spool rather than caching it across
// retries, so a retry after a split always reflects the current unacked
// head.
func (s *Sender) sendBatch(ctx context.Context, spool *buffer.Spool, n int) {
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		batch, cursor, err := spool.Next(n)
		if err != nil {
			s.recordErr(fmt.Errorf("reading spool: %w", err))
			s.cfg.Sleep(ctx, s.cfg.EmptyPollInterval)
			continue
		}
		if len(batch) == 0 {
			return // already acked out from under us (shouldn't happen, single reader, but be safe)
		}

		resp, postErr := s.post(ctx, batch)
		if postErr != nil {
			s.recordErr(fmt.Errorf("POST %s: %w", s.cfg.URL, postErr))
			attempt++
			s.cfg.Sleep(ctx, s.backoff(attempt))
			continue
		}

		switch {
		case resp.StatusCode/100 == 2:
			drain(resp)
			if err := spool.Ack(cursor); err != nil {
				s.recordErr(fmt.Errorf("ack: %w", err))
			}
			atomic.AddInt64(&s.shipped, int64(len(batch)))
			return

		case resp.StatusCode == http.StatusRequestEntityTooLarge:
			drain(resp)
			if len(batch) == 1 {
				// Un-splittable: drop it, count it, and Ack past it so the
				// sender doesn't spin forever on one record the server will
				// never accept.
				if err := spool.Ack(cursor); err != nil {
					s.recordErr(fmt.Errorf("ack after drop: %w", err))
				}
				atomic.AddInt64(&s.oversized, 1)
				log.Printf("ship: WARN dropping oversized record (413 on a single-record batch, %d bytes)", len(batch[0]))
				return
			}
			half := len(batch) / 2
			s.sendBatch(ctx, spool, half)
			if ctx.Err() != nil {
				return
			}
			s.sendBatch(ctx, spool, n-half)
			return

		case resp.StatusCode == http.StatusTooManyRequests:
			drain(resp)
			s.recordErr(fmt.Errorf("429 rate limited by %s", s.cfg.URL))
			s.cfg.Sleep(ctx, s.cfg.RateLimitBackoff)
			continue

		case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden:
			drain(resp)
			// Loud auth at runtime: log ERROR and keep retrying the same
			// batch — never silently spin, never drop it either (the key
			// may be fixed by an operator without losing buffered data).
			s.recordErr(fmt.Errorf("%s rejected key (HTTP %d) — keeping and retrying batch", s.cfg.URL, resp.StatusCode))
			attempt++
			s.cfg.Sleep(ctx, s.backoff(attempt))
			continue

		default:
			drain(resp)
			s.recordErr(fmt.Errorf("POST %s: unexpected HTTP %d", s.cfg.URL, resp.StatusCode))
			attempt++
			s.cfg.Sleep(ctx, s.backoff(attempt))
			continue
		}
	}
}

// backoff returns the exponential backoff duration for the given 1-based
// attempt count, bounded by [BackoffBase, BackoffMax].
func (s *Sender) backoff(attempt int) time.Duration {
	d := s.cfg.BackoffBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= s.cfg.BackoffMax {
			return s.cfg.BackoffMax
		}
	}
	return d
}

// buildWireArray joins already-marshaled JSON record objects into one JSON
// array, verbatim — no re-marshaling, per the wire-format contract (records
// are marshaled once, at spool-append time).
func buildWireArray(batch [][]byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, rec := range batch {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(rec)
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

// post gzips batch (nil/empty encodes as "[]") and POSTs it to the ingest
// endpoint with the auth header. The request uses its own timeout derived
// from context.Background(), independent of ctx, so a caller cancelling ctx
// for shutdown lets an in-flight attempt finish rather than aborting it
// mid-request; ctx is still accepted so callers can bound Preflight.
func (s *Sender) post(ctx context.Context, batch [][]byte) (*http.Response, error) {
	body := buildWireArray(batch)

	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(body); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
	defer cancel()
	// Still honor an already-cancelled caller ctx immediately (e.g.
	// Preflight, which has no reason to outlive its own caller).
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.cfg.URL+"/api/v1/ingest", bytes.NewReader(gz.Bytes()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.Key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	return s.cfg.HTTPClient.Do(req)
}

func drain(resp *http.Response) {
	if resp == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}
