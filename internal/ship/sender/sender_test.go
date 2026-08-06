package sender

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/ship/buffer"
)

// --- test harness ------------------------------------------------------

// noSleep is a Sleep replacement that returns instantly (but still checks
// ctx so tests bounding retries via ctx cancellation still terminate),
// making "retry forever" behaviors run in milliseconds instead of minutes.
func noSleep(ctx context.Context, _ time.Duration) {
	select {
	case <-ctx.Done():
	default:
	}
}

// recordingSleep wraps noSleep but also appends every requested duration to
// a shared, mutex-protected slice, so a test can assert the backoff
// sequence without actually waiting it out.
func recordingSleep(got *[]time.Duration, mu *sync.Mutex) func(context.Context, time.Duration) {
	return func(ctx context.Context, d time.Duration) {
		mu.Lock()
		*got = append(*got, d)
		mu.Unlock()
		noSleep(ctx, d)
	}
}

func openSpool(t *testing.T) *buffer.Spool {
	t.Helper()
	s, err := buffer.Open(t.TempDir(), 512<<20)
	if err != nil {
		t.Fatalf("buffer.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func appendRecords(t *testing.T, s *buffer.Spool, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rec, _ := json.Marshal(map[string]string{
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"service":   "web",
			"message":   fmt.Sprintf("line %d", i),
		})
		if err := s.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

// decodeBatch gunzips r's body and unmarshals it as a JSON array of raw
// message objects, returning how many records it contained.
func decodeBatch(t *testing.T, r *http.Request) []map[string]any {
	t.Helper()
	if enc := r.Header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", enc)
	}
	gz, err := gzip.NewReader(r.Body)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gz.Close()
	body, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gunzipped body: %v", err)
	}
	var recs []map[string]any
	if err := json.Unmarshal(body, &recs); err != nil {
		t.Fatalf("unmarshal batch: %v (body=%s)", err, body)
	}
	return recs
}

// --- Behavior 1: batching shape, gzip, auth header, ack on 2xx ---------

func TestSenderBatchingGzipAuthAndAck(t *testing.T) {
	var gotAuth string
	var gotRecords []map[string]any
	var calls int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		gotAuth = r.Header.Get("Authorization")
		gotRecords = decodeBatch(t, r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 3)

	s := New(Config{URL: srv.URL, Key: "sekret", Sleep: noSleep})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		shipped, _, _ := s.Stats()
		if shipped == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for 3 records to ship, got %d", shipped)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if gotAuth != "Bearer sekret" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer sekret")
	}
	if len(gotRecords) != 3 {
		t.Fatalf("server saw %d records, want 3", len(gotRecords))
	}
	if gotRecords[0]["service"] != "web" {
		t.Errorf("record[0].service = %v, want web", gotRecords[0]["service"])
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("server got %d calls, want 1 (all 3 records fit in one batch)", calls)
	}
}

func TestSenderBatchCapped500Records(t *testing.T) {
	var maxSeen int
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recs := decodeBatch(t, r)
		mu.Lock()
		if len(recs) > maxSeen {
			maxSeen = len(recs)
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 620) // > 500, forces at least two batches

	s := New(Config{URL: srv.URL, Key: "k", Sleep: noSleep})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		shipped, _, _ := s.Stats()
		if shipped == 620 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out, shipped=%d", shipped)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if maxSeen > 500 {
		t.Errorf("largest batch had %d records, want <= 500", maxSeen)
	}
}

// --- Behavior 2: 5xx/network retry with backoff, same batch resent -----

func TestSenderRetriesOn5xxThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 4 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		decodeBatch(t, r) // still a well-formed batch on the retry
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 2)

	var got []time.Duration
	var mu sync.Mutex
	s := New(Config{URL: srv.URL, Key: "k", Sleep: recordingSleep(&got, &mu), BackoffBase: time.Second, BackoffMax: 30 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		shipped, _, _ := s.Stats()
		if shipped == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for ship after retries, calls=%d", atomic.LoadInt32(&calls))
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	if atomic.LoadInt32(&calls) != 4 {
		t.Errorf("server got %d calls, want 4 (3 failures + 1 success)", calls)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) < 3 {
		t.Fatalf("recorded %d backoff sleeps, want >= 3", len(got))
	}
	// Exponential: 1s, 2s, 4s (capped at 30s) for attempts 1,2,3.
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("backoff[%d] = %v, want %v", i, got[i], w)
		}
	}
}

func TestSenderRetriesForeverOnPersistentFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 1)

	s := New(Config{URL: srv.URL, Key: "k", Sleep: noSleep})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx timeout")
	}

	shipped, _, lastErr := s.Stats()
	if shipped != 0 {
		t.Errorf("shipped = %d, want 0 (server never succeeded)", shipped)
	}
	if lastErr == "" {
		t.Error("lastErr empty, want a recorded error from the persistent 500s")
	}
}

// --- Behavior 3: 413 split-in-half; single oversize record dropped -----

func TestSender413SplitsBatchInHalf(t *testing.T) {
	var mu sync.Mutex
	var sizes []int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recs := decodeBatch(t, r)
		mu.Lock()
		sizes = append(sizes, len(recs))
		mu.Unlock()
		if len(recs) > 1 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 4)

	s := New(Config{URL: srv.URL, Key: "k", Sleep: noSleep})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		shipped, _, _ := s.Stats()
		if shipped == 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out, shipped=%d", shipped)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	// First call sees all 4 (413), then splits recurse down to batches of 1
	// that each succeed. We just assert every batch of size 1 succeeded and
	// the initial 4-record batch was rejected.
	if sizes[0] != 4 {
		t.Fatalf("first batch size = %d, want 4", sizes[0])
	}
	ones := 0
	for _, sz := range sizes[1:] {
		if sz == 1 {
			ones++
		}
	}
	if ones != 4 {
		t.Errorf("expected all 4 records eventually sent alone (size-1 batches), got %d such calls (sizes=%v)", ones, sizes)
	}
}

func TestSender413SingleRecordDroppedAndCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decodeBatch(t, r)
		w.WriteHeader(http.StatusRequestEntityTooLarge) // always too big, no matter the size
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 1)

	s := New(Config{URL: srv.URL, Key: "k", Sleep: noSleep})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		_, dropped, _ := s.Stats()
		if dropped == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for oversize record to be dropped")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	shipped, dropped, _ := s.Stats()
	if shipped != 0 || dropped != 1 {
		t.Errorf("shipped=%d dropped=%d, want shipped=0 dropped=1", shipped, dropped)
	}

	// Acked-past: spool should now report nothing pending.
	recs, _, err := spool.Next(10)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("spool still has %d unacked records after drop, want 0", len(recs))
	}
}

// --- Behavior 4: 429 backs off 5s -------------------------------------

func TestSender429BacksOff5s(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		decodeBatch(t, r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 1)

	var got []time.Duration
	var mu sync.Mutex
	s := New(Config{URL: srv.URL, Key: "k", Sleep: recordingSleep(&got, &mu), RateLimitBackoff: 5 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		shipped, _, _ := s.Stats()
		if shipped == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for ship after 429")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 || got[0] != 5*time.Second {
		t.Errorf("first backoff = %v, want 5s (got sequence %v)", got, got)
	}
}

// --- Behavior 5: preflight auth (fatal at startup, retry at runtime) ---

func TestPreflightSuccessOn2xxAnd400(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gz, _ := gzip.NewReader(r.Body)
				gotBody, _ = io.ReadAll(gz)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			s := New(Config{URL: srv.URL, Key: "k", Sleep: noSleep})
			if err := s.Preflight(context.Background()); err != nil {
				t.Fatalf("Preflight: %v", err)
			}
			if string(gotBody) != "[]" {
				t.Errorf("preflight body = %q, want []", gotBody)
			}
		})
	}
}

func TestPreflightFatalOn401And403(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprintf("status_%d", code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer srv.Close()

			s := New(Config{URL: srv.URL, Key: "bad-key", Sleep: noSleep})
			err := s.Preflight(context.Background())
			if err == nil {
				t.Fatal("Preflight succeeded, want error naming the auth failure")
			}
		})
	}
}

func TestRuntimeAuthFailureLogsAndKeepsRetryingSameBatch(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		decodeBatch(t, r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	spool := openSpool(t)
	appendRecords(t, spool, 1)

	s := New(Config{URL: srv.URL, Key: "k", Sleep: noSleep})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.Run(ctx, spool); close(done) }()

	deadline := time.After(5 * time.Second)
	for {
		shipped, _, _ := s.Stats()
		if shipped == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out: runtime 401 should retry and eventually succeed, not give up")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done

	_, _, lastErr := s.Stats()
	if lastErr == "" {
		t.Error("lastErr empty, want the 401s to have been recorded (loud auth)")
	}
	if atomic.LoadInt32(&calls) != 3 {
		t.Errorf("calls = %d, want 3 (2x 401 + 1 success), same batch retried each time", calls)
	}
}
