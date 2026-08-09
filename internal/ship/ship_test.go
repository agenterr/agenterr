package ship

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/ship/buffer"
	"github.com/agenterr/agenterr/internal/ship/docker"
	"github.com/agenterr/agenterr/internal/ship/process"
	"github.com/agenterr/agenterr/internal/ship/sender"
)

// --- stub docker surface ------------------------------------------------
//
// Task 3 already proves the real docker.Client's wire framing against a
// fake daemon (internal/ship/docker's unix-socket test harness). What this
// package's orchestrator owns is the WIRING: routing lines from whatever
// implements dockerSurface into the right per-container joiner, advancing
// SetSince, and reacting to start/die events. A hand-written stub exercises
// that wiring directly without re-standing-up a fake Docker Engine API.

type stubDocker struct {
	mu         sync.Mutex
	containers []docker.Container
	logs       map[string]chan process.Line
	events     chan docker.Event
}

func newStubDocker() *stubDocker {
	return &stubDocker{logs: map[string]chan process.Line{}, events: make(chan docker.Event, 8)}
}

func (s *stubDocker) Ping(context.Context) error { return nil }

func (s *stubDocker) Containers(context.Context) ([]docker.Container, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]docker.Container, len(s.containers))
	copy(out, s.containers)
	return out, nil
}

func (s *stubDocker) Events(context.Context) (<-chan docker.Event, error) {
	return s.events, nil
}

func (s *stubDocker) Logs(_ context.Context, id string, _ time.Time) (<-chan process.Line, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.logs[id]
	if !ok {
		return nil, fmt.Errorf("stubDocker: no log stream registered for %s", id)
	}
	return ch, nil
}

func (s *stubDocker) addContainer(ct docker.Container, lines chan process.Line) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.containers = append(s.containers, ct)
	s.logs[ct.ID] = lines
}

// --- test helpers --------------------------------------------------------

func openTestSpool(t *testing.T) *buffer.Spool {
	t.Helper()
	sp, err := buffer.Open(t.TempDir(), 512<<20)
	if err != nil {
		t.Fatalf("buffer.Open: %v", err)
	}
	t.Cleanup(func() { _ = sp.Close() })
	return sp
}

// drainRecords reads and unmarshals every currently-spooled record without
// acking it.
func drainRecords(t *testing.T, sp *buffer.Spool) []wireRecord {
	t.Helper()
	raw, _, err := sp.Next(1000)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	out := make([]wireRecord, len(raw))
	for i, r := range raw {
		if err := json.Unmarshal(r, &out[i]); err != nil {
			t.Fatalf("unmarshal record %d: %v (raw=%s)", i, err, r)
		}
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stubSender is a senderRunner that never talks over the network — it's
// used by orchestrator tests that only care about what lands in the spool
// (already proven correct/complete by the sender package's own tests), not
// about end-to-end delivery.
type stubSender struct {
	runCalled chan struct{}
	once      sync.Once
}

func newStubSender() *stubSender { return &stubSender{runCalled: make(chan struct{})} }

func (s *stubSender) Run(ctx context.Context, _ *buffer.Spool) {
	s.once.Do(func() { close(s.runCalled) })
	<-ctx.Done()
}

func (s *stubSender) Stats() (int64, int64, string) { return 0, 0, "" }

// --- Behavior 6: docker lines -> per-container joiner -> spool; SetSince --

func TestOrchestratorDockerLinesJoinedAndSetSinceAdvances(t *testing.T) {
	sp := openTestSpool(t)
	dc := newStubDocker()
	linesA := make(chan process.Line, 8)
	dc.addContainer(docker.Container{ID: "c1", Name: "web-1", Labels: map[string]string{"com.docker.compose.service": "web"}}, linesA)

	cfg := Config{Docker: true, JoinWindowMS: 20}
	snd := newStubSender()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = run(ctx, cfg, sp, snd, dc); close(runDone) }()

	<-snd.runCalled // pipeline is up

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	linesA <- process.Line{Text: "hello", Time: t1}
	linesA <- process.Line{Text: "world", Time: t2}
	close(linesA) // simulate the container's log stream ending (death)

	// Wait for both records to actually land — Since("c1") can already read
	// t2 one event-loop iteration before the eof-triggered flush appends
	// the second (still-pending-at-that-point) record, so gating on Since
	// alone would race; the record count is what the assertions below
	// actually depend on.
	waitFor(t, 5*time.Second, func() bool {
		return len(drainRecords(t, sp)) == 2
	})
	since, ok := sp.Since("c1")
	if !ok || !since.Equal(t2) {
		t.Errorf("Since(c1) = %v, %v; want %v, true", since, ok, t2)
	}

	recs := drainRecords(t, sp)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2 (%+v)", len(recs), recs)
	}
	if recs[0].Service != "web" || recs[0].Message != "hello" {
		t.Errorf("record 0 = %+v", recs[0])
	}
	if recs[1].Service != "web" || recs[1].Message != "world" {
		t.Errorf("record 1 = %+v", recs[1])
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}
}

func TestOrchestratorDockerMultilineJoinedIntoOneRecord(t *testing.T) {
	sp := openTestSpool(t)
	dc := newStubDocker()
	lines := make(chan process.Line, 8)
	dc.addContainer(docker.Container{ID: "c1", Name: "app", Labels: nil}, lines)

	cfg := Config{Docker: true, JoinWindowMS: 20}
	snd := newStubSender()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = run(ctx, cfg, sp, snd, dc); close(runDone) }()
	<-snd.runCalled

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A stack-trace-style continuation ("\tat ...") joins with the previous
	// line per the ship semantics doc's continuation rules (process package
	// behavior — this test just proves the orchestrator routes lines to the
	// SAME joiner in order so that rule actually gets to apply).
	lines <- process.Line{Text: "error occurred", Time: base}
	lines <- process.Line{Text: "\tat foo.bar()", Time: base.Add(time.Millisecond)}
	close(lines)

	waitFor(t, 5*time.Second, func() bool {
		return len(drainRecords(t, sp)) == 1
	})

	recs := drainRecords(t, sp)
	want := "error occurred\n\tat foo.bar()"
	if len(recs) != 1 || recs[0].Message != want {
		t.Fatalf("got %+v, want single joined record %q", recs, want)
	}

	cancel()
	<-runDone
}

// --- Behavior 6 cont'd: docker start event adds a tailer ----------------

func TestOrchestratorDockerStartEventAddsTailer(t *testing.T) {
	sp := openTestSpool(t)
	dc := newStubDocker()
	// No containers at startup; one appears via a "start" event.
	cfg := Config{Docker: true, JoinWindowMS: 20}
	snd := newStubSender()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = run(ctx, cfg, sp, snd, dc); close(runDone) }()
	<-snd.runCalled

	lines := make(chan process.Line, 4)
	dc.addContainer(docker.Container{ID: "late1", Name: "late", Labels: nil}, lines)
	dc.events <- docker.Event{Action: "start", ID: "late1"}

	lines <- process.Line{Text: "hi", Time: time.Now()}
	close(lines)

	waitFor(t, 5*time.Second, func() bool {
		return len(drainRecords(t, sp)) == 1
	})
	recs := drainRecords(t, sp)
	if recs[0].Service != "late" || recs[0].Message != "hi" {
		t.Errorf("record = %+v, want service=late message=hi", recs[0])
	}

	cancel()
	<-runDone
}

// --- Behavior 6 cont'd: excluded container is never tailed --------------

func TestOrchestratorExcludedContainerNotTailed(t *testing.T) {
	sp := openTestSpool(t)
	dc := newStubDocker()
	// Unbuffered: a send only succeeds if a tailer goroutine is actually
	// reading — the only reliable way to prove "nobody's tailing this"
	// (a buffered channel would accept the send regardless of a reader).
	lines := make(chan process.Line)
	dc.addContainer(docker.Container{ID: "c1", Name: "worker", Labels: nil}, lines)

	cfg := Config{Docker: true, JoinWindowMS: 20, Exclude: []string{"worker"}}
	snd := newStubSender()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = run(ctx, cfg, sp, snd, dc); close(runDone) }()
	<-snd.runCalled

	sent := make(chan bool, 1)
	go func() {
		select {
		case lines <- process.Line{Text: "should never be read", Time: time.Now()}:
			sent <- true
		case <-time.After(200 * time.Millisecond):
			sent <- false
		}
	}()
	if <-sent {
		t.Fatal("excluded container's log channel was read from — it should never have been tailed")
	}

	cancel()
	<-runDone
}

// --- Behavior 6 cont'd: file source end-to-end through a real sender ----

func TestOrchestratorFileSourceEndToEndThroughRealSender(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logPath, []byte("boot ok\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	var mu sync.Mutex
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip.NewReader: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, _ := io.ReadAll(gz)
		var recs []map[string]any
		if len(body) > 0 {
			if err := json.Unmarshal(body, &recs); err != nil {
				t.Errorf("unmarshal batch: %v", err)
			}
		}
		mu.Lock()
		got = append(got, recs...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sp := openTestSpool(t)
	cfg := Config{
		Files:        []string{logPath + "=app"},
		JoinWindowMS: 20,
	}
	snd := sender.New(sender.Config{URL: srv.URL, Key: "k"})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = run(ctx, cfg, sp, snd, nil); close(runDone) }()

	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1
	})

	mu.Lock()
	if got[0]["service"] != "app" || got[0]["message"] != "boot ok" {
		t.Errorf("delivered record = %+v, want service=app message=%q", got[0], "boot ok")
	}
	mu.Unlock()

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}
}

// --- Behavior: shutdown order — producers/joiners flush before spool closes

func TestOrchestratorShutdownFlushesPendingRecordBeforeReturning(t *testing.T) {
	sp := openTestSpool(t)
	dc := newStubDocker()
	lines := make(chan process.Line, 4)
	dc.addContainer(docker.Container{ID: "c1", Name: "app", Labels: nil}, lines)

	// A long join window: without a flush-on-shutdown, this record would
	// sit pending forever and never make it to the spool at all.
	cfg := Config{Docker: true, JoinWindowMS: 60_000}
	snd := newStubSender()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = run(ctx, cfg, sp, snd, dc); close(runDone) }()
	<-snd.runCalled

	lines <- process.Line{Text: "still pending", Time: time.Now()}
	time.Sleep(30 * time.Millisecond) // let it reach the joiner before we cancel
	cancel()

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after ctx cancel")
	}

	recs := drainRecords(t, sp)
	if len(recs) != 1 || recs[0].Message != "still pending" {
		t.Fatalf("got %+v, want the pending record flushed to the spool on shutdown", recs)
	}
}

// --- self-log line ---------------------------------------------------

func TestSelfLogEmitsCountersAtLeastOnce(t *testing.T) {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	sp := openTestSpool(t)
	snd := newStubSender()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	appendDropped := new(atomic.Int64)
	go func() { runSelfLog(ctx, sp, snd, appendDropped); close(done) }()
	<-done

	if !bytes.Contains(buf.Bytes(), []byte("ship: INFO shipped=")) {
		t.Errorf("self-log output = %q, want a shipped=/buffer_dropped=/... INFO line", buf.String())
	}
	if !bytes.Contains(buf.Bytes(), []byte("append_dropped=")) {
		t.Errorf("self-log output = %q, want an append_dropped= counter", buf.String())
	}
}

func TestSelfLogLineIncludesEveryCounter(t *testing.T) {
	line := selfLogLine(10, 2, 3, 4, "boom")
	want := `ship: INFO shipped=10 buffer_dropped=2 oversized_dropped=3 append_dropped=4 last_error="boom"`
	if line != want {
		t.Errorf("selfLogLine = %q, want %q", line, want)
	}
}

// --- append-path drops are counted, not just logged --------------------

func TestAppendRecordCountsSpoolAppendFailure(t *testing.T) {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	sp := openTestSpool(t)
	// Close the spool out from under appendRecord so spool.Append fails —
	// the simplest way to force that path without a narrow spool interface
	// or wrapper: buffer.Spool.Append writes through the closed file handle
	// and surfaces the OS's "file already closed" error.
	if err := sp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	appendDropped := new(atomic.Int64)
	appendRecord(sp, "web", process.Record{Text: "hello", Time: time.Now()}, appendDropped)

	if got := appendDropped.Load(); got != 1 {
		t.Errorf("appendDropped = %d, want 1", got)
	}
	if !bytes.Contains(buf.Bytes(), []byte("ship: WARN spool append failed")) {
		t.Errorf("log output = %q, want a WARN about the failed append", buf.String())
	}
}

// TestOrchestratorCountsAppendDropsInSelfLog drives the failure through the
// full joiner loop (not just appendRecord directly) so the wiring from
// "a record failed to append" to "the self-log line reflects it" is proven
// end to end, matching how a real spool-write failure (e.g. disk full)
// would surface during a run.
func TestOrchestratorCountsAppendDropsInSelfLog(t *testing.T) {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })

	sp := openTestSpool(t)
	evCh := make(chan sourceEvent, 4)
	appendDropped := new(atomic.Int64)

	loopDone := make(chan struct{})
	go func() {
		runJoinerLoop(sp, evCh, time.Hour, appendDropped)
		close(loopDone)
	}()

	// Close the spool from under the running joiner loop before feeding it
	// a line — every subsequent append attempt now fails.
	if err := sp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	evCh <- sourceEvent{key: "c1", service: "web", line: process.Line{Text: "hello", Time: time.Now()}}
	evCh <- sourceEvent{key: "c1", eof: true} // forces a flush, and thus the failing append, deterministically
	close(evCh)
	<-loopDone

	if got := appendDropped.Load(); got != 1 {
		t.Fatalf("appendDropped = %d, want 1", got)
	}

	line := selfLogLine(0, sp.Dropped(), 0, appendDropped.Load(), "")
	if !bytes.Contains([]byte(line), []byte("append_dropped=1")) {
		t.Errorf("self-log line = %q, want append_dropped=1", line)
	}
}
