package docker

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- test harness -----------------------------------------------------------

// fakeDaemon serves a fake Docker Engine API over a unix socket in
// t.TempDir; httptest.Server can't bind a unix socket directly, so this
// wires an http.Server to a net.Listen("unix", ...) by hand.
type fakeDaemon struct {
	sockPath string
	srv      *http.Server
}

func startFakeDaemon(t *testing.T, handler http.Handler) *fakeDaemon {
	t.Helper()
	// Deliberately not t.TempDir(): its path embeds the (sometimes long)
	// test name, and unix socket paths are capped at ~104 bytes on macOS
	// (sockaddr_un.sun_path) — a short os.MkdirTemp dir keeps us under that.
	dir, err := os.MkdirTemp("", "dsock")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sockPath := filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go srv.Serve(ln) //nolint:errcheck

	t.Cleanup(func() {
		_ = srv.Close()
	})

	return &fakeDaemon{sockPath: sockPath, srv: srv}
}

// writeFrame writes one Docker multiplex frame header+payload as separate
// Write+Flush calls, so a frame boundary can be forced to land mid-header or
// mid-payload from the client's point of view (the whole point of behavior 3).
// hdrSplit is the byte offset within the 8-byte header where the write is
// torn in two.
func writeFrame(w http.ResponseWriter, flusher http.Flusher, streamType byte, payload []byte, hdrSplit int) {
	hdr := make([]byte, 8)
	hdr[0] = streamType
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(payload)))

	// Split the header itself across two writes to exercise a torn header.
	w.Write(hdr[:hdrSplit]) //nolint:errcheck
	flusher.Flush()
	w.Write(hdr[hdrSplit:]) //nolint:errcheck
	flusher.Flush()

	// Split the payload roughly in half across two writes to exercise a
	// torn payload.
	mid := len(payload) / 2
	if mid > 0 {
		w.Write(payload[:mid]) //nolint:errcheck
		flusher.Flush()
	}
	w.Write(payload[mid:]) //nolint:errcheck
	flusher.Flush()
}

// --- Behavior 1: Containers/Ping happy path; ServiceName table -------------

func TestPingAndContainers(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/_ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/containers/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Built as a variable (rather than an inline multi-line literal)
		// so the nolint directive below lands on the same physical line as
		// the Write call it's suppressing errcheck for.
		body := []byte(`[
			{"Id":"abc123","Names":["/web-1"],"Labels":{"com.docker.compose.service":"web"}},
			{"Id":"def456","Names":["/standalone"],"Labels":{}}
		]`)
		w.Write(body) //nolint:errcheck
	})
	d := startFakeDaemon(t, mux)
	c := NewClient(d.sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	cts, err := c.Containers(ctx)
	if err != nil {
		t.Fatalf("Containers: %v", err)
	}
	if len(cts) != 2 {
		t.Fatalf("got %d containers, want 2", len(cts))
	}
	if cts[0].ID != "abc123" || cts[0].Name != "web-1" {
		t.Errorf("container 0 = %+v", cts[0])
	}
	if cts[1].ID != "def456" || cts[1].Name != "standalone" {
		t.Errorf("container 1 = %+v", cts[1])
	}
}

func TestServiceName(t *testing.T) {
	tests := []struct {
		name string
		ct   Container
		want string
	}{
		{
			name: "swarm label wins over compose and name",
			ct: Container{
				Name: "task.1.xyz",
				Labels: map[string]string{
					"com.docker.swarm.service.name": "orders-api",
					"com.docker.compose.service":    "orders",
				},
			},
			want: "orders-api",
		},
		{
			name: "compose label used when no swarm label",
			ct: Container{
				Name:   "myproj_web_1",
				Labels: map[string]string{"com.docker.compose.service": "web"},
			},
			want: "web",
		},
		{
			name: "falls back to container name when no labels",
			ct: Container{
				Name:   "standalone",
				Labels: map[string]string{},
			},
			want: "standalone",
		},
		{
			name: "sanitizes disallowed runes to underscore",
			ct: Container{
				Name:   "svc",
				Labels: map[string]string{"com.docker.compose.service": "my.svc name!"},
			},
			want: "my_svc_name_",
		},
		{
			name: "nil labels map falls back to name",
			ct: Container{
				Name:   "n0_dashes-ok",
				Labels: nil,
			},
			want: "n0_dashes-ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ServiceName(tt.ct)
			if got != tt.want {
				t.Errorf("ServiceName(%+v) = %q, want %q", tt.ct, got, tt.want)
			}
		})
	}
}

// --- Behavior 2: Selected matrix --------------------------------------------

func TestSelected(t *testing.T) {
	tests := []struct {
		name    string
		svcName string
		labels  map[string]string
		only    []string
		exclude []string
		want    bool
	}{
		{
			name:    "no only, no exclude: selected",
			svcName: "web",
			want:    true,
		},
		{
			name:    "excluded",
			svcName: "web",
			exclude: []string{"web", "worker"},
			want:    false,
		},
		{
			name:    "not in exclude list: selected",
			svcName: "web",
			exclude: []string{"worker"},
			want:    true,
		},
		{
			name:    "only set, name present: selected",
			svcName: "web",
			only:    []string{"web", "worker"},
			want:    true,
		},
		{
			name:    "only set, name absent: not selected",
			svcName: "db",
			only:    []string{"web", "worker"},
			want:    false,
		},
		{
			name:    "only wins over exclude being empty (name in only)",
			svcName: "web",
			only:    []string{"web"},
			exclude: nil,
			want:    true,
		},
		{
			name:    "both set: only minus exclude",
			svcName: "web",
			only:    []string{"web", "worker"},
			exclude: []string{"web"},
			want:    false,
		},
		{
			name:    "agenterr.ignore=true always excludes, even if in only",
			svcName: "web",
			labels:  map[string]string{"agenterr.ignore": "true"},
			only:    []string{"web"},
			want:    false,
		},
		{
			name:    "agenterr.ignore=false does not exclude",
			svcName: "web",
			labels:  map[string]string{"agenterr.ignore": "false"},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Selected(tt.svcName, tt.labels, tt.only, tt.exclude)
			if got != tt.want {
				t.Errorf("Selected(%q, %v, only=%v, exclude=%v) = %v, want %v",
					tt.svcName, tt.labels, tt.only, tt.exclude, got, tt.want)
			}
		})
	}
}

// --- Behavior 3: multiplexed frames spanning chunk boundaries --------------

func TestLogsDemuxAcrossChunkBoundaries(t *testing.T) {
	const cid = "cnt1"
	line1 := fmt.Sprintf("%s stdout line one\n", "2026-08-06T12:00:00.000000001Z")
	line2 := fmt.Sprintf("%s stderr line two\n", "2026-08-06T12:00:00.000000002Z")
	line3 := fmt.Sprintf("%s stdout line three\n", "2026-08-06T12:00:00.000000003Z")

	mux := http.NewServeMux()
	mux.HandleFunc("/containers/"+cid+"/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"Config":{"Tty":false}}`)) //nolint:errcheck
	})
	mux.HandleFunc("/containers/"+cid+"/logs", func(w http.ResponseWriter, _ *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		// Interleave stdout(1) and stderr(2) frames, each header and
		// payload torn across separate writes+flushes.
		// line1's header is torn at offset 6 — inside the 4-byte
		// big-endian length field (bytes 4-7) — rather than at the
		// stream-type/reserved boundary; the others tear at the more
		// obvious offset 3 for variety.
		writeFrame(w, flusher, 1, []byte(line1), 6)
		writeFrame(w, flusher, 2, []byte(line2), 3)
		writeFrame(w, flusher, 1, []byte(line3), 3)
		// End of stream: handler returns, connection closes -> client EOF.
	})
	d := startFakeDaemon(t, mux)
	c := NewClient(d.sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.Logs(ctx, cid, time.Time{})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}

	var got []string
	for l := range ch {
		got = append(got, l.Text)
		if l.Time.IsZero() {
			t.Errorf("line %q has zero Time", l.Text)
		}
	}

	want := []string{"stdout line one", "stderr line two", "stdout line three"}
	if len(got) != len(want) {
		t.Fatalf("got %d lines %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
	if c.UnparsedTimestamps() != 0 {
		t.Errorf("UnparsedTimestamps = %d, want 0", c.UnparsedTimestamps())
	}
}

func TestLogsUnparsableTimestampPrefixKeptWholeAndCounted(t *testing.T) {
	const cid = "cnt2"
	raw := "not-a-timestamp still one line\n"

	mux := http.NewServeMux()
	mux.HandleFunc("/containers/"+cid+"/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"Config":{"Tty":false}}`)) //nolint:errcheck
	})
	mux.HandleFunc("/containers/"+cid+"/logs", func(w http.ResponseWriter, _ *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		writeFrame(w, flusher, 1, []byte(raw), 3)
	})
	d := startFakeDaemon(t, mux)
	c := NewClient(d.sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.Logs(ctx, cid, time.Time{})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var got []string
	for l := range ch {
		got = append(got, l.Text)
	}
	if len(got) != 1 || got[0] != "not-a-timestamp still one line" {
		t.Fatalf("got %v, want whole unparsable line kept intact", got)
	}
	if c.UnparsedTimestamps() != 1 {
		t.Errorf("UnparsedTimestamps = %d, want 1", c.UnparsedTimestamps())
	}
}

// --- Behavior 4: Events surface start/die, channel closes on ctx cancel ----

func TestEventsAndCtxCancel(t *testing.T) {
	blockUntil := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"Action":"start","Actor":{"ID":"c1"}}` + "\n")) //nolint:errcheck
		flusher.Flush()
		w.Write([]byte(`{"Action":"die","Actor":{"ID":"c1"}}` + "\n")) //nolint:errcheck
		flusher.Flush()
		// Now block (simulating a long-lived stream) until the request's
		// context is cancelled by the client, or the test times out.
		select {
		case <-r.Context().Done():
		case <-blockUntil:
		}
	})
	d := startFakeDaemon(t, mux)
	defer close(blockUntil)
	c := NewClient(d.sockPath)

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.Events(ctx)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	ev1 := <-ch
	if ev1.Action != "start" || ev1.ID != "c1" {
		t.Errorf("event 1 = %+v, want start/c1", ev1)
	}
	ev2 := <-ch
	if ev2.Action != "die" || ev2.ID != "c1" {
		t.Errorf("event 2 = %+v, want die/c1", ev2)
	}

	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatalf("expected channel closed after ctx cancel, got another event")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for events channel to close after ctx cancel")
	}
}

// --- Behavior 5: TTY container yields lines from a raw (non-demuxed) stream

func TestLogsTTYRawStream(t *testing.T) {
	const cid = "cnt3"
	body := "2026-08-06T12:00:00.000000001Z tty line one\n" +
		"2026-08-06T12:00:00.000000002Z tty line two\n"

	mux := http.NewServeMux()
	mux.HandleFunc("/containers/"+cid+"/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"Config":{"Tty":true}}`)) //nolint:errcheck
	})
	mux.HandleFunc("/containers/"+cid+"/logs", func(w http.ResponseWriter, _ *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		// Raw stream: no 8-byte frame headers at all, just split mid-line
		// across writes to prove line splitting works independent of
		// chunk boundaries.
		w.Write([]byte(body[:20])) //nolint:errcheck
		flusher.Flush()
		w.Write([]byte(body[20:])) //nolint:errcheck
		flusher.Flush()
	})
	d := startFakeDaemon(t, mux)
	c := NewClient(d.sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.Logs(ctx, cid, time.Time{})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var got []string
	for l := range ch {
		got = append(got, l.Text)
	}
	want := []string{"tty line one", "tty line two"}
	if len(got) != len(want) {
		t.Fatalf("got %d lines %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// --- Finding 1 fix: watchCtxCancel exits on normal stream end (EOF)
// without parking on ctx.Done() forever, and does close the body on an
// actual ctx cancel. -------------------------------------------------------

func TestWatchCtxCancel_ExitsOnDoneWithoutClosingBody(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // never fired before done closes below — proves EOF path, not cancel path

	done := make(chan struct{})
	var closed atomic.Bool
	exited := watchCtxCancel(ctx, done, func() { closed.Store(true) })

	close(done) // simulates the read loop finishing normally (EOF)

	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher goroutine did not exit after done closed — leak on normal EOF")
	}
	if closed.Load() {
		t.Error("closeFn must not be called when the stream ended on its own (done), not via ctx cancel")
	}
}

func TestWatchCtxCancel_ClosesBodyOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	defer close(done)

	var closed atomic.Bool
	exited := watchCtxCancel(ctx, done, func() { closed.Store(true) })

	cancel()

	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher goroutine did not exit after ctx cancel")
	}
	if !closed.Load() {
		t.Error("closeFn must be called on ctx cancel")
	}
}

// TestLogsEOFDoesNotLeakWatcherGoroutine is a behavioral regression test for
// the same leak, driven through the real Logs path end to end: with a
// long-lived ctx that is never cancelled, repeatedly streaming a container's
// logs to completion (EOF) must not accumulate one parked watcher goroutine
// per call.
func TestLogsEOFDoesNotLeakWatcherGoroutine(t *testing.T) {
	const cid = "cnt-leak"
	ts := "2026-08-06T12:00:00.000000001Z"

	mux := http.NewServeMux()
	mux.HandleFunc("/containers/"+cid+"/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"Config":{"Tty":false}}`)) //nolint:errcheck
	})
	mux.HandleFunc("/containers/"+cid+"/logs", func(w http.ResponseWriter, _ *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		writeFrame(w, flusher, 1, []byte(ts+" line\n"), 3)
		// handler returns -> connection closes -> client sees EOF, not ctx-cancel
	})
	d := startFakeDaemon(t, mux)
	c := NewClient(d.sockPath)

	// Deliberately never cancelled: if the watcher only exits on
	// ctx.Done(), it would stay parked for every one of these calls.
	ctx := context.Background()
	transport := c.httpc.Transport.(*http.Transport)

	transport.CloseIdleConnections() // drop any pooled keep-alive conn goroutines before the baseline
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const iterations = 20
	for i := 0; i < iterations; i++ {
		ch, err := c.Logs(ctx, cid, time.Time{})
		if err != nil {
			t.Fatalf("Logs iteration %d: %v", i, err)
		}
		for v := range ch {
			_ = v // drain to EOF close
		}
	}
	transport.CloseIdleConnections() // same: keep-alive pooling is unrelated to the watcher fix under test

	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		cur := runtime.NumGoroutine()
		if cur <= baseline+2 { // small slack for scheduler/runtime bookkeeping goroutines
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine count %d after %d EOF'd Logs calls, baseline %d — watcher goroutines leaking",
				cur, iterations, baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- Finding 2: a line split across a frame boundary (including mid-way
// through a multibyte UTF-8 rune) must reassemble correctly. -------------

func TestLogsCrossFrameLineSplitMidUTF8Rune(t *testing.T) {
	const cid = "cnt-splitline"
	ts := "2026-08-06T12:00:00.000000001Z"
	text := "hello 日本語 world" // contains 3-byte-encoded multibyte runes
	full := []byte(ts + " " + text + "\n")

	// Land the split one byte into the 3-byte UTF-8 encoding of '日', so
	// frame A ends mid-rune and frame B continues it — the critical case:
	// every other test's frames carry whole lines.
	idx := strings.Index(string(full), "日")
	if idx < 0 {
		t.Fatal("test setup: expected rune not found in encoded line")
	}
	splitAt := idx + 1

	mux := http.NewServeMux()
	mux.HandleFunc("/containers/"+cid+"/json", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"Config":{"Tty":false}}`)) //nolint:errcheck
	})
	mux.HandleFunc("/containers/"+cid+"/logs", func(w http.ResponseWriter, _ *http.Request) {
		flusher := w.(http.Flusher)
		w.WriteHeader(http.StatusOK)
		// Two separate Docker frames whose payloads split the SAME line
		// (and a multibyte rune within it) — not just one frame torn
		// across writes like the other tests.
		writeFrame(w, flusher, 1, full[:splitAt], 3)
		writeFrame(w, flusher, 1, full[splitAt:], 3)
	})
	d := startFakeDaemon(t, mux)
	c := NewClient(d.sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.Logs(ctx, cid, time.Time{})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	var got []string
	for l := range ch {
		got = append(got, l.Text)
	}
	if len(got) != 1 {
		t.Fatalf("got %d lines %v, want exactly 1 reassembled line", len(got), got)
	}
	if got[0] != text {
		t.Errorf("reassembled line = %q, want %q", got[0], text)
	}
}
