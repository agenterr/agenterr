// This file drives agenterr-ship end to end over the file source: a real
// server subprocess, a real ship subprocess tailing a real file, proving the
// full delivery chain (tail -> join -> spool -> send -> ingest -> REST) with
// no mocks anywhere in the path. It shares package e2e with e2e_test.go and
// reuses that file's harness type/helpers (doJSON/postRaw/doRaw, DTOs,
// testCreateProjectAndKeys) rather than duplicating them — see that file's
// package doc for what's off-limits (no product-internal imports).
//
// Kept in its own file per the ship brief: e2e_test.go is already long, and
// this suite's restart/subprocess choreography doesn't share much shape with
// the single-server-lifetime flow there.
//
//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestShipE2E proves agenterr-ship's file source end to end:
//  1. boot a real server;
//  2. write a log file with an ANSI-colored line, a real multiline Go panic
//     dump (blank line + non-indented frames included — panic-mode join),
//     and a JSON slog error line; run the ship binary as a subprocess with a
//     short join window;
//  3. assert over REST that the panic landed as ONE joined stacktrace
//     record (not fragmented across several), the slog line's severity got
//     lifted (findable via min_severity=error), and no ANSI escape bytes
//     survived into any stored body;
//  4. kill the server, append more lines to the file, restart the server on
//     the same DB, and prove the appended lines still arrive — the spool
//     survived the outage. Then SIGTERM ship and require a clean exit.
//  5. separately, start ship against a bad key and require a non-zero exit
//     with stderr naming the auth failure.
func TestShipE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e: skipped under -short")
	}

	// One build serves both roles below: the server subprocess and the ship
	// subprocess are the same binary, dispatched by argv[0]=="ship" (see
	// cmd/agenterr/main.go's dispatchTarget). Building once here — instead of
	// letting a reused testBuildAndStart build it again per test — is purely
	// a speed optimization; nothing else depends on it.
	binPath := filepath.Join(t.TempDir(), "agenterr")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	buildCmd := exec.CommandContext(buildCtx, "go", "build", "-o", binPath, "github.com/agenterr/agenterr/cmd/agenterr")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("go build: %v", err)
	}

	h := &harness{t: t}

	// Reserve the port once and reuse it across the step-4 restart: ship's
	// --url must keep pointing at a valid address the whole time (see
	// startShipServer's doc for the restart contract).
	addr := reservePort(t)
	dbPath := filepath.Join(t.TempDir(), "agenterr.db")

	startShipServer(h, binPath, addr, dbPath)
	t.Cleanup(func() { stopShipServer(h) })

	t.Run("create project and keys", h.testCreateProjectAndKeys)
	if t.Failed() {
		t.Fatal("ship e2e: cannot continue past project/key creation failure")
	}

	// ---- 2: write the log file, start ship tailing it ----

	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "svc-ship.log")

	const ansiLine = "\x1b[31mERROR something broke\x1b[0m"
	// A real Go panic dump: a blank line and a non-indented "exit status 2"
	// trailer both interleave with the indented frames below. Pre-amendment
	// (non-panic-mode) joining would end the record at the blank line — see
	// the "panic joined into one record" subtest below for the assertion
	// that would fail under that older behavior.
	panicLines := []string{
		"panic: runtime error: index out of range [3] with length 3",
		"",
		"goroutine 1 [running]:",
		"main.processOrder(...)",
		"\t/app/internal/orders/process.go:88 +0x1b4",
		"main.main()",
		"\t/app/cmd/svc-ship/main.go:22 +0x45",
		"exit status 2",
	}
	const slogErrorLine = `{"level":"error","msg":"downstream dependency unavailable","request_id":"e2e-ship-1"}`

	// Order matters here: panic mode continues a record through EVERY
	// subsequent line — including one that would otherwise start fresh —
	// until the join-window flush (see process.Joiner.Feed's panicMode
	// short-circuit). Putting the slog JSON line after the panic dump would
	// glue it onto the still-open panic record instead of letting it start
	// its own, which would both break this line's structured-body lifting
	// and falsely inflate what "joined" means for the panic assertion
	// below. The panic dump is last, so it's the one left pending for the
	// join-window ticker to flush.
	lines := append([]string{ansiLine, slogErrorLine}, panicLines...)
	initial := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write log file: %v", err)
	}

	shipDataDir := filepath.Join(t.TempDir(), "ship-data")
	ship := startShip(t, binPath, h.baseURL, h.ingestKey, logPath, shipDataDir)
	t.Cleanup(func() { killIfRunning(ship) })

	// ---- 3: assert via REST ----

	// The panic body is asserted via log search, not issue grouping: per the
	// ship semantics doc, ship never sends a severity, and the server only
	// derives one from a structured (JSON/logfmt) body — see
	// internal/core/structured.go's ParseStructuredBody and
	// internal/core/detect.go's IsEvent. A raw, uncaught Go panic dump is
	// neither JSON nor logfmt (it's exactly the free-text shape panic-mode
	// joining exists to keep intact), so it lands at the default INFO
	// severity and never becomes a grouped issue on its own — that's a real,
	// current product boundary, not a gap in this test. What ship's
	// panic-mode join is actually responsible for, and what this proves end
	// to end, is that the whole dump — blank line and non-indented frames
	// included — arrives as ONE stored record instead of fragmenting.
	t.Run("panic joined into one record", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		var logs []logDTO
		for time.Now().Before(deadline) {
			logs = nil
			h.doJSON(t, "GET", "/api/v1/logs?q=index+out+of+range", h.adminKey, nil, 200, &logs)
			if len(logs) > 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if len(logs) == 0 {
			t.Fatalf("panic record never appeared within deadline")
		}
		if len(logs) != 1 {
			t.Fatalf("expected exactly one stored record for the panic dump (one joined record, not several), got %d: %+v", len(logs), logs)
		}
		body := logs[0].Body

		// The load-bearing assertion: everything after the blank line —
		// the second frame and the "exit status 2" trailer — must be part
		// of the SAME stored body. Without panic-mode joining (the
		// pre-amendment behavior the ship semantics doc calls out), the
		// blank line ends the record early and this text would live in a
		// separate, unrelated log entry instead.
		for _, want := range []string{
			"goroutine 1 [running]:",
			"process.go:88",
			"main.go:22",
			"exit status 2",
		} {
			if !strings.Contains(body, want) {
				t.Errorf("panic body missing joined content %q — panic-mode join did not fold post-blank-line frames into one record; full body:\n%s", want, body)
			}
		}
		if strings.Contains(body, "\x1b") {
			t.Errorf("panic body unexpectedly contains a raw ESC byte: %q", body)
		}
	})

	t.Run("ANSI-colored line stored without escape codes", func(t *testing.T) {
		var logs []logDTO
		h.doJSON(t, "GET", "/api/v1/logs?q=something+broke", h.adminKey, nil, 200, &logs)
		if len(logs) == 0 {
			t.Fatalf("expected the ANSI-colored line to be searchable by its plain-text content")
		}
		for _, l := range logs {
			if strings.Contains(l.Body, "\x1b[") {
				t.Errorf("stored body retains an ANSI escape sequence: %q", l.Body)
			}
			if !strings.Contains(l.Body, "ERROR something broke") {
				t.Errorf("stored body missing expected plain text: %q", l.Body)
			}
		}
	})

	t.Run("slog error line severity-lifted", func(t *testing.T) {
		deadline := time.Now().Add(10 * time.Second)
		var logs []logDTO
		for time.Now().Before(deadline) {
			logs = nil
			h.doJSON(t, "GET", "/api/v1/logs?q=downstream+dependency&min_severity=error", h.adminKey, nil, 200, &logs)
			if len(logs) > 0 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if len(logs) == 0 {
			t.Fatal("slog JSON line not searchable by lifted severity=error — structured-body severity lifting did not fire")
		}
		if logs[0].Body != "downstream dependency unavailable" {
			t.Errorf("body = %q, want the lifted msg field", logs[0].Body)
		}
		if logs[0].Severity != "ERROR" {
			t.Errorf("severity = %q, want ERROR (lifted from the JSON body's level field)", logs[0].Severity)
		}
	})

	// ---- 4: kill the server, append lines, restart on the same DB,
	// confirm spool survival, then SIGTERM ship ----

	t.Run("spool survives a server outage, restart, and clean ship shutdown", func(t *testing.T) {
		stopShipServer(h)

		// Append while the server is down: the file tailer keeps polling
		// (pollInterval defaults to 500ms, see internal/ship/file/tail.go)
		// and the joiner keeps appending to the on-disk spool regardless of
		// whether the sender can currently reach anything — buffering is
		// upstream of the network entirely, per the ship semantics doc's
		// backpressure order (sources -> buffer -> sender).
		const appendedLine = `{"level":"error","msg":"order retry exhausted after outage","request_id":"e2e-ship-2"}`
		f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("open log file to append: %v", err)
		}
		if _, err := f.WriteString(appendedLine + "\n"); err != nil {
			t.Fatalf("append log line: %v", err)
		}
		f.Close()

		// Restart quickly (well under one backoff cycle) on the SAME
		// address and SAME db path: ship's --url stays valid throughout
		// (this test never gave ship a new one), and the server's earlier
		// admin key, project, and ingest key are all still valid because
		// they live in the DB file, not in server memory. Restarting fast
		// keeps this test's timing bounded — the sender's exponential
		// backoff (1s..30s, see internal/ship/sender/sender.go) resets to
		// 1s on the first failure after the prior run's idle (no-attempt)
		// state, so a sub-second outage means ship's very next retry
		// (after a ~1s wait) already lands on the freshly-restarted
		// server, rather than risking a much longer backoff window.
		time.Sleep(300 * time.Millisecond)
		startShipServer(h, binPath, addr, dbPath)

		deadline := time.Now().Add(20 * time.Second)
		var logs []logDTO
		for time.Now().Before(deadline) {
			logs = nil
			h.doJSON(t, "GET", "/api/v1/logs?q=retry+exhausted&min_severity=error", h.adminKey, nil, 200, &logs)
			if len(logs) > 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if len(logs) == 0 {
			t.Fatal("appended-during-outage line never arrived after restart — spool did not survive/resume")
		}
		if logs[0].Body != "order retry exhausted after outage" {
			t.Errorf("body = %q, want the lifted msg field", logs[0].Body)
		}

		if err := ship.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatalf("send SIGTERM to ship: %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- ship.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("ship did not exit 0 after SIGTERM: %v\nstdout:\n%s\nstderr:\n%s",
					err, ship.stdout.String(), ship.stderr.String())
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("ship did not exit within 10s of SIGTERM\nstdout:\n%s\nstderr:\n%s",
				ship.stdout.String(), ship.stderr.String())
		}
	})
}

// TestShipBadKeyE2E is a separate top-level test (rather than a subtest of
// TestShipE2E) because it needs its own short-lived server and doesn't
// depend on anything TestShipE2E sets up — keeping it independent means a
// failure here can't be blamed on ordering with the longer suite above.
func TestShipBadKeyE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e: skipped under -short")
	}

	binPath := filepath.Join(t.TempDir(), "agenterr")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	buildCmd := exec.CommandContext(buildCtx, "go", "build", "-o", binPath, "github.com/agenterr/agenterr/cmd/agenterr")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("go build: %v", err)
	}

	h := &harness{t: t}
	addr := reservePort(t)
	dbPath := filepath.Join(t.TempDir(), "agenterr.db")
	startShipServer(h, binPath, addr, dbPath)
	t.Cleanup(func() { stopShipServer(h) })

	t.Run("create project and keys", h.testCreateProjectAndKeys)
	if t.Failed() {
		t.Fatal("ship bad-key e2e: cannot continue past project/key creation failure")
	}

	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "svc-badkey.log")
	if err := os.WriteFile(logPath, []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write log file: %v", err)
	}
	shipDataDir := filepath.Join(t.TempDir(), "ship-data")

	badKey := h.ingestKey + "-not-the-real-key"
	cmd := exec.Command(binPath, "ship",
		"--file", logPath+"=svc-badkey",
		"--url", h.baseURL,
		"--key", badKey,
		"--data-dir", shipDataDir,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		t.Fatalf("expected ship to exit non-zero for a bad key, got exit 0; stderr:\n%s", stderr.String())
	}
	// Preflight's error path (internal/ship/sender/sender.go) names the
	// rejection and the HTTP status explicitly: "<url> rejected key (HTTP
	// 401/403) — check --key/AGENTERR_SHIP_KEY". Assert on that shape
	// rather than the literal word "auth" so the test isn't coupled to
	// exact wording, only to it clearly identifying an auth failure.
	got := stderr.String()
	if !strings.Contains(got, "rejected key") || !strings.Contains(got, "--key") {
		t.Errorf("stderr does not name the auth failure: %q", got)
	}
	if !strings.Contains(got, "401") && !strings.Contains(got, "403") {
		t.Errorf("stderr does not carry the rejecting HTTP status: %q", got)
	}
}

// ---- helpers local to the ship suite ----

// reservePort binds and immediately frees a port, mirroring
// testBuildAndStart's technique (see that function's comment for the
// inherent TOCTOU caveat) — needed here, on top of what testBuildAndStart
// already does, because step 4 must restart the server on the exact same
// address ship was already configured to send to.
func reservePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return addr
}

// startShipServer starts (or restarts) the real agenterr server binary
// bound to addr against dbPath, waits for /healthz, and — on a first boot
// only — parses the printed admin key into h.adminKey. On a restart (dbPath
// already initialized) no bootstrap block is printed, so the previously
// parsed admin key, project, and keys all remain valid: they live in the DB
// file, not the process.
func startShipServer(h *harness, binPath, addr, dbPath string) {
	t := h.t
	t.Helper()

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"AGENTERR_LISTEN="+addr,
		"AGENTERR_DB="+dbPath,
		"AGENTERR_ADMIN_PASSWORD=e2e-ship-test-password",
	)
	h.stdout = &syncBuffer{}
	h.stderr = &syncBuffer{}
	cmd.Stdout = h.stdout
	cmd.Stderr = h.stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start agenterr server: %v", err)
	}
	h.cmd = cmd
	h.baseURL = "http://" + addr

	if err := h.waitHealthy(5 * time.Second); err != nil {
		t.Fatalf("agenterr server never became healthy: %v\nstdout:\n%s\nstderr:\n%s",
			err, h.stdout.String(), h.stderr.String())
	}

	if h.adminKey == "" {
		key, err := parseAdminKey(h.stdout.String())
		if err != nil {
			t.Fatalf("parse admin key from stdout: %v\nstdout:\n%s", err, h.stdout.String())
		}
		h.adminKey = key
	}
}

// stopShipServer kills the currently-running server process started by
// startShipServer, if any, and waits for it to be reaped. It's safe to call
// more than once (e.g. once explicitly for the restart, once again from
// t.Cleanup) since a nil/already-exited process is a no-op.
func stopShipServer(h *harness) {
	if h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = h.cmd.Process.Kill()
	_, _ = h.cmd.Process.Wait()
}

// shipProcess bundles a running ship subprocess with its captured output,
// so assertions after a failure (a non-zero exit, a timeout) can print what
// it actually said.
type shipProcess struct {
	*exec.Cmd
	stdout *syncBuffer
	stderr *syncBuffer
}

// startShip runs `agenterr ship --file logPath=SERVICE ...` as a subprocess
// against the given server, with a short join window so the test doesn't
// have to wait out the production 1000ms default.
func startShip(t *testing.T, binPath, url, key, logPath, dataDir string) *shipProcess {
	t.Helper()
	cmd := exec.Command(binPath, "ship",
		"--file", logPath+"=svc-ship",
		"--url", url,
		"--key", key,
		"--data-dir", dataDir,
		"--join-window-ms", "300",
	)
	sp := &shipProcess{Cmd: cmd, stdout: &syncBuffer{}, stderr: &syncBuffer{}}
	cmd.Stdout = sp.stdout
	cmd.Stderr = sp.stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agenterr ship: %v", err)
	}
	return sp
}

// killIfRunning is the leak-proofing safety net for the ship subprocess,
// mirroring harness.shutdown's role for the server: if a subtest fails
// before the happy-path SIGTERM+Wait in TestShipE2E runs, this still reaps
// the child instead of leaking it past the test.
func killIfRunning(sp *shipProcess) {
	if sp == nil || sp.Process == nil {
		return
	}
	_ = sp.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = sp.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = sp.Process.Kill()
		<-done
	}
}
