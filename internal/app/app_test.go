package app

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/agenterr/agenterr/internal/config"
)

// TestModuleGraph proves the fx graph is complete — every provider's
// dependencies are satisfied by some other provider — without running
// any of it. fx.ValidateApp does not invoke constructors, so this is
// safe to run under `go test` even though loadConfig would otherwise
// choke on the test binary's own flags.
func TestModuleGraph(t *testing.T) {
	if err := fx.ValidateApp(Module); err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
}

// freePort binds a TCP listener on an ephemeral port, captures its
// address, and closes it — a small, accepted race (something else could
// grab the port before the app binds it) but the simplest way to get a
// real, addressable port for an httptest-style poll loop.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// testConfig returns a config.Config wired to a temp DB and a free port,
// suitable for fx.Replace-ing the config provider so tests never touch
// loadConfig's os.Args parsing.
func testConfig(t *testing.T, adminPassword string) config.Config {
	t.Helper()
	return config.Config{
		ListenAddr:    freePort(t),
		DBPath:        filepath.Join(t.TempDir(), "agenterr.db"),
		AdminPassword: adminPassword,
		BufferSize:    1000,
		FlushEveryMS:  50,
		MaxBodyBytes:  5 << 20,
	}
}

// TestAppStartsAndServes is the pre-e2e smoke test: build the real graph
// (temp DB, real edges, real HTTP server) over a free port, start it,
// poll /healthz until it answers 200 with the expected shape, then stop
// cleanly.
func TestAppStartsAndServes(t *testing.T) {
	cfg := testConfig(t, "test-admin-password")

	app := fxtest.New(t,
		Module,
		fx.Replace(cfg),
	)

	app.RequireStart()

	url := "http://" + cfg.ListenAddr + "/healthz"
	var body []byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			b, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr == nil && resp.StatusCode == http.StatusOK {
				body = b
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("healthz never returned 200 within deadline (last err=%v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal healthz body %q: %v", body, err)
	}
	if got["status"] != "ok" {
		t.Errorf("status = %v, want ok", got["status"])
	}
	if got["db"] != "ok" {
		t.Errorf("db = %v, want ok", got["db"])
	}
	if _, ok := got["pipeline_depth"]; !ok {
		t.Errorf("missing pipeline_depth field: %v", got)
	}

	app.RequireStop()
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	orig := os.Stdout
	os.Stdout = w

	fn()

	os.Stdout = orig
	w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	return string(out)
}

// TestBootstrapPrintsOnceOnFirstRun runs the app with no admin password
// configured against a fresh DB and checks that the generated password,
// the minted admin key, and a setup URL are all printed exactly once.
// Starting a second app against the *same* DB path must print nothing,
// because the admin key (and its bootstrap marker) already exist.
func TestBootstrapPrintsOnceOnFirstRun(t *testing.T) {
	cfg := testConfig(t, "") // empty AdminPassword -> generated + printed

	out1 := captureStdout(t, func() {
		app := fxtest.New(t, Module, fx.Replace(cfg))
		app.RequireStart()
		app.RequireStop()
	})

	if !strings.Contains(out1, "Generated admin password:") {
		t.Errorf("first run: missing generated password line, got:\n%s", out1)
	}
	if !strings.Contains(out1, "agt_admin_") {
		t.Errorf("first run: missing admin key (agt_admin_ prefix), got:\n%s", out1)
	}
	if !strings.Contains(out1, "Setup URL:") {
		t.Errorf("first run: missing setup URL, got:\n%s", out1)
	}

	// Second run, same DB path (same cfg value, in particular same
	// DBPath and its ".bootstrap" marker), but on a fresh free port so
	// the two apps' listeners don't collide.
	cfg2 := cfg
	cfg2.ListenAddr = freePort(t)

	out2 := captureStdout(t, func() {
		app := fxtest.New(t, Module, fx.Replace(cfg2))
		app.RequireStart()
		app.RequireStop()
	})

	if out2 != "" {
		t.Errorf("second run against same DB: expected no bootstrap output, got:\n%s", out2)
	}
}
