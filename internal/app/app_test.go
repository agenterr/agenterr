package app

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

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
			_ = resp.Body.Close()
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
	_ = w.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}
	return string(out)
}

var generatedPasswordRe = regexp.MustCompile(`Generated admin password: (\S+)`)

// TestBootstrapPrintsOnceOnFirstRun covers three of the ledger-ruled
// bootstrap behaviors end to end against a real sqlite file:
//
//   - (a) first run (no admin password configured) prints the generated
//     password, a minted agt_admin_ key, and a setup URL, exactly once.
//   - (b) a restart against the *same* DB prints nothing (the admin key
//     and password hash both already exist) — and, the actual point of
//     persisting the hash rather than regenerating it, the *first run's
//     password* still authenticates against the restarted app's /login.
//   - (c) a file-only restore (copy the db file to a new path, no
//     marker file involved since there is none) also prints nothing on
//     boot, and does not mint a second admin key.
func TestBootstrapPrintsOnceOnFirstRun(t *testing.T) {
	cfg := testConfig(t, "") // empty AdminPassword -> generated + printed

	var out1 string
	app1 := fxtest.New(t, Module, fx.Replace(cfg))
	out1 = captureStdout(t, func() { app1.RequireStart() })
	app1.RequireStop()

	if !strings.Contains(out1, "Generated admin password:") {
		t.Fatalf("first run: missing generated password line, got:\n%s", out1)
	}
	if !strings.Contains(out1, "agt_admin_") {
		t.Errorf("first run: missing admin key (agt_admin_ prefix), got:\n%s", out1)
	}
	if !strings.Contains(out1, "Setup URL:") {
		t.Errorf("first run: missing setup URL, got:\n%s", out1)
	}

	m := generatedPasswordRe.FindStringSubmatch(out1)
	if m == nil {
		t.Fatalf("could not extract generated password from first-run output:\n%s", out1)
	}
	firstRunPassword := m[1]

	// (b) Second run, same DB path, fresh free port so the two apps'
	// listeners don't collide.
	cfg2 := cfg
	cfg2.ListenAddr = freePort(t)

	app2 := fxtest.New(t, Module, fx.Replace(cfg2))
	out2 := captureStdout(t, func() { app2.RequireStart() })
	if out2 != "" {
		t.Errorf("second run against same DB: expected no bootstrap output, got:\n%s", out2)
	}

	if err := loginAs(cfg2.ListenAddr, firstRunPassword); err != nil {
		t.Errorf("second run: first run's password no longer authenticates: %v", err)
	}
	app2.RequireStop()

	// (c) File-only restore: copy the db file (this is exactly what a
	// Litestream restore or a plain file copy produces — no marker file
	// exists anywhere, because there is no marker mechanism) to a new
	// path and boot against the copy.
	restorePath := filepath.Join(t.TempDir(), "restored.db")
	copyFile(t, cfg.DBPath, restorePath)

	cfg3 := cfg
	cfg3.DBPath = restorePath
	cfg3.ListenAddr = freePort(t)

	app3 := fxtest.New(t, Module, fx.Replace(cfg3))
	out3 := captureStdout(t, func() { app3.RequireStart() })
	app3.RequireStop()

	if out3 != "" {
		t.Errorf("file-only restore: expected no bootstrap output, got:\n%s", out3)
	}

	n := countAdminKeys(t, restorePath)
	if n != 1 {
		t.Errorf("file-only restore: %d admin keys, want exactly 1 (no re-mint)", n)
	}
}

// loginAs POSTs password to /login on addr and returns nil only if the
// server responds with the 303 SeeOther that Login handlers.Submit
// returns on success (see internal/web/handlers/login.go) — a 401 means
// the password was rejected. The client must not auto-follow that
// redirect: doing so with no cookie jar would drop the just-set session
// cookie and land back on GET /login (200), masking a real success as
// what looks like a failure.
func loginAs(addr, password string) error {
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.PostForm("http://"+addr+"/login", url.Values{"password": {password}})
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusSeeOther {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("login status = %d, want 303 (body=%s)", resp.StatusCode, body)
	}
	return nil
}

// copyFile copies src to dst byte-for-byte, standing in for a Litestream
// restore or a plain `cp` of the database file.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

// countAdminKeys opens a completely independent connection to the sqlite
// file at path and counts rows in keys where kind='admin' directly. This
// intentionally bypasses *sqlite.DB (which exposes no count-of-keys
// method — HasAdminKey only reports existence, which is all production
// code needs) so the test can assert the precise "exactly one, no
// re-mint" claim without adding a test-only method to production code.
func countAdminKeys(t *testing.T, path string) int {
	t.Helper()
	conn, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("sql.Open(%s): %v", path, err)
	}
	defer func() { _ = conn.Close() }()

	var n int
	if err := conn.QueryRow(`SELECT COUNT(*) FROM keys WHERE kind = 'admin'`).Scan(&n); err != nil {
		t.Fatalf("count admin keys: %v", err)
	}
	return n
}
