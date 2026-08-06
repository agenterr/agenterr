package server

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/agenterr/agenterr/internal/api"
	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/config"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/ingest"
	"github.com/agenterr/agenterr/internal/ingest/jsonapi"
	"github.com/agenterr/agenterr/internal/ingest/otlp"
	"github.com/agenterr/agenterr/internal/mcp"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/web"
)

// newTestDeps constructs a full Deps over a real temp-file sqlite store and
// the real edges, mirroring how cmd/agenterr will wire things. This is a
// composition test, not a re-test of each edge's own behavior.
func newTestDeps(t *testing.T) (Deps, *sqlite.DB) {
	t.Helper()

	db, err := sqlite.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	hash, err := bcrypt.GenerateFromPassword([]byte("adminpass"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	a := auth.New(db, hash)

	pipe := pipeline.New(db, core.DefaultGrouper{}, pipeline.NopNotifier{}, pipeline.Options{})

	deps := Deps{
		Cfg:       config.Config{ListenAddr: ":0"},
		Store:     db,
		Pipe:      pipe,
		Ingesters: []ingest.Ingester{jsonapi.New(pipe, 0), otlp.New(pipe, 0)},
		Auth:      a,
		API:       api.New(db, db),
		MCP:       mcp.New(db, db),
		Web:       web.New(db, db, a),
	}
	return deps, db
}

func TestHealthz_OK(t *testing.T) {
	deps, _ := newTestDeps(t)
	srv := New(deps)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["status"] != "ok" {
		t.Errorf("status field = %v, want ok", got["status"])
	}
	if got["db"] != "ok" {
		t.Errorf("db field = %v, want ok", got["db"])
	}
	if _, ok := got["pipeline_depth"]; !ok {
		t.Errorf("missing pipeline_depth field: %v", got)
	}
}

func TestHealthz_DBDown(t *testing.T) {
	deps, db := newTestDeps(t)
	srv := New(deps)

	_ = db.Close()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["status"] != "degraded" {
		t.Errorf("status field = %v, want degraded", got["status"])
	}
	if got["db"] != "error" {
		t.Errorf("db field = %v, want error", got["db"])
	}
	body := rec.Body.String()
	if strings.Contains(body, "sql") || strings.Contains(body, "database is closed") {
		t.Errorf("body leaked internal error detail: %s", body)
	}
}

func TestPanicRecovery(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/boom", http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic("kaboom")
	}))
	h := withMiddleware(mux, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rec := httptest.NewRecorder()

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic escaped middleware: %v", r)
			}
		}()
		h.ServeHTTP(rec, req)
	}()

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("content-type = %q, want json", ct)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not valid JSON: %v; body=%s", err, rec.Body.String())
	}

	// process survives: a second request on the same handler still works
	req2 := httptest.NewRequest(http.MethodGet, "/healthz-not-mounted", nil)
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusNotFound {
		t.Fatalf("second request status = %d, want 404 (server still alive)", rec2.Code)
	}
}

// TestErrAbortHandlerPropagates proves recoverMiddleware re-panics
// http.ErrAbortHandler rather than turning it into a 500 JSON body: it must
// reach net/http's own connection-level recovery (which silently aborts
// the connection, per the stdlib's documented contract for streaming
// handlers) instead of being swallowed here. Exercised over a real
// httptest.Server, since http.ErrAbortHandler's behavior is a property of
// net/http's connection handling — an httptest.ResponseRecorder has no
// connection to abort.
func TestErrAbortHandlerPropagates(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/abort", http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	var logBuf bytes.Buffer
	h := withMiddleware(mux, slog.New(slog.NewTextHandler(&logBuf, nil)))

	ts := httptest.NewServer(h)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/abort")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected a connection error from the aborted handler, got a response (status %d)", resp.StatusCode)
	}
	if strings.Contains(err.Error(), "500") {
		t.Errorf("client error suggests a 500 response was sent, want silent abort: %v", err)
	}

	// process survives: the same server answers a normal request fine.
	resp2, err2 := http.Get(ts.URL + "/healthz-not-mounted")
	if err2 != nil {
		t.Fatalf("server did not survive the aborted handler: %v", err2)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (server still alive)", resp2.StatusCode)
	}

	if strings.Contains(logBuf.String(), `"error"`) && strings.Contains(logBuf.String(), "500") {
		t.Errorf("recover logged the abort as a normal panic: %s", logBuf.String())
	}
}

func TestIngestMountedRequiresAuth(t *testing.T) {
	deps, _ := newTestDeps(t)
	srv := New(deps)

	req := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
}

func TestWebMountedRedirectsUnauthenticated(t *testing.T) {
	deps, _ := newTestDeps(t)
	srv := New(deps)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303; body=%s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Errorf("Location = %q, want /login", loc)
	}
}

func TestAPIMountedRequiresAuth(t *testing.T) {
	deps, _ := newTestDeps(t)
	srv := New(deps)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/issues", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequestLogMiddleware(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	mux := http.NewServeMux()
	mux.Handle("/thing", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("hi"))
	}))
	h := withMiddleware(mux, logger)

	req := httptest.NewRequest(http.MethodGet, "/thing?secret=shouldnotleak", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	out := buf.String()
	if !strings.Contains(out, "GET") {
		t.Errorf("log missing method: %s", out)
	}
	if !strings.Contains(out, "/thing") {
		t.Errorf("log missing path: %s", out)
	}
	if !strings.Contains(out, "418") {
		t.Errorf("log missing status: %s", out)
	}
	if strings.Contains(out, "secret=shouldnotleak") {
		t.Errorf("log leaked query string: %s", out)
	}
}

var _ store.Store = (*sqlite.DB)(nil)
