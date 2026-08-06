// Tests for the agenterr-mcp stdio→HTTP proxy. The proxy's core is run(),
// which speaks MCP over a pair of io.ReadCloser/io.WriteCloser (stdin/stdout
// in production, in-memory pipes here) and forwards every request to a
// remote Streamable HTTP MCP server. These tests boot the *real*
// internal/mcp Server (backed by a hand-written fake store, mirroring the
// pattern in internal/mcp/tools_test.go) on httptest, run the proxy against
// it, and drive the proxy with a real SDK client over stdio pipes — proving
// the whole stack: stdio ⇄ proxy ⇄ Streamable HTTP ⇄ real MCP server.
package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	agmcp "github.com/agenterr/agenterr/internal/mcp"
	"github.com/agenterr/agenterr/internal/store"
)

// ---- fakeStore: minimal store.Reader + store.Admin, mirrors
// internal/mcp/tools_test.go's fakeStore ----

type fakeStore struct {
	keys map[string]struct {
		projectID int64
		kind      string
	}
	projects []core.Project
}

func (f *fakeStore) Issues(_ context.Context, _ store.IssueFilter) ([]core.Issue, error) {
	return nil, nil
}

func (f *fakeStore) Issue(_ context.Context, _ int64) (core.Issue, []core.Event, error) {
	return core.Issue{}, nil, store.ErrNotFound
}

func (f *fakeStore) SearchLogs(_ context.Context, _ store.LogFilter) ([]core.Log, error) {
	return nil, nil
}

func (f *fakeStore) LogContext(_ context.Context, _ int64, _ int) ([]core.Log, error) {
	return nil, nil
}

func (f *fakeStore) Stats(_ context.Context, _ store.StatsFilter) (store.Stats, error) {
	return store.Stats{}, nil
}

func (f *fakeStore) CreateProject(_ context.Context, _ string, _ int) (core.Project, error) {
	return core.Project{}, errors.New("unused")
}

func (f *fakeStore) Projects(_ context.Context) ([]core.Project, error) {
	return f.projects, nil
}

func (f *fakeStore) SetIssueStatus(_ context.Context, _ int64, _ core.IssueStatus) error {
	return errors.New("unused")
}

func (f *fakeStore) MintKey(_ context.Context, _ int64, _ string) (string, error) {
	return "", errors.New("unused")
}

func (f *fakeStore) LookupKey(_ context.Context, plaintext string) (int64, string, error) {
	e, ok := f.keys[plaintext]
	if !ok {
		return 0, "", store.ErrNotFound
	}
	return e.projectID, e.kind, nil
}

// newTestServer boots a real internal/mcp Server, wrapped in real key auth,
// on an httptest server. Returns the server URL and a valid api key.
func newTestServer(t *testing.T) (url string, apiKey string) {
	t.Helper()
	fs := &fakeStore{
		keys: map[string]struct {
			projectID int64
			kind      string
		}{
			"agt_api_valid": {projectID: 1, kind: "api"},
		},
		projects: []core.Project{{ID: 1, Name: "demo", Slug: "demo"}},
	}
	a := auth.New(fs, []byte{})
	srv := agmcp.New(fs, fs)
	mux := http.NewServeMux()
	srv.Mount(mux, a)
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)
	return httpSrv.URL + "/mcp", "agt_api_valid"
}

func TestProxy_ListToolsAndCallTool(t *testing.T) {
	url, key := newTestServer(t)

	clientR, proxyW := io.Pipe() // proxy stdout -> client stdin
	proxyR, clientW := io.Pipe() // client stdout -> proxy stdin

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- run(ctx, url, key, proxyR, proxyW)
	}()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	transport := &mcpsdk.IOTransport{Reader: clientR, Writer: clientW}
	cs, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = cs.Close() }()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools.Tools) != 8 {
		names := make([]string, len(tools.Tools))
		for i, tl := range tools.Tools {
			names[i] = tl.Name
		}
		t.Fatalf("got %d tools, want 8: %v", len(tools.Tools), names)
	}

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "list_projects",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(res.Content) == 0 {
		t.Fatalf("empty content")
	}
	tc, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok || tc.Text == "" {
		t.Fatalf("want non-empty text content, got %#v", res.Content)
	}

	_ = cs.Close()
	_ = clientW.Close()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("run() returned error after clean shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after client disconnect")
	}
}

func TestProxy_BadKey_FailsAtStartup(t *testing.T) {
	url, _ := newTestServer(t)

	stdinR, _ := io.Pipe()
	_, stdoutW := io.Pipe()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := run(ctx, url, "agt_api_bogus", stdinR, stdoutW)
	if err == nil {
		t.Fatal("run() with a bad key: want error, got nil")
	}
}

// TestProxy_ContextCancellation proves ctx cancellation (standing in for
// main's signal.NotifyContext on SIGINT/SIGTERM) makes run() return
// cleanly — nil, not ctx.Err() — rather than looking like a proxy
// failure. No client ever connects over stdio; run() must still notice
// the connected *remote* session and its local.Run() stdio loop, tear
// both down, and return promptly once ctx is cancelled.
func TestProxy_ContextCancellation(t *testing.T) {
	url, key := newTestServer(t)

	stdinR, stdinW := io.Pipe()
	_, stdoutW := io.Pipe()
	defer func() { _ = stdinW.Close() }()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- run(ctx, url, key, stdinR, stdoutW) }()

	// Give run() a moment to finish connecting to the remote before
	// cancelling, so this actually exercises the "torn down mid-session"
	// path rather than racing the initial connect.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run() after ctx cancellation: want nil, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after ctx cancellation")
	}
}

// TestProxy_RemoteErrorPropagates calls a tool the remote server doesn't
// have and asserts the failure reaches the local client as a tool-call
// error (either a JSON-RPC-level error or an IsError result — the SDK is
// free to represent an unknown-tool failure either way), and that the
// proxy is still alive afterward: a subsequent tools/list must still
// succeed. This is the "don't crash on remote errors" half of the proxy's
// contract from a mid-session failure, as opposed to the connect-time
// failure covered by TestProxy_BadKey_FailsAtStartup.
func TestProxy_RemoteErrorPropagates(t *testing.T) {
	url, key := newTestServer(t)

	clientR, proxyW := io.Pipe()
	proxyR, clientW := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- run(ctx, url, key, proxyR, proxyW)
	}()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	transport := &mcpsdk.IOTransport{Reader: clientR, Writer: clientW}
	cs, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = cs.Close() }()

	res, err := cs.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "does_not_exist",
		Arguments: map[string]any{},
	})
	if err == nil && (res == nil || !res.IsError) {
		t.Fatalf("CallTool for an unknown tool: want an error or an IsError result, got res=%#v err=%v", res, err)
	}

	// The proxy must still be alive: a subsequent, valid call succeeds.
	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools after a remote error: %v", err)
	}
	if len(tools.Tools) != 8 {
		t.Fatalf("got %d tools after a remote error, want 8", len(tools.Tools))
	}

	_ = cs.Close()
	_ = clientW.Close()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("run() returned error after clean shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return after client disconnect")
	}
}

func TestResolveConfig(t *testing.T) {
	env := func(vals map[string]string) func(string) string {
		return func(k string) string { return vals[k] }
	}

	tests := []struct {
		name    string
		args    []string
		env     map[string]string
		wantURL string
		wantKey string
		wantErr bool
	}{
		{
			name:    "flags win over env",
			args:    []string{"--url", "https://flag.example.com", "--key", "agt_api_flag"},
			env:     map[string]string{"AGENTERR_URL": "https://env.example.com", "AGENTERR_API_KEY": "agt_api_env"},
			wantURL: "https://flag.example.com/mcp",
			wantKey: "agt_api_flag",
		},
		{
			name:    "env fallback when flags unset",
			args:    nil,
			env:     map[string]string{"AGENTERR_URL": "https://env.example.com", "AGENTERR_API_KEY": "agt_api_env"},
			wantURL: "https://env.example.com/mcp",
			wantKey: "agt_api_env",
		},
		{
			name:    "url already has /mcp suffix",
			args:    []string{"--url", "https://flag.example.com/mcp", "--key", "agt_api_flag"},
			wantURL: "https://flag.example.com/mcp",
			wantKey: "agt_api_flag",
		},
		{
			name:    "missing both",
			args:    nil,
			env:     map[string]string{},
			wantErr: true,
		},
		{
			name:    "missing key only",
			args:    []string{"--url", "https://flag.example.com"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := resolveConfig(tt.args, env(tt.env))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveConfig(%v): want error, got nil", tt.args)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveConfig(%v): %v", tt.args, err)
			}
			if cfg.url != tt.wantURL {
				t.Errorf("url = %q, want %q", cfg.url, tt.wantURL)
			}
			if cfg.key != tt.wantKey {
				t.Errorf("key = %q, want %q", cfg.key, tt.wantKey)
			}
		})
	}
}
