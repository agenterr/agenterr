// Package e2e drives the real, built agenterr binary end to end: bootstrap,
// REST project/key admin, OTLP + JSON ingest, issue grouping and
// regression-reopen, log search/context, and the MCP edge — nothing here
// imports the product's internal packages, only what a real client sees
// over the wire. Guarded by the "e2e" build tag so it never runs as part
// of `go test ./...`.
//
//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const adminPassword = "e2e-test-password"

// syncBuffer is a concurrency-safe bytes.Buffer: the test reads it (for
// assertions and admin-key parsing) while the subprocess is still writing
// to it via cmd.Stdout/cmd.Stderr.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// harness holds everything the ordered subtests share: the running
// process, its captured output, its base URL, and the credentials/IDs
// minted along the way.
type harness struct {
	t       *testing.T
	baseURL string
	cmd     *exec.Cmd
	stdout  *syncBuffer
	stderr  *syncBuffer

	adminKey  string
	ingestKey string
	apiKey    string
	projectID int64

	otlpIssueID int64
	jsonIssueID int64
}

func TestE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e: skipped under -short")
	}

	h := &harness{t: t}

	t.Run("build and start", h.testBuildAndStart)
	if t.Failed() {
		t.Fatal("e2e: cannot continue past build/start failure")
	}
	defer h.shutdown()

	t.Run("create project and mint keys", h.testCreateProjectAndKeys)
	t.Run("ingest OTLP and JSON batches", h.testIngest)
	t.Run("issues list and grouping", h.testIssuesList)
	t.Run("issue detail", h.testIssueDetail)
	t.Run("log search and context", h.testLogSearchAndContext)
	t.Run("resolve then regression reopen", h.testResolveAndRegression)
	t.Run("mcp tools", h.testMCP)
	t.Run("admin route scoping guard", h.testScopingGuard)
	t.Run("graceful shutdown", h.testGracefulShutdown)
}

// ---- 1: build the real binary and boot it ----

func (h *harness) testBuildAndStart(t *testing.T) {
	binPath := filepath.Join(t.TempDir(), "agenterr")
	buildCmd := exec.Command("go", "build", "-o", binPath, "github.com/agenterr/agenterr/cmd/agenterr")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("go build: %v", err)
	}

	// Bind a free port ourselves: the config layer has no port-report
	// mechanism (":0" would let the OS pick, but nothing tells us what it
	// picked), so we pick one, free it, and hand it to the child. There is
	// an inherent TOCTOU race here, but it's the same technique used
	// throughout Go's own net/http tests and is good enough for a local
	// e2e run.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}

	dbPath := filepath.Join(t.TempDir(), "agenterr.db")

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"AGENTERR_LISTEN="+addr,
		"AGENTERR_DB="+dbPath,
		"AGENTERR_ADMIN_PASSWORD="+adminPassword,
	)
	h.stdout = &syncBuffer{}
	h.stderr = &syncBuffer{}
	cmd.Stdout = h.stdout
	cmd.Stderr = h.stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start agenterr: %v", err)
	}
	h.cmd = cmd
	h.baseURL = "http://" + addr

	if err := h.waitHealthy(5 * time.Second); err != nil {
		t.Fatalf("agenterr never became healthy: %v\nstdout:\n%s\nstderr:\n%s", err, h.stdout.String(), h.stderr.String())
	}

	key, err := parseAdminKey(h.stdout.String())
	if err != nil {
		t.Fatalf("parse admin key from stdout: %v\nstdout:\n%s", err, h.stdout.String())
	}
	h.adminKey = key

	// With AGENTERR_ADMIN_PASSWORD set, the generated-password line must
	// not appear — only the admin key portion prints (see
	// internal/app/lifecycle.go: printBootstrap only writes the
	// "Generated admin password" line when creds.password != "", which is
	// only ever non-empty when the password was freshly generated, i.e.
	// never when it was supplied via env).
	if strings.Contains(h.stdout.String(), "Generated admin password") {
		t.Errorf("stdout unexpectedly contains a generated-password line despite AGENTERR_ADMIN_PASSWORD being set:\n%s", h.stdout.String())
	}
}

func (h *harness) waitHealthy(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(h.baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for /healthz: %w", lastErr)
}

var adminKeyRe = regexp.MustCompile(`agt_admin_[A-Za-z0-9_-]+`)

func parseAdminKey(stdout string) (string, error) {
	m := adminKeyRe.FindString(stdout)
	if m == "" {
		return "", fmt.Errorf("no agt_admin_... key found in stdout")
	}
	return m, nil
}

// ---- 2: create project, mint keys ----

func (h *harness) testCreateProjectAndKeys(t *testing.T) {
	var proj struct {
		ID            int64  `json:"id"`
		Name          string `json:"name"`
		Slug          string `json:"slug"`
		RetentionDays int    `json:"retention_days"`
	}
	h.doJSON(t, http.MethodPost, "/api/v1/projects", h.adminKey,
		map[string]any{"name": "checkout-api", "retention_days": 14},
		http.StatusCreated, &proj)
	if proj.ID == 0 {
		t.Fatalf("expected non-zero project id, got %+v", proj)
	}
	h.projectID = proj.ID

	var ingestKeyResp struct {
		Key string `json:"key"`
	}
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/projects/%d/keys", proj.ID), h.adminKey,
		map[string]any{"kind": "ingest"}, http.StatusCreated, &ingestKeyResp)
	if !strings.HasPrefix(ingestKeyResp.Key, "agt_ingest_") {
		t.Fatalf("expected agt_ingest_ prefixed key, got %q", ingestKeyResp.Key)
	}
	h.ingestKey = ingestKeyResp.Key

	var apiKeyResp struct {
		Key string `json:"key"`
	}
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/projects/%d/keys", proj.ID), h.adminKey,
		map[string]any{"kind": "api"}, http.StatusCreated, &apiKeyResp)
	if !strings.HasPrefix(apiKeyResp.Key, "agt_api_") {
		t.Fatalf("expected agt_api_ prefixed key, got %q", apiKeyResp.Key)
	}
	h.apiKey = apiKeyResp.Key
}

// ---- 3: ingest OTLP + JSON batches ----

func (h *harness) testIngest(t *testing.T) {
	otlpBody, err := os.ReadFile("fixtures/otlp_batch.json")
	if err != nil {
		t.Fatalf("read otlp fixture: %v", err)
	}
	h.postRaw(t, "/v1/logs", h.ingestKey, "application/json", otlpBody, http.StatusAccepted)

	jsonBatch := []map[string]any{
		{
			"message":     "database connection pool exhausted",
			"severity":    "error",
			"service":     "checkout-api",
			"environment": "production",
			"attributes": map[string]any{
				"exception.type":    "PoolExhaustedError",
				"exception.message": "no connections available",
			},
		},
		{
			"message":     "scheduled backup completed",
			"severity":    "info",
			"service":     "checkout-api",
			"environment": "production",
		},
	}
	jsonBody, err := json.Marshal(jsonBatch)
	if err != nil {
		t.Fatalf("marshal json ingest batch: %v", err)
	}
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", jsonBody, http.StatusAccepted)
}

// ---- 4/5: issues list, grouping, detail ----

type issueDTO struct {
	ID          int64  `json:"id"`
	ProjectID   int64  `json:"project_id"`
	Fingerprint string `json:"fingerprint"`
	Title       string `json:"title"`
	Severity    string `json:"severity"`
	Status      string `json:"status"`
	FirstSeen   string `json:"first_seen"`
	LastSeen    string `json:"last_seen"`
	Count       int64  `json:"count"`
}

func (h *harness) testIssuesList(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	var issues []issueDTO
	var found issueDTO
	for time.Now().Before(deadline) {
		issues = nil
		h.doJSON(t, http.MethodGet, "/api/v1/issues?status=open", h.adminKey, nil, http.StatusOK, &issues)
		for _, iss := range issues {
			if strings.Contains(iss.Title, "PaymentError") && iss.Count == 3 {
				found = iss
				break
			}
		}
		if found.ID != 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if found.ID == 0 {
		t.Fatalf("OTLP PaymentError issue with count=3 never appeared within deadline; last issues: %+v", issues)
	}
	h.otlpIssueID = found.ID

	var jsonIssue issueDTO
	for _, iss := range issues {
		if strings.Contains(iss.Title, "PoolExhaustedError") {
			jsonIssue = iss
			break
		}
	}
	if jsonIssue.ID == 0 {
		t.Fatalf("JSON PoolExhaustedError issue never appeared; issues: %+v", issues)
	}
	if jsonIssue.Count != 1 {
		t.Errorf("expected JSON error issue count=1, got %d", jsonIssue.Count)
	}
	h.jsonIssueID = jsonIssue.ID

	// Exactly these two issues should exist for this project — no
	// spurious grouping/splitting.
	if len(issues) != 2 {
		t.Errorf("expected exactly 2 open issues, got %d: %+v", len(issues), issues)
	}
}

func (h *harness) testIssueDetail(t *testing.T) {
	var body struct {
		Issue  issueDTO `json:"issue"`
		Events []struct {
			LogID int64 `json:"log_id"`
			Log   struct {
				Body string `json:"body"`
			} `json:"log"`
		} `json:"events"`
	}
	h.doJSON(t, http.MethodGet, fmt.Sprintf("/api/v1/issues/%d", h.otlpIssueID), h.adminKey, nil, http.StatusOK, &body)

	if body.Issue.ID != h.otlpIssueID {
		t.Fatalf("issue id mismatch: got %d want %d", body.Issue.ID, h.otlpIssueID)
	}
	if len(body.Events) != 3 {
		t.Errorf("expected 3 sample events on the OTLP issue, got %d", len(body.Events))
	}
	for _, e := range body.Events {
		if !strings.Contains(e.Log.Body, "payment charge failed") {
			t.Errorf("sample event body missing expected text: %q", e.Log.Body)
		}
	}
}

// ---- log search + context ----

type logDTO struct {
	ID          int64  `json:"id"`
	ProjectID   int64  `json:"project_id"`
	Time        string `json:"time"`
	Severity    string `json:"severity"`
	Body        string `json:"body"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
}

func (h *harness) testLogSearchAndContext(t *testing.T) {
	var logs []logDTO
	h.doJSON(t, http.MethodGet, "/api/v1/logs?q=charge", h.adminKey, nil, http.StatusOK, &logs)
	if len(logs) == 0 {
		t.Fatalf("expected at least one log matching q=charge")
	}
	for _, l := range logs {
		if !strings.Contains(l.Body, "charge") {
			t.Errorf("search result body doesn't contain query substring: %q", l.Body)
		}
	}

	targetID := logs[0].ID
	var ctxLogs []logDTO
	h.doJSON(t, http.MethodGet, fmt.Sprintf("/api/v1/logs/%d/context", targetID), h.adminKey, nil, http.StatusOK, &ctxLogs)
	if len(ctxLogs) == 0 {
		t.Fatalf("expected non-empty context window for log %d", targetID)
	}
	var foundTarget bool
	for _, l := range ctxLogs {
		if l.ID == targetID {
			foundTarget = true
			break
		}
	}
	if !foundTarget {
		t.Errorf("context window for log %d doesn't include the log itself: %+v", targetID, ctxLogs)
	}
}

// ---- resolve, then regression reopen through the whole stack ----

func (h *harness) testResolveAndRegression(t *testing.T) {
	h.doRaw(t, http.MethodPatch, fmt.Sprintf("/api/v1/issues/%d", h.otlpIssueID), h.adminKey,
		"application/json", []byte(`{"status":"resolved"}`), http.StatusNoContent)

	var afterResolve struct {
		Issue issueDTO `json:"issue"`
	}
	h.doJSON(t, http.MethodGet, fmt.Sprintf("/api/v1/issues/%d", h.otlpIssueID), h.adminKey, nil, http.StatusOK, &afterResolve)
	if afterResolve.Issue.Status != "resolved" {
		t.Fatalf("expected status=resolved after PATCH, got %q", afterResolve.Issue.Status)
	}

	// Re-send the same OTLP batch: the three PaymentError records hit the
	// same fingerprint and must flip the issue back to "open" — a
	// regression, exercised through ingest -> pipeline -> store -> REST.
	otlpBody, err := os.ReadFile("fixtures/otlp_batch.json")
	if err != nil {
		t.Fatalf("read otlp fixture: %v", err)
	}
	h.postRaw(t, "/v1/logs", h.ingestKey, "application/json", otlpBody, http.StatusAccepted)

	deadline := time.Now().Add(5 * time.Second)
	var last issueDTO
	for time.Now().Before(deadline) {
		var body struct {
			Issue issueDTO `json:"issue"`
		}
		h.doJSON(t, http.MethodGet, fmt.Sprintf("/api/v1/issues/%d", h.otlpIssueID), h.adminKey, nil, http.StatusOK, &body)
		last = body.Issue
		if last.Status == "open" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if last.Status != "open" {
		t.Fatalf("issue never reopened after regression; last status %q, count %d", last.Status, last.Count)
	}
	if last.Count != 6 {
		t.Errorf("expected count=6 after re-sending the 3-error batch a second time, got %d", last.Count)
	}
}

// ---- MCP ----

func (h *harness) testMCP(t *testing.T) {
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "e2e-test", Version: "0.0.1"}, nil)
	transport := &mcpsdk.StreamableClientTransport{
		Endpoint:   h.baseURL + "/mcp",
		HTTPClient: &http.Client{Transport: bearerRoundTripper{token: h.apiKey, base: http.DefaultTransport}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	session, err := client.Connect(ctx, transport, nil)
	if err != nil {
		t.Fatalf("mcp connect: %v", err)
	}
	defer session.Close()

	toolsResult, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("tools/list: %v", err)
	}
	if len(toolsResult.Tools) != 8 {
		names := make([]string, len(toolsResult.Tools))
		for i, tl := range toolsResult.Tools {
			names[i] = tl.Name
		}
		t.Errorf("expected 8 tools, got %d: %v", len(toolsResult.Tools), names)
	}

	listResult, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "list_issues",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call list_issues: %v", err)
	}
	text := mcpTextContent(listResult)
	if !strings.Contains(text, "PoolExhaustedError") {
		t.Errorf("list_issues text content missing expected issue title: %q", text)
	}

	// list_projects over the wire, with a project-scoped api key, must
	// show only this project — proving project scoping works end to end
	// through MCP, not just REST.
	projResult, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "list_projects",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call list_projects: %v", err)
	}
	projText := mcpTextContent(projResult)
	if !strings.Contains(projText, "1 projects:") {
		t.Errorf("expected list_projects to show exactly 1 project for a project-scoped key, got: %q", projText)
	}
	if !strings.Contains(projText, "checkout-api") {
		t.Errorf("list_projects missing expected project slug: %q", projText)
	}
}

type bearerRoundTripper struct {
	token string
	base  http.RoundTripper
}

func (rt bearerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+rt.token)
	return rt.base.RoundTrip(req)
}

func mcpTextContent(res *mcpsdk.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcpsdk.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

// ---- scoping guard: a project api key must not reach admin-only routes ----

func (h *harness) testScopingGuard(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, h.baseURL+"/api/v1/projects", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/v1/projects with api key: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 for a project api key on an admin-only route, got %d", resp.StatusCode)
	}
}

// ---- graceful shutdown ----

func (h *harness) testGracefulShutdown(t *testing.T) {
	if err := h.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- h.cmd.Wait() }()

	select {
	case err := <-done:
		h.cmd = nil // shutdown() must not wait/signal an already-reaped process
		if err != nil {
			t.Fatalf("process did not exit 0 after SIGTERM: %v\nstdout:\n%s\nstderr:\n%s", err, h.stdout.String(), h.stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("process did not exit within 5s of SIGTERM\nstdout:\n%s\nstderr:\n%s", h.stdout.String(), h.stderr.String())
	}

	for _, panicMarker := range []string{"panic:", "goroutine "} {
		if strings.Contains(h.stdout.String(), panicMarker) {
			t.Errorf("stdout contains a panic trace marker %q", panicMarker)
		}
		if strings.Contains(h.stderr.String(), panicMarker) {
			t.Errorf("stderr contains a panic trace marker %q", panicMarker)
		}
	}
}

// shutdown is the top-level defer's safety net: if an earlier subtest
// failed before testGracefulShutdown ran (or ran but didn't reach the
// signal), this makes sure the child process is never leaked.
func (h *harness) shutdown() {
	if h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = h.cmd.Process.Kill()
	_, _ = h.cmd.Process.Wait()
}

// ---- small HTTP helpers ----

func (h *harness) doJSON(t *testing.T, method, path, bearer string, body any, wantStatus int, out any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, h.baseURL+path, reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d; body: %s", method, path, resp.StatusCode, wantStatus, respBody)
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			t.Fatalf("%s %s: decode response: %v; body: %s", method, path, err, respBody)
		}
	}
}

func (h *harness) postRaw(t *testing.T, path, bearer, contentType string, body []byte, wantStatus int) {
	t.Helper()
	h.doRaw(t, http.MethodPost, path, bearer, contentType, body, wantStatus)
}

func (h *harness) doRaw(t *testing.T, method, path, bearer, contentType string, body []byte, wantStatus int) {
	t.Helper()
	req, err := http.NewRequest(method, h.baseURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d; body: %s", method, path, resp.StatusCode, wantStatus, respBody)
	}
}
