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

	// alertReceiver and alertMCPRuleID are set by testAlerting and read by
	// testMCP, so the MCP subtest can drive test_alert_rule against the
	// same live webhook receiver instead of standing up a second one.
	alertReceiver  *webhookReceiver
	alertMCPRuleID int64

	// shutdownOnce guards shutdown so it's safe to call it both from
	// t.Cleanup (registered right after the process starts, so a failure
	// anywhere afterwards — including inside testBuildAndStart itself,
	// e.g. a healthz timeout or a key-parse failure — still reaps the
	// child) and from the happy-path SIGTERM subtest, without either
	// signaling or waiting on an already-reaped process twice.
	shutdownOnce sync.Once
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

	t.Run("create project and mint keys", h.testCreateProjectAndKeys)
	t.Run("ingest OTLP and JSON batches", h.testIngest)
	t.Run("issues list and grouping", h.testIssuesList)
	t.Run("issue detail", h.testIssueDetail)
	t.Run("log search and context", h.testLogSearchAndContext)
	t.Run("structured JSON body lifted and grouped", h.testStructuredBodyParsing)
	t.Run("resolve then regression reopen", h.testResolveAndRegression)
	t.Run("noise rules drop and are reported", h.testNoiseRules)
	t.Run("alert rules deliver over the real webhook edge", h.testAlerting)
	t.Run("mcp tools", h.testMCP)
	t.Run("admin route scoping guard", h.testScopingGuard)
	t.Run("graceful shutdown", h.testGracefulShutdown)
}

// ---- 1: build the real binary and boot it ----

func (h *harness) testBuildAndStart(t *testing.T) {
	// Both temp dirs hang off h.t (the whole TestE2E), never this
	// subtest's t: a subtest-scoped TempDir is deleted the moment this
	// subtest returns, pulling the database directory out from under the
	// still-running server — every later file open (pool growth, WAL/shm
	// reopen) then fails with SQLITE_CANTOPEN, intermittently poisoning
	// whichever subtest happens to need the write.
	binPath := filepath.Join(h.t.TempDir(), "agenterr")
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelBuild()
	buildCmd := exec.CommandContext(buildCtx, "go", "build", "-o", binPath, "github.com/agenterr/agenterr/cmd/agenterr")
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

	dbPath := filepath.Join(h.t.TempDir(), "agenterr.db")

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"AGENTERR_LISTEN="+addr,
		"AGENTERR_DB="+dbPath,
		"AGENTERR_ADMIN_PASSWORD="+adminPassword,
		// Default is 30s (see internal/config/config.go); a short interval
		// here lets testNoiseRules poll the drop counters flushing to the
		// store instead of waiting out the production default.
		"AGENTERR_NOISE_FLUSH_MS=200",
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
	// Registered on h.t (the whole TestE2E), not the "build and start"
	// subtest's own *testing.T, and immediately after Start succeeds —
	// not after this subtest returns — so a failure anywhere from here on
	// (including later in this same subtest: a healthz timeout, a
	// key-parse failure) still reaps the child when TestE2E finishes.
	h.t.Cleanup(h.shutdown)
	// On any failure, surface the server's own stderr — without this, a
	// server-side error (a failed write, a dropped batch) manifests only
	// as an opaque assertion timeout in whichever subtest noticed it.
	h.t.Cleanup(func() {
		if h.t.Failed() {
			h.t.Logf("server stderr:\n%s", h.stderr.String())
		}
	})

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

// ---- structured-body parsing: a raw JSON line, ingested with no severity
// field, must come back queryable by lifted severity with the inner msg
// lifted to Body, and must have produced a grouped issue (level=error
// triggers event detection) ----

func (h *harness) testStructuredBodyParsing(t *testing.T) {
	// The outer envelope is the normal JSON-ingest shape (as in testIngest);
	// "message" carries no "severity" sibling, so the record starts life at
	// the parser's default SeverityInfo. Its value is itself a JSON object
	// string with "level"/"msg"/"request_id" — the structured body the
	// pipeline is expected to lift from.
	structuredBody := `[{"message":"{\"level\":\"error\",\"msg\":\"invoice sync failed\",\"request_id\":\"e2e-req-1\"}","service":"e2e-structured"}]`
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", []byte(structuredBody), http.StatusAccepted)

	// Ingestion is accepted synchronously but the pipeline (parse -> annotate
	// -> store -> group) flushes asynchronously, so poll rather than
	// assuming it's already landed by the time we query (see
	// testIssuesList's identical idiom below).
	deadline := time.Now().Add(5 * time.Second)
	var logs []logDTO
	for time.Now().Before(deadline) {
		logs = nil
		h.doJSON(t, http.MethodGet, "/api/v1/logs?q=invoice+sync+failed&min_severity=error", h.adminKey, nil, http.StatusOK, &logs)
		if len(logs) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(logs) == 0 {
		t.Fatal("structured body not searchable by lifted severity")
	}
	if logs[0].Body != "invoice sync failed" {
		t.Errorf("body = %q, want lifted msg", logs[0].Body)
	}

	// Issue grouping runs asynchronously in the pipeline too, so poll until
	// it lands.
	deadline = time.Now().Add(5 * time.Second)
	var issues []issueDTO
	found := false
	for time.Now().Before(deadline) {
		issues = nil
		h.doJSON(t, http.MethodGet, "/api/v1/issues?status=open", h.adminKey, nil, http.StatusOK, &issues)
		for _, is := range issues {
			if strings.Contains(is.Title, "invoice sync failed") {
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Errorf("lifted error did not produce a grouped issue; open issues: %+v", issues)
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

// ---- noise rules: drop-on-ingest, counted, reported, and the
// parse_bodies override that keeps a body raw ----

type noiseRuleDTO struct {
	ID           int64  `json:"id"`
	Kind         string `json:"kind"`
	Service      string `json:"service"`
	Severity     string `json:"severity"`
	Pattern      string `json:"pattern"`
	N            int    `json:"n"`
	Enabled      bool   `json:"enabled"`
	DroppedCount int64  `json:"dropped_count"`
}

type noiseReportDTO struct {
	TopServices []struct {
		Service string `json:"service"`
		Logs    int64  `json:"logs"`
	} `json:"top_services"`
	Rules        []noiseRuleDTO `json:"rules"`
	TotalDropped int64          `json:"total_dropped"`
}

// testNoiseRules proves the noise-rule pipeline end to end: a
// severity_floor rule dropping below warn for one service, counted
// (dropped_count==2, surfaced via the REST noise report once the engine's
// flush ticker — configured at 200ms for this run, see
// AGENTERR_NOISE_FLUSH_MS in testBuildAndStart — persists it), and the
// parse_bodies override that keeps an ingested body raw (and therefore
// unmatched by a severity-based search) instead of being lifted.
func (h *harness) testNoiseRules(t *testing.T) {
	// 1: create the rule via REST with the admin key — drop e2e-noisy
	// below warn.
	var rule noiseRuleDTO
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/projects/%d/noise-rules", h.projectID), h.adminKey,
		map[string]any{
			"kind":     "severity_floor",
			"service":  "e2e-noisy",
			"severity": "warn",
			"enabled":  true,
		}, http.StatusOK, &rule)
	if rule.ID == 0 {
		t.Fatalf("expected non-zero noise rule id, got %+v", rule)
	}

	// 2: three records for e2e-noisy — an info-severity one lifted from a
	// structured body, an explicit warn, and a debug-severity one also
	// lifted from a structured body. Only warn clears the floor.
	batch := `[` +
		`{"message":"{\"level\":\"info\",\"msg\":\"e2e-noisy info dropped\"}","service":"e2e-noisy"},` +
		`{"message":"e2e-noisy warn kept","severity":"warn","service":"e2e-noisy"},` +
		`{"message":"{\"level\":\"debug\",\"msg\":\"e2e-noisy debug dropped\"}","service":"e2e-noisy"}` +
		`]`
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", []byte(batch), http.StatusAccepted)

	deadline := time.Now().Add(5 * time.Second)
	var logs []logDTO
	for time.Now().Before(deadline) {
		logs = nil
		h.doJSON(t, http.MethodGet, "/api/v1/logs?service=e2e-noisy", h.adminKey, nil, http.StatusOK, &logs)
		if len(logs) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(logs) != 1 {
		t.Fatalf("expected exactly 1 stored log for e2e-noisy (the warn record; info/debug should be dropped), got %d: %+v\nstderr:\n%s", len(logs), logs, h.stderr.String())
	}
	if logs[0].Body != "e2e-noisy warn kept" {
		t.Errorf("stored e2e-noisy log body = %q, want the warn record's lifted body", logs[0].Body)
	}
	if logs[0].Severity != "WARN" {
		t.Errorf("stored e2e-noisy log severity = %q, want WARN", logs[0].Severity)
	}

	// 3: the noise report shows dropped_count==2 for the rule once the
	// engine's flush ticker persists the in-memory counters.
	deadline = time.Now().Add(5 * time.Second)
	var report noiseReportDTO
	var found noiseRuleDTO
	for time.Now().Before(deadline) {
		h.doJSON(t, http.MethodGet, fmt.Sprintf("/api/v1/noise-report?project=%d", h.projectID), h.adminKey, nil, http.StatusOK, &report)
		for _, r := range report.Rules {
			if r.ID == rule.ID {
				found = r
				break
			}
		}
		if found.DroppedCount == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if found.DroppedCount != 2 {
		t.Fatalf("expected rule %d dropped_count==2, got %+v (full report: %+v)", rule.ID, found, report)
	}

	// 4: with parse_bodies=false, a structured body is stored raw and its
	// (unlifted) severity stays below the min_severity=error filter. Use a
	// different service so the severity_floor rule above doesn't also
	// swallow this record.
	h.doRaw(t, http.MethodPatch, fmt.Sprintf("/api/v1/projects/%d", h.projectID), h.adminKey,
		"application/json", []byte(`{"parse_bodies":false}`), http.StatusNoContent)

	rawBody := `{"level":"error","msg":"should stay raw"}`
	ingestEntry := fmt.Sprintf(`[{"message":%q,"service":"e2e-parseoff"}]`, rawBody)
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", []byte(ingestEntry), http.StatusAccepted)

	deadline = time.Now().Add(5 * time.Second)
	var parseOffLogs []logDTO
	for time.Now().Before(deadline) {
		parseOffLogs = nil
		h.doJSON(t, http.MethodGet, "/api/v1/logs?service=e2e-parseoff", h.adminKey, nil, http.StatusOK, &parseOffLogs)
		if len(parseOffLogs) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(parseOffLogs) != 1 {
		t.Fatalf("expected exactly 1 stored log for e2e-parseoff, got %d: %+v", len(parseOffLogs), parseOffLogs)
	}
	if parseOffLogs[0].Body != rawBody {
		t.Errorf("e2e-parseoff body = %q, want raw unparsed %q (parse_bodies=false must not lift it)", parseOffLogs[0].Body, rawBody)
	}
	if parseOffLogs[0].Severity == "ERROR" {
		t.Errorf("e2e-parseoff severity lifted to ERROR despite parse_bodies=false")
	}

	var minSevLogs []logDTO
	h.doJSON(t, http.MethodGet, "/api/v1/logs?service=e2e-parseoff&min_severity=error", h.adminKey, nil, http.StatusOK, &minSevLogs)
	if len(minSevLogs) != 0 {
		t.Errorf("min_severity=error unexpectedly matched the raw-body record (parse_bodies=false should have left its severity below error): %+v", minSevLogs)
	}

	// Reset for anything running after this subtest.
	h.doRaw(t, http.MethodPatch, fmt.Sprintf("/api/v1/projects/%d", h.projectID), h.adminKey,
		"application/json", []byte(`{"parse_bodies":true}`), http.StatusNoContent)
}

// ---- alerting: rule creation, delivery, cooldown suppression, regression,
// threshold windows, and test-fire, all proven against a real webhook
// receiver running in this test process ----

type alertRuleDTO struct {
	ID              int64             `json:"id"`
	ProjectID       int64             `json:"project_id"`
	Name            string            `json:"name"`
	Kind            string            `json:"kind"`
	Service         string            `json:"service"`
	Environment     string            `json:"environment"`
	MinSeverity     string            `json:"min_severity"`
	N               int               `json:"n"`
	WindowMinutes   int               `json:"window_minutes"`
	CooldownSeconds int               `json:"cooldown_seconds"`
	URL             string            `json:"url"`
	Headers         map[string]string `json:"headers,omitempty"`
	Enabled         bool              `json:"enabled"`
	LastFired       *string           `json:"last_fired"`
	LastError       string            `json:"last_error"`
}

// alertPayloadDTO mirrors the wire shape documented in the alert semantics
// doc and implemented by internal/alerts/deliver.go's alertPayload:
// {"rule":{"id","name","kind"},"project_id","issue":{...},"event_count",
// "window_minutes","fired_at"[,"test"]}.
type alertPayloadDTO struct {
	Rule struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
		Kind string `json:"kind"`
	} `json:"rule"`
	ProjectID int64 `json:"project_id"`
	Issue     struct {
		ID        int64  `json:"id"`
		Title     string `json:"title"`
		Severity  string `json:"severity"`
		Count     int64  `json:"count"`
		FirstSeen string `json:"first_seen"`
		LastSeen  string `json:"last_seen"`
	} `json:"issue"`
	EventCount    int    `json:"event_count"`
	WindowMinutes int    `json:"window_minutes"`
	FiredAt       string `json:"fired_at"`
	Test          bool   `json:"test"`
}

// webhookReceiver is a plain net/http server bound to a 127.0.0.1 ephemeral
// port, standing in for a real alert destination: every POST body lands on
// a buffered channel for the test to drain, and the receiver always
// answers 200 so deliveries succeed on the first attempt (no retry/backoff
// noise in the assertions below).
type webhookReceiver struct {
	url string
	ch  chan []byte
}

// newWebhookReceiver starts the receiver and registers its shutdown on
// h.t (the whole-test T) — not any subtest's t — so it stays alive for
// every subtest that needs it (testAlerting, then testMCP's test_alert_rule
// call), mirroring the temp-dir rule in testBuildAndStart.
func newWebhookReceiver(h *harness) *webhookReceiver {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatalf("webhook receiver: listen: %v", err)
	}
	ch := make(chan []byte, 32)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			select {
			case ch <- body:
			default:
				// Never block the delivery worker's HTTP client on a full
				// channel; a stuck receiver must not hang the subject
				// under test.
			}
			w.WriteHeader(http.StatusOK)
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return &webhookReceiver{url: "http://" + ln.Addr().String(), ch: ch}
}

// recv blocks for up to timeout for the next delivered body, failing the
// test on timeout.
func (wr *webhookReceiver) recv(t *testing.T, timeout time.Duration) []byte {
	t.Helper()
	select {
	case b := <-wr.ch:
		return b
	case <-time.After(timeout):
		t.Fatalf("webhook receiver: no delivery within %s", timeout)
		return nil
	}
}

// expectNone asserts nothing arrives during a bounded quiet window. quiet
// only needs to comfortably exceed the pipeline's async
// ingest->evaluate->enqueue path (observed well under 200ms elsewhere in
// this suite); it deliberately isn't unbounded since there is no positive
// event to wait for — a real spurious delivery would show up well inside
// this window.
func (wr *webhookReceiver) expectNone(t *testing.T, quiet time.Duration) {
	t.Helper()
	select {
	case b := <-wr.ch:
		t.Fatalf("webhook receiver: unexpected delivery: %s", b)
	case <-time.After(quiet):
	}
}

// testAlerting drives the alerting feature end to end against a real
// webhook receiver: new_issue delivery + cooldown suppression, a
// resolve->regression reopen delivering a regression payload, a threshold
// rule firing once N events land in the window, and the synchronous
// REST test-fire route. Every rule here is scoped to a service name no
// other subtest uses (e2e-alerts), both because rules only ever evaluate
// events ingested after they're created (so earlier subtests' ingests
// can't retroactively fire them) and defensively, in case a later subtest
// is reordered above this one.
func (h *harness) testAlerting(t *testing.T) {
	wr := newWebhookReceiver(h)
	h.alertReceiver = wr

	const svc = "e2e-alerts"

	// 1: a new_issue rule and a regression rule, both scoped to
	// svc/min_severity=error and both pointing at the receiver. Two
	// separate rules because kind is exclusive per rule; sharing one
	// receiver lets both steps below assert off the same channel.
	var newIssueRule alertRuleDTO
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/projects/%d/alert-rules", h.projectID), h.adminKey,
		map[string]any{
			"name":         "e2e new issue",
			"kind":         "new_issue",
			"service":      svc,
			"min_severity": "error",
			"url":          wr.url,
		}, http.StatusOK, &newIssueRule)
	if newIssueRule.ID == 0 {
		t.Fatalf("expected non-zero alert rule id, got %+v", newIssueRule)
	}
	if newIssueRule.CooldownSeconds != 900 {
		t.Errorf("expected cooldown_seconds to default to 900 when omitted, got %d", newIssueRule.CooldownSeconds)
	}

	var regressionRule alertRuleDTO
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/projects/%d/alert-rules", h.projectID), h.adminKey,
		map[string]any{
			"name":         "e2e regression",
			"kind":         "regression",
			"service":      svc,
			"min_severity": "error",
			"url":          wr.url,
		}, http.StatusOK, &regressionRule)
	if regressionRule.ID == 0 {
		t.Fatalf("expected non-zero alert rule id, got %+v", regressionRule)
	}

	errorBatch, err := json.Marshal([]map[string]any{{
		"message":  "e2e alert boom",
		"severity": "error",
		"service":  svc,
	}})
	if err != nil {
		t.Fatalf("marshal error batch: %v", err)
	}
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", errorBatch, http.StatusAccepted)

	raw := wr.recv(t, 5*time.Second)
	newIssueRaw := raw
	var newIssuePayload alertPayloadDTO
	if err := json.Unmarshal(raw, &newIssuePayload); err != nil {
		t.Fatalf("decode new_issue payload: %v; body: %s", err, raw)
	}
	if newIssuePayload.Rule.ID != newIssueRule.ID || newIssuePayload.Rule.Kind != "new_issue" {
		t.Errorf("expected rule.id=%d rule.kind=new_issue, got %+v", newIssueRule.ID, newIssuePayload.Rule)
	}
	if !strings.Contains(newIssuePayload.Issue.Title, "e2e alert boom") {
		t.Errorf("expected issue.title to contain the ingested message, got %q", newIssuePayload.Issue.Title)
	}
	// Pin the wire contract explicitly: issue.severity is UPPERCASE, not
	// the lowercase form the ingest request used or the REST DTO renders
	// min_severity in.
	if !bytes.Contains(raw, []byte(`"severity":"ERROR"`)) {
		t.Errorf("expected payload to carry uppercase issue.severity, got: %s", raw)
	}
	if newIssuePayload.Issue.ID == 0 {
		t.Fatalf("expected non-zero issue.id in new_issue payload, got %+v", newIssuePayload)
	}
	alertIssueID := newIssuePayload.Issue.ID

	// last_fired set: poll the list endpoint rather than assuming the
	// store write (which happens after delivery, on the worker goroutine)
	// has landed by the time recv returned.
	deadline := time.Now().Add(5 * time.Second)
	var fired bool
	for time.Now().Before(deadline) {
		var rules []alertRuleDTO
		h.doJSON(t, http.MethodGet, fmt.Sprintf("/api/v1/projects/%d/alert-rules", h.projectID), h.adminKey, nil, http.StatusOK, &rules)
		for _, r := range rules {
			if r.ID == newIssueRule.ID && r.LastFired != nil {
				fired = true
				break
			}
		}
		if fired {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !fired {
		t.Fatal("new_issue rule's last_fired was never set")
	}

	// 2: the same error again is neither new (the fingerprint's already
	// been seen) nor past cooldown (900s default) — no second delivery.
	// This is a bounded negative wait, not proof of "never": it's
	// comfortably longer than the async ingest->evaluate->enqueue path
	// exercised in step 1 above.
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", errorBatch, http.StatusAccepted)
	wr.expectNone(t, 700*time.Millisecond)

	// 3: resolve the issue, then re-ingest the same error — a regression
	// reopen — and expect the regression rule (never fired before, so its
	// own cooldown doesn't block it) to deliver.
	h.doRaw(t, http.MethodPatch, fmt.Sprintf("/api/v1/issues/%d", alertIssueID), h.adminKey,
		"application/json", []byte(`{"status":"resolved"}`), http.StatusNoContent)
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", errorBatch, http.StatusAccepted)

	raw = wr.recv(t, 5*time.Second)
	var regressionPayload alertPayloadDTO
	if err := json.Unmarshal(raw, &regressionPayload); err != nil {
		t.Fatalf("decode regression payload: %v; body: %s", err, raw)
	}
	if regressionPayload.Rule.ID != regressionRule.ID || regressionPayload.Rule.Kind != "regression" {
		t.Errorf("expected rule.id=%d rule.kind=regression, got %+v", regressionRule.ID, regressionPayload.Rule)
	}
	if !strings.Contains(regressionPayload.Issue.Title, "e2e alert boom") {
		t.Errorf("expected regression payload's issue.title to contain the ingested message, got %q", regressionPayload.Issue.Title)
	}

	// 4: a threshold rule (n=3, window 1 minute, service-scoped, explicit
	// 1s cooldown so a later test-fire in step 5 isn't itself suppressed).
	var thresholdRule alertRuleDTO
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/projects/%d/alert-rules", h.projectID), h.adminKey,
		map[string]any{
			"name":             "e2e threshold",
			"kind":             "threshold",
			"service":          svc,
			"n":                3,
			"window_minutes":   1,
			"cooldown_seconds": 1,
			"url":              wr.url,
		}, http.StatusOK, &thresholdRule)
	if thresholdRule.ID == 0 {
		t.Fatalf("expected non-zero alert rule id, got %+v", thresholdRule)
	}
	if thresholdRule.CooldownSeconds != 1 {
		t.Errorf("expected explicit cooldown_seconds=1 to be honored (not the 900 default), got %d", thresholdRule.CooldownSeconds)
	}

	// The pipeline only ever calls the alerts engine for entries
	// core.IsEvent flags — severity >= error, or an exception attribute
	// present (internal/pipeline/pipeline.go: "if !e.IsEvent { continue }"
	// gates the Notifier call before it happens) — so a threshold rule can
	// only ever count error-and-up records, regardless of its own
	// min_severity. Each message is distinct so these are 3 fresh
	// fingerprints (three new issues), but that's harmless here: the
	// new_issue rule already spent its cooldown on step 1's issue and
	// stays silent for 900s, so only the threshold rule delivers below.
	thresholdBatch := make([]map[string]any, 3)
	for i := range thresholdBatch {
		thresholdBatch[i] = map[string]any{
			"message":  fmt.Sprintf("e2e alert threshold %d", i),
			"severity": "error",
			"service":  svc,
		}
	}
	thresholdBody, err := json.Marshal(thresholdBatch)
	if err != nil {
		t.Fatalf("marshal threshold batch: %v", err)
	}
	h.postRaw(t, "/api/v1/ingest", h.ingestKey, "application/json", thresholdBody, http.StatusAccepted)

	raw = wr.recv(t, 5*time.Second)
	var thresholdPayload alertPayloadDTO
	if err := json.Unmarshal(raw, &thresholdPayload); err != nil {
		t.Fatalf("decode threshold payload: %v; body: %s", err, raw)
	}
	if thresholdPayload.Rule.ID != thresholdRule.ID || thresholdPayload.Rule.Kind != "threshold" {
		t.Errorf("expected rule.id=%d rule.kind=threshold, got %+v", thresholdRule.ID, thresholdPayload.Rule)
	}
	if thresholdPayload.EventCount < 3 {
		t.Errorf("expected event_count >= 3, got %d", thresholdPayload.EventCount)
	}
	if thresholdPayload.WindowMinutes != 1 {
		t.Errorf("expected window_minutes=1, got %d", thresholdPayload.WindowMinutes)
	}
	// Pin the wire contract explicitly: threshold payloads carry
	// event_count/window_minutes as real JSON keys (the new_issue and
	// regression payloads above must not — deliver.go's omitempty tags
	// only populate them for the threshold kind).
	if !bytes.Contains(raw, []byte(`"event_count"`)) || !bytes.Contains(raw, []byte(`"window_minutes"`)) {
		t.Errorf("expected threshold payload to carry event_count/window_minutes keys, got: %s", raw)
	}
	if bytes.Contains(newIssueRaw, []byte(`"event_count"`)) {
		t.Errorf("new_issue payload unexpectedly carries event_count: %s", newIssueRaw)
	}

	// 5: POST /api/v1/alert-rules/{id}/test fires synchronously with
	// "test":true.
	var testResp struct {
		Delivered bool `json:"delivered"`
	}
	h.doJSON(t, http.MethodPost, fmt.Sprintf("/api/v1/alert-rules/%d/test", thresholdRule.ID), h.adminKey, nil, http.StatusOK, &testResp)
	if !testResp.Delivered {
		t.Errorf("expected {\"delivered\":true} from the REST test-fire route, got %+v", testResp)
	}

	raw = wr.recv(t, 5*time.Second)
	if !bytes.Contains(raw, []byte(`"test":true`)) {
		t.Errorf("expected REST test-fire payload to carry test:true, got: %s", raw)
	}

	// testMCP drives test_alert_rule through the MCP edge next, against
	// this same threshold rule and receiver.
	h.alertMCPRuleID = thresholdRule.ID
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
	names := make([]string, len(toolsResult.Tools))
	byName := make(map[string]bool, len(toolsResult.Tools))
	for i, tl := range toolsResult.Tools {
		names[i] = tl.Name
		byName[tl.Name] = true
	}
	if len(toolsResult.Tools) != 17 {
		t.Errorf("expected 17 tools, got %d: %v", len(toolsResult.Tools), names)
	}
	// The five noise-control tools, added alongside the REST noise-rule
	// surface, and the four alert-rule tools, added alongside the REST
	// alert-rule surface: confirm each is actually registered on the live
	// server, not just that the total count happens to match.
	for _, want := range []string{
		"list_noise_rules", "upsert_noise_rule", "delete_noise_rule",
		"get_noise_report", "set_project_parse",
		"list_alert_rules", "upsert_alert_rule", "delete_alert_rule", "test_alert_rule",
	} {
		if !byName[want] {
			t.Errorf("expected tool %q in tools/list, got: %v", want, names)
		}
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

	// list_noise_rules and get_noise_report over MCP: a project-scoped
	// key's own project is authoritative for both (project_id omitted), so
	// these must surface the severity_floor rule testNoiseRules created —
	// proving the noise-control tools, not just the REST edge, are wired
	// through the live server.
	rulesResult, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "list_noise_rules",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call list_noise_rules: %v", err)
	}
	rulesText := mcpTextContent(rulesResult)
	if !strings.Contains(rulesText, "e2e-noisy") {
		t.Errorf("list_noise_rules missing expected rule service: %q", rulesText)
	}

	reportResult, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "get_noise_report",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("call get_noise_report: %v", err)
	}
	reportText := mcpTextContent(reportResult)
	if !strings.Contains(reportText, "e2e-noisy") {
		t.Errorf("get_noise_report missing expected rule service: %q", reportText)
	}
	if !strings.Contains(reportText, "2 total dropped") {
		t.Errorf("get_noise_report expected 2 total dropped (from testNoiseRules), got: %q", reportText)
	}

	// test_alert_rule over MCP: fires the same threshold rule testAlerting
	// created, against the same live webhook receiver, proving the MCP
	// edge reaches the alerts engine and not just REST.
	testResult, err := session.CallTool(ctx, &mcpsdk.CallToolParams{
		Name:      "test_alert_rule",
		Arguments: map[string]any{"id": h.alertMCPRuleID},
	})
	if err != nil {
		t.Fatalf("call test_alert_rule: %v", err)
	}
	testText := mcpTextContent(testResult)
	if !strings.Contains(testText, "delivered") {
		t.Errorf("test_alert_rule result missing delivery confirmation: %q", testText)
	}
	testRaw := h.alertReceiver.recv(t, 5*time.Second)
	if !bytes.Contains(testRaw, []byte(`"test":true`)) {
		t.Errorf("test_alert_rule webhook payload missing test:true: %s", testRaw)
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
		// The process is already reaped: mark shutdownOnce consumed so
		// the t.Cleanup registered in testBuildAndStart becomes a no-op
		// instead of signaling/waiting on it a second time.
		h.shutdownOnce.Do(func() {})
		if err != nil {
			t.Fatalf("process did not exit 0 after SIGTERM: %v\nstdout:\n%s\nstderr:\n%s", err, h.stdout.String(), h.stderr.String())
		}
	// The bound must exceed the server's own worst-case shutdown budget:
	// OnStop gives srv.Shutdown up to shutdownServerBudget (7s, see
	// internal/app/lifecycle.go) before moving on to drain — a lingering
	// client connection (e.g. the MCP session's streamable transport) can
	// legitimately consume that whole budget. 5s here used to flake.
	case <-time.After(15 * time.Second):
		t.Fatalf("process did not exit within 15s of SIGTERM\nstdout:\n%s\nstderr:\n%s", h.stdout.String(), h.stderr.String())
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

// shutdown is the leak-proofing safety net, registered via t.Cleanup right
// after the process starts (see testBuildAndStart): if any subtest fails
// before testGracefulShutdown runs — or before it reaches the signal — this
// makes sure the child process is never leaked. shutdownOnce makes it safe
// to call unconditionally from t.Cleanup even when testGracefulShutdown
// already reaped the process itself on the happy path: only the first
// caller (whichever runs first) actually signals/waits.
func (h *harness) shutdown() {
	h.shutdownOnce.Do(func() {
		if h.cmd == nil || h.cmd.Process == nil {
			return
		}
		_ = h.cmd.Process.Kill()
		_, _ = h.cmd.Process.Wait()
	})
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
