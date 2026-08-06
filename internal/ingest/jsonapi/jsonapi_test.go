package jsonapi

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/store"
)

// fakeSink captures logs passed to Enqueue and can be configured to return
// an error, so tests can assert exactly what jsonapi mapped without a real
// pipeline.
type fakeSink struct {
	logs []core.Log
	err  error
}

func (f *fakeSink) Enqueue(logs []core.Log) error {
	if f.err != nil {
		return f.err
	}
	f.logs = append(f.logs, logs...)
	return nil
}

// fakeAdmin is a minimal store.Admin backed by a map, mirroring the pattern
// in internal/auth/auth_test.go, so RequireKey can be exercised for real
// without a database.
type fakeAdmin struct {
	keys map[string]struct {
		projectID int64
		kind      string
	}
}

func (f *fakeAdmin) CreateProject(_ context.Context, _ string, _ int) (core.Project, error) {
	panic("unused")
}
func (f *fakeAdmin) Projects(_ context.Context) ([]core.Project, error) { panic("unused") }
func (f *fakeAdmin) SetIssueStatus(_ context.Context, _ int64, _ core.IssueStatus) error {
	panic("unused")
}
func (f *fakeAdmin) MintKey(_ context.Context, _ int64, _ string) (string, error) {
	panic("unused")
}
func (f *fakeAdmin) LookupKey(_ context.Context, plaintext string) (int64, string, error) {
	e, ok := f.keys[plaintext]
	if !ok {
		return 0, "", store.ErrNotFound
	}
	return e.projectID, e.kind, nil
}

const testProjectID int64 = 42
const validKey = "agt_ingest_valid"

func newTestServer(sink *fakeSink) *httptest.Server {
	return newTestServerMaxBody(sink, 0)
}

// newTestServerMaxBody is newTestServer but with an explicit maxBody, so
// tests can exercise the 413 path with a tiny limit rather than the real
// (5MB) default.
func newTestServerMaxBody(sink *fakeSink, maxBody int64) *httptest.Server {
	admin := &fakeAdmin{keys: map[string]struct {
		projectID int64
		kind      string
	}{
		validKey:        {projectID: testProjectID, kind: "ingest"},
		"agt_api_valid": {projectID: testProjectID, kind: "api"},
	}}
	a := auth.New(admin, []byte{})

	h := New(sink, maxBody)
	mux := http.NewServeMux()
	h.Mount(mux, a)
	return httptest.NewServer(mux)
}

func post(t *testing.T, srv *httptest.Server, key string, body []byte) *http.Response {
	t.Helper()
	return postWithEncoding(t, srv, key, "", body)
}

// postWithEncoding is post but with an explicit Content-Encoding header,
// mirroring otlp_test.go's helper of the same name/shape, so the gzip cases
// below exercise ingest.ReadBoundedBody through the same request path a
// real gzip-encoding client (e.g. agenterr-ship) uses.
func postWithEncoding(t *testing.T, srv *httptest.Server, key, contentEncoding string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/ingest", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

// gzipBytes gzip-compresses data, failing the test on error — mirrors
// otlp_test.go's helper of the same name.
func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestIngest_HappyPath(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	before := time.Now().UTC()
	body := `[{"severity":"error","message":"boom","attributes":{"k":"v"}}]`
	resp := post(t, srv, validKey, []byte(body))
	defer func() { _ = resp.Body.Close() }()
	after := time.Now().UTC()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	var respBody struct {
		Accepted int `json:"accepted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if respBody.Accepted != 1 {
		t.Errorf("accepted = %d, want 1", respBody.Accepted)
	}

	if len(sink.logs) != 1 {
		t.Fatalf("sink got %d logs, want 1", len(sink.logs))
	}
	l := sink.logs[0]
	if l.ProjectID != testProjectID {
		t.Errorf("ProjectID = %d, want %d", l.ProjectID, testProjectID)
	}
	if l.Severity != core.SeverityError {
		t.Errorf("Severity = %v, want Error", l.Severity)
	}
	if l.Body != "boom" {
		t.Errorf("Body = %q, want boom", l.Body)
	}
	if l.Attrs["k"] != "v" {
		t.Errorf("Attrs[k] = %q, want v", l.Attrs["k"])
	}
	if l.Time.Before(before) || l.Time.After(after) {
		t.Errorf("Time = %v, want between %v and %v", l.Time, before, after)
	}
}

func TestIngest_FieldTolerance_MessageAliases(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"message", `[{"message":"hello"}]`},
		{"msg", `[{"msg":"hello"}]`},
		{"body", `[{"body":"hello"}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &fakeSink{}
			srv := newTestServer(sink)
			defer srv.Close()

			resp := post(t, srv, validKey, []byte(tt.body))
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", resp.StatusCode)
			}
			if len(sink.logs) != 1 {
				t.Fatalf("sink got %d logs, want 1", len(sink.logs))
			}
			if sink.logs[0].Body != "hello" {
				t.Errorf("Body = %q, want hello", sink.logs[0].Body)
			}
		})
	}
}

func TestIngest_FieldTolerance_MessagePriority(t *testing.T) {
	// message wins over msg and body when multiple are present.
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	body := `[{"message":"from-message","msg":"from-msg","body":"from-body"}]`
	resp := post(t, srv, validKey, []byte(body))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if sink.logs[0].Body != "from-message" {
		t.Errorf("Body = %q, want from-message", sink.logs[0].Body)
	}
}

func TestIngest_FieldTolerance_Timestamps(t *testing.T) {
	rfc := "2020-01-02T03:04:05Z"
	rfcTime, err := time.Parse(time.RFC3339, rfc)
	if err != nil {
		t.Fatalf("parse rfc time: %v", err)
	}

	unixSeconds := int64(1577934245) // 2020-01-02T03:04:05Z
	unixTime := time.Unix(unixSeconds, 0).UTC()

	tests := []struct {
		name string
		body string
		want time.Time
	}{
		{"timestamp RFC3339", `[{"message":"m","timestamp":"2020-01-02T03:04:05Z"}]`, rfcTime},
		{"time RFC3339", `[{"message":"m","time":"2020-01-02T03:04:05Z"}]`, rfcTime},
		{"ts RFC3339", `[{"message":"m","ts":"2020-01-02T03:04:05Z"}]`, rfcTime},
		{"ts unix seconds", `[{"message":"m","ts":1577934245}]`, unixTime},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &fakeSink{}
			srv := newTestServer(sink)
			defer srv.Close()

			resp := post(t, srv, validKey, []byte(tt.body))
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", resp.StatusCode)
			}
			if !sink.logs[0].Time.Equal(tt.want) {
				t.Errorf("Time = %v, want %v", sink.logs[0].Time, tt.want)
			}
		})
	}
}

func TestIngest_SingleObject_BecomesBatchOfOne(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, []byte(`{"message":"solo"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if len(sink.logs) != 1 {
		t.Fatalf("sink got %d logs, want 1", len(sink.logs))
	}
	if sink.logs[0].Body != "solo" {
		t.Errorf("Body = %q, want solo", sink.logs[0].Body)
	}
}

func TestIngest_EmptyArray_NoOp(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, []byte(`[]`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	var respBody struct {
		Accepted int `json:"accepted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if respBody.Accepted != 0 {
		t.Errorf("accepted = %d, want 0", respBody.Accepted)
	}
	if len(sink.logs) != 0 {
		t.Errorf("sink got %d logs, want 0", len(sink.logs))
	}
}

func TestIngest_SinkFull_Returns429(t *testing.T) {
	sink := &fakeSink{err: pipeline.ErrFull}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, []byte(`[{"message":"m"}]`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
}

func TestIngest_OversizeBody_Returns413(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	// Build a >5MB body: a JSON array padded with a long message string.
	huge := strings.Repeat("a", 6<<20)
	body := `[{"message":"` + huge + `"}]`

	resp := post(t, srv, validKey, []byte(body))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

// TestIngest_ConfiguredMaxBody_Returns413 proves the maxBody constructor
// parameter (wired from cfg.MaxBodyBytes in internal/app) is actually
// enforced, not just accepted and ignored: a body a few bytes over a tiny
// configured limit gets rejected even though it is nowhere near the
// package's ingest.MaxBody default.
func TestIngest_ConfiguredMaxBody_Returns413(t *testing.T) {
	sink := &fakeSink{}
	const tinyMaxBody = 20
	srv := newTestServerMaxBody(sink, tinyMaxBody)
	defer srv.Close()

	body := `[{"message":"this is over twenty bytes"}]`
	if len(body) <= tinyMaxBody {
		t.Fatalf("test body (%d bytes) must exceed tinyMaxBody (%d bytes)", len(body), tinyMaxBody)
	}

	resp := post(t, srv, validKey, []byte(body))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

// TestIngest_Gzip_HappyPath proves the JSON edge decompresses a gzip body
// before decoding it — the shape agenterr-ship's sender always sends (see
// internal/ship/sender), and previously unhandled here: only the OTLP edge
// had a gzip-aware reader until it was lifted into the shared
// ingest.ReadBoundedBody and wired into this edge too.
func TestIngest_Gzip_HappyPath(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	body := []byte(`[{"severity":"error","message":"boom","attributes":{"k":"v"}}]`)
	gzipped := gzipBytes(t, body)

	resp := postWithEncoding(t, srv, validKey, "gzip", gzipped)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	if len(sink.logs) != 1 {
		t.Fatalf("sink got %d logs, want 1", len(sink.logs))
	}
	l := sink.logs[0]
	if l.Severity != core.SeverityError {
		t.Errorf("Severity = %v, want Error", l.Severity)
	}
	if l.Body != "boom" {
		t.Errorf("Body = %q, want boom", l.Body)
	}
	if l.Attrs["k"] != "v" {
		t.Errorf("Attrs[k] = %q, want v", l.Attrs["k"])
	}
}

func TestIngest_GzipBadStream_Returns400(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := postWithEncoding(t, srv, validKey, "gzip", []byte("not actually gzip"))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if len(sink.logs) != 0 {
		t.Errorf("sink got %d logs, want 0", len(sink.logs))
	}
}

// TestIngest_GzipDecompressedOversize_Returns413 is the zip-bomb bound: a
// small compressed payload that decompresses past ingest.MaxBody (5MB) must
// still be rejected, proving the size cap applies to the decompressed
// stream and not just the compressed wire bytes.
func TestIngest_GzipDecompressedOversize_Returns413(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	huge := make([]byte, 6<<20)
	gzipped := gzipBytes(t, huge)

	resp := postWithEncoding(t, srv, validKey, "gzip", gzipped)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	if len(sink.logs) != 0 {
		t.Errorf("sink got %d logs, want 0", len(sink.logs))
	}
}

func TestIngest_MalformedJSON_Returns400(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, []byte(`{"not":"valid`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIngest_WrongMethod_Returns405(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/ingest", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+validKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestIngest_WrongKindKey_Returns401(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, "agt_api_valid", []byte(`[{"message":"m"}]`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}
