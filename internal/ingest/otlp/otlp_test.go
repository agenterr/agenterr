package otlp

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/store"

	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// fakeSink captures logs passed to Enqueue and can be configured to return
// an error, mirroring jsonapi_test.go's fakeSink.
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
// in jsonapi_test.go, so RequireKey can be exercised for real without a
// database.
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
	admin := &fakeAdmin{keys: map[string]struct {
		projectID int64
		kind      string
	}{
		validKey:        {projectID: testProjectID, kind: "ingest"},
		"agt_api_valid": {projectID: testProjectID, kind: "api"},
	}}
	a := auth.New(admin, []byte{})

	h := New(sink, 0)
	mux := http.NewServeMux()
	h.Mount(mux, a)
	return httptest.NewServer(mux)
}

func post(t *testing.T, srv *httptest.Server, key, contentType string, body []byte) *http.Response {
	t.Helper()
	return postWithEncoding(t, srv, key, contentType, "", body)
}

func postWithEncoding(t *testing.T, srv *httptest.Server, key, contentType, contentEncoding string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/logs", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

// gzipBytes gzip-compresses data, failing the test on error.
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

func loadFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/logs.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return data
}

func fixtureRequest(t *testing.T) *collectorlogspb.ExportLogsServiceRequest {
	t.Helper()
	var req collectorlogspb.ExportLogsServiceRequest
	if err := protojson.Unmarshal(loadFixture(t), &req); err != nil {
		t.Fatalf("protojson.Unmarshal fixture: %v", err)
	}
	return &req
}

func assertFixtureLogs(t *testing.T, logs []core.Log) {
	t.Helper()
	if len(logs) != 2 {
		t.Fatalf("sink got %d logs, want 2", len(logs))
	}

	errLog := logs[0]
	if errLog.ProjectID != testProjectID {
		t.Errorf("errLog.ProjectID = %d, want %d", errLog.ProjectID, testProjectID)
	}
	if errLog.Service != "payment-api" {
		t.Errorf("errLog.Service = %q, want payment-api", errLog.Service)
	}
	if errLog.Release != "1.4.2" {
		t.Errorf("errLog.Release = %q, want 1.4.2", errLog.Release)
	}
	if errLog.Environment != "production" {
		t.Errorf("errLog.Environment = %q, want production", errLog.Environment)
	}
	if errLog.Severity != core.SeverityError {
		t.Errorf("errLog.Severity = %v, want Error", errLog.Severity)
	}
	if errLog.Body != "failed to insert payment row" {
		t.Errorf("errLog.Body = %q, want %q", errLog.Body, "failed to insert payment row")
	}
	if errLog.Attrs["exception.type"] != "*pq.Error" {
		t.Errorf("errLog.Attrs[exception.type] = %q, want *pq.Error", errLog.Attrs["exception.type"])
	}
	if errLog.Attrs["exception.message"] != "duplicate key value violates unique constraint" {
		t.Errorf("errLog.Attrs[exception.message] = %q, want %q", errLog.Attrs["exception.message"], "duplicate key value violates unique constraint")
	}
	wantTraceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	if errLog.TraceID != wantTraceID {
		t.Errorf("errLog.TraceID = %q, want %q", errLog.TraceID, wantTraceID)
	}
	wantTime := time.Unix(0, 1700000000000000000).UTC()
	if !errLog.Time.Equal(wantTime) {
		t.Errorf("errLog.Time = %v, want %v", errLog.Time, wantTime)
	}

	infoLog := logs[1]
	if infoLog.Severity != core.SeverityInfo {
		t.Errorf("infoLog.Severity = %v, want Info", infoLog.Severity)
	}
	if infoLog.Body != "payment processed" {
		t.Errorf("infoLog.Body = %q, want payment processed", infoLog.Body)
	}
	if infoLog.Service != "payment-api" {
		t.Errorf("infoLog.Service = %q, want payment-api (resource-level)", infoLog.Service)
	}
	if infoLog.Attrs["http.method"] != "POST" {
		t.Errorf("infoLog.Attrs[http.method] = %q, want POST", infoLog.Attrs["http.method"])
	}
	if infoLog.Attrs["http.status_code"] != "200" {
		t.Errorf("infoLog.Attrs[http.status_code] = %q, want 200", infoLog.Attrs["http.status_code"])
	}
	if infoLog.TraceID != "" {
		t.Errorf("infoLog.TraceID = %q, want empty", infoLog.TraceID)
	}
}

func TestServeLogs_JSON_HappyPath(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, "application/json", loadFixture(t))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	body, err := readAll(resp)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	var out collectorlogspb.ExportLogsServiceResponse
	if err := protojson.Unmarshal(body, &out); err != nil {
		t.Fatalf("protojson.Unmarshal response: %v", err)
	}

	assertFixtureLogs(t, sink.logs)
}

func TestServeLogs_Protobuf_HappyPath(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	req := fixtureRequest(t)
	pbBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	resp := post(t, srv, validKey, "application/x-protobuf", pbBytes)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", ct)
	}

	body, err := readAll(resp)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	var out collectorlogspb.ExportLogsServiceResponse
	if err := proto.Unmarshal(body, &out); err != nil {
		t.Fatalf("proto.Unmarshal response: %v", err)
	}

	assertFixtureLogs(t, sink.logs)
}

func TestServeLogs_WrongContentType_Returns415(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, "text/plain", []byte("hello"))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", resp.StatusCode)
	}
}

func TestServeLogs_SinkFull_Returns429(t *testing.T) {
	sink := &fakeSink{err: pipeline.ErrFull}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := post(t, srv, validKey, "application/json", loadFixture(t))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
}

func TestServeLogs_OversizeBody_Returns413(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	huge := bytes.Repeat([]byte("a"), 6<<20)
	resp := post(t, srv, validKey, "application/json", huge)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}

func TestServeLogs_WrongMethod_Returns405(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/logs", nil)
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

func TestServeLogs_GzipProtobuf_HappyPath(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	req := fixtureRequest(t)
	pbBytes, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	gzipped := gzipBytes(t, pbBytes)

	resp := postWithEncoding(t, srv, validKey, "application/x-protobuf", "gzip", gzipped)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	assertFixtureLogs(t, sink.logs)
}

func TestServeLogs_GzipCaseInsensitive_HappyPath(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	gzipped := gzipBytes(t, loadFixture(t))

	resp := postWithEncoding(t, srv, validKey, "application/json", "GZIP", gzipped)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	assertFixtureLogs(t, sink.logs)
}

func TestServeLogs_GzipDecompressedOversize_Returns413(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	// 6MB of zeros compresses down to a tiny gzip payload, but decompresses
	// past ingest.MaxBody (5MB) — a minimal zip-bomb shape.
	huge := make([]byte, 6<<20)
	gzipped := gzipBytes(t, huge)

	resp := postWithEncoding(t, srv, validKey, "application/json", "gzip", gzipped)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	if len(sink.logs) != 0 {
		t.Errorf("sink got %d logs, want 0", len(sink.logs))
	}
}

func TestServeLogs_UnknownContentEncoding_Returns415(t *testing.T) {
	sink := &fakeSink{}
	srv := newTestServer(sink)
	defer srv.Close()

	resp := postWithEncoding(t, srv, validKey, "application/json", "br", loadFixture(t))
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want 415", resp.StatusCode)
	}
}

func readAll(resp *http.Response) ([]byte, error) {
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ---- recordToLog unit tests ----

func strVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func intVal(i int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: i}}
}

func kv(key string, v *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: v}
}

func TestRecordToLog_SeverityTextFallback_WhenSeverityNumberZero(t *testing.T) {
	rec := &logspb.LogRecord{
		SeverityNumber: 0,
		SeverityText:   "warn",
		Body:           strVal("m"),
	}
	got := recordToLog(nil, rec)
	if got.Severity != core.SeverityWarn {
		t.Errorf("Severity = %v, want Warn", got.Severity)
	}
}

func TestRecordToLog_ZeroTime_DefaultsToNow(t *testing.T) {
	before := time.Now().UTC()
	rec := &logspb.LogRecord{TimeUnixNano: 0, Body: strVal("m")}
	got := recordToLog(nil, rec)
	after := time.Now().UTC()

	if got.Time.Before(before) || got.Time.After(after) {
		t.Errorf("Time = %v, want between %v and %v", got.Time, before, after)
	}
}

func TestRecordToLog_DeploymentEnvironmentName_PreferredOverLegacy(t *testing.T) {
	res := &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			kv("deployment.environment", strVal("legacy-env")),
			kv("deployment.environment.name", strVal("new-env")),
		},
	}
	rec := &logspb.LogRecord{Body: strVal("m")}
	got := recordToLog(res, rec)
	if got.Environment != "new-env" {
		t.Errorf("Environment = %q, want new-env", got.Environment)
	}
}

func TestRecordToLog_DeploymentEnvironment_LegacyOnly(t *testing.T) {
	res := &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			kv("deployment.environment", strVal("legacy-env")),
		},
	}
	rec := &logspb.LogRecord{Body: strVal("m")}
	got := recordToLog(res, rec)
	if got.Environment != "legacy-env" {
		t.Errorf("Environment = %q, want legacy-env", got.Environment)
	}
}

func TestRecordToLog_NonStringBody_RendersValue(t *testing.T) {
	rec := &logspb.LogRecord{Body: intVal(42)}
	got := recordToLog(nil, rec)
	if got.Body != "42" {
		t.Errorf("Body = %q, want 42", got.Body)
	}
}

func TestRecordToLog_ZeroTraceID_IsEmpty(t *testing.T) {
	rec := &logspb.LogRecord{
		Body:    strVal("m"),
		TraceId: make([]byte, 16), // all-zero
	}
	got := recordToLog(nil, rec)
	if got.TraceID != "" {
		t.Errorf("TraceID = %q, want empty", got.TraceID)
	}
}

func TestRecordToLog_TraceID_HexEncoded(t *testing.T) {
	raw, err := hex.DecodeString("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatalf("hex.DecodeString: %v", err)
	}
	rec := &logspb.LogRecord{
		Body:    strVal("m"),
		TraceId: raw,
	}
	got := recordToLog(nil, rec)
	if got.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("TraceID = %q, want 4bf92f3577b34da6a3ce929d0e0e4736", got.TraceID)
	}
}

func TestDecodeLogs_MultipleResources_EachRecordKeepsOwnResource(t *testing.T) {
	req := &collectorlogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						kv("service.name", strVal("service-a")),
						kv("region", strVal("us-east")),
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{LogRecords: []*logspb.LogRecord{{Body: strVal("from a")}}},
				},
			},
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						kv("service.name", strVal("service-b")),
						kv("region", strVal("eu-west")),
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{LogRecords: []*logspb.LogRecord{{Body: strVal("from b")}}},
				},
			},
		},
	}

	logs := decodeLogs(testProjectID, req)
	if len(logs) != 2 {
		t.Fatalf("got %d logs, want 2", len(logs))
	}

	a, b := logs[0], logs[1]
	if a.Service != "service-a" {
		t.Errorf("logs[0].Service = %q, want service-a", a.Service)
	}
	if a.Attrs["region"] != "us-east" {
		t.Errorf("logs[0].Attrs[region] = %q, want us-east", a.Attrs["region"])
	}
	if a.Body != "from a" {
		t.Errorf("logs[0].Body = %q, want %q", a.Body, "from a")
	}

	if b.Service != "service-b" {
		t.Errorf("logs[1].Service = %q, want service-b", b.Service)
	}
	if b.Attrs["region"] != "eu-west" {
		t.Errorf("logs[1].Attrs[region] = %q, want eu-west", b.Attrs["region"])
	}
	if b.Body != "from b" {
		t.Errorf("logs[1].Body = %q, want %q", b.Body, "from b")
	}
}

func TestRecordToLog_ResourceAttrs_MergedIntoAttrs(t *testing.T) {
	res := &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			kv("service.name", strVal("svc")),
			kv("host.name", strVal("host-1")),
		},
	}
	rec := &logspb.LogRecord{Body: strVal("m")}
	got := recordToLog(res, rec)
	if got.Attrs["host.name"] != "host-1" {
		t.Errorf("Attrs[host.name] = %q, want host-1", got.Attrs["host.name"])
	}
	if _, ok := got.Attrs["service.name"]; ok {
		t.Errorf("Attrs should not contain service.name (mapped to Service field)")
	}
}
