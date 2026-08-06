package core

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestParseStructuredBody_JSON(t *testing.T) {
	arrival := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		in   Log
		want func(t *testing.T, got Log)
	}{
		{
			name: "slog error line lifts level msg and attrs",
			in: Log{Time: arrival, Severity: SeverityInfo,
				Body: `{"time":"2026-08-06T11:59:30Z","level":"ERROR","msg":"charge failed","request_id":"req-42","attempt":3}`},
			want: func(t *testing.T, got Log) {
				if got.Severity != SeverityError {
					t.Errorf("severity = %v, want ERROR", got.Severity)
				}
				if got.Body != "charge failed" {
					t.Errorf("body = %q, want %q", got.Body, "charge failed")
				}
				if got.Attrs["request_id"] != "req-42" {
					t.Errorf("request_id = %q, want req-42", got.Attrs["request_id"])
				}
				if got.Attrs["attempt"] != "3" {
					t.Errorf("attempt = %q, want 3", got.Attrs["attempt"])
				}
				wantT := time.Date(2026, 8, 6, 11, 59, 30, 0, time.UTC)
				if !got.Time.Equal(wantT) {
					t.Errorf("time = %v, want %v", got.Time, wantT)
				}
			},
		},
		{
			name: "explicit non-info severity is not overridden",
			in: Log{Time: arrival, Severity: SeverityWarn,
				Body: `{"level":"error","msg":"x"}`},
			want: func(t *testing.T, got Log) {
				if got.Severity != SeverityWarn {
					t.Errorf("severity = %v, want WARN (explicit wins)", got.Severity)
				}
			},
		},
		{
			name: "plain text body untouched",
			in:   Log{Time: arrival, Body: "connection refused to db:5432"},
			want: func(t *testing.T, got Log) {
				if got.Body != "connection refused to db:5432" || got.Attrs != nil {
					t.Errorf("plain text was modified: %+v", got)
				}
			},
		},
		{
			name: "JSON object without message or level keys untouched",
			in:   Log{Time: arrival, Body: `{"foo":"bar","baz":1}`},
			want: func(t *testing.T, got Log) {
				if got.Body != `{"foo":"bar","baz":1}` || got.Attrs != nil {
					t.Errorf("keyless JSON was lifted: %+v", got)
				}
			},
		},
		{
			name: "invalid JSON with braces untouched",
			in:   Log{Time: arrival, Body: `{"msg": broken}`},
			want: func(t *testing.T, got Log) {
				if got.Body != `{"msg": broken}` {
					t.Errorf("invalid JSON was modified: %q", got.Body)
				}
			},
		},
		{
			name: "trailing content after object rejected",
			in:   Log{Time: arrival, Body: `{"msg":"a"} {"msg":"b"}`},
			want: func(t *testing.T, got Log) {
				if got.Body != `{"msg":"a"} {"msg":"b"}` {
					t.Errorf("multi-object body was lifted: %q", got.Body)
				}
			},
		},
		{
			name: "numeric level and epoch millis time",
			in: Log{Time: arrival, Severity: SeverityInfo,
				Body: `{"lvl":17,"message":"boom","ts":1786017570000}`},
			want: func(t *testing.T, got Log) {
				if got.Severity != SeverityError {
					t.Errorf("severity = %v, want ERROR (OTLP 17)", got.Severity)
				}
				if got.Body != "boom" {
					t.Errorf("body = %q, want boom", got.Body)
				}
				if got.Time.Equal(arrival) {
					t.Error("epoch millis ts was not applied")
				}
			},
		},
		{
			name: "time outside sanity window ignored",
			in: Log{Time: arrival, Severity: SeverityInfo,
				Body: `{"msg":"old","time":"2020-01-01T00:00:00Z"}`},
			want: func(t *testing.T, got Log) {
				if !got.Time.Equal(arrival) {
					t.Errorf("time = %v, want arrival time kept", got.Time)
				}
			},
		},
		{
			name: "nested object flattens one level, deeper is JSON-encoded",
			in: Log{Time: arrival,
				Body: `{"msg":"m","http":{"method":"GET","meta":{"a":1}},"tags":["x","y"]}`},
			want: func(t *testing.T, got Log) {
				if got.Attrs["http.method"] != "GET" {
					t.Errorf("http.method = %q, want GET", got.Attrs["http.method"])
				}
				if got.Attrs["http.meta"] != `{"a":1}` {
					t.Errorf("http.meta = %q, want JSON-encoded", got.Attrs["http.meta"])
				}
				if got.Attrs["tags"] != `["x","y"]` {
					t.Errorf("tags = %q, want JSON-encoded array", got.Attrs["tags"])
				}
			},
		},
		{
			name: "existing attrs never overwritten and passthroughs respect non-empty",
			in: Log{Time: arrival, Service: "api", Attrs: map[string]string{"request_id": "orig"},
				Body: `{"msg":"m","request_id":"new","service":"other","trace_id":"t1"}`},
			want: func(t *testing.T, got Log) {
				if got.Attrs["request_id"] != "orig" {
					t.Errorf("request_id overwritten: %q", got.Attrs["request_id"])
				}
				if got.Service != "api" {
					t.Errorf("service overwritten: %q", got.Service)
				}
				if got.TraceID != "t1" {
					t.Errorf("trace_id = %q, want t1 (was empty)", got.TraceID)
				}
			},
		},
		{
			name: "attr cap and value truncation",
			in: Log{Time: arrival,
				Body: bigJSONBody(60, 3000)},
			want: func(t *testing.T, got Log) {
				if len(got.Attrs) != 50 {
					t.Errorf("lifted %d attrs, want 50", len(got.Attrs))
				}
				for k, v := range got.Attrs {
					if len(v) > 2000 {
						t.Errorf("attr %s length %d exceeds 2000", k, len(v))
					}
				}
			},
		},
		{
			name: "empty msg falls through to message",
			in: Log{Time: arrival, Severity: SeverityInfo,
				Body: `{"msg":"","message":"hi","level":"warn"}`},
			want: func(t *testing.T, got Log) {
				if got.Body != "hi" {
					t.Errorf("body = %q, want hi (empty msg should fall through)", got.Body)
				}
				if got.Severity != SeverityWarn {
					t.Errorf("severity = %v, want WARN", got.Severity)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want(t, ParseStructuredBody(tt.in))
		})
	}
}

func TestParseStructuredBody_AttrsNotMutated(t *testing.T) {
	arrival := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	// Keep a reference to the original map to verify it's not mutated.
	orig := map[string]string{"orig_key": "orig_val"}
	in := Log{Time: arrival, Attrs: orig,
		Body: `{"msg":"m","new_key":"new_val"}`}
	got := ParseStructuredBody(in)

	// Verify the returned Log has both keys.
	if got.Attrs["orig_key"] != "orig_val" {
		t.Errorf("original attr lost in got.Attrs: %v", got.Attrs)
	}
	if got.Attrs["new_key"] != "new_val" {
		t.Errorf("new attr not lifted: %v", got.Attrs)
	}
	if len(got.Attrs) != 2 {
		t.Errorf("got.Attrs count = %d, want 2", len(got.Attrs))
	}

	// Verify the caller's original map was NOT mutated.
	if len(orig) != 1 || orig["orig_key"] != "orig_val" {
		t.Errorf("caller's original Attrs map was mutated: %v", orig)
	}
}

func TestParseStructuredBody_Logfmt(t *testing.T) {
	arrival := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		in   Log
		want func(t *testing.T, got Log)
	}{
		{
			name: "logfmt error line lifts level msg and attrs",
			in: Log{Time: arrival, Severity: SeverityInfo,
				Body: `level=error msg="write failed" request_id=req-9 retries=2`},
			want: func(t *testing.T, got Log) {
				if got.Severity != SeverityError {
					t.Errorf("severity = %v, want ERROR", got.Severity)
				}
				if got.Body != "write failed" {
					t.Errorf("body = %q, want %q", got.Body, "write failed")
				}
				if got.Attrs["request_id"] != "req-9" || got.Attrs["retries"] != "2" {
					t.Errorf("attrs = %v", got.Attrs)
				}
			},
		},
		{
			name: "prose containing equals sign untouched",
			in:   Log{Time: arrival, Body: `retry count=3 exceeded for host db-1`},
			want: func(t *testing.T, got Log) {
				if got.Body != `retry count=3 exceeded for host db-1` || got.Attrs != nil {
					t.Errorf("prose was lifted: %+v", got)
				}
			},
		},
		{
			name: "single pair untouched (needs at least two)",
			in:   Log{Time: arrival, Body: `level=info`},
			want: func(t *testing.T, got Log) {
				if got.Attrs != nil {
					t.Errorf("single-pair line was lifted: %+v", got)
				}
			},
		},
		{
			name: "pairs without message or level key untouched",
			in:   Log{Time: arrival, Body: `foo=bar baz=qux`},
			want: func(t *testing.T, got Log) {
				if got.Body != `foo=bar baz=qux` || got.Attrs != nil {
					t.Errorf("keyless logfmt was lifted: %+v", got)
				}
			},
		},
		{
			name: "multiline body untouched",
			in:   Log{Time: arrival, Body: "level=error msg=a\nlevel=error msg=b"},
			want: func(t *testing.T, got Log) {
				if got.Attrs != nil {
					t.Errorf("multiline was lifted: %+v", got)
				}
			},
		},
		{
			name: "quoted value with escaped quote",
			in: Log{Time: arrival,
				Body: `level=warn msg="disk \"a\" slow" dev=sda`},
			want: func(t *testing.T, got Log) {
				if got.Body != `disk "a" slow` {
					t.Errorf("body = %q", got.Body)
				}
				if got.Attrs["dev"] != "sda" {
					t.Errorf("dev = %q", got.Attrs["dev"])
				}
			},
		},
		{
			name: "tab-separated logfmt line",
			in: Log{Time: arrival, Severity: SeverityInfo,
				Body: "level=error\tmsg=\"tabbed line\"\tid=3"},
			want: func(t *testing.T, got Log) {
				if got.Severity != SeverityError {
					t.Errorf("severity = %v, want ERROR", got.Severity)
				}
				if got.Body != "tabbed line" {
					t.Errorf("body = %q, want %q", got.Body, "tabbed line")
				}
				if got.Attrs["id"] != "3" {
					t.Errorf("id = %q, want 3", got.Attrs["id"])
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want(t, ParseStructuredBody(tt.in))
		})
	}
}

// bigJSONBody builds {"msg":"m","k00":"vvv...","k01":...} with n extra keys
// whose values are valueLen bytes long.
func bigJSONBody(n, valueLen int) string {
	var b strings.Builder
	b.WriteString(`{"msg":"m"`)
	val := strings.Repeat("v", valueLen)
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `,"k%02d":%q`, i, val)
	}
	b.WriteString("}")
	return b.String()
}
