// Package jsonapi is Agenterr's plain-JSON ingest edge: a single POST
// endpoint that accepts a JSON array (or single object) of loosely-shaped
// log records and forwards them to the pipeline.
package jsonapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/ingest"
	"github.com/agenterr/agenterr/internal/pipeline"
)

// Handler serves the plain-JSON ingest endpoint. It implements
// ingest.Ingester.
type Handler struct {
	sink    ingest.Sink
	maxBody int64
}

// New constructs a Handler that forwards decoded logs to sink. maxBody caps
// the accepted request body size in bytes; maxBody <= 0 falls back to
// ingest.MaxBody.
func New(sink ingest.Sink, maxBody int64) *Handler {
	if maxBody <= 0 {
		maxBody = ingest.MaxBody
	}
	return &Handler{sink: sink, maxBody: maxBody}
}

// Mount registers the ingest route behind key auth.
func (h *Handler) Mount(mux *http.ServeMux, keys auth.KeyAuth) {
	mux.Handle("POST /api/v1/ingest", keys.RequireKey("ingest", http.HandlerFunc(h.serveIngest)))
}

func (h *Handler) serveIngest(w http.ResponseWriter, r *http.Request) {
	projectID, ok := auth.ProjectFromContext(r.Context())
	if !ok {
		// RequireKey guarantees a project ID on every request that reaches
		// this handler; this branch exists only to avoid a silent zero
		// ProjectID if that contract is ever broken.
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBody)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "error reading request body")
		return
	}

	wireLogs, err := decodeWireLogs(data)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if len(wireLogs) == 0 {
		writeAccepted(w, 0)
		return
	}

	logs := make([]core.Log, len(wireLogs))
	for i, wl := range wireLogs {
		logs[i] = core.Log{
			ProjectID:   projectID,
			Time:        wl.Time,
			Severity:    wl.Severity,
			Body:        wl.Body,
			Service:     wl.Service,
			Environment: wl.Environment,
			Release:     wl.Release,
			TraceID:     wl.TraceID,
			Attrs:       wl.Attrs,
		}
	}

	if err := h.sink.Enqueue(logs); err != nil {
		if errors.Is(err, pipeline.ErrFull) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return
		}
		// Not expected in practice (Sink is documented to only ever return
		// ErrFull), but handled rather than panicking.
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeAccepted(w, len(logs))
}

// decodeWireLogs accepts either a JSON array of log objects or a single log
// object (treated as a batch of one).
func decodeWireLogs(data []byte) ([]wireLog, error) {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var logs []wireLog
		if err := json.Unmarshal(data, &logs); err != nil {
			return nil, fmt.Errorf("decoding log array: %w", err)
		}
		return logs, nil
	}

	var single wireLog
	if err := json.Unmarshal(data, &single); err != nil {
		return nil, fmt.Errorf("decoding log object: %w", err)
	}
	return []wireLog{single}, nil
}

func writeAccepted(w http.ResponseWriter, n int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, `{"accepted":%d}`, n)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, err := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	if err != nil {
		// msg is always a static string literal from callers in this
		// file, so Marshal cannot realistically fail.
		body = []byte(`{"error":"internal error"}`)
	}
	_, _ = w.Write(body)
}

// wireLog is the tolerant wire shape of a single ingested log: field names
// vary by client, timestamps may be strings or unix seconds, and most
// fields are optional. UnmarshalJSON normalizes all of that into a
// core.Log-shaped record.
type wireLog struct {
	Body        string
	Time        time.Time
	Severity    core.Severity
	Service     string
	Environment string
	Release     string
	TraceID     string
	Attrs       map[string]string
}

// UnmarshalJSON implements the field tolerance described on wireLog: the
// message may arrive as message/msg/body (first non-empty wins, in that
// order), the timestamp as timestamp/time/ts (first parseable wins, in that
// order) as either an RFC3339(Nano) string or a unix-seconds JSON number,
// and severity as a free-form string parsed by core.ParseSeverity (absent
// defaults to INFO, matching ParseSeverity's own zero-value behavior).
func (w *wireLog) UnmarshalJSON(data []byte) error {
	var aux struct {
		Message     *string                    `json:"message"`
		Msg         *string                    `json:"msg"`
		Body        *string                    `json:"body"`
		Timestamp   json.RawMessage            `json:"timestamp"`
		Time        json.RawMessage            `json:"time"`
		Ts          json.RawMessage            `json:"ts"`
		Severity    string                     `json:"severity"`
		Attributes  map[string]json.RawMessage `json:"attributes"`
		Service     string                     `json:"service"`
		Environment string                     `json:"environment"`
		Release     string                     `json:"release"`
		TraceID     string                     `json:"trace_id"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	for _, cand := range []*string{aux.Message, aux.Msg, aux.Body} {
		if cand != nil && *cand != "" {
			w.Body = *cand
			break
		}
	}

	w.Time = parseTimestamp(aux.Timestamp, aux.Time, aux.Ts)
	w.Severity = core.ParseSeverity(aux.Severity)
	w.Service = aux.Service
	w.Environment = aux.Environment
	w.Release = aux.Release
	w.TraceID = aux.TraceID

	if len(aux.Attributes) > 0 {
		w.Attrs = make(map[string]string, len(aux.Attributes))
		for k, raw := range aux.Attributes {
			w.Attrs[k] = scalarToString(raw)
		}
	}

	return nil
}

// parseTimestamp returns the first candidate that parses as either an
// RFC3339(Nano) string or a unix-seconds JSON number, in the order given.
// A missing or wholly unparseable timestamp falls back to now, in UTC.
func parseTimestamp(candidates ...json.RawMessage) time.Time {
	for _, raw := range candidates {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		if t, ok := tryParseTimestamp(raw); ok {
			return t
		}
	}
	return time.Now().UTC()
}

func tryParseTimestamp(raw json.RawMessage) (time.Time, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UTC(), true
		}
		return time.Time{}, false
	}

	var seconds float64
	if err := json.Unmarshal(raw, &seconds); err == nil {
		return time.Unix(int64(seconds), 0).UTC(), true
	}

	return time.Time{}, false
}

// scalarToString renders a JSON scalar (string, number, bool, null) as a
// string for storage in core.Log.Attrs, which is untyped. Strings pass
// through verbatim; everything else is formatted with %v.
func scalarToString(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}

	var v any
	if err := json.Unmarshal(raw, &v); err == nil {
		return fmt.Sprintf("%v", v)
	}

	return string(raw)
}
