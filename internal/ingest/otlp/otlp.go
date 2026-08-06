// Package otlp is Agenterr's OTLP/HTTP logs ingest edge: the
// spec-mandated POST /v1/logs endpoint that makes Agenterr a drop-in
// target for Vector, Fluent Bit, Grafana Alloy, and the OTel Collector.
// It accepts an ExportLogsServiceRequest as either binary protobuf
// (application/x-protobuf) or protojson (application/json).
package otlp

import (
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/ingest"
	"github.com/agenterr/agenterr/internal/pipeline"

	collectorlogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	contentTypeProtobuf = "application/x-protobuf"
	contentTypeJSON     = "application/json"
)

// Handler serves the OTLP/HTTP logs ingest endpoint. It implements
// ingest.Ingester.
type Handler struct {
	sink    ingest.Sink
	maxBody int64
}

// New constructs a Handler that forwards decoded logs to sink. maxBody caps
// the accepted (decompressed) request body size in bytes; maxBody <= 0
// falls back to ingest.MaxBody.
func New(sink ingest.Sink, maxBody int64) *Handler {
	if maxBody <= 0 {
		maxBody = ingest.MaxBody
	}
	return &Handler{sink: sink, maxBody: maxBody}
}

// Mount registers the OTLP-spec-mandated route behind key auth.
func (h *Handler) Mount(mux *http.ServeMux, keys auth.KeyAuth) {
	mux.Handle("POST /v1/logs", keys.RequireKey("ingest", http.HandlerFunc(h.serveLogs)))
}

func (h *Handler) serveLogs(w http.ResponseWriter, r *http.Request) {
	projectID, ok := auth.ProjectFromContext(r.Context())
	if !ok {
		// RequireKey guarantees a project ID on every request that reaches
		// this handler; this branch exists only to avoid a silent zero
		// ProjectID if that contract is ever broken.
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (mediaType != contentTypeProtobuf && mediaType != contentTypeJSON) {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported content type")
		return
	}

	data, ok := readBoundedBody(w, r, h.maxBody)
	if !ok {
		return
	}

	req, err := unmarshalRequest(mediaType, data)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid OTLP request body")
		return
	}

	logs := decodeLogs(projectID, req)
	if len(logs) > 0 && !h.enqueue(w, logs) {
		return
	}

	writeAccepted(w, mediaType)
}

// readBoundedBody reads r's (possibly gzip-compressed) body, bounded to
// maxBody either way, writing an appropriate error response and
// returning ok=false on any failure (bad encoding, bad gzip stream, or
// body over the limit).
func readBoundedBody(w http.ResponseWriter, r *http.Request, maxBody int64) ([]byte, bool) {
	// MaxBytesReader bounds the wire (possibly gzip-compressed) stream; for
	// gzip requests the decompressed stream is separately bounded below to
	// guard against zip bombs (a small compressed body expanding far past
	// maxBody).
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)

	var reader io.Reader
	switch enc := r.Header.Get("Content-Encoding"); {
	case enc == "":
		reader = r.Body
	case strings.EqualFold(enc, "gzip"):
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid gzip body")
			return nil, false
		}
		defer func() { _ = gz.Close() }()
		reader = io.LimitReader(gz, maxBody+1)
	default:
		writeError(w, http.StatusUnsupportedMediaType, "unsupported content encoding")
		return nil, false
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		}
		writeError(w, http.StatusBadRequest, "error reading request body")
		return nil, false
	}
	if int64(len(data)) > maxBody {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return nil, false
	}
	return data, true
}

// unmarshalRequest decodes data per mediaType (protobuf or JSON) into an
// OTLP ExportLogsServiceRequest.
func unmarshalRequest(mediaType string, data []byte) (*collectorlogspb.ExportLogsServiceRequest, error) {
	var req collectorlogspb.ExportLogsServiceRequest
	var err error
	if mediaType == contentTypeProtobuf {
		err = proto.Unmarshal(data, &req)
	} else {
		err = protojson.Unmarshal(data, &req)
	}
	if err != nil {
		return nil, err
	}
	return &req, nil
}

// enqueue hands logs to the sink, writing the appropriate error response
// and returning false if that fails.
func (h *Handler) enqueue(w http.ResponseWriter, logs []core.Log) bool {
	if err := h.sink.Enqueue(logs); err != nil {
		if errors.Is(err, pipeline.ErrFull) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "too many requests")
			return false
		}
		// Not expected in practice (Sink is documented to only ever
		// return ErrFull), but handled rather than panicking.
		writeError(w, http.StatusInternalServerError, "internal error")
		return false
	}
	return true
}

// decodeLogs walks resource -> scope -> records, mapping each record with
// recordToLog and stamping the caller's project ID.
func decodeLogs(projectID int64, req *collectorlogspb.ExportLogsServiceRequest) []core.Log {
	var logs []core.Log
	for _, rl := range req.GetResourceLogs() {
		res := rl.GetResource()
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				l := recordToLog(res, rec)
				l.ProjectID = projectID
				logs = append(logs, l)
			}
		}
	}
	return logs
}

// recordToLog maps a single OTLP resource + log record pair to a core.Log.
// Kept pure (no I/O, no side effects) so it can be unit-tested directly.
func recordToLog(res *resourcepb.Resource, rec *logspb.LogRecord) core.Log {
	var log core.Log

	var legacyEnv, newEnv string
	var attrs map[string]string
	for _, kv := range res.GetAttributes() {
		val := anyValueToString(kv.GetValue())
		switch kv.GetKey() {
		case "service.name":
			log.Service = val
		case "service.version":
			log.Release = val
		case "deployment.environment":
			legacyEnv = val
		case "deployment.environment.name":
			newEnv = val
		default:
			if attrs == nil {
				attrs = make(map[string]string)
			}
			attrs[kv.GetKey()] = val
		}
	}
	if newEnv != "" {
		log.Environment = newEnv
	} else {
		log.Environment = legacyEnv
	}

	// severityNumber, when present, maps to core.Severity via the OTLP
	// numeric bands (see core.ParseSeverity's numeric-string handling,
	// mirrored here directly on the SeverityNumber enum):
	//   1-4 TRACE, 5-8 DEBUG, 9-12 INFO, 13-16 WARN, 17-20 ERROR, 21-24 FATAL.
	// severityText is used only as a fallback when severityNumber is unset
	// (0 / UNSPECIFIED).
	if n := rec.GetSeverityNumber(); n != 0 {
		log.Severity = severityFromNumber(n)
	} else {
		log.Severity = core.ParseSeverity(rec.GetSeverityText())
	}

	log.Body = anyValueToString(rec.GetBody())

	for _, kv := range rec.GetAttributes() {
		if attrs == nil {
			attrs = make(map[string]string)
		}
		attrs[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	log.Attrs = attrs

	if t := rec.GetTimeUnixNano(); t != 0 {
		log.Time = time.Unix(0, int64(t)).UTC()
	} else {
		log.Time = time.Now().UTC()
	}

	if tid := rec.GetTraceId(); len(tid) > 0 && !isAllZero(tid) {
		log.TraceID = hex.EncodeToString(tid)
	}

	return log
}

// severityFromNumber maps an OTLP SeverityNumber to core.Severity via the
// numeric bands defined by the OTLP log data model.
func severityFromNumber(n logspb.SeverityNumber) core.Severity {
	switch {
	case n >= 21: // FATAL, FATAL2-4
		return core.SeverityFatal
	case n >= 17: // ERROR, ERROR2-4
		return core.SeverityError
	case n >= 13: // WARN, WARN2-4
		return core.SeverityWarn
	case n >= 9: // INFO, INFO2-4
		return core.SeverityInfo
	case n >= 5: // DEBUG, DEBUG2-4
		return core.SeverityDebug
	case n >= 1: // TRACE, TRACE2-4
		return core.SeverityTrace
	default:
		return core.SeverityInfo
	}
}

// anyValueToString renders an OTLP AnyValue as a string: string values pass
// through verbatim; scalar (bool/int/double) values render via %v;
// kvlist/array values render via their compact protojson string form.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%v", x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%v", x.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%v", x.DoubleValue)
	case *commonpb.AnyValue_BytesValue:
		return fmt.Sprintf("%v", x.BytesValue)
	case *commonpb.AnyValue_KvlistValue, *commonpb.AnyValue_ArrayValue:
		b, err := protojson.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	default:
		return ""
	}
}

func isAllZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// writeAccepted writes an empty ExportLogsServiceResponse marshaled in the
// same content type as the request, per the OTLP/HTTP spec.
func writeAccepted(w http.ResponseWriter, mediaType string) {
	resp := &collectorlogspb.ExportLogsServiceResponse{}

	var body []byte
	var err error
	if mediaType == contentTypeProtobuf {
		body, err = proto.Marshal(resp)
	} else {
		body, err = protojson.Marshal(resp)
	}
	if err != nil {
		// resp is always empty, so Marshal cannot realistically fail.
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.Header().Set("Content-Type", mediaType)
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, err := json.Marshal(struct {
		Error string `json:"error"`
	}{msg})
	if err != nil {
		// msg is always a static string literal from callers in this file,
		// so Marshal cannot realistically fail.
		body = []byte(`{"error":"internal error"}`)
	}
	_, _ = w.Write(body)
}
