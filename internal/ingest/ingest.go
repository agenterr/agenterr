// Package ingest turns wire formats into core.Logs and feeds the pipeline.
package ingest

import (
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
)

// Ingester mounts a wire-format-specific edge (e.g. plain JSON, OTLP) onto
// mux, wiring it behind keys' key auth.
type Ingester interface {
	Mount(mux *http.ServeMux, keys auth.KeyAuth)
}

// Sink accepts fully-mapped logs for durable writing. Satisfied by
// *pipeline.Pipeline; edges depend on this narrow interface rather than the
// concrete pipeline so they can be tested with a fake.
type Sink interface {
	Enqueue(logs []core.Log) error
}

// MaxBody is the maximum request body size (bytes) any ingest edge accepts.
// Requests larger than this are rejected with 413 before they are decoded.
const MaxBody = 5 << 20

// ReadBoundedBody reads r's body — decompressing it first when
// Content-Encoding: gzip is set — bounded to maxBody either way. Shared by
// every ingest edge (OTLP and plain JSON alike) so gzip support, the
// wire-format agenterr-ship always uses (see internal/ship/sender), isn't
// something each edge has to remember to wire up on its own.
//
// On any failure (an unsupported encoding, a corrupt gzip stream, or a body
// over maxBody either compressed or decompressed) it calls writeErr with an
// appropriate status and message and returns ok=false; callers should return
// immediately without writing anything else.
func ReadBoundedBody(w http.ResponseWriter, r *http.Request, maxBody int64, writeErr func(status int, msg string)) ([]byte, bool) {
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
			writeErr(http.StatusBadRequest, "invalid gzip body")
			return nil, false
		}
		defer func() { _ = gz.Close() }()
		reader = io.LimitReader(gz, maxBody+1)
	default:
		writeErr(http.StatusUnsupportedMediaType, "unsupported content encoding")
		return nil, false
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(http.StatusRequestEntityTooLarge, "request body too large")
			return nil, false
		}
		writeErr(http.StatusBadRequest, "error reading request body")
		return nil, false
	}
	if int64(len(data)) > maxBody {
		writeErr(http.StatusRequestEntityTooLarge, "request body too large")
		return nil, false
	}
	return data, true
}
