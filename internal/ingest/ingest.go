// Package ingest turns wire formats into core.Logs and feeds the pipeline.
package ingest

import (
	"net/http"

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
