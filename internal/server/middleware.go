package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

// withMiddleware wraps h with the two process-wide layers every request
// passes through, outermost first: recover, then request logging. Auth is
// NOT applied here — it stays per-route, already wired inside each edge's
// Mount call, so unauthenticated requests still reach a handler that can
// answer 401/303 itself (see server_test.go's mount-order assertions).
func withMiddleware(h http.Handler, logger *slog.Logger) http.Handler {
	return recoverMiddleware(requestLogMiddleware(h, logger), logger)
}

// recoverMiddleware is the outermost layer: a panic anywhere below it
// (including in requestLogMiddleware or a route handler) is caught, logged
// with a stack trace, and turned into a 500 JSON response instead of
// crashing the process — a single agent's malformed payload must not take
// down ingestion for every other project.
func recoverMiddleware(h http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error("panic recovered",
					"error", rec,
					"stack", string(debug.Stack()),
					"method", r.Method,
					"path", r.URL.Path,
				)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "internal error"})
			}
		}()
		h.ServeHTTP(w, r)
	})
}

// requestLogMiddleware logs one slog.Info line per request: method, path,
// status, duration, and response bytes. Deliberately excludes the query
// string (may carry search terms — internal/api/logs and internal/web's
// search both put user-supplied text there) and any request headers
// (Authorization carries bearer keys).
func requestLogMiddleware(h http.Handler, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(sw, r)
		logger.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"bytes", sw.bytes,
		)
	})
}

// statusWriter wraps http.ResponseWriter to capture the status code and
// byte count written, for the request log — net/http gives no other way
// to observe what a handler answered with.
type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (sw *statusWriter) WriteHeader(code int) {
	sw.status = code
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(b []byte) (int, error) {
	n, err := sw.ResponseWriter.Write(b)
	sw.bytes += n
	return n, err
}

// Flush passes through to the underlying ResponseWriter's http.Flusher,
// when it has one. The MCP edge (internal/mcp) streams over SSE and needs
// this to reach the client incrementally; without it, wrapping would
// silently buffer a streaming response until it closed.
func (sw *statusWriter) Flush() {
	if f, ok := sw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
