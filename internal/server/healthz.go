package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/store"
)

// pingTimeout bounds the store read healthz uses to prove the DB is
// reachable — short enough that a stuck store doesn't hang the uptime
// check, long enough not to false-positive under normal load.
const pingTimeout = 1 * time.Second

// Healthz reports process liveness for the cloud poller and uptime checks.
// It is deliberately unauthenticated (mounted with no auth wrapper) since
// those callers have no key. It pings the store with a cheap read
// (store.Admin.Projects) rather than trusting that the process is up: a
// wedged database (disk full, locked file) should flip this to degraded
// even though the HTTP server itself is still answering.
func Healthz(st store.Store, p *pipeline.Pipeline) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), pingTimeout)
		defer cancel()

		w.Header().Set("Content-Type", "application/json")

		if _, err := st.Projects(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			// Never include err.Error(): it may carry a filesystem path or
			// driver detail an external health-check consumer shouldn't see.
			json.NewEncoder(w).Encode(map[string]string{
				"status": "degraded",
				"db":     "error",
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"status":         "ok",
			"pipeline_depth": p.Pending(),
			"db":             "ok",
		})
	})
}
