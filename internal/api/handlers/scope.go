package handlers

import (
	"net/http"

	"github.com/agenterr/agenterr/internal/auth"
)

// callerScope resolves the requesting key's project ID and whether it is
// an instance-level "admin" key. RequireKey guarantees both are present
// in context by the time a handler runs.
//
// Every data-route handler (issues, logs, stats) uses this to decide
// authority over the ?project query param and over which rows an
// id-based lookup may return: an admin key is unscoped and the client's
// own filters/IDs are trusted as given; any other key kind is
// project-bound and ProjectFromContext is authoritative — it overrides
// client-supplied project filters and gates every id-based lookup so a
// row belonging to another project 404s instead of leaking.
func callerScope(r *http.Request) (projectID int64, isAdmin bool) {
	projectID, _ = auth.ProjectFromContext(r.Context())
	kind, _ := auth.KindFromContext(r.Context())
	return projectID, kind == "admin"
}
