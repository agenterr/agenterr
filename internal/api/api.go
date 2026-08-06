// Package api is Agenterr's REST v1 edge (/api/v1) — the surface both
// the built-in web UI and the future cloud control plane drive. It reads
// via store.Reader and administers via store.Admin; it never touches
// store.Writer (log writes go through the pipeline only).
package api

import (
	"net/http"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/api/handlers"
	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// API mounts the /api/v1 route table.
type API struct {
	projects   *handlers.Projects
	issues     *handlers.Issues
	logs       *handlers.Logs
	stats      *handlers.Stats
	noiseRules *handlers.NoiseRules
	alertRules *handlers.AlertRules
}

// New constructs an API reading via reader and administering via admin. nr
// and rulesEngine back the noise-rule management/report routes: nr for
// plain reads, rulesEngine for every mutation so the pipeline's cached
// view of noise rules stays fresh (see internal/rules.Engine). ar and
// alertsEngine are the alert-rule analog (see internal/alerts.Engine),
// which additionally backs TestFire.
func New(reader store.Reader, admin store.Admin, nr store.NoiseRules, rulesEngine *rules.Engine, ar store.AlertRules, alertsEngine *alerts.Engine) *API {
	return &API{
		projects:   &handlers.Projects{Admin: admin, Engine: rulesEngine},
		issues:     &handlers.Issues{Reader: reader, Admin: admin},
		logs:       &handlers.Logs{Reader: reader},
		stats:      &handlers.Stats{Reader: reader, NR: nr},
		noiseRules: &handlers.NoiseRules{NR: nr, Reader: reader, Engine: rulesEngine},
		alertRules: &handlers.AlertRules{AR: ar, Engine: alertsEngine},
	}
}

// Mount registers every /api/v1 route on mux, behind key auth.
//
// Management routes (project CRUD, key minting) require an instance-level
// "admin" key. Data routes (issues, logs, stats) require an "api" key —
// which an "admin" key also satisfies (see auth.RequireKey's hierarchy) —
// and each data handler further scopes itself by the caller's project
// unless the caller is admin (see handlers.callerScope).
func (a *API) Mount(mux *http.ServeMux, keys auth.KeyAuth) {
	admin := func(h http.HandlerFunc) http.Handler {
		return keys.RequireKey("admin", h)
	}
	wrap := func(h http.HandlerFunc) http.Handler {
		return keys.RequireKey("api", h)
	}

	mux.Handle("POST /api/v1/projects", admin(a.projects.Create))
	mux.Handle("GET /api/v1/projects", admin(a.projects.List))
	mux.Handle("POST /api/v1/projects/{id}/keys", admin(a.projects.MintKey))
	// Update (parse_bodies toggle) is an "api"-tier route, not admin-only:
	// a project-scoped key can flip its own project's toggle, mirroring
	// the noise-rule list/create scope-override pattern (see
	// handlers.Projects.Update).
	mux.Handle("PATCH /api/v1/projects/{id}", wrap(a.projects.Update))

	mux.Handle("GET /api/v1/issues", wrap(a.issues.List))
	mux.Handle("GET /api/v1/issues/{id}", wrap(a.issues.Get))
	mux.Handle("PATCH /api/v1/issues/{id}", wrap(a.issues.UpdateStatus))

	mux.Handle("GET /api/v1/logs", wrap(a.logs.Search))
	mux.Handle("GET /api/v1/logs/{id}/context", wrap(a.logs.Context))

	mux.Handle("GET /api/v1/stats", wrap(a.stats.Get))

	mux.Handle("GET /api/v1/projects/{id}/noise-rules", wrap(a.noiseRules.List))
	mux.Handle("POST /api/v1/projects/{id}/noise-rules", wrap(a.noiseRules.Create))
	mux.Handle("DELETE /api/v1/noise-rules/{id}", wrap(a.noiseRules.Delete))
	mux.Handle("GET /api/v1/noise-report", wrap(a.noiseRules.Report))

	mux.Handle("GET /api/v1/projects/{id}/alert-rules", wrap(a.alertRules.List))
	mux.Handle("POST /api/v1/projects/{id}/alert-rules", wrap(a.alertRules.Create))
	mux.Handle("DELETE /api/v1/alert-rules/{id}", wrap(a.alertRules.Delete))
	mux.Handle("POST /api/v1/alert-rules/{id}/test", wrap(a.alertRules.Test))
}
