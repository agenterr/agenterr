// Package auth provides the two authentication seams Agenterr needs: key
// auth for machines (ingest agents, API clients) and a cookie session for
// the single human admin. It wraps store.Admin.LookupKey for keys and
// bcrypt + an in-memory token map for sessions — no session table, because
// there is exactly one admin.
package auth

import (
	"context"
	"sync"
	"time"

	"net/http"

	"github.com/agenterr/agenterr/internal/store"
)

// KeyAuth authenticates machine callers (ingest agents, API clients) via a
// bearer key minted by store.Admin.MintKey.
type KeyAuth interface {
	// RequireKey wraps h; expects "Authorization: Bearer <key>" of the
	// given kind ("ingest" or "api"); on success injects the project ID
	// into the request context (read back with ProjectFromContext).
	RequireKey(kind string, h http.Handler) http.Handler
}

// SessionAuth authenticates the single human admin via a cookie session.
type SessionAuth interface {
	// RequireSession wraps h; redirects to /login (303) when the session
	// cookie is absent, unknown, or expired.
	RequireSession(h http.Handler) http.Handler
	// Login checks password against the admin's bcrypt hash; on success
	// it sets the session cookie on w, using r to decide whether the
	// cookie should be marked Secure. Returns an error on mismatch —
	// the caller renders the failure.
	Login(w http.ResponseWriter, r *http.Request, password string) error
	// Logout invalidates r's session (if any) and expires the cookie.
	Logout(w http.ResponseWriter, r *http.Request)
}

// Auth implements both KeyAuth and SessionAuth.
type Auth struct {
	admin             store.Admin
	adminPasswordHash []byte

	// mu guards sessions. Single-admin deployment: there is never more
	// than one human user, so an in-memory map is sufficient — no
	// session table, no cross-process sharing, no persistence across
	// restarts (a restart just forces re-login).
	mu       sync.Mutex
	sessions map[string]time.Time // token -> expiry
}

// New constructs an Auth wrapping admin for key lookups and
// adminPasswordHash (a bcrypt hash) for session login.
func New(admin store.Admin, adminPasswordHash []byte) *Auth {
	return &Auth{
		admin:             admin,
		adminPasswordHash: adminPasswordHash,
		sessions:          make(map[string]time.Time),
	}
}

type contextKey int

const (
	projectIDKey contextKey = iota
	kindKey
)

// ProjectFromContext returns the project ID injected by RequireKey, if
// any. For an instance-level "admin" key this is always 0 — such keys are
// not scoped to a project, so callers must also check KindFromContext
// before treating 0 as meaningful.
func ProjectFromContext(ctx context.Context) (int64, bool) {
	v, ok := ctx.Value(projectIDKey).(int64)
	return v, ok
}

// KindFromContext returns the key kind ("ingest", "api", or "admin")
// injected by RequireKey, if any. This is the actual kind the caller
// authenticated with — which may be broader than the kind a route
// required, since an "admin" key satisfies any RequireKey check.
func KindFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(kindKey).(string)
	return v, ok
}
