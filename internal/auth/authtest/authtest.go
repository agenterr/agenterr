// Package authtest builds request contexts carrying the same project ID /
// key-kind values auth.RequireKey injects, for tests that call handlers
// directly (bypassing the HTTP middleware chain) but still need a
// context that looks like one RequireKey produced.
//
// It does this by actually running auth.RequireKey — the real production
// code path — against a minimal stub key lookup that always resolves to
// the wanted scope, rather than poking context values in directly. That
// matters: it means the production auth package never has to export a
// "mint arbitrary scope" shortcut (an earlier version did, as
// auth.NewTestContext, and it was a forgeable-admin landmine sitting in
// a package every production caller imports). No production file
// imports authtest — this package exists purely for other packages'
// tests.
package authtest

import (
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/store"
)

// stubAdmin implements just enough of store.Admin to satisfy
// auth.RequireKey's single LookupKey call; every other method is
// unreachable from that call path and left to panic via the embedded nil
// interface if ever invoked.
type stubAdmin struct {
	store.Admin
	projectID int64
	kind      string
}

func (s stubAdmin) LookupKey(context.Context, string) (int64, string, error) {
	return s.projectID, s.kind, nil
}

// Context returns a context carrying projectID and kind exactly as
// auth.RequireKey(kind, ...) would inject them for a request bearing a
// valid key of that kind.
func Context(projectID int64, kind string) context.Context {
	a := auth.New(stubAdmin{projectID: projectID, kind: kind}, nil)

	var captured context.Context
	h := a.RequireKey(kind, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = r.Context()
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer authtest")
	h.ServeHTTP(httptest.NewRecorder(), req)

	return captured
}
