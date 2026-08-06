package auth

import (
	"context"
	"net/http"
	"strings"
)

// unauthorized writes a uniform 401 body regardless of why auth failed —
// missing header, malformed header, unknown key, or kind mismatch all look
// identical to the caller, so nothing about key validity leaks.
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
}

// RequireKey wraps h; expects "Authorization: Bearer <key>" of the given
// kind ("ingest" or "api"); on success injects the project ID into ctx.
func (a *Auth) RequireKey(kind string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			unauthorized(w)
			return
		}

		projectID, gotKind, err := a.admin.LookupKey(r.Context(), token)
		if err != nil {
			unauthorized(w)
			return
		}
		if gotKind != kind {
			unauthorized(w)
			return
		}

		ctx := context.WithValue(r.Context(), projectIDKey, projectID)
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <token>"
// header value. Returns ok=false for missing, empty, or malformed headers.
func bearerToken(header string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if token == "" {
		return "", false
	}
	return token, true
}
