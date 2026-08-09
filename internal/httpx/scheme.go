// Package httpx holds tiny request-inspection helpers shared by edges
// that need to know how a request really arrived (directly or through a
// TLS-terminating reverse proxy).
package httpx

import (
	"net/http"
	"strings"
)

// IsHTTPS reports whether the request arrived over TLS — directly, or
// via a reverse proxy that set X-Forwarded-Proto. The header may be a
// comma-separated chain; only the first (client-facing) hop counts.
// Trusting the header unguarded is fine for its two uses (cookie Secure
// flag, display-only snippet URLs): a client forging it can only affect
// its own session.
func IsHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	proto := r.Header.Get("X-Forwarded-Proto")
	if i := strings.IndexByte(proto, ','); i >= 0 {
		proto = proto[:i]
	}
	return strings.TrimSpace(proto) == "https"
}
