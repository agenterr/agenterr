package auth

import (
	"errors"
	"net"
	"net/http"
	"time"
)

const (
	// loginFailLimit failed attempts per loginFailWindow, per client IP,
	// before Login refuses even correct passwords. bcrypt remains the
	// per-attempt cost; this bounds the attempt rate itself.
	loginFailLimit  = 5
	loginFailWindow = time.Minute
)

// ErrRateLimited is returned by Login when the client has exceeded
// loginFailLimit failures within loginFailWindow.
var ErrRateLimited = errors.New("too many login attempts")

type failWindow struct {
	count int
	start time.Time
}

// clientIP keys the limiter by the TCP peer, never X-Forwarded-For: a
// forged XFF would let an attacker rotate keys at will. Behind a reverse
// proxy every client shares the proxy's IP — an accepted trade-off for a
// single-admin tool, documented in the README.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// Callers hold a.mu.
func (a *Auth) loginAllowed(ip string, now time.Time) bool {
	fw, ok := a.failures[ip]
	if !ok || now.Sub(fw.start) >= loginFailWindow {
		return true
	}
	return fw.count < loginFailLimit
}

// Callers hold a.mu.
func (a *Auth) recordLoginFailure(ip string, now time.Time) {
	fw := a.failures[ip]
	if now.Sub(fw.start) >= loginFailWindow {
		fw = failWindow{start: now}
	}
	fw.count++
	a.failures[ip] = fw
}

// Callers hold a.mu.
func sweepExpiredFailures(failures map[string]failWindow, now time.Time) {
	for ip, fw := range failures {
		if now.Sub(fw.start) >= loginFailWindow {
			delete(failures, ip)
		}
	}
}
