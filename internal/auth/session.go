package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/agenterr/agenterr/internal/httpx"
	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookieName = "agenterr_session"
	sessionTTL        = 7 * 24 * time.Hour
)

// Login checks password against the admin's bcrypt hash. On success it
// mints a random session token, records its expiry, and sets the session
// cookie on w. On mismatch it returns an error and sets no cookie — the
// caller is responsible for rendering the failure (e.g. re-showing the
// login form).
func (a *Auth) Login(w http.ResponseWriter, r *http.Request, password string) error {
	ip := clientIP(r)
	now := time.Now()

	a.mu.Lock()
	sweepExpiredFailures(a.failures, now)
	allowed := a.loginAllowed(ip, now)
	a.mu.Unlock()
	if !allowed {
		return ErrRateLimited
	}

	if err := bcrypt.CompareHashAndPassword(a.adminPasswordHash, []byte(password)); err != nil {
		a.mu.Lock()
		a.recordLoginFailure(ip, now)
		a.mu.Unlock()
		return errors.New("invalid password")
	}

	token, err := newSessionToken()
	if err != nil {
		return err
	}

	expiry := time.Now().Add(sessionTTL)
	a.mu.Lock()
	sweepExpiredSessions(a.sessions) // single admin -> map is tiny; O(handful)
	a.sessions[token] = expiry
	delete(a.failures, ip)
	a.mu.Unlock()

	// Secure tracks how the request arrived (direct TLS or a proxy's
	// X-Forwarded-Proto) so plain-HTTP localhost quickstarts still work
	// while TLS deployments get a Secure cookie automatically.
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expiry,
		HttpOnly: true,
		Secure:   httpx.IsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
	})
	return nil
}

// sweepExpiredSessions deletes all expired entries from sessions. Callers
// must hold a.mu. Session count is bounded by concurrent logins from a
// single admin, so a full-map scan on every Login is negligible — this
// keeps the map from growing unboundedly since expired entries are
// otherwise never removed except lazily on lookup (see RequireSession).
func sweepExpiredSessions(sessions map[string]time.Time) {
	now := time.Now()
	for token, expiry := range sessions {
		if now.After(expiry) {
			delete(sessions, token)
		}
	}
}

// Logout invalidates r's session token (if any) and expires the cookie on
// w.
func (a *Auth) Logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookieName); err == nil {
		a.mu.Lock()
		delete(a.sessions, c.Value)
		a.mu.Unlock()
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   httpx.IsHTTPS(r),
		SameSite: http.SameSiteLaxMode,
	})
}

// RequireSession wraps h; redirects to /login (303) when the session
// cookie is absent, unknown, or expired.
func (a *Auth) RequireSession(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookieName)
		if err != nil {
			redirectToLogin(w, r)
			return
		}

		a.mu.Lock()
		expiry, ok := a.sessions[c.Value]
		expired := ok && time.Now().After(expiry)
		if expired {
			// Delete-on-read: an expired entry found during lookup is
			// purged immediately rather than waiting for the next
			// Login sweep.
			delete(a.sessions, c.Value)
		}
		a.mu.Unlock()

		if !ok || expired {
			redirectToLogin(w, r)
			return
		}

		h.ServeHTTP(w, r)
	})
}

func redirectToLogin(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// newSessionToken returns a random 32-byte token, hex-encoded.
func newSessionToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
