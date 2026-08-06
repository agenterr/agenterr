package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

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
func (a *Auth) Login(w http.ResponseWriter, password string) error {
	if err := bcrypt.CompareHashAndPassword(a.adminPasswordHash, []byte(password)); err != nil {
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
	a.mu.Unlock()

	// Not marked Secure: the MVP self-host quickstart runs plain HTTP on
	// localhost or a VPS-internal address, and a Secure cookie would
	// silently break login there. Deployments that terminate TLS in
	// front of the UI should keep it behind that TLS boundary regardless
	// (reverse proxy, VPN, etc). Tracked follow-up: revisit once hosted
	// deployments (which do terminate TLS) exist, and make Secure
	// conditional on that.
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expiry,
		HttpOnly: true,
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
