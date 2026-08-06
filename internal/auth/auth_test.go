package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
	"golang.org/x/crypto/bcrypt"
)

// fakeAdmin is a minimal store.Admin implementation backed by a map, for
// testing key lookup without a real database. Every method besides
// LookupKey is unused by auth and panics if called.
type fakeAdmin struct {
	keys map[string]struct {
		projectID int64
		kind      string
	}
}

func (f *fakeAdmin) CreateProject(ctx context.Context, name string, retentionDays int) (core.Project, error) {
	panic("unused")
}

func (f *fakeAdmin) Projects(ctx context.Context) ([]core.Project, error) {
	panic("unused")
}

func (f *fakeAdmin) SetIssueStatus(ctx context.Context, id int64, s core.IssueStatus) error {
	panic("unused")
}

func (f *fakeAdmin) MintKey(ctx context.Context, projectID int64, kind string) (string, error) {
	panic("unused")
}

func (f *fakeAdmin) LookupKey(ctx context.Context, plaintext string) (int64, string, error) {
	e, ok := f.keys[plaintext]
	if !ok {
		return 0, "", store.ErrNotFound
	}
	return e.projectID, e.kind, nil
}

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{keys: map[string]struct {
		projectID int64
		kind      string
	}{
		"agt_ingest_valid": {projectID: 42, kind: "ingest"},
		"agt_api_valid":    {projectID: 42, kind: "api"},
		"agt_admin_valid":  {projectID: 0, kind: "admin"},
	}}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestRequireKey(t *testing.T) {
	tests := []struct {
		name       string
		authHeader string
		kind       string
		wantStatus int
		wantProj   int64
	}{
		{"missing header", "", "ingest", http.StatusUnauthorized, 0},
		{"malformed basic", "Basic xyz", "ingest", http.StatusUnauthorized, 0},
		{"malformed bearer no token", "Bearer", "ingest", http.StatusUnauthorized, 0},
		{"unknown key", "Bearer agt_ingest_bogus", "ingest", http.StatusUnauthorized, 0},
		{"valid ingest key on ingest route", "Bearer agt_ingest_valid", "ingest", http.StatusOK, 42},
		{"valid api key on ingest route", "Bearer agt_api_valid", "ingest", http.StatusUnauthorized, 0},
		{"valid api key on api route", "Bearer agt_api_valid", "api", http.StatusOK, 42},
		{"admin key on api route", "Bearer agt_admin_valid", "api", http.StatusOK, 0},
		{"admin key on ingest route", "Bearer agt_admin_valid", "ingest", http.StatusOK, 0},
		{"admin key on admin route", "Bearer agt_admin_valid", "admin", http.StatusOK, 0},
		{"api key on admin route", "Bearer agt_api_valid", "admin", http.StatusUnauthorized, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := New(newFakeAdmin(), []byte{})

			var gotProj int64
			var gotOK bool
			handler := a.RequireKey(tt.kind, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotProj, gotOK = ProjectFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rw := httptest.NewRecorder()
			handler.ServeHTTP(rw, req)

			if rw.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rw.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusUnauthorized {
				if got := rw.Body.String(); got != `{"error":"unauthorized"}` {
					t.Errorf("body = %q, want unauthorized JSON", got)
				}
				if gotOK {
					t.Errorf("handler should not have been called on 401")
				}
				return
			}
			if !gotOK {
				t.Fatalf("ProjectFromContext ok = false, want true")
			}
			if gotProj != tt.wantProj {
				t.Errorf("project = %d, want %d", gotProj, tt.wantProj)
			}
		})
	}
}

func TestProjectFromContext_Absent(t *testing.T) {
	_, ok := ProjectFromContext(context.Background())
	if ok {
		t.Fatal("expected ok=false for context with no project")
	}
}

func TestKindFromContext_Absent(t *testing.T) {
	_, ok := KindFromContext(context.Background())
	if ok {
		t.Fatal("expected ok=false for context with no kind")
	}
}

func TestKindFromContext_SetByRequireKey(t *testing.T) {
	a := New(newFakeAdmin(), []byte{})

	var gotKind string
	var gotOK bool
	handler := a.RequireKey("api", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKind, gotOK = KindFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer agt_admin_valid")
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)

	if !gotOK {
		t.Fatal("KindFromContext ok = false, want true")
	}
	if gotKind != "admin" {
		t.Errorf("kind = %q, want admin", gotKind)
	}
}

const testPassword = "correct horse battery staple"

func newTestAuth(t *testing.T) *Auth {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt.GenerateFromPassword: %v", err)
	}
	return New(newFakeAdmin(), hash)
}

func TestLogin_WrongPassword(t *testing.T) {
	a := newTestAuth(t)
	rw := httptest.NewRecorder()
	err := a.Login(rw, "not the password")
	if err == nil {
		t.Fatal("expected error for wrong password")
	}
	if len(rw.Result().Cookies()) != 0 {
		t.Errorf("expected no cookie set on failed login")
	}
}

func TestLogin_RightPassword(t *testing.T) {
	a := newTestAuth(t)
	rw := httptest.NewRecorder()
	if err := a.Login(rw, testPassword); err != nil {
		t.Fatalf("Login: %v", err)
	}
	cookies := rw.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected 1 cookie, got %d", len(cookies))
	}
	c := cookies[0]
	if c.Name != "agenterr_session" {
		t.Errorf("cookie name = %q, want agenterr_session", c.Name)
	}
	if c.Value == "" {
		t.Error("cookie value empty")
	}
	if !c.HttpOnly {
		t.Error("cookie should be HttpOnly")
	}
	if c.SameSite != http.SameSiteLaxMode {
		t.Errorf("SameSite = %v, want Lax", c.SameSite)
	}
	if c.Path != "/" {
		t.Errorf("Path = %q, want /", c.Path)
	}
}

func TestRequireSession(t *testing.T) {
	a := newTestAuth(t)
	loginRW := httptest.NewRecorder()
	if err := a.Login(loginRW, testPassword); err != nil {
		t.Fatalf("Login: %v", err)
	}
	sessionCookie := loginRW.Result().Cookies()[0]

	handler := a.RequireSession(okHandler())

	t.Run("with valid cookie", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(sessionCookie)
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req)
		if rw.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rw.Code)
		}
	})

	t.Run("without cookie", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req)
		if rw.Code != http.StatusSeeOther {
			t.Errorf("status = %d, want 303", rw.Code)
		}
		if loc := rw.Header().Get("Location"); loc != "/login" {
			t.Errorf("Location = %q, want /login", loc)
		}
	})

	t.Run("unknown token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(&http.Cookie{Name: "agenterr_session", Value: "bogus-token"})
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req)
		if rw.Code != http.StatusSeeOther {
			t.Errorf("status = %d, want 303", rw.Code)
		}
	})

	t.Run("after logout", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(sessionCookie)
		logoutRW := httptest.NewRecorder()
		a.Logout(logoutRW, req)

		req2 := httptest.NewRequest(http.MethodGet, "/", nil)
		req2.AddCookie(sessionCookie)
		rw := httptest.NewRecorder()
		handler.ServeHTTP(rw, req2)
		if rw.Code != http.StatusSeeOther {
			t.Errorf("status = %d, want 303 after logout", rw.Code)
		}
	})

	t.Run("expired session", func(t *testing.T) {
		a2 := newTestAuth(t)
		expiredRW := httptest.NewRecorder()
		if err := a2.Login(expiredRW, testPassword); err != nil {
			t.Fatalf("Login: %v", err)
		}
		expiredCookie := expiredRW.Result().Cookies()[0]
		a2.setSessionExpiry(expiredCookie.Value, time.Now().Add(-time.Minute))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(expiredCookie)
		rw := httptest.NewRecorder()
		a2.RequireSession(okHandler()).ServeHTTP(rw, req)
		if rw.Code != http.StatusSeeOther {
			t.Errorf("status = %d, want 303 for expired session", rw.Code)
		}
	})

	t.Run("expired session is purged from the map on read", func(t *testing.T) {
		a3 := newTestAuth(t)
		loginRW := httptest.NewRecorder()
		if err := a3.Login(loginRW, testPassword); err != nil {
			t.Fatalf("Login: %v", err)
		}
		expiredCookie := loginRW.Result().Cookies()[0]
		a3.setSessionExpiry(expiredCookie.Value, time.Now().Add(-time.Minute))

		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.AddCookie(expiredCookie)
		rw := httptest.NewRecorder()
		a3.RequireSession(okHandler()).ServeHTTP(rw, req)
		if rw.Code != http.StatusSeeOther {
			t.Errorf("status = %d, want 303 for expired session", rw.Code)
		}

		if a3.hasSession(expiredCookie.Value) {
			t.Errorf("expected expired session to be purged from the map, but it is still present")
		}
	})
}

// setSessionExpiry is a test-only helper that reaches into Auth's
// internal session map to force a token's expiry, so expiry behavior can
// be tested without waiting 7 days.
func (a *Auth) setSessionExpiry(token string, expiry time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sessions[token] = expiry
}

// hasSession is a test-only helper that reports whether token is still
// present in Auth's internal session map, used to verify expired entries
// get purged rather than lingering forever.
func (a *Auth) hasSession(token string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.sessions[token]
	return ok
}

func TestLogout_ExpiresCookie(t *testing.T) {
	a := newTestAuth(t)
	loginRW := httptest.NewRecorder()
	if err := a.Login(loginRW, testPassword); err != nil {
		t.Fatalf("Login: %v", err)
	}
	sessionCookie := loginRW.Result().Cookies()[0]

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(sessionCookie)
	rw := httptest.NewRecorder()
	a.Logout(rw, req)

	cookies := rw.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("expected 1 cookie set on logout, got %d", len(cookies))
	}
	if !cookies[0].Expires.Before(time.Now()) {
		t.Errorf("expected cookie to be expired")
	}
}
