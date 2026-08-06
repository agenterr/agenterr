package handlers

import (
	"html/template"
	"net/http"

	"github.com/agenterr/agenterr/internal/auth"
)

// Login serves GET/POST /login and POST /logout.
type Login struct {
	Auth auth.SessionAuth
	Tpl  *template.Template // templates/login.html (its own standalone layout)
}

// NewLogin constructs a Login handler.
func NewLogin(a auth.SessionAuth, tpl *template.Template) *Login {
	return &Login{Auth: a, Tpl: tpl}
}

// Page handles GET /login.
func (h *Login) Page(w http.ResponseWriter, r *http.Request) {
	renderFull(w, h.Tpl, map[string]any{})
}

// Submit handles POST /login. On success it redirects to /; on failure it
// re-renders the login form with an error, 401.
func (h *Login) Submit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	password := r.FormValue("password")
	if err := h.Auth.Login(w, password); err != nil {
		renderFullStatus(w, h.Tpl, http.StatusUnauthorized, map[string]any{"Error": "Wrong password."})
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// Logout handles POST /logout.
func (h *Login) Logout(w http.ResponseWriter, r *http.Request) {
	h.Auth.Logout(w, r)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
