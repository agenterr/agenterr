package handlers

import (
	"errors"
	"html/template"
	"net/http"
	"strconv"

	"github.com/agenterr/agenterr/internal/store"
)

// Settings serves GET/POST /settings: the project list, project creation,
// and key minting.
type Settings struct {
	Admin store.Admin
	Tpl   *template.Template // templates/settings.html + layout
}

// NewSettings constructs a Settings handler.
func NewSettings(a store.Admin, tpl *template.Template) *Settings {
	return &Settings{Admin: a, Tpl: tpl}
}

// Page handles GET /settings.
func (h *Settings) Page(w http.ResponseWriter, r *http.Request) {
	projects, err := h.Admin.Projects(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	renderFull(w, h.Tpl, map[string]any{"Projects": projects, "BaseURL": baseURL(r)})
}

// CreateProject handles POST /settings/projects. It always renders back
// the "content" fragment — the whole settings-content div, which the
// create form's hx-target swaps in place — since the response needs to
// show both the updated project list and (unlike mint-key) never a
// plaintext key.
func (h *Settings) CreateProject(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	name := r.FormValue("name")
	days, convErr := strconv.Atoi(r.FormValue("retention_days"))
	if name == "" || convErr != nil || days < 1 {
		http.Error(w, "name and a positive retention_days are required", http.StatusBadRequest)
		return
	}

	if _, err := h.Admin.CreateProject(r.Context(), name, days); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	projects, err := h.Admin.Projects(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	renderFragment(w, h.Tpl, "content", map[string]any{"Projects": projects, "BaseURL": baseURL(r)})
}

// MintKey handles POST /settings/projects/{id}/keys. The plaintext key is
// returned exactly once, in the same fragment response — the store never
// discloses it again after this.
func (h *Settings) MintKey(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	kind := r.FormValue("kind")
	if kind != "ingest" && kind != "api" {
		http.Error(w, "kind must be ingest or api", http.StatusBadRequest)
		return
	}

	key, err := h.Admin.MintKey(r.Context(), id, kind)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	projects, err := h.Admin.Projects(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	renderFragment(w, h.Tpl, "content", map[string]any{
		"Projects":   projects,
		"MintedKey":  key,
		"MintedKind": kind,
		"BaseURL":    baseURL(r),
	})
}
