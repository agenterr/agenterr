package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// Projects serves the /api/v1/projects routes.
type Projects struct {
	Admin  store.Admin
	Engine *rules.Engine
}

type projectDTO struct {
	ID            int64  `json:"id"`
	Name          string `json:"name"`
	Slug          string `json:"slug"`
	RetentionDays int    `json:"retention_days"`
	ParseBodies   bool   `json:"parse_bodies"`
}

func toProjectDTO(p core.Project) projectDTO {
	return projectDTO{ID: p.ID, Name: p.Name, Slug: p.Slug, RetentionDays: p.RetentionDays, ParseBodies: p.ParseBodies}
}

// Create handles POST /api/v1/projects.
func (p *Projects) Create(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name          string `json:"name"`
		RetentionDays int    `json:"retention_days"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}
	if body.Name == "" {
		respondErr(w, http.StatusBadRequest, "name: required")
		return
	}
	if body.RetentionDays < 1 {
		respondErr(w, http.StatusBadRequest, "retention_days: must be positive")
		return
	}

	proj, err := p.Admin.CreateProject(r.Context(), body.Name, body.RetentionDays)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusCreated, toProjectDTO(proj))
}

// List handles GET /api/v1/projects.
func (p *Projects) List(w http.ResponseWriter, r *http.Request) {
	projects, err := p.Admin.Projects(r.Context())
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	dtos := make([]projectDTO, len(projects))
	for i, proj := range projects {
		dtos[i] = toProjectDTO(proj)
	}
	respond(w, http.StatusOK, dtos)
}

// MintKey handles POST /api/v1/projects/{id}/keys.
func (p *Projects) MintKey(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	var body struct {
		Kind string `json:"kind"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}
	if body.Kind != "ingest" && body.Kind != "api" {
		respondErr(w, http.StatusBadRequest, "kind: must be ingest or api")
		return
	}

	key, err := p.Admin.MintKey(r.Context(), id, body.Kind)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusCreated, map[string]string{"key": key})
}

// Update handles PATCH /api/v1/projects/{id}. parse_bodies is the only
// mutable field for now; unknown fields are rejected rather than
// silently ignored.
func (p *Projects) Update(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	var body struct {
		ParseBodies *bool `json:"parse_bodies"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}
	if body.ParseBodies == nil {
		respondErr(w, http.StatusBadRequest, "parse_bodies: required")
		return
	}

	projects, err := p.Admin.Projects(r.Context())
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	found := false
	for _, proj := range projects {
		if proj.ID == id {
			found = true
			break
		}
	}
	if !found {
		respondErr(w, http.StatusNotFound, "not found")
		return
	}

	if err := p.Engine.SetParseBodies(r.Context(), id, *body.ParseBodies); err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
