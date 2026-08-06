package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// Issues serves the /api/v1/issues routes.
type Issues struct {
	Reader store.Reader
	Admin  store.Admin
}

type issueDTO struct {
	ID          int64  `json:"id"`
	ProjectID   int64  `json:"project_id"`
	Fingerprint string `json:"fingerprint"`
	Title       string `json:"title"`
	Severity    string `json:"severity"`
	Status      string `json:"status"`
	FirstSeen   string `json:"first_seen"`
	LastSeen    string `json:"last_seen"`
	Count       int64  `json:"count"`
}

func toIssueDTO(i core.Issue) issueDTO {
	return issueDTO{
		ID:          i.ID,
		ProjectID:   i.ProjectID,
		Fingerprint: i.Fingerprint,
		Title:       i.Title,
		Severity:    i.Severity.String(),
		Status:      string(i.Status),
		FirstSeen:   formatTime(i.FirstSeen),
		LastSeen:    formatTime(i.LastSeen),
		Count:       i.Count,
	}
}

func toIssueDTOs(issues []core.Issue) []issueDTO {
	dtos := make([]issueDTO, len(issues))
	for i, iss := range issues {
		dtos[i] = toIssueDTO(iss)
	}
	return dtos
}

type eventDTO struct {
	LogID   int64  `json:"log_id"`
	IssueID int64  `json:"issue_id"`
	Time    string `json:"time"`
	Log     logDTO `json:"log"`
}

func toEventDTO(e core.Event) eventDTO {
	return eventDTO{
		LogID:   e.LogID,
		IssueID: e.IssueID,
		Time:    formatTime(e.Time),
		Log:     toLogDTO(e.Log),
	}
}

func toEventDTOs(events []core.Event) []eventDTO {
	dtos := make([]eventDTO, len(events))
	for i, e := range events {
		dtos[i] = toEventDTO(e)
	}
	return dtos
}

// isValidStatus reports whether s is one of the known core.IssueStatus
// values.
func isValidStatus(s core.IssueStatus) bool {
	switch s {
	case core.StatusOpen, core.StatusResolved, core.StatusIgnored:
		return true
	}
	return false
}

// List handles GET /api/v1/issues.
func (i *Issues) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f store.IssueFilter

	if v := q.Get("project"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "project: invalid")
			return
		}
		f.ProjectID = id
	}
	f.Environment = q.Get("environment")
	if v := q.Get("status"); v != "" {
		f.Status = core.IssueStatus(v)
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "since: invalid")
			return
		}
		f.Since = t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "limit: invalid")
			return
		}
		f.Limit = n
	}

	issues, err := i.Reader.Issues(r.Context(), f)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toIssueDTOs(issues))
}

// Get handles GET /api/v1/issues/{id}.
func (i *Issues) Get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	issue, events, err := i.Reader.Issue(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}

	respond(w, http.StatusOK, map[string]any{
		"issue":  toIssueDTO(issue),
		"events": toEventDTOs(events),
	})
}

// UpdateStatus handles PATCH /api/v1/issues/{id}.
func (i *Issues) UpdateStatus(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}
	status := core.IssueStatus(body.Status)
	if !isValidStatus(status) {
		respondErr(w, http.StatusBadRequest, "status: must be open, resolved, or ignored")
		return
	}

	if err := i.Admin.SetIssueStatus(r.Context(), id, status); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
