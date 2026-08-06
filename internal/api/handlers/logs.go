package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// Logs serves the /api/v1/logs routes.
type Logs struct {
	Reader store.Reader
}

type logDTO struct {
	ID          int64             `json:"id"`
	ProjectID   int64             `json:"project_id"`
	Time        string            `json:"time"`
	Severity    string            `json:"severity"`
	Body        string            `json:"body"`
	Service     string            `json:"service"`
	Environment string            `json:"environment"`
	Release     string            `json:"release"`
	TraceID     string            `json:"trace_id"`
	Attrs       map[string]string `json:"attrs,omitempty"`
}

func toLogDTO(l core.Log) logDTO {
	return logDTO{
		ID:          l.ID,
		ProjectID:   l.ProjectID,
		Time:        formatTime(l.Time),
		Severity:    l.Severity.String(),
		Body:        l.Body,
		Service:     l.Service,
		Environment: l.Environment,
		Release:     l.Release,
		TraceID:     l.TraceID,
		Attrs:       l.Attrs,
	}
}

func toLogDTOs(logs []core.Log) []logDTO {
	dtos := make([]logDTO, len(logs))
	for i, l := range logs {
		dtos[i] = toLogDTO(l)
	}
	return dtos
}

// formatTime renders t in RFC3339Nano, UTC — the wire timestamp format used
// across every /api/v1 response.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// Search handles GET /api/v1/logs.
func (l *Logs) Search(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f store.LogFilter

	callerProjectID, isAdmin := callerScope(r)
	if isAdmin {
		if v := q.Get("project"); v != "" {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				respondErr(w, http.StatusBadRequest, "project: invalid")
				return
			}
			f.ProjectID = id
		}
	} else {
		// A project-bound key's own project is authoritative — any
		// client-supplied ?project is ignored rather than trusted.
		f.ProjectID = callerProjectID
	}

	f.Query = q.Get("q")
	if v := q.Get("min_severity"); v != "" {
		f.MinSeverity = core.ParseSeverity(v)
	}
	f.Service = q.Get("service")
	f.Environment = q.Get("env")
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "since: invalid")
			return
		}
		f.Since = t
	}
	if v := q.Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "until: invalid")
			return
		}
		f.Until = t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "limit: invalid")
			return
		}
		if n < 0 {
			respondErr(w, http.StatusBadRequest, "limit: must be >= 0")
			return
		}
		f.Limit = n
	}

	logs, err := l.Reader.SearchLogs(r.Context(), f)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toLogDTOs(logs))
}

// Context handles GET /api/v1/logs/{id}/context.
func (l *Logs) Context(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	n := 20
	if v := r.URL.Query().Get("n"); v != "" {
		nn, err := strconv.Atoi(v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "n: invalid")
			return
		}
		if nn < 0 {
			respondErr(w, http.StatusBadRequest, "n: must be >= 0")
			return
		}
		n = nn
	}

	logs, err := l.Reader.LogContext(r.Context(), id, n)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	if !isAdmin && len(logs) > 0 && logs[0].ProjectID != callerProjectID {
		// The context window is always scoped to a single project (all
		// its rows come from the same project as the target log), so
		// checking the first entry is sufficient. 404, not 403 — don't
		// leak that the ID exists in another project.
		respondErr(w, http.StatusNotFound, "not found")
		return
	}

	respond(w, http.StatusOK, toLogDTOs(logs))
}
