package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/agenterr/agenterr/internal/store"
)

// Aggregate serves the /api/v1/aggregate route.
type Aggregate struct {
	Reader store.Reader
}

type aggregateRowDTO struct {
	Key    string `json:"key"`
	Logs   int64  `json:"logs"`
	Events int64  `json:"events"`
}

func toAggregateDTOs(rows []store.AggregateRow) []aggregateRowDTO {
	dtos := make([]aggregateRowDTO, len(rows))
	for i, r := range rows {
		dtos[i] = aggregateRowDTO{Key: r.Key, Logs: r.Logs, Events: r.Events}
	}
	return dtos
}

// Get handles GET /api/v1/aggregate. Unlike /api/v1/logs and
// /api/v1/stats, an admin key must supply ?project explicitly:
// store.Reader.Aggregate has no "all projects" mode (mirrors the
// noise-report route's ServiceCounts requirement — see
// NoiseRules.Report).
func (a *Aggregate) Get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	groupBy := q.Get("group_by")
	switch groupBy {
	case "service", "severity", "hour", "day":
	default:
		respondErr(w, http.StatusBadRequest, "group_by: must be service, severity, hour, or day")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	var projectID int64
	if isAdmin {
		v := q.Get("project")
		if v == "" {
			respondErr(w, http.StatusBadRequest, "project: required")
			return
		}
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "project: invalid")
			return
		}
		projectID = id
	} else {
		// A project-bound key's own project is authoritative — any
		// client-supplied ?project is ignored rather than trusted.
		projectID = callerProjectID
	}

	f := store.AggregateFilter{ProjectID: projectID, GroupBy: groupBy}
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

	rows, err := a.Reader.Aggregate(r.Context(), f)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toAggregateDTOs(rows))
}
