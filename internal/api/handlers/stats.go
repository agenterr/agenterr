package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/agenterr/agenterr/internal/store"
)

// Stats serves the /api/v1/stats route.
type Stats struct {
	Reader store.Reader
	NR     store.NoiseRules
}

type dayCountDTO struct {
	Day    string `json:"day"`
	Logs   int64  `json:"logs"`
	Events int64  `json:"events"`
}

type statsDTO struct {
	Logs       int64         `json:"logs"`
	Events     int64         `json:"events"`
	OpenIssues int64         `json:"open_issues"`
	Dropped    int64         `json:"dropped"`
	PerDay     []dayCountDTO `json:"per_day"`
}

func toStatsDTO(s store.Stats, dropped int64) statsDTO {
	perDay := make([]dayCountDTO, len(s.PerDay))
	for i, d := range s.PerDay {
		perDay[i] = dayCountDTO{Day: d.Day, Logs: d.Logs, Events: d.Events}
	}
	return statsDTO{Logs: s.Logs, Events: s.Events, OpenIssues: s.OpenIssues, Dropped: dropped, PerDay: perDay}
}

// Get handles GET /api/v1/stats.
func (s *Stats) Get(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f store.StatsFilter

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
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondErr(w, http.StatusBadRequest, "since: invalid")
			return
		}
		f.Since = t
	}

	stats, err := s.Reader.Stats(r.Context(), f)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}

	// f.ProjectID conveniently doubles as the NoiseRules scope: 0 means
	// "no ?project given by an admin key", which store.NoiseRules also
	// treats as "all projects" — the same fan-out this field needs.
	ruleRows, err := s.NR.NoiseRules(r.Context(), f.ProjectID)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	var dropped int64
	for _, rr := range ruleRows {
		dropped += rr.DroppedCount
	}

	respond(w, http.StatusOK, toStatsDTO(stats, dropped))
}
