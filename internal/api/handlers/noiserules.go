package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// defaultReportHours is the noise report's lookback window when the
// caller does not supply ?hours.
const defaultReportHours = 24

// validNoiseKinds mirrors core.NoiseRuleKind's three known values.
var validNoiseKinds = map[core.NoiseRuleKind]bool{
	core.NoiseSeverityFloor: true,
	core.NoiseDropMatch:     true,
	core.NoiseSample:        true,
}

// NoiseRules serves the noise-rule management routes and the noise
// report. Mutations go through Engine (never NR directly) so the
// pipeline's cached view stays fresh; NR and Reader back plain reads.
type NoiseRules struct {
	NR     store.NoiseRules
	Reader store.Reader
	Engine *rules.Engine
}

type noiseRuleDTO struct {
	ID           int64  `json:"id"`
	Kind         string `json:"kind"`
	Service      string `json:"service"`
	Severity     string `json:"severity"`
	Pattern      string `json:"pattern"`
	N            int    `json:"n"`
	Enabled      bool   `json:"enabled"`
	DroppedCount int64  `json:"dropped_count"`
}

func toNoiseRuleDTO(row store.NoiseRuleRow) noiseRuleDTO {
	return noiseRuleDTO{
		ID:           row.ID,
		Kind:         string(row.Kind),
		Service:      row.Service,
		Severity:     strings.ToLower(row.Severity.String()),
		Pattern:      row.Pattern,
		N:            row.N,
		Enabled:      row.Enabled,
		DroppedCount: row.DroppedCount,
	}
}

func toNoiseRuleDTOs(rows []store.NoiseRuleRow) []noiseRuleDTO {
	dtos := make([]noiseRuleDTO, len(rows))
	for i, r := range rows {
		dtos[i] = toNoiseRuleDTO(r)
	}
	return dtos
}

// List handles GET /api/v1/projects/{id}/noise-rules.
func (n *NoiseRules) List(w http.ResponseWriter, r *http.Request) {
	pathID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	projectID := pathID
	if !isAdmin {
		// A project-bound key's own project is authoritative — the path
		// id is ignored rather than trusted, mirroring how issues/logs
		// handlers treat a client-supplied ?project.
		projectID = callerProjectID
	}

	rowsList, err := n.NR.NoiseRules(r.Context(), projectID)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toNoiseRuleDTOs(rowsList))
}

type noiseRuleBody struct {
	ID       int64  `json:"id"`
	Kind     string `json:"kind"`
	Service  string `json:"service"`
	Severity string `json:"severity"`
	Pattern  string `json:"pattern"`
	N        int    `json:"n"`
	// Enabled is a pointer so omitting it is distinguishable from an
	// explicit false: on create, nil defaults to true (an agent that
	// forgets the field still gets a live rule, not one that silently
	// never fires); on update, nil preserves the rule's current enabled
	// state instead of clobbering it back to disabled.
	Enabled *bool `json:"enabled"`
}

// validateNoiseParams rejects create-time params that can never match
// anything for the given kind. A rule that's accepted and stored but never
// fires looks like success, silently breaking the
// see-noise -> add-rule -> verify loop.
func validateNoiseParams(kind core.NoiseRuleKind, severity, pattern string, n int) error {
	switch kind {
	case core.NoiseSeverityFloor:
		if severity == "" {
			return errors.New("severity: required for severity_floor")
		}
	case core.NoiseDropMatch:
		if pattern == "" {
			return errors.New("pattern: required for drop_match")
		}
	case core.NoiseSample:
		if severity == "" {
			return errors.New("severity: required for sample")
		}
		if n <= 1 {
			return errors.New("n: must be >= 2 for sample")
		}
	}
	return nil
}

// Create handles POST /api/v1/projects/{id}/noise-rules (upsert: body.ID
// unset inserts, set updates).
func (n *NoiseRules) Create(w http.ResponseWriter, r *http.Request) {
	pathID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	var body noiseRuleBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}

	kind := core.NoiseRuleKind(body.Kind)
	if !validNoiseKinds[kind] {
		respondErr(w, http.StatusBadRequest, "kind: must be severity_floor, drop_match, or sample")
		return
	}
	severity, ok := core.ParseSeverityStrict(body.Severity)
	if !ok {
		respondErr(w, http.StatusBadRequest, "severity: unknown value")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	projectID := pathID
	if !isAdmin {
		projectID = callerProjectID
	}

	// enabled starts at the create default (true) or, for an update, the
	// rule's current state — either is overridden below if the caller
	// supplied an explicit value.
	enabled := true
	if body.ID != 0 {
		// Updating an existing rule: fetch it both to verify ownership
		// (non-admin — otherwise a project-bound key could hijack another
		// project's rule by ID) and to seed enabled's preserve-on-omit
		// default. Admins aren't scoped to one project, so their lookup
		// spans all projects (projectID 0); a miss here still falls
		// through to Upsert's own not-found handling.
		lookupProjectID := callerProjectID
		if isAdmin {
			lookupProjectID = 0
		}
		existing, ok := findNoiseRule(r, n.NR, body.ID, lookupProjectID)
		if !isAdmin && !ok {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		if ok {
			enabled = existing.Enabled
		}
	} else if err := validateNoiseParams(kind, body.Severity, body.Pattern, body.N); err != nil {
		respondErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.Enabled != nil {
		enabled = *body.Enabled
	}

	row, err := n.Engine.Upsert(r.Context(), core.NoiseRule{
		ID:        body.ID,
		ProjectID: projectID,
		Kind:      kind,
		Service:   body.Service,
		Severity:  severity,
		Pattern:   body.Pattern,
		N:         body.N,
		Enabled:   enabled,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toNoiseRuleDTO(row))
}

// findNoiseRule looks up ruleID among projectID's rules (projectID 0 = all
// projects — used for admin lookups that aren't scoped to one project).
func findNoiseRule(r *http.Request, nr store.NoiseRules, ruleID, projectID int64) (store.NoiseRuleRow, bool) {
	rows, err := nr.NoiseRules(r.Context(), projectID)
	if err != nil {
		return store.NoiseRuleRow{}, false
	}
	for _, row := range rows {
		if row.ID == ruleID {
			return row, true
		}
	}
	return store.NoiseRuleRow{}, false
}

// ruleBelongsToProject reports whether ruleID is among projectID's rules.
func ruleBelongsToProject(r *http.Request, nr store.NoiseRules, ruleID, projectID int64) bool {
	_, ok := findNoiseRule(r, nr, ruleID, projectID)
	return ok
}

// Delete handles DELETE /api/v1/noise-rules/{id}.
func (n *NoiseRules) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	if !isAdmin && !ruleBelongsToProject(r, n.NR, id, callerProjectID) {
		// Row exists but belongs to another project (or doesn't exist at
		// all): 404 either way, not 403 — a project-bound key must not
		// learn that another project's rule ID exists.
		respondErr(w, http.StatusNotFound, "not found")
		return
	}

	if err := n.Engine.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type noiseReportDTO struct {
	TopServices  []serviceCountDTO `json:"top_services"`
	Rules        []noiseRuleDTO    `json:"rules"`
	TotalDropped int64             `json:"total_dropped"`
}

type serviceCountDTO struct {
	Service string `json:"service"`
	Logs    int64  `json:"logs"`
}

// Report handles GET /api/v1/noise-report.
func (n *NoiseRules) Report(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

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

	hours := defaultReportHours
	if v := q.Get("hours"); v != "" {
		h, err := strconv.Atoi(v)
		if err != nil || h < 0 {
			respondErr(w, http.StatusBadRequest, "hours: invalid")
			return
		}
		hours = h
	}
	since := time.Now().UTC().Add(-time.Duration(hours) * time.Hour)

	topServices, err := n.Reader.ServiceCounts(r.Context(), projectID, since)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	ruleRows, err := n.NR.NoiseRules(r.Context(), projectID)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}

	svcDTOs := make([]serviceCountDTO, len(topServices))
	var totalDropped int64
	for i, s := range topServices {
		svcDTOs[i] = serviceCountDTO{Service: s.Service, Logs: s.Logs}
	}
	for _, rr := range ruleRows {
		totalDropped += rr.DroppedCount
	}

	respond(w, http.StatusOK, noiseReportDTO{
		TopServices:  svcDTOs,
		Rules:        toNoiseRuleDTOs(ruleRows),
		TotalDropped: totalDropped,
	})
}
