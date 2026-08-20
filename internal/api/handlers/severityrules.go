package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// SeverityRules serves the severity-lift rule management routes.
// Mutations go through Engine (never SR directly) so the pipeline's
// cached view stays fresh; SR backs plain reads.
type SeverityRules struct {
	SR     store.SeverityRules
	Engine *rules.Engine
}

type severityRuleDTO struct {
	ID          int64  `json:"id"`
	Service     string `json:"service"`
	Pattern     string `json:"pattern"`
	Severity    string `json:"severity"`
	Enabled     bool   `json:"enabled"`
	LiftedCount int64  `json:"lifted_count"`
}

func toSeverityRuleDTO(row store.SeverityRuleRow) severityRuleDTO {
	return severityRuleDTO{
		ID:          row.ID,
		Service:     row.Service,
		Pattern:     row.Pattern,
		Severity:    strings.ToLower(row.Severity.String()),
		Enabled:     row.Enabled,
		LiftedCount: row.LiftedCount,
	}
}

func toSeverityRuleDTOs(rows []store.SeverityRuleRow) []severityRuleDTO {
	dtos := make([]severityRuleDTO, len(rows))
	for i, r := range rows {
		dtos[i] = toSeverityRuleDTO(r)
	}
	return dtos
}

// List handles GET /api/v1/projects/{id}/severity-rules.
func (sv *SeverityRules) List(w http.ResponseWriter, r *http.Request) {
	pathID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	projectID := pathID
	if !isAdmin {
		// A project-bound key's own project is authoritative — the path
		// id is ignored rather than trusted, mirroring the noise-rules
		// handler.
		projectID = callerProjectID
	}

	rowsList, err := sv.SR.SeverityRules(r.Context(), projectID)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toSeverityRuleDTOs(rowsList))
}

type severityRuleBody struct {
	ID       int64  `json:"id"`
	Service  string `json:"service"`
	Pattern  string `json:"pattern"`
	Severity string `json:"severity"`
	// Enabled is a pointer so omitting it is distinguishable from an
	// explicit false: on create, nil defaults to true; on update, nil
	// preserves the rule's current enabled state instead of clobbering
	// it back to disabled.
	Enabled *bool `json:"enabled"`
}

// validateSeverityRuleBody validates the two fields every severity rule
// requires regardless of create vs. update: a non-empty, compilable
// pattern and a severity strictly above info (rules only lift — see
// core.SeverityRule's doc comment). Returns the parsed severity.
func validateSeverityRuleBody(pattern, severity string) (core.Severity, error) {
	if pattern == "" {
		return 0, errors.New("pattern: required")
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return 0, fmt.Errorf("pattern: invalid regexp: %w", err)
	}
	sev, ok := core.ParseSeverityStrict(severity)
	if !ok {
		return 0, errors.New("severity: unknown value")
	}
	if sev <= core.SeverityInfo {
		return 0, errors.New("severity: must be above info — severity rules only lift")
	}
	return sev, nil
}

// Create handles POST /api/v1/projects/{id}/severity-rules (upsert:
// body.ID unset inserts, set updates).
func (sv *SeverityRules) Create(w http.ResponseWriter, r *http.Request) {
	pathID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	var body severityRuleBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}

	severity, err := validateSeverityRuleBody(body.Pattern, body.Severity)
	if err != nil {
		respondErr(w, http.StatusBadRequest, err.Error())
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
		// through to UpsertSeverity's own not-found handling.
		lookupProjectID := callerProjectID
		if isAdmin {
			lookupProjectID = 0
		}
		existing, ok := findSeverityRule(r, sv.SR, body.ID, lookupProjectID)
		if !isAdmin && !ok {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		if ok {
			enabled = existing.Enabled
		}
	}
	if body.Enabled != nil {
		enabled = *body.Enabled
	}

	row, err := sv.Engine.UpsertSeverity(r.Context(), core.SeverityRule{
		ID:        body.ID,
		ProjectID: projectID,
		Service:   body.Service,
		Pattern:   body.Pattern,
		Severity:  severity,
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
	respond(w, http.StatusOK, toSeverityRuleDTO(row))
}

// findSeverityRule looks up ruleID among projectID's rules (projectID 0 =
// all projects — used for admin lookups that aren't scoped to one
// project).
func findSeverityRule(r *http.Request, sr store.SeverityRules, ruleID, projectID int64) (store.SeverityRuleRow, bool) {
	rows, err := sr.SeverityRules(r.Context(), projectID)
	if err != nil {
		return store.SeverityRuleRow{}, false
	}
	for _, row := range rows {
		if row.ID == ruleID {
			return row, true
		}
	}
	return store.SeverityRuleRow{}, false
}

// severityRuleBelongsToProject reports whether ruleID is among projectID's
// rules.
func severityRuleBelongsToProject(r *http.Request, sr store.SeverityRules, ruleID, projectID int64) bool {
	_, ok := findSeverityRule(r, sr, ruleID, projectID)
	return ok
}

// Delete handles DELETE /api/v1/severity-rules/{id}.
func (sv *SeverityRules) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	if !isAdmin && !severityRuleBelongsToProject(r, sv.SR, id, callerProjectID) {
		// Row exists but belongs to another project (or doesn't exist at
		// all): 404 either way, not 403 — a project-bound key must not
		// learn that another project's rule ID exists.
		respondErr(w, http.StatusNotFound, "not found")
		return
	}

	if err := sv.Engine.DeleteSeverity(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
