package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// maxAlertHeaders and maxAlertHeadersBytes cap a rule's webhook headers so
// an agent (or a malformed client) can't wedge an unbounded map into
// storage or the outbound POST. maxAlertHeadersBytes sums every key+value
// byte length across the map.
const (
	maxAlertHeaders      = 32
	maxAlertHeadersBytes = 4096
)

// validAlertKinds mirrors core.AlertRuleKind's three known values.
var validAlertKinds = map[core.AlertRuleKind]bool{
	core.AlertNewIssue:   true,
	core.AlertRegression: true,
	core.AlertThreshold:  true,
}

// AlertRules serves the alert-rule management routes. Mutations go through
// Engine (never AR directly) so the in-memory cache IssueEvent/TestFire
// read stays fresh — mirrors handlers.NoiseRules.
type AlertRules struct {
	AR     store.AlertRules
	Engine *alerts.Engine
}

type alertRuleDTO struct {
	ID              int64             `json:"id"`
	ProjectID       int64             `json:"project_id"`
	Name            string            `json:"name"`
	Kind            string            `json:"kind"`
	Service         string            `json:"service"`
	Environment     string            `json:"environment"`
	MinSeverity     string            `json:"min_severity"`
	N               int               `json:"n"`
	WindowMinutes   int               `json:"window_minutes"`
	CooldownSeconds int               `json:"cooldown_seconds"`
	URL             string            `json:"url"`
	Headers         map[string]string `json:"headers,omitempty"`
	Enabled         bool              `json:"enabled"`
	LastFired       *string           `json:"last_fired"`
	LastError       string            `json:"last_error"`
}

func toAlertRuleDTO(row store.AlertRuleRow) alertRuleDTO {
	dto := alertRuleDTO{
		ID:              row.ID,
		ProjectID:       row.ProjectID,
		Name:            row.Name,
		Kind:            string(row.Kind),
		Service:         row.Service,
		Environment:     row.Environment,
		N:               row.N,
		WindowMinutes:   row.WindowMinutes,
		CooldownSeconds: row.CooldownSeconds,
		URL:             row.URL,
		Headers:         row.Headers,
		Enabled:         row.Enabled,
		LastError:       row.LastError,
	}
	// MinSeverity's zero value (SeverityTrace) means "any" per the alert
	// semantics doc; the DTO renders its name either way — "never set" vs
	// "explicitly trace" isn't tracked separately, matching
	// core.AlertRule's own representation.
	dto.MinSeverity = strings.ToLower(row.MinSeverity.String())
	if !row.LastFired.IsZero() {
		s := row.LastFired.UTC().Format(rfc3339)
		dto.LastFired = &s
	}
	return dto
}

const rfc3339 = "2006-01-02T15:04:05Z07:00"

func toAlertRuleDTOs(rows []store.AlertRuleRow) []alertRuleDTO {
	dtos := make([]alertRuleDTO, len(rows))
	for i, r := range rows {
		dtos[i] = toAlertRuleDTO(r)
	}
	return dtos
}

// List handles GET /api/v1/projects/{id}/alert-rules.
func (a *AlertRules) List(w http.ResponseWriter, r *http.Request) {
	pathID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	projectID := pathID
	if !isAdmin {
		// A project-bound key's own project is authoritative — the path id
		// is ignored rather than trusted, mirroring noise-rules List.
		projectID = callerProjectID
	}

	rows, err := a.AR.AlertRules(r.Context(), projectID)
	if err != nil {
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toAlertRuleDTOs(rows))
}

type alertRuleBody struct {
	ID              int64             `json:"id"`
	Name            string            `json:"name"`
	Kind            string            `json:"kind"`
	Service         string            `json:"service"`
	Environment     string            `json:"environment"`
	MinSeverity     string            `json:"min_severity"`
	N               int               `json:"n"`
	WindowMinutes   int               `json:"window_minutes"`
	CooldownSeconds int               `json:"cooldown_seconds"`
	URL             string            `json:"url"`
	Headers         map[string]string `json:"headers"`
	// Enabled is a pointer so omitting it is distinguishable from an
	// explicit false: on create, nil defaults to true; on update, nil
	// preserves the rule's current enabled state instead of clobbering it
	// back to disabled — same contract as noise rules.
	Enabled *bool `json:"enabled"`
}

// validateAlertRuleBody applies the alert semantics doc's validation,
// which runs on both create and update (unlike noise rules' create-only
// params check): kind known; url parses as http/https; threshold requires
// n>=1 and window_minutes>=1; cooldown_seconds>=0; min_severity a strict
// severity name when present; headers within the size caps.
func validateAlertRuleBody(body alertRuleBody, kind core.AlertRuleKind) error {
	if !validAlertKinds[kind] {
		return errors.New("kind: must be new_issue, regression, or threshold")
	}
	if body.URL == "" {
		return errors.New("url: required")
	}
	u, err := url.Parse(body.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("url: must be a valid http or https URL")
	}
	if kind == core.AlertThreshold {
		if body.N < 1 {
			return errors.New("n: must be >= 1 for threshold")
		}
		if body.WindowMinutes < 1 {
			return errors.New("window_minutes: must be >= 1 for threshold")
		}
	}
	if body.CooldownSeconds < 0 {
		return errors.New("cooldown_seconds: must be >= 0")
	}
	// ParseSeverityStrict accepts "" as "not supplied" (-> SeverityTrace,
	// meaning "any"), so this rejects only genuine typos.
	if _, ok := core.ParseSeverityStrict(body.MinSeverity); !ok {
		return errors.New("min_severity: unknown value")
	}
	if err := validateAlertHeaders(body.Headers); err != nil {
		return err
	}
	return nil
}

// validateAlertHeaders rejects header maps that could wedge an unbounded
// payload into storage or the outbound webhook POST: more than
// maxAlertHeaders entries, or more than maxAlertHeadersBytes summed across
// every key+value.
func validateAlertHeaders(h map[string]string) error {
	if len(h) > maxAlertHeaders {
		return errors.New("headers: too many entries (max 32)")
	}
	var total int
	for k, v := range h {
		total += len(k) + len(v)
	}
	if total > maxAlertHeadersBytes {
		return errors.New("headers: too large (max 4KB total)")
	}
	return nil
}

// Create handles POST /api/v1/projects/{id}/alert-rules (upsert: body.ID
// unset inserts, set updates).
func (a *AlertRules) Create(w http.ResponseWriter, r *http.Request) {
	pathID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	var body alertRuleBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		respondErr(w, http.StatusBadRequest, "body: invalid JSON")
		return
	}

	kind := core.AlertRuleKind(body.Kind)
	if err := validateAlertRuleBody(body, kind); err != nil {
		respondErr(w, http.StatusBadRequest, err.Error())
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	projectID := pathID
	if !isAdmin {
		projectID = callerProjectID
	}

	// Already validated above; ok is guaranteed true here.
	minSeverity, _ := core.ParseSeverityStrict(body.MinSeverity)

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
		existing, ok := findAlertRule(r, a.AR, body.ID, lookupProjectID)
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

	row, err := a.Engine.Upsert(r.Context(), core.AlertRule{
		ID:              body.ID,
		ProjectID:       projectID,
		Name:            body.Name,
		Kind:            kind,
		Service:         body.Service,
		Environment:     body.Environment,
		MinSeverity:     minSeverity,
		N:               body.N,
		WindowMinutes:   body.WindowMinutes,
		CooldownSeconds: body.CooldownSeconds,
		URL:             body.URL,
		Headers:         body.Headers,
		Enabled:         enabled,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	respond(w, http.StatusOK, toAlertRuleDTO(row))
}

// findAlertRule looks up ruleID among projectID's rules (projectID 0 = all
// projects — used for admin lookups that aren't scoped to one project).
func findAlertRule(r *http.Request, ar store.AlertRules, ruleID, projectID int64) (store.AlertRuleRow, bool) {
	rows, err := ar.AlertRules(r.Context(), projectID)
	if err != nil {
		return store.AlertRuleRow{}, false
	}
	for _, row := range rows {
		if row.ID == ruleID {
			return row, true
		}
	}
	return store.AlertRuleRow{}, false
}

// alertRuleBelongsToProject reports whether ruleID is among projectID's
// rules.
func alertRuleBelongsToProject(r *http.Request, ar store.AlertRules, ruleID, projectID int64) bool {
	_, ok := findAlertRule(r, ar, ruleID, projectID)
	return ok
}

// Delete handles DELETE /api/v1/alert-rules/{id}.
func (a *AlertRules) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	if !isAdmin && !alertRuleBelongsToProject(r, a.AR, id, callerProjectID) {
		// Row exists but belongs to another project (or doesn't exist at
		// all): 404 either way, not 403 — a project-bound key must not
		// learn that another project's rule ID exists.
		respondErr(w, http.StatusNotFound, "not found")
		return
	}

	if err := a.Engine.Delete(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			respondErr(w, http.StatusNotFound, "not found")
			return
		}
		respondErr(w, http.StatusInternalServerError, "internal")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// Test handles POST /api/v1/alert-rules/{id}/test: fires rule id's webhook
// synchronously with a sample issue and reports the delivery outcome
// directly, rather than making the caller poll last_error.
func (a *AlertRules) Test(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		respondErr(w, http.StatusBadRequest, "id: invalid")
		return
	}

	callerProjectID, isAdmin := callerScope(r)
	if !isAdmin && !alertRuleBelongsToProject(r, a.AR, id, callerProjectID) {
		respondErr(w, http.StatusNotFound, "not found")
		return
	}

	if err := a.Engine.TestFire(r.Context(), id); err != nil {
		respondErr(w, http.StatusBadGateway, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]bool{"delivered": true})
}
