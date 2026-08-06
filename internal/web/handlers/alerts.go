package handlers

import (
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/store"
)

// Alerts serves GET /alerts: a read-only status page listing every alert
// rule across every project (admin session sees the whole instance,
// mirroring Search/Settings) plus an htmx test-fire button per rule. There
// are no create/edit forms in v1 — that's API/MCP territory; this page is
// a verification surface.
type Alerts struct {
	AR     store.AlertRules
	Engine *alerts.Engine
	Tpl    *template.Template // templates/alerts.html + layout
}

// NewAlerts constructs an Alerts handler.
func NewAlerts(ar store.AlertRules, engine *alerts.Engine, tpl *template.Template) *Alerts {
	return &Alerts{AR: ar, Engine: engine, Tpl: tpl}
}

// alertRuleView adds the nav page's scope-summary rendering on top of the
// stored row, kept here (not in the DTO the REST API returns) since it's
// presentation-only.
type alertRuleView struct {
	store.AlertRuleRow
	ScopeSummary string
}

func scopeSummary(r store.AlertRuleRow) string {
	var parts []string
	if r.Service != "" {
		parts = append(parts, "service="+r.Service)
	}
	if r.Environment != "" {
		parts = append(parts, "env="+r.Environment)
	}
	if r.MinSeverity != 0 {
		parts = append(parts, "min_severity="+strings.ToLower(r.MinSeverity.String()))
	}
	if len(parts) == 0 {
		return "any"
	}
	return strings.Join(parts, ", ")
}

func toAlertRuleViews(rows []store.AlertRuleRow) []alertRuleView {
	views := make([]alertRuleView, len(rows))
	for i, r := range rows {
		views[i] = alertRuleView{AlertRuleRow: r, ScopeSummary: scopeSummary(r)}
	}
	return views
}

// Page handles GET /alerts: every project's rules (projectID 0 = all, the
// admin session is instance-wide — see package doc).
func (h *Alerts) Page(w http.ResponseWriter, r *http.Request) {
	rows, err := h.AR.AlertRules(r.Context(), 0)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	renderFull(w, h.Tpl, map[string]any{"Rules": toAlertRuleViews(rows)})
}

// Test handles POST /alerts/{id}/test: fires the rule synchronously via
// Engine.TestFire and renders the inline result fragment (success or
// error) that replaces the button's row-status cell.
func (h *Alerts) Test(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	deliveryErr := h.Engine.TestFire(r.Context(), id)
	data := map[string]any{"ID": id}
	if deliveryErr != nil {
		data["Error"] = deliveryErr.Error()
	} else {
		data["Delivered"] = true
	}
	renderFragment(w, h.Tpl, "test-result", data)
}
