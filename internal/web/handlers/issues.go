package handlers

import (
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// Issues serves the issues list and detail screens, plus the resolve/
// ignore row actions.
type Issues struct {
	Reader    store.Reader
	Admin     store.Admin
	listTpl   *template.Template // templates/issues.html + layout
	detailTpl *template.Template // templates/issue.html + layout
}

// NewIssues constructs an Issues handler. list and detail are the parsed
// page template sets for issues.html and issue.html respectively.
func NewIssues(r store.Reader, a store.Admin, list, detail *template.Template) *Issues {
	return &Issues{Reader: r, Admin: a, listTpl: list, detailTpl: detail}
}

// windowSince maps a "24h"/"7d"/"30d" window token to a Since cutoff;
// unrecognized or empty values default to 24h.
func windowSince(window string) time.Time {
	d := 24 * time.Hour
	switch window {
	case "7d":
		d = 7 * 24 * time.Hour
	case "30d":
		d = 30 * 24 * time.Hour
	}
	return time.Now().Add(-d)
}

// List handles GET / — the issues list. Filters (project, env, status,
// time window) come from the query string and, on an htmx-driven filter
// change, only the table body fragment is rendered back.
func (h *Issues) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f store.IssueFilter

	if v := q.Get("project"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			f.ProjectID = id
		}
	}
	f.Environment = q.Get("env")
	if v := q.Get("status"); v != "" {
		f.Status = core.IssueStatus(v)
	}
	window := q.Get("window")
	if window == "" {
		window = "24h"
	}
	f.Since = windowSince(window)

	issues, err := h.Reader.Issues(r.Context(), f)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	data := map[string]any{
		"Issues":  issues,
		"Status":  string(f.Status),
		"Env":     f.Environment,
		"Window":  window,
		"Project": f.ProjectID,
		"BaseURL": baseURL(r),
	}
	if isHX(r) {
		renderFragment(w, h.listTpl, "issues-rows", data)
		return
	}
	renderFull(w, h.listTpl, data)
}

// Detail handles GET /issues/{id}: the sample event body, an occurrence
// sparkline, and the resolve/ignore actions.
func (h *Issues) Detail(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	issue, events, err := h.Reader.Issue(r.Context(), id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	var sample *core.Log
	if len(events) > 0 {
		sample = &events[len(events)-1].Log
	}

	// Stats is a project-wide, not per-issue, series (the store exposes no
	// per-issue day counts) — used here as a proxy trend for the issue's
	// project since the issue first appeared. Approximate, not exact.
	stats, _ := h.Reader.Stats(r.Context(), store.StatsFilter{ProjectID: issue.ProjectID, Since: issue.FirstSeen})

	data := map[string]any{
		"Issue":     issue,
		"Sample":    sample,
		"Sparkline": buildSparkline(stats.PerDay),
	}
	renderFull(w, h.detailTpl, data)
}

// buildSparkline renders per-day event counts as inline SVG polyline
// points (a 0-100 x 0-20 viewBox), per the design system's sparkline
// spec: no axes, no fills, single stroke.
func buildSparkline(days []store.DayCount) string {
	if len(days) == 0 {
		return ""
	}
	var maxEvents int64 = 1
	for _, d := range days {
		if d.Events > maxEvents {
			maxEvents = d.Events
		}
	}
	n := len(days)
	denom := n - 1
	if denom < 1 {
		denom = 1
	}
	pts := make([]string, n)
	for i, d := range days {
		x := float64(i) / float64(denom) * 100
		y := 20 - (float64(d.Events)/float64(maxEvents))*20
		pts[i] = fmt.Sprintf("%.1f,%.1f", x, y)
	}
	return strings.Join(pts, " ")
}

// Resolve handles POST /issues/{id}/resolve.
func (h *Issues) Resolve(w http.ResponseWriter, r *http.Request) {
	h.setStatus(w, r, core.StatusResolved)
}

// Ignore handles POST /issues/{id}/ignore.
func (h *Issues) Ignore(w http.ResponseWriter, r *http.Request) {
	h.setStatus(w, r, core.StatusIgnored)
}

// setStatus updates the issue's status and renders back just the status
// chip fragment, which the button's hx-post swaps in place.
func (h *Issues) setStatus(w http.ResponseWriter, r *http.Request, status core.IssueStatus) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}

	if err := h.Admin.SetIssueStatus(r.Context(), id, status); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	issue, _, err := h.Reader.Issue(r.Context(), id)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	renderFragment(w, h.detailTpl, "status-chip", map[string]any{"Issue": issue})
}
