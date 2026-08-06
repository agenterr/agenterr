package handlers

import (
	"errors"
	"html/template"
	"net/http"
	"strconv"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// Search serves GET /search: free-text + severity + time-window log
// search, with an optional auto-refresh toggle.
type Search struct {
	Reader store.Reader
	Tpl    *template.Template // templates/search.html + layout
}

// NewSearch constructs a Search handler.
func NewSearch(r store.Reader, tpl *template.Template) *Search {
	return &Search{Reader: r, Tpl: tpl}
}

// List handles GET /search.
func (h *Search) List(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var f store.LogFilter
	f.Query = q.Get("q")
	if v := q.Get("project"); v != "" {
		if id, err := strconv.ParseInt(v, 10, 64); err == nil {
			f.ProjectID = id
		}
	}
	severity := q.Get("severity")
	if severity != "" {
		f.MinSeverity = core.ParseSeverity(severity)
	}
	window := q.Get("window")
	if window == "" {
		window = "24h"
	}
	f.Since = windowSince(window)

	autoRefresh := q.Get("refresh") == "1"

	logs, err := h.Reader.SearchLogs(r.Context(), f)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	data := map[string]any{
		"Query":       f.Query,
		"Severity":    severity,
		"Window":      window,
		"AutoRefresh": autoRefresh,
		"Logs":        logs,
		"QueryString": r.URL.RawQuery,
	}
	if isHX(r) {
		renderFragment(w, h.Tpl, "search-results", data)
		return
	}
	renderFull(w, h.Tpl, data)
}

// Context handles GET /logs/{id}/context: expands a search result row into
// the surrounding ±N log lines. n defaults to 5.
func (h *Search) Context(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	n := 5
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}

	logs, err := h.Reader.LogContext(r.Context(), id, n)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	renderFragment(w, h.Tpl, "context-rows", map[string]any{"Logs": logs})
}
