// Package handlers implements the admin-session-authenticated web UI's
// screen handlers (issues, search, settings, login). Every handler renders
// server-side html/template: full pages on normal navigation, and just the
// swapped fragment when the request carries the HX-Request header (an
// htmx-driven filter change, row action, or auto-refresh tick).
package handlers

import (
	"html/template"
	"net/http"
	"time"

	"github.com/agenterr/agenterr/internal/core"
)

// FuncMap is shared by every page template set.
var FuncMap = template.FuncMap{
	"severityClass": severityClass,
	"sevChip":       sevChip,
	"fmtTime":       fmtTime,
	"relTime":       relTime,
}

// isHX reports whether r was issued by htmx (hx-get/hx-post), meaning the
// caller wants just the swapped fragment rather than a full page.
func isHX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}

// renderFull executes tpl's "layout" template — a full HTML document.
func renderFull(w http.ResponseWriter, tpl *template.Template, data any) {
	renderFullStatus(w, tpl, http.StatusOK, data)
}

// renderFullStatus is renderFull with an explicit status code, for cases
// like a failed login where the full page re-renders with a non-200.
func renderFullStatus(w http.ResponseWriter, tpl *template.Template, status int, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := tpl.ExecuteTemplate(w, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// renderFragment executes the named block within tpl — the htmx swap
// target only, no surrounding document.
func renderFragment(w http.ResponseWriter, tpl *template.Template, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// baseURL reconstructs the scheme+host the request arrived on, for
// building copy-paste snippets (curl, Vector sink, claude mcp add) that
// point back at this instance.
func baseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

// severityClass maps a core.Severity to the CSS token suffix used by
// app.css (sev-error, sev-warn, sev-info, sev-debug) per the design
// system's severity-dot/chip spec. FATAL renders as error — the palette
// defines no separate fatal hue.
func severityClass(s core.Severity) string {
	switch s {
	case core.SeverityError, core.SeverityFatal:
		return "error"
	case core.SeverityWarn:
		return "warn"
	case core.SeverityInfo:
		return "info"
	default:
		return "debug"
	}
}

// sevChip renders the fixed-width 4-character mono label the design
// system specifies for severity chips (ERRO/WARN/INFO/DEBU).
func sevChip(s core.Severity) string {
	switch s {
	case core.SeverityFatal:
		return "FATL"
	case core.SeverityError:
		return "ERRO"
	case core.SeverityWarn:
		return "WARN"
	case core.SeverityInfo:
		return "INFO"
	case core.SeverityTrace:
		return "TRAC"
	default:
		return "DEBU"
	}
}

// fmtTime renders the absolute timestamp format tables use per the design
// system: "YYYY-MM-DD HH:MM:SS.mmm", UTC.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000")
}

// relTime renders a short relative time ("2m ago") for space-tight
// columns, per the design system's timestamp spec. Absolute time belongs
// in the title attribute where callers need it; this helper only formats
// the text.
func relTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return itoa(int(d/time.Minute)) + "m ago"
	case d < 24*time.Hour:
		return itoa(int(d/time.Hour)) + "h ago"
	default:
		return itoa(int(d/(24*time.Hour))) + "d ago"
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
