package mcp

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// This file holds the pure render/format helpers used by the tool handlers
// in tools.go: turning store results into the compact text blocks each MCP
// tool returns, plus the small parsing helpers (parseSince, ...) that
// support them. None of these touch the store or the MCP SDK, so they're
// kept separate and are straightforward to unit-test in isolation.

func renderProjects(rows []projectRow) string {
	lines := make([]string, 0, len(rows)+1)
	lines = append(lines, fmt.Sprintf("%d projects:", len(rows)))
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("#%d %s — %d open issues, %d logs (24h)", r.ID, r.Slug, r.OpenIssues, r.Logs24h))
	}
	return strings.Join(lines, "\n")
}

func issuesHeaderLine(total int, hdr issueListHeader) string {
	scope := ""
	if hdr.ProjectSlug != "" {
		scope = " in " + hdr.ProjectSlug
	}
	var parens []string
	if hdr.Environment != "" {
		parens = append(parens, hdr.Environment)
	}
	if hdr.SinceLabel != "" {
		parens = append(parens, hdr.SinceLabel)
	}
	parenStr := ""
	if len(parens) > 0 {
		parenStr = " (" + strings.Join(parens, ", ") + ")"
	}
	return fmt.Sprintf("%d %s issues%s%s:", total, hdr.Status, scope, parenStr)
}

func renderIssueRow(iss core.Issue, now time.Time) string {
	return fmt.Sprintf("#%d ×%d [%s] %s (last %s, first %s)",
		iss.ID, iss.Count, iss.Severity.String(), iss.Title,
		relTime(iss.LastSeen, now), relTime(iss.FirstSeen, now))
}

func renderIssues(issues []core.Issue, limit int, now time.Time, hdr issueListHeader) string {
	total := len(issues)
	shown := issues
	truncated := false
	if len(shown) > limit {
		shown = shown[:limit]
		truncated = true
	}

	lines := make([]string, 0, len(shown)+2)
	lines = append(lines, issuesHeaderLine(total, hdr))
	for _, iss := range shown {
		lines = append(lines, renderIssueRow(iss, now))
	}
	if truncated {
		lines = append(lines, fmt.Sprintf("(+%d more — refine filters)", total-limit))
	}
	return strings.Join(lines, "\n")
}

func renderIssue(iss core.Issue, events []core.Event, now time.Time) string {
	lines := []string{
		fmt.Sprintf("#%d [%s] %s", iss.ID, iss.Severity.String(), iss.Title),
		fmt.Sprintf("status: %s  fingerprint: %s", iss.Status, iss.Fingerprint),
		fmt.Sprintf("first seen: %s  last seen: %s  count: %d",
			formatRFC3339(iss.FirstSeen), formatRFC3339(iss.LastSeen), iss.Count),
	}

	envs, releases := envsAndReleases(events)
	lines = append(lines, "environments: "+strings.Join(envs, ", "))
	lines = append(lines, "releases: "+strings.Join(releases, ", "))

	hist := perDayHistogram(events, now, 7)
	histStrs := make([]string, len(hist))
	for i, c := range hist {
		histStrs[i] = strconv.Itoa(c)
	}
	lines = append(lines, "7d: "+strings.Join(histStrs, " "))

	if newest, ok := newestEvent(events); ok {
		lines = append(lines, "")
		lines = append(lines, fmt.Sprintf("newest event (%s):", formatRFC3339(newest.Time)))
		lines = append(lines, "service: "+newest.Log.Service)
		lines = append(lines, truncateChars(newest.Log.Body, maxBodyChars))
		if st, ok := newest.Log.Attrs["exception.stacktrace"]; ok {
			lines = append(lines, "exception.stacktrace:")
			lines = append(lines, truncateChars(st, maxStacktraceChars))
		}
		keys := make([]string, 0, len(newest.Log.Attrs))
		for k := range newest.Log.Attrs {
			if k == "exception.stacktrace" {
				continue
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			lines = append(lines, fmt.Sprintf("%s=%s", k, newest.Log.Attrs[k]))
		}
	}

	return strings.Join(lines, "\n")
}

func envsAndReleases(events []core.Event) (envs, releases []string) {
	envSet := map[string]struct{}{}
	relSet := map[string]struct{}{}
	for _, e := range events {
		if e.Log.Environment != "" {
			envSet[e.Log.Environment] = struct{}{}
		}
		if e.Log.Release != "" {
			relSet[e.Log.Release] = struct{}{}
		}
	}
	for e := range envSet {
		envs = append(envs, e)
	}
	for r := range relSet {
		releases = append(releases, r)
	}
	sort.Strings(envs)
	sort.Strings(releases)
	return envs, releases
}

func perDayHistogram(events []core.Event, now time.Time, days int) []int {
	counts := make([]int, days)
	base := now.UTC().Truncate(24 * time.Hour)
	for _, e := range events {
		eventDay := e.Time.UTC().Truncate(24 * time.Hour)
		offsetDays := int(base.Sub(eventDay).Hours() / 24)
		idx := days - 1 - offsetDays
		if idx >= 0 && idx < days {
			counts[idx]++
		}
	}
	return counts
}

func newestEvent(events []core.Event) (core.Event, bool) {
	if len(events) == 0 {
		return core.Event{}, false
	}
	newest := events[0]
	for _, e := range events[1:] {
		if e.Time.After(newest.Time) {
			newest = e
		}
	}
	return newest, true
}

func logsHeaderLine(total int, hdr logListHeader) string {
	scope := ""
	if hdr.ProjectSlug != "" {
		scope = " in " + hdr.ProjectSlug
	}
	var parens []string
	if hdr.Query != "" {
		parens = append(parens, fmt.Sprintf("q=%q", hdr.Query))
	}
	if hdr.Service != "" {
		parens = append(parens, hdr.Service)
	}
	if hdr.Environment != "" {
		parens = append(parens, hdr.Environment)
	}
	if hdr.SinceLabel != "" {
		parens = append(parens, hdr.SinceLabel)
	}
	parenStr := ""
	if len(parens) > 0 {
		parenStr = " (" + strings.Join(parens, ", ") + ")"
	}
	return fmt.Sprintf("%d logs%s%s:", total, scope, parenStr)
}

func renderLogRow(l core.Log, prefix string) string {
	return fmt.Sprintf("%s%s [%s] %s %s id=%d",
		prefix, formatRFC3339(l.Time), l.Severity.String(), l.Service,
		truncateChars(firstLineOf(l.Body), maxRowLineChars), l.ID)
}

func renderLogs(logs []core.Log, limit int, hdr logListHeader) string {
	total := len(logs)
	shown := logs
	truncated := false
	if len(shown) > limit {
		shown = shown[:limit]
		truncated = true
	}

	lines := make([]string, 0, len(shown)+2)
	lines = append(lines, logsHeaderLine(total, hdr))
	for _, l := range shown {
		lines = append(lines, renderLogRow(l, ""))
	}
	if truncated {
		lines = append(lines, fmt.Sprintf("(+%d more — refine filters)", total-limit))
	}
	return strings.Join(lines, "\n")
}

func renderLogContext(logs []core.Log, targetID int64) string {
	lines := make([]string, 0, len(logs)+1)
	lines = append(lines, fmt.Sprintf("%d logs:", len(logs)))
	for _, l := range logs {
		prefix := ""
		if l.ID == targetID {
			prefix = "→ "
		}
		lines = append(lines, renderLogRow(l, prefix))
	}
	return strings.Join(lines, "\n")
}

// svcOrAny renders a rule's service filter, using "any" for the empty
// string (which core.NoiseRule.Matches treats as "any service").
func svcOrAny(service string) string {
	if service == "" {
		return "any"
	}
	return service
}

// noiseRuleLine renders one rule as a single compact line, showing only
// the params that kind actually uses (severity_floor: service+severity;
// drop_match: service+pattern; sample: service+severity+n) plus its
// enabled state and persisted drop count.
func noiseRuleLine(row store.NoiseRuleRow) string {
	var params string
	switch row.Kind {
	case core.NoiseSeverityFloor:
		params = fmt.Sprintf(" service=%s severity=%s", svcOrAny(row.Service), strings.ToLower(row.Severity.String()))
	case core.NoiseDropMatch:
		params = fmt.Sprintf(" service=%s pattern=%q", svcOrAny(row.Service), row.Pattern)
	case core.NoiseSample:
		params = fmt.Sprintf(" service=%s severity=%s n=%d", svcOrAny(row.Service), strings.ToLower(row.Severity.String()), row.N)
	default:
		params = fmt.Sprintf(" service=%s", svcOrAny(row.Service))
	}
	return fmt.Sprintf("#%d %s%s enabled=%v dropped=%d", row.ID, row.Kind, params, row.Enabled, row.DroppedCount)
}

func renderNoiseRules(rows []store.NoiseRuleRow, projectSlug string) string {
	scope := ""
	if projectSlug != "" {
		scope = " in " + projectSlug
	}
	lines := make([]string, 0, len(rows)+1)
	lines = append(lines, fmt.Sprintf("%d noise rules%s:", len(rows), scope))
	for _, r := range rows {
		lines = append(lines, noiseRuleLine(r))
	}
	return strings.Join(lines, "\n")
}

func renderNoiseRule(row store.NoiseRuleRow) string {
	return "rule " + noiseRuleLine(row)
}

func renderNoiseReport(topServices []store.ServiceCount, rules []store.NoiseRuleRow, totalDropped int64, hoursLabel string) string {
	lines := make([]string, 0, len(topServices)+len(rules)+3)
	lines = append(lines, fmt.Sprintf("noise report (%s): %d services, %d rules, %d total dropped",
		hoursLabel, len(topServices), len(rules), totalDropped))

	lines = append(lines, "top services by volume:")
	for _, s := range topServices {
		lines = append(lines, fmt.Sprintf("%s: %d logs", s.Service, s.Logs))
	}

	lines = append(lines, "rules:")
	for _, r := range rules {
		lines = append(lines, noiseRuleLine(r))
	}
	return strings.Join(lines, "\n")
}

func renderStats(st store.Stats) string {
	lines := []string{
		fmt.Sprintf("logs: %d  events: %d  open issues: %d", st.Logs, st.Events, st.OpenIssues),
	}
	for _, d := range st.PerDay {
		lines = append(lines, fmt.Sprintf("%s: %d logs, %d events", d.Day, d.Logs, d.Events))
	}
	return strings.Join(lines, "\n")
}

// renderEngineBlock renders get_stats's optional engine block: segment
// count, stored (flushed) rows, the raw-template-fallback percentage,
// average bytes per stored record, and unflushed (memtable) rows. Rate
// and bytes/record are guarded against a zero Rows denominator (nothing
// flushed yet) rather than dividing by zero.
func renderEngineBlock(es store.EngineStats) string {
	return fmt.Sprintf("engine: segments=%d stored_rows=%d raw_fallback=%.1f%% bytes_per_record=%.1f unflushed_rows=%d",
		es.Segments, es.Rows, rawFallbackPct(es), bytesPerRecord(es), es.MemRows)
}

// rawFallbackPct is the percentage of stored (flushed) rows that fell
// back to raw storage (no template match), 0 when nothing has flushed.
func rawFallbackPct(es store.EngineStats) float64 {
	if es.Rows == 0 {
		return 0
	}
	return float64(es.RawRows) / float64(es.Rows) * 100
}

// bytesPerRecord is average on-disk bytes per stored (flushed) row, 0
// when nothing has flushed.
func bytesPerRecord(es store.EngineStats) float64 {
	if es.Rows == 0 {
		return 0
	}
	return float64(es.SizeBytes) / float64(es.Rows)
}

// renderAggregate renders aggregate_logs's compact table: a header count
// line followed by one KEY/LOGS/EVENTS row per bucket, in the order
// store.Reader.Aggregate already returned them (service by Logs desc,
// severity by numeric Key desc, hour/day by Key asc — Aggregate's
// contract, not re-sorted here). Severity keys (decimal severity
// numbers) render as their canonical name via core.Severity; every other
// group_by's Key is already the display value.
func renderAggregate(rows []store.AggregateRow, groupBy string) string {
	lines := make([]string, 0, len(rows)+2)
	lines = append(lines, fmt.Sprintf("%d rows (group_by=%s):", len(rows), groupBy))
	lines = append(lines, "KEY LOGS EVENTS")
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("%s %d %d", aggregateKeyLabel(groupBy, r.Key), r.Logs, r.Events))
	}
	return strings.Join(lines, "\n")
}

// aggregateKeyLabel renders one AggregateRow.Key for display. For
// group_by=severity, Key is core.Severity's decimal string form; an
// unparseable key (shouldn't happen, but never worth failing the whole
// render over) falls back to the raw key rather than panicking or
// erroring.
func aggregateKeyLabel(groupBy, key string) string {
	if groupBy != "severity" {
		return key
	}
	n, err := strconv.Atoi(key)
	if err != nil {
		return key
	}
	return core.Severity(n).String()
}

// Payload caps that keep a single tool result within a token-frugal
// budget even when the underlying log body or stack trace is huge (a
// multi-KB panic dump, a giant JSON blob logged as the body, etc).
// Without these, get_issue's single newest-sample-event block — the one
// place a full body/stacktrace is rendered — could dwarf every other
// tool's entire output.
const (
	maxBodyChars       = 2000 // get_issue's newest-event body
	maxStacktraceChars = 4000 // get_issue's exception.stacktrace attr
	maxRowLineChars    = 300  // one row's body-first-line, in list/context tools
)

// truncateChars caps s at maxLen runes, appending a marker that states the
// true total so an agent knows content was cut rather than assuming the
// value was simply short.
func truncateChars(s string, maxLen int) string {
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}
	return string(r[:maxLen]) + fmt.Sprintf("… (truncated, %d chars total)", len(r))
}

// relTime renders t relative to now as a compact human string: "45s ago",
// "2m ago", "3h ago", "2d ago". Never negative — a t in now's future
// (clock skew) renders as "0s ago".
func relTime(t, now time.Time) string {
	d := now.Sub(t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// formatRFC3339 renders t in RFC3339, UTC — compact and unambiguous,
// without the sub-second precision the REST edge uses (token frugality).
func formatRFC3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// firstLineOf returns the first line of body, so a multi-line log (e.g. one
// with an embedded stack trace) still renders as a single list row.
func firstLineOf(body string) string {
	if i := strings.IndexByte(body, '\n'); i >= 0 {
		return body[:i]
	}
	return body
}

// parseSince parses a duration ("24h", "7d") or an RFC3339 timestamp,
// returning the corresponding absolute time relative to now.
func parseSince(s string, now time.Time) (time.Time, error) {
	if d, ok := parseFrugalDuration(s); ok {
		return now.Add(-d), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("invalid duration or timestamp %q", s)
}

// parseFrugalDuration supports Go's standard duration syntax (h/m/s/...)
// plus a "Nd" day suffix, which time.ParseDuration doesn't understand.
func parseFrugalDuration(s string) (time.Duration, bool) {
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil || n < 0 {
			return 0, false
		}
		return time.Duration(n) * 24 * time.Hour, true
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false
	}
	return d, true
}
