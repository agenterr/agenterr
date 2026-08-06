package mcp

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// registerTools binds each of the eight tools to the underlying MCP
// server. Every tool = an input schema struct + a handler that calls the
// store + a small pure render function.
func (s *Server) registerTools() {
	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "list_projects",
		Description: "List projects visible to this key, with open issue and 24h log counts. Project-scoped keys see only their own project.",
	}, s.listProjects)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "list_issues",
		Description: "List issues (grouped errors), newest activity first. Defaults to open issues from the last relevant window.",
	}, s.listIssues)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "get_issue",
		Description: "Full detail for one issue: severity, status, seen-range, environments/releases, a 7-day occurrence histogram, and the newest sample event (with stack trace if present).",
	}, s.getIssue)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "search_logs",
		Description: "Search raw logs by text, severity, service, environment, and time range.",
	}, s.searchLogs)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "get_log_context",
		Description: "Fetch the logs immediately before and after a given log, to see what led up to it.",
	}, s.getLogContext)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "resolve_issue",
		Description: "Mark an issue resolved.",
	}, s.resolveIssue)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "ignore_issue",
		Description: "Mark an issue ignored (silences it without resolving).",
	}, s.ignoreIssue)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "get_stats",
		Description: "Log/event volume and open-issue counts, with a per-day breakdown.",
	}, s.getStats)
}

// errNotFound is the tool-level "not found" error: returned both when a
// row genuinely doesn't exist and when it belongs to another project
// (a project-scoped key must not learn that the row exists elsewhere).
var errNotFound = errors.New("not found")

// ---- list_projects ----

type listProjectsInput struct{}

type projectRow struct {
	ID         int64
	Slug       string
	OpenIssues int64
	Logs24h    int64
}

func (s *Server) listProjects(ctx context.Context, _ *mcpsdk.CallToolRequest, _ listProjectsInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)

	projects, err := s.admin.Projects(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !isAdmin {
		filtered := projects[:0]
		for _, p := range projects {
			if p.ID == callerProjectID {
				filtered = append(filtered, p)
			}
		}
		projects = filtered
	}
	sort.Slice(projects, func(i, j int) bool { return projects[i].ID < projects[j].ID })

	now := s.clock()
	rows := make([]projectRow, 0, len(projects))
	for _, p := range projects {
		st, err := s.reader.Stats(ctx, store.StatsFilter{ProjectID: p.ID, Since: now.Add(-24 * time.Hour)})
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, projectRow{ID: p.ID, Slug: p.Slug, OpenIssues: st.OpenIssues, Logs24h: st.Logs})
	}
	return textResult(renderProjects(rows)), nil, nil
}

func renderProjects(rows []projectRow) string {
	lines := make([]string, 0, len(rows)+1)
	lines = append(lines, fmt.Sprintf("%d projects:", len(rows)))
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("#%d %s — %d open issues, %d logs (24h)", r.ID, r.Slug, r.OpenIssues, r.Logs24h))
	}
	return strings.Join(lines, "\n")
}

// ---- list_issues ----

type listIssuesInput struct {
	Project     int64  `json:"project,omitempty" jsonschema:"Project ID; admin keys only, project-scoped keys always see only their own project"`
	Environment string `json:"environment,omitempty" jsonschema:"Filter by environment"`
	Status      string `json:"status,omitempty" jsonschema:"open, resolved, or ignored (default: open)"`
	Since       string `json:"since,omitempty" jsonschema:"Duration like 24h or 7d, or an RFC3339 timestamp"`
	Limit       int    `json:"limit,omitempty" jsonschema:"Max rows to return (default 20)"`
}

type issueListHeader struct {
	Status      string
	ProjectSlug string
	Environment string
	SinceLabel  string
}

func (s *Server) listIssues(ctx context.Context, _ *mcpsdk.CallToolRequest, in listIssuesInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)

	var f store.IssueFilter
	if isAdmin {
		f.ProjectID = in.Project
	} else {
		f.ProjectID = callerProjectID
	}
	f.Environment = in.Environment

	status := in.Status
	if status == "" {
		status = string(core.StatusOpen)
	}
	f.Status = core.IssueStatus(status)

	now := s.clock()
	if in.Since != "" {
		t, err := parseSince(in.Since, now)
		if err != nil {
			return nil, nil, err
		}
		f.Since = t
	}
	f.Limit = fetchCap

	issues, err := s.reader.Issues(ctx, f)
	if err != nil {
		return nil, nil, err
	}

	hdr := issueListHeader{Status: status, Environment: in.Environment, SinceLabel: in.Since}
	if f.ProjectID != 0 {
		if slug, ok := s.projectSlug(ctx, f.ProjectID); ok {
			hdr.ProjectSlug = slug
		}
	}
	return textResult(renderIssues(issues, renderLimit(in.Limit), now, hdr)), nil, nil
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

// ---- get_issue ----

type getIssueInput struct {
	ID int64 `json:"id" jsonschema:"Issue ID"`
}

func (s *Server) getIssue(ctx context.Context, _ *mcpsdk.CallToolRequest, in getIssueInput) (*mcpsdk.CallToolResult, any, error) {
	iss, events, err := s.reader.Issue(ctx, in.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errorResult(errNotFound), nil, nil
		}
		return nil, nil, err
	}

	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && iss.ProjectID != callerProjectID {
		return errorResult(errNotFound), nil, nil
	}

	return textResult(renderIssue(iss, events, s.clock())), nil, nil
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
		lines = append(lines, newest.Log.Body)
		if st, ok := newest.Log.Attrs["exception.stacktrace"]; ok {
			lines = append(lines, "exception.stacktrace:")
			lines = append(lines, st)
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

// ---- search_logs ----

type searchLogsInput struct {
	Project     int64  `json:"project,omitempty" jsonschema:"Project ID; admin keys only"`
	Query       string `json:"query,omitempty" jsonschema:"Full-text match on log body"`
	MinSeverity string `json:"min_severity,omitempty" jsonschema:"Minimum severity: trace, debug, info, warn, error, fatal"`
	Service     string `json:"service,omitempty" jsonschema:"Filter by service name"`
	Environment string `json:"environment,omitempty" jsonschema:"Filter by environment"`
	Since       string `json:"since,omitempty" jsonschema:"Duration like 24h or 7d, or an RFC3339 timestamp"`
	Until       string `json:"until,omitempty" jsonschema:"Duration like 24h or 7d, or an RFC3339 timestamp"`
	Limit       int    `json:"limit,omitempty" jsonschema:"Max rows to return (default 20)"`
}

type logListHeader struct {
	ProjectSlug string
	Query       string
	Service     string
	Environment string
	SinceLabel  string
}

func (s *Server) searchLogs(ctx context.Context, _ *mcpsdk.CallToolRequest, in searchLogsInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)

	var f store.LogFilter
	if isAdmin {
		f.ProjectID = in.Project
	} else {
		f.ProjectID = callerProjectID
	}
	f.Query = in.Query
	if in.MinSeverity != "" {
		f.MinSeverity = core.ParseSeverity(in.MinSeverity)
	}
	f.Service = in.Service
	f.Environment = in.Environment

	now := s.clock()
	if in.Since != "" {
		t, err := parseSince(in.Since, now)
		if err != nil {
			return nil, nil, err
		}
		f.Since = t
	}
	if in.Until != "" {
		t, err := parseSince(in.Until, now)
		if err != nil {
			return nil, nil, err
		}
		f.Until = t
	}
	f.Limit = fetchCap

	logs, err := s.reader.SearchLogs(ctx, f)
	if err != nil {
		return nil, nil, err
	}

	hdr := logListHeader{Query: in.Query, Service: in.Service, Environment: in.Environment, SinceLabel: in.Since}
	if f.ProjectID != 0 {
		if slug, ok := s.projectSlug(ctx, f.ProjectID); ok {
			hdr.ProjectSlug = slug
		}
	}
	return textResult(renderLogs(logs, renderLimit(in.Limit), hdr)), nil, nil
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
		prefix, formatRFC3339(l.Time), l.Severity.String(), l.Service, firstLineOf(l.Body), l.ID)
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

// ---- get_log_context ----

type getLogContextInput struct {
	LogID int64 `json:"log_id" jsonschema:"The log ID to center the context window on"`
	N     int   `json:"n,omitempty" jsonschema:"Number of logs to include on each side (default 20)"`
}

func (s *Server) getLogContext(ctx context.Context, _ *mcpsdk.CallToolRequest, in getLogContextInput) (*mcpsdk.CallToolResult, any, error) {
	n := in.N
	if n <= 0 {
		n = defaultContextN
	}

	logs, err := s.reader.LogContext(ctx, in.LogID, n)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errorResult(errNotFound), nil, nil
		}
		return nil, nil, err
	}

	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && len(logs) > 0 && logs[0].ProjectID != callerProjectID {
		return errorResult(errNotFound), nil, nil
	}

	return textResult(renderLogContext(logs, in.LogID)), nil, nil
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

// ---- resolve_issue / ignore_issue ----

type resolveIssueInput struct {
	ID int64 `json:"id" jsonschema:"Issue ID"`
}

type ignoreIssueInput struct {
	ID int64 `json:"id" jsonschema:"Issue ID"`
}

func (s *Server) resolveIssue(ctx context.Context, _ *mcpsdk.CallToolRequest, in resolveIssueInput) (*mcpsdk.CallToolResult, any, error) {
	return s.setIssueStatus(ctx, in.ID, core.StatusResolved, "resolved")
}

func (s *Server) ignoreIssue(ctx context.Context, _ *mcpsdk.CallToolRequest, in ignoreIssueInput) (*mcpsdk.CallToolResult, any, error) {
	return s.setIssueStatus(ctx, in.ID, core.StatusIgnored, "ignored")
}

func (s *Server) setIssueStatus(ctx context.Context, id int64, status core.IssueStatus, label string) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin {
		iss, _, err := s.reader.Issue(ctx, id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return errorResult(errNotFound), nil, nil
			}
			return nil, nil, err
		}
		if iss.ProjectID != callerProjectID {
			return errorResult(errNotFound), nil, nil
		}
	}

	if err := s.admin.SetIssueStatus(ctx, id, status); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errorResult(errNotFound), nil, nil
		}
		return nil, nil, err
	}
	return textResult(fmt.Sprintf("issue #%d %s", id, label)), nil, nil
}

// ---- get_stats ----

type getStatsInput struct {
	Project int64  `json:"project,omitempty" jsonschema:"Project ID; admin keys only"`
	Since   string `json:"since,omitempty" jsonschema:"Duration like 24h or 7d, or an RFC3339 timestamp"`
}

func (s *Server) getStats(ctx context.Context, _ *mcpsdk.CallToolRequest, in getStatsInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)

	var f store.StatsFilter
	if isAdmin {
		f.ProjectID = in.Project
	} else {
		f.ProjectID = callerProjectID
	}

	now := s.clock()
	if in.Since != "" {
		t, err := parseSince(in.Since, now)
		if err != nil {
			return nil, nil, err
		}
		f.Since = t
	}

	st, err := s.reader.Stats(ctx, f)
	if err != nil {
		return nil, nil, err
	}
	return textResult(renderStats(st)), nil, nil
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

// ---- shared helpers ----

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
