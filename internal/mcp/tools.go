package mcp

import (
	"context"
	"fmt"
	"sort"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// registerTools binds each of the seventeen tools to the underlying MCP
// server. Every tool = an input schema struct + a handler that calls the
// store + a small pure render function (see render.go). The eight
// original tools are registered here; the five noise-control tools are
// registered by registerNoiseTools (see noise.go), and the four
// alert-rule tools by registerAlertTools (see alert.go).
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

	s.registerNoiseTools()
	s.registerAlertTools()
}

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
		return errorResult(toolErr(err)), nil, nil
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
			return errorResult(toolErr(err)), nil, nil
		}
		rows = append(rows, projectRow{ID: p.ID, Slug: p.Slug, OpenIssues: st.OpenIssues, Logs24h: st.Logs})
	}
	return textResult(renderProjects(rows)), nil, nil
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
		return errorResult(toolErr(err)), nil, nil
	}

	hdr := issueListHeader{Status: status, Environment: in.Environment, SinceLabel: in.Since}
	if f.ProjectID != 0 {
		if slug, ok := s.projectSlug(ctx, f.ProjectID); ok {
			hdr.ProjectSlug = slug
		}
	}
	return textResult(renderIssues(issues, renderLimit(in.Limit), now, hdr)), nil, nil
}

// ---- get_issue ----

type getIssueInput struct {
	ID int64 `json:"id" jsonschema:"Issue ID"`
}

func (s *Server) getIssue(ctx context.Context, _ *mcpsdk.CallToolRequest, in getIssueInput) (*mcpsdk.CallToolResult, any, error) {
	iss, events, err := s.reader.Issue(ctx, in.ID)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}

	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && iss.ProjectID != callerProjectID {
		return errorResult(errNotFound), nil, nil
	}

	return textResult(renderIssue(iss, events, s.clock())), nil, nil
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
		return errorResult(toolErr(err)), nil, nil
	}

	hdr := logListHeader{Query: in.Query, Service: in.Service, Environment: in.Environment, SinceLabel: in.Since}
	if f.ProjectID != 0 {
		if slug, ok := s.projectSlug(ctx, f.ProjectID); ok {
			hdr.ProjectSlug = slug
		}
	}
	return textResult(renderLogs(logs, renderLimit(in.Limit), hdr)), nil, nil
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
		return errorResult(toolErr(err)), nil, nil
	}

	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && len(logs) > 0 && logs[0].ProjectID != callerProjectID {
		return errorResult(errNotFound), nil, nil
	}

	return textResult(renderLogContext(logs, in.LogID)), nil, nil
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
			return errorResult(toolErr(err)), nil, nil
		}
		if iss.ProjectID != callerProjectID {
			return errorResult(errNotFound), nil, nil
		}
	}

	if err := s.admin.SetIssueStatus(ctx, id, status); err != nil {
		return errorResult(toolErr(err)), nil, nil
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
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(renderStats(st)), nil, nil
}
