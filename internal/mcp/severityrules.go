package mcp

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// This file holds the three severity-lift tools: listing/upserting/deleting
// rules that raise the severity of plain-text logs which print errors
// without a level. Split from tools.go to keep that file under the repo's
// line-count guideline, mirroring noise.go's split.

// registerSeverityTools binds the three severity-lift tools. Called from
// registerTools in tools.go.
func (s *Server) registerSeverityTools() {
	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "list_severity_rules",
		Description: "List the severity-lift rules configured for a project, with each rule's service, pattern, target severity, enabled state, and how many logs it has lifted so far.",
	}, s.listSeverityRules)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "upsert_severity_rule",
		Description: "Lift the severity of plain-text logs that print errors without a level (e.g. a GORM 'record not found' line at info). Pattern is a Go regex over the log body; the rule fires only on logs still at info or below, and only raises severity. Pass id to update an existing rule.",
	}, s.upsertSeverityRule)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "delete_severity_rule",
		Description: "Remove a severity rule so its matching logs stop being lifted.",
	}, s.deleteSeverityRule)
}

// ---- list_severity_rules ----

type listSeverityRulesInput struct {
	ProjectID int64 `json:"project_id,omitempty" jsonschema:"Project ID; admin keys only — project-scoped keys always see only their own project. Admin omitting this sees rules across every project."`
}

func (s *Server) listSeverityRules(ctx context.Context, _ *mcpsdk.CallToolRequest, in listSeverityRulesInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	projectID := in.ProjectID
	if !isAdmin {
		projectID = callerProjectID
	}

	rows, err := s.sr.SeverityRules(ctx, projectID)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}

	slug := ""
	if projectID != 0 {
		if sl, ok := s.projectSlug(ctx, projectID); ok {
			slug = sl
		}
	}
	return textResult(renderSeverityRules(rows, slug)), nil, nil
}

// ---- upsert_severity_rule ----

type upsertSeverityRuleInput struct {
	ID        int64  `json:"id,omitempty" jsonschema:"Existing rule ID to update; omit to create a new rule"`
	ProjectID int64  `json:"project_id,omitempty" jsonschema:"Project ID; admin keys only — project-scoped keys always target their own project"`
	Service   string `json:"service,omitempty" jsonschema:"Service name to match; omit to match any service"`
	Pattern   string `json:"pattern" jsonschema:"Go regexp matched against the log body"`
	Severity  string `json:"severity" jsonschema:"Severity to lift matching logs to; must be above info (e.g. warn, error, fatal)"`
	Enabled   *bool  `json:"enabled,omitempty" jsonschema:"Whether the rule is active; defaults to true on create, on update omitted preserves the current value"`
}

// findSeverityRule looks up ruleID among projectID's rules (projectID 0 =
// all projects — used for admin lookups that aren't scoped to one
// project). Mirrors internal/api/handlers.findSeverityRule.
func (s *Server) findSeverityRule(ctx context.Context, ruleID, projectID int64) (store.SeverityRuleRow, bool) {
	rows, err := s.sr.SeverityRules(ctx, projectID)
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
// rules. Mirrors internal/api/handlers.severityRuleBelongsToProject.
func (s *Server) severityRuleBelongsToProject(ctx context.Context, ruleID, projectID int64) bool {
	_, ok := s.findSeverityRule(ctx, ruleID, projectID)
	return ok
}

func (s *Server) upsertSeverityRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in upsertSeverityRuleInput) (*mcpsdk.CallToolResult, any, error) {
	if in.Pattern == "" {
		return errorResult(errors.New("pattern: required")), nil, nil
	}
	if _, err := regexp.Compile(in.Pattern); err != nil {
		return errorResult(fmt.Errorf("pattern: invalid regexp: %w", err)), nil, nil
	}
	severity, ok := core.ParseSeverityStrict(in.Severity)
	if !ok {
		return errorResult(errors.New("severity: unknown value")), nil, nil
	}
	if severity <= core.SeverityInfo {
		return errorResult(errors.New("severity: must be above info — severity rules only lift")), nil, nil
	}

	callerProjectID, isAdmin := callerScope(ctx)
	projectID := in.ProjectID
	if !isAdmin {
		projectID = callerProjectID
	} else if projectID == 0 {
		return errorResult(errors.New("project_id: required for admin keys")), nil, nil
	}

	// enabled starts at the create default (true) or, for an update, the
	// rule's current state — either is overridden below if the caller
	// supplied an explicit value.
	enabled := true
	if in.ID != 0 {
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
		existing, ok := s.findSeverityRule(ctx, in.ID, lookupProjectID)
		if !isAdmin && !ok {
			return errorResult(errNotFound), nil, nil
		}
		if ok {
			enabled = existing.Enabled
		}
	}
	if in.Enabled != nil {
		enabled = *in.Enabled
	}

	row, err := s.engine.UpsertSeverity(ctx, core.SeverityRule{
		ID:        in.ID,
		ProjectID: projectID,
		Service:   in.Service,
		Pattern:   in.Pattern,
		Severity:  severity,
		Enabled:   enabled,
	})
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(renderSeverityRule(row)), nil, nil
}

// ---- delete_severity_rule ----

type deleteSeverityRuleInput struct {
	ID int64 `json:"id" jsonschema:"Severity rule ID"`
}

func (s *Server) deleteSeverityRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in deleteSeverityRuleInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && !s.severityRuleBelongsToProject(ctx, in.ID, callerProjectID) {
		// Row exists but belongs to another project (or doesn't exist at
		// all): "not found" either way — a project-bound key must not
		// learn that another project's rule ID exists.
		return errorResult(errNotFound), nil, nil
	}

	if err := s.engine.DeleteSeverity(ctx, in.ID); err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(fmt.Sprintf("severity rule #%d deleted", in.ID)), nil, nil
}
