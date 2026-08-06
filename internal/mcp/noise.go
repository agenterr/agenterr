package mcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// This file holds the five noise-control tools: listing/upserting/deleting
// rules, the noise report, and the parse-bodies toggle. Split from
// tools.go to keep that file under the repo's line-count guideline — the
// registration and handler pattern is identical to the tools there.

// defaultReportHours is get_noise_report's lookback window when the
// caller doesn't supply hours. Mirrors defaultReportHours in
// internal/api/handlers/noiserules.go.
const defaultReportHours = 24

// validNoiseKinds mirrors core.NoiseRuleKind's three known values (and
// internal/api/handlers.validNoiseKinds — same validation, different
// edge).
var validNoiseKinds = map[core.NoiseRuleKind]bool{
	core.NoiseSeverityFloor: true,
	core.NoiseDropMatch:     true,
	core.NoiseSample:        true,
}

// validateNoiseParams rejects create-time params that can never match
// anything for the given kind. A rule that's accepted and stored but never
// fires looks like success, silently breaking the
// see-noise -> add-rule -> verify loop. Mirrors
// internal/api/handlers.validateNoiseParams.
func validateNoiseParams(kind core.NoiseRuleKind, severity, pattern string, n int) error {
	switch kind {
	case core.NoiseSeverityFloor:
		if severity == "" {
			return errors.New("severity: required for severity_floor")
		}
	case core.NoiseDropMatch:
		if pattern == "" {
			return errors.New("pattern: required for drop_match")
		}
	case core.NoiseSample:
		if severity == "" {
			return errors.New("severity: required for sample")
		}
		if n <= 1 {
			return errors.New("n: must be >= 2 for sample")
		}
	}
	return nil
}

// registerNoiseTools binds the five noise-control tools. Called from
// registerTools in tools.go.
func (s *Server) registerNoiseTools() {
	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "list_noise_rules",
		Description: "List the ingest-filtering rules configured for a project, with each rule's kind, params, enabled state, and how many records it has dropped so far.",
	}, s.listNoiseRules)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "upsert_noise_rule",
		Description: "Create or update a noise rule (severity_floor, drop_match, or sample) that stops chatty, low-value logs from being ingested. Pass id to update an existing rule. Follow up with get_noise_report to confirm it's actually dropping the volume you expected.",
	}, s.upsertNoiseRule)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "delete_noise_rule",
		Description: "Remove a noise rule so its matching records start being ingested again.",
	}, s.deleteNoiseRule)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "get_noise_report",
		Description: "Run this when ingest volume looks dominated by low-value logs: shows the top services by log volume alongside every configured rule's drop count, so you can see what's noisy and whether existing rules are handling it. Follow with upsert_noise_rule and verify with a second report.",
	}, s.getNoiseReport)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "set_project_parse",
		Description: "Toggle whether ingest parses structured fields out of a project's log bodies.",
	}, s.setProjectParse)
}

// ---- list_noise_rules ----

type listNoiseRulesInput struct {
	ProjectID int64 `json:"project_id,omitempty" jsonschema:"Project ID; admin keys only — project-scoped keys always see only their own project. Admin omitting this sees rules across every project."`
}

func (s *Server) listNoiseRules(ctx context.Context, _ *mcpsdk.CallToolRequest, in listNoiseRulesInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	projectID := in.ProjectID
	if !isAdmin {
		projectID = callerProjectID
	}

	rows, err := s.nr.NoiseRules(ctx, projectID)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}

	slug := ""
	if projectID != 0 {
		if sl, ok := s.projectSlug(ctx, projectID); ok {
			slug = sl
		}
	}
	return textResult(renderNoiseRules(rows, slug)), nil, nil
}

// ---- upsert_noise_rule ----

type upsertNoiseRuleInput struct {
	ID        int64  `json:"id,omitempty" jsonschema:"Existing rule ID to update; omit to create a new rule"`
	ProjectID int64  `json:"project_id,omitempty" jsonschema:"Project ID; admin keys only — project-scoped keys always target their own project"`
	Kind      string `json:"kind" jsonschema:"severity_floor, drop_match, or sample"`
	Service   string `json:"service,omitempty" jsonschema:"Service name to match; omit to match any service"`
	Severity  string `json:"severity,omitempty" jsonschema:"Severity name (trace, debug, info, warn, error, fatal): the floor for severity_floor, the band ceiling for sample"`
	Pattern   string `json:"pattern,omitempty" jsonschema:"Substring to match in the log body (drop_match only)"`
	N         int    `json:"n,omitempty" jsonschema:"Keep 1 of every N banded records (sample only)"`
	Enabled   *bool  `json:"enabled,omitempty" jsonschema:"Whether the rule is active; defaults to true on create, on update omitted preserves the current value"`
}

// findNoiseRule looks up ruleID among projectID's rules (projectID 0 = all
// projects — used for admin lookups that aren't scoped to one project).
// Mirrors internal/api/handlers.findNoiseRule.
func (s *Server) findNoiseRule(ctx context.Context, ruleID, projectID int64) (store.NoiseRuleRow, bool) {
	rows, err := s.nr.NoiseRules(ctx, projectID)
	if err != nil {
		return store.NoiseRuleRow{}, false
	}
	for _, row := range rows {
		if row.ID == ruleID {
			return row, true
		}
	}
	return store.NoiseRuleRow{}, false
}

// ruleBelongsToProject reports whether ruleID is among projectID's rules.
// Mirrors internal/api/handlers.ruleBelongsToProject.
func (s *Server) ruleBelongsToProject(ctx context.Context, ruleID, projectID int64) bool {
	_, ok := s.findNoiseRule(ctx, ruleID, projectID)
	return ok
}

func (s *Server) upsertNoiseRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in upsertNoiseRuleInput) (*mcpsdk.CallToolResult, any, error) {
	kind := core.NoiseRuleKind(in.Kind)
	if !validNoiseKinds[kind] {
		return errorResult(errors.New("kind: must be severity_floor, drop_match, or sample")), nil, nil
	}
	severity, ok := core.ParseSeverityStrict(in.Severity)
	if !ok {
		return errorResult(errors.New("severity: unknown value")), nil, nil
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
		// through to Upsert's own not-found handling.
		lookupProjectID := callerProjectID
		if isAdmin {
			lookupProjectID = 0
		}
		existing, ok := s.findNoiseRule(ctx, in.ID, lookupProjectID)
		if !isAdmin && !ok {
			return errorResult(errNotFound), nil, nil
		}
		if ok {
			enabled = existing.Enabled
		}
	} else if err := validateNoiseParams(kind, in.Severity, in.Pattern, in.N); err != nil {
		return errorResult(err), nil, nil
	}
	if in.Enabled != nil {
		enabled = *in.Enabled
	}

	row, err := s.engine.Upsert(ctx, core.NoiseRule{
		ID:        in.ID,
		ProjectID: projectID,
		Kind:      kind,
		Service:   in.Service,
		Severity:  severity,
		Pattern:   in.Pattern,
		N:         in.N,
		Enabled:   enabled,
	})
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(renderNoiseRule(row)), nil, nil
}

// ---- delete_noise_rule ----

type deleteNoiseRuleInput struct {
	ID int64 `json:"id" jsonschema:"Noise rule ID"`
}

func (s *Server) deleteNoiseRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in deleteNoiseRuleInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && !s.ruleBelongsToProject(ctx, in.ID, callerProjectID) {
		// Row exists but belongs to another project (or doesn't exist at
		// all): "not found" either way — a project-bound key must not
		// learn that another project's rule ID exists.
		return errorResult(errNotFound), nil, nil
	}

	if err := s.engine.Delete(ctx, in.ID); err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(fmt.Sprintf("noise rule #%d deleted", in.ID)), nil, nil
}

// ---- get_noise_report ----

type getNoiseReportInput struct {
	ProjectID int64 `json:"project_id,omitempty" jsonschema:"Project ID; required for admin keys (ServiceCounts has no all-projects mode), ignored for project-scoped keys"`
	Hours     int   `json:"hours,omitempty" jsonschema:"Lookback window in hours (default 24)"`
}

func (s *Server) getNoiseReport(ctx context.Context, _ *mcpsdk.CallToolRequest, in getNoiseReportInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	var projectID int64
	if isAdmin {
		if in.ProjectID == 0 {
			return errorResult(errors.New("project_id: required for admin keys")), nil, nil
		}
		projectID = in.ProjectID
	} else {
		projectID = callerProjectID
	}

	hours := in.Hours
	if hours <= 0 {
		hours = defaultReportHours
	}
	since := s.clock().Add(-time.Duration(hours) * time.Hour)

	topServices, err := s.reader.ServiceCounts(ctx, projectID, since)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	ruleRows, err := s.nr.NoiseRules(ctx, projectID)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}

	var totalDropped int64
	for _, r := range ruleRows {
		totalDropped += r.DroppedCount
	}

	return textResult(renderNoiseReport(topServices, ruleRows, totalDropped, fmt.Sprintf("%dh", hours))), nil, nil
}

// ---- set_project_parse ----

type setProjectParseInput struct {
	ProjectID   int64 `json:"project_id" jsonschema:"Project ID"`
	ParseBodies bool  `json:"parse_bodies" jsonschema:"Whether ingest parses structured fields out of this project's log bodies"`
}

func (s *Server) setProjectParse(ctx context.Context, _ *mcpsdk.CallToolRequest, in setProjectParseInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	projectID := in.ProjectID
	if !isAdmin {
		projectID = callerProjectID
	}

	projects, err := s.admin.Projects(ctx)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	found := false
	for _, p := range projects {
		if p.ID == projectID {
			found = true
			break
		}
	}
	if !found {
		return errorResult(errNotFound), nil, nil
	}

	if err := s.engine.SetParseBodies(ctx, projectID, in.ParseBodies); err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(fmt.Sprintf("project #%d parse_bodies=%v", projectID, in.ParseBodies)), nil, nil
}
