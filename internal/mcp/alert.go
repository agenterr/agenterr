package mcp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// This file holds the four alert-rule tools: listing, upserting, deleting,
// and a synchronous test-fire. Split from tools.go for the same reason as
// noise.go. Validation and *bool/*int resolution semantics mirror
// internal/api/handlers/alertrules.go (the REST twin) exactly — same
// rules, different edge.

// defaultAlertCooldownSeconds is the alert semantics doc's documented
// default: applied when upsert_alert_rule omits cooldown_seconds entirely
// on create. Mirrors handlers.defaultCooldownSeconds.
const defaultAlertCooldownSeconds = 900

// maxAlertHeaders and maxAlertHeadersBytes cap a rule's webhook headers so
// an agent (or a malformed client) can't wedge an unbounded map into
// storage or the outbound POST. Mirrors handlers.maxAlertHeaders /
// handlers.maxAlertHeadersBytes.
const (
	maxAlertHeaders      = 32
	maxAlertHeadersBytes = 4096
)

// validAlertKinds mirrors core.AlertRuleKind's three known values (and
// internal/api/handlers.validAlertKinds — same validation, different
// edge).
var validAlertKinds = map[core.AlertRuleKind]bool{
	core.AlertNewIssue:   true,
	core.AlertRegression: true,
	core.AlertThreshold:  true,
}

// registerAlertTools binds the four alert-rule tools. Called from
// registerTools in tools.go.
func (s *Server) registerAlertTools() {
	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "list_alert_rules",
		Description: "List the alert rules configured for a project: kind, scope, threshold/cooldown params, webhook URL, enabled state, and the outcome of the last delivery attempt.",
	}, s.listAlertRules)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "upsert_alert_rule",
		Description: "Create or update an alert rule (new_issue, regression, or threshold) that posts a webhook when matching events occur. Pass id to update an existing rule. Rules evaluate only error/exception events (records that group into issues); min_severity narrows within that set. A rule that's accepted but never test-fired can still be silently broken — always follow up with test_alert_rule to confirm the receiver actually gets it before trusting the rule to fire for real.",
	}, s.upsertAlertRule)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "delete_alert_rule",
		Description: "Remove an alert rule so it stops firing.",
	}, s.deleteAlertRule)

	mcpsdk.AddTool(s.mcp, &mcpsdk.Tool{
		Name:        "test_alert_rule",
		Description: "Send rule id's webhook immediately with a sample issue (test:true in the payload) and report whether delivery succeeded. Run this right after upsert_alert_rule as part of the create -> test-fire -> trust loop, and again any time you change a rule's url or headers.",
	}, s.testAlertRule)
}

// ---- list_alert_rules ----

type listAlertRulesInput struct {
	ProjectID int64 `json:"project_id,omitempty" jsonschema:"Project ID; admin keys only — project-scoped keys always see only their own project. Admin omitting this sees rules across every project."`
}

func (s *Server) listAlertRules(ctx context.Context, _ *mcpsdk.CallToolRequest, in listAlertRulesInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	projectID := in.ProjectID
	if !isAdmin {
		projectID = callerProjectID
	}

	rows, err := s.ar.AlertRules(ctx, projectID)
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}

	slug := ""
	if projectID != 0 {
		if sl, ok := s.projectSlug(ctx, projectID); ok {
			slug = sl
		}
	}
	return textResult(renderAlertRules(rows, slug)), nil, nil
}

// ---- upsert_alert_rule ----

type upsertAlertRuleInput struct {
	ID              int64             `json:"id,omitempty" jsonschema:"Existing rule ID to update; omit to create a new rule"`
	ProjectID       int64             `json:"project_id,omitempty" jsonschema:"Project ID; admin keys only — project-scoped keys always target their own project"`
	Name            string            `json:"name,omitempty" jsonschema:"Human-readable rule name"`
	Kind            string            `json:"kind" jsonschema:"new_issue (fires the first time a fingerprint is seen), regression (fires when a resolved issue reopens), or threshold (fires when N matching events land within a sliding window)"`
	Service         string            `json:"service,omitempty" jsonschema:"Service name to match; omit to match any service"`
	Environment     string            `json:"environment,omitempty" jsonschema:"Environment to match; omit to match any environment"`
	MinSeverity     string            `json:"min_severity,omitempty" jsonschema:"Minimum severity to match (trace, debug, info, warn, error, fatal); omit to match any severity"`
	N               int               `json:"n,omitempty" jsonschema:"Event count threshold (threshold kind only; must be >= 1)"`
	WindowMinutes   int               `json:"window_minutes,omitempty" jsonschema:"Sliding window in minutes (threshold kind only; must be >= 1)"`
	CooldownSeconds *int              `json:"cooldown_seconds,omitempty" jsonschema:"Seconds of silence after a fire before the rule can fire again; defaults to 900 on create, on update omitted preserves the current value. An explicit 0 disables cooldown (fire every time)."`
	URL             string            `json:"url" jsonschema:"Webhook URL the rule POSTs to when it fires (http or https)"`
	Headers         map[string]string `json:"headers,omitempty" jsonschema:"Static headers sent with the webhook POST"`
	Enabled         *bool             `json:"enabled,omitempty" jsonschema:"Whether the rule is active; defaults to true on create, on update omitted preserves the current value"`
}

// validateAlertRuleInput applies the alert semantics doc's validation,
// which runs on both create and update: kind known; url parses as http or
// https; threshold requires n>=1 and window_minutes>=1; min_severity a
// strict severity name when present; headers within the size caps.
// cooldown_seconds is validated separately (validateAlertCooldown) once
// its create-default/update-preserve value has been resolved. Mirrors
// handlers.validateAlertRuleBody.
func validateAlertRuleInput(in upsertAlertRuleInput, kind core.AlertRuleKind) error {
	if !validAlertKinds[kind] {
		return errors.New("kind: must be new_issue, regression, or threshold")
	}
	if in.URL == "" {
		return errors.New("url: required")
	}
	u, err := url.Parse(in.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("url: must be a valid http or https URL")
	}
	if kind == core.AlertThreshold {
		if in.N < 1 {
			return errors.New("n: must be >= 1 for threshold")
		}
		if in.WindowMinutes < 1 {
			return errors.New("window_minutes: must be >= 1 for threshold")
		}
	}
	// ParseSeverityStrict accepts "" as "not supplied" (-> SeverityTrace,
	// meaning "any"), so this rejects only genuine typos.
	if _, ok := core.ParseSeverityStrict(in.MinSeverity); !ok {
		return errors.New("min_severity: unknown value")
	}
	return validateAlertHeaders(in.Headers)
}

// validateAlertCooldown checks the already-resolved (default-filled or
// preserved) cooldown value — never the raw wire pointer, since nil isn't
// a value to validate. Mirrors handlers.validateCooldownSeconds.
func validateAlertCooldown(n int) error {
	if n < 0 {
		return errors.New("cooldown_seconds: must be >= 0")
	}
	return nil
}

// validateAlertHeaders rejects header maps that could wedge an unbounded
// payload into storage or the outbound webhook POST. Mirrors
// handlers.validateAlertHeaders.
func validateAlertHeaders(h map[string]string) error {
	if len(h) > maxAlertHeaders {
		return fmt.Errorf("headers: too many entries (max %d)", maxAlertHeaders)
	}
	var total int
	for k, v := range h {
		total += len(k) + len(v)
	}
	if total > maxAlertHeadersBytes {
		return fmt.Errorf("headers: too large (max %d bytes total)", maxAlertHeadersBytes)
	}
	return nil
}

// findAlertRule looks up ruleID among projectID's rules (projectID 0 = all
// projects — used for admin lookups that aren't scoped to one project).
// Mirrors internal/api/handlers.findAlertRule.
func (s *Server) findAlertRule(ctx context.Context, ruleID, projectID int64) (store.AlertRuleRow, bool) {
	rows, err := s.ar.AlertRules(ctx, projectID)
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
// rules. Mirrors internal/api/handlers.alertRuleBelongsToProject.
func (s *Server) alertRuleBelongsToProject(ctx context.Context, ruleID, projectID int64) bool {
	_, ok := s.findAlertRule(ctx, ruleID, projectID)
	return ok
}

func (s *Server) upsertAlertRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in upsertAlertRuleInput) (*mcpsdk.CallToolResult, any, error) {
	kind := core.AlertRuleKind(in.Kind)
	if err := validateAlertRuleInput(in, kind); err != nil {
		return errorResult(err), nil, nil
	}

	callerProjectID, isAdmin := callerScope(ctx)
	projectID := in.ProjectID
	if !isAdmin {
		projectID = callerProjectID
	} else if projectID == 0 {
		return errorResult(errors.New("project_id: required for admin keys")), nil, nil
	}

	// Already validated above; ok is guaranteed true here.
	minSeverity, _ := core.ParseSeverityStrict(in.MinSeverity)

	// enabled/cooldownSeconds each start at their create default (true /
	// defaultAlertCooldownSeconds) or, for an update, the rule's current
	// state — either is overridden below if the caller supplied an
	// explicit value. Both follow the same *T-pointer contract: nil is
	// distinguishable from an explicit zero value.
	enabled := true
	cooldownSeconds := defaultAlertCooldownSeconds
	if in.ID != 0 {
		// Updating an existing rule: fetch it both to verify ownership
		// (non-admin — otherwise a project-bound key could hijack another
		// project's rule by ID) and to seed enabled/cooldownSeconds'
		// preserve-on-omit defaults. Admins aren't scoped to one project,
		// so their lookup spans all projects (projectID 0); a miss here
		// still falls through to Upsert's own not-found handling.
		lookupProjectID := callerProjectID
		if isAdmin {
			lookupProjectID = 0
		}
		existing, ok := s.findAlertRule(ctx, in.ID, lookupProjectID)
		if !isAdmin && !ok {
			return errorResult(errNotFound), nil, nil
		}
		if ok {
			enabled = existing.Enabled
			cooldownSeconds = existing.CooldownSeconds
		}
	}
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	if in.CooldownSeconds != nil {
		cooldownSeconds = *in.CooldownSeconds
	}
	if err := validateAlertCooldown(cooldownSeconds); err != nil {
		return errorResult(err), nil, nil
	}

	row, err := s.alertsEngine.Upsert(ctx, core.AlertRule{
		ID:              in.ID,
		ProjectID:       projectID,
		Name:            in.Name,
		Kind:            kind,
		Service:         in.Service,
		Environment:     in.Environment,
		MinSeverity:     minSeverity,
		N:               in.N,
		WindowMinutes:   in.WindowMinutes,
		CooldownSeconds: cooldownSeconds,
		URL:             in.URL,
		Headers:         in.Headers,
		Enabled:         enabled,
	})
	if err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(renderAlertRule(row)), nil, nil
}

// ---- delete_alert_rule ----

type deleteAlertRuleInput struct {
	ID int64 `json:"id" jsonschema:"Alert rule ID"`
}

func (s *Server) deleteAlertRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in deleteAlertRuleInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && !s.alertRuleBelongsToProject(ctx, in.ID, callerProjectID) {
		// Row exists but belongs to another project (or doesn't exist at
		// all): "not found" either way — a project-bound key must not
		// learn that another project's rule ID exists.
		return errorResult(errNotFound), nil, nil
	}

	if err := s.alertsEngine.Delete(ctx, in.ID); err != nil {
		return errorResult(toolErr(err)), nil, nil
	}
	return textResult(fmt.Sprintf("alert rule #%d deleted", in.ID)), nil, nil
}

// ---- test_alert_rule ----

type testAlertRuleInput struct {
	ID int64 `json:"id" jsonschema:"Alert rule ID"`
}

// testAlertRule fires rule id's webhook synchronously with a sample issue
// and reports the delivery outcome directly, mirroring
// handlers.AlertRules.Test. The delivery error text (e.g. "webhook
// returned status 500") is deliberately surfaced verbatim rather than
// routed through toolErr — it's TestFire's own diagnostic, not a store
// error that could leak internals.
func (s *Server) testAlertRule(ctx context.Context, _ *mcpsdk.CallToolRequest, in testAlertRuleInput) (*mcpsdk.CallToolResult, any, error) {
	callerProjectID, isAdmin := callerScope(ctx)
	if !isAdmin && !s.alertRuleBelongsToProject(ctx, in.ID, callerProjectID) {
		return errorResult(errNotFound), nil, nil
	}

	if err := s.alertsEngine.TestFire(ctx, in.ID); err != nil {
		return errorResult(err), nil, nil
	}
	return textResult(fmt.Sprintf("alert rule #%d test-fired: delivered", in.ID)), nil, nil
}

// ---- rendering ----

// alertRuleLine renders one alert rule as a single compact line: scope
// (service/environment/min_severity), threshold params when the kind uses
// them, cooldown, destination URL, enabled state, and the last delivery
// outcome.
func alertRuleLine(row store.AlertRuleRow) string {
	scope := fmt.Sprintf("service=%s environment=%s min_severity=%s",
		svcOrAny(row.Service), svcOrAny(row.Environment), strings.ToLower(row.MinSeverity.String()))

	var kindParams string
	if row.Kind == core.AlertThreshold {
		kindParams = fmt.Sprintf(" n=%d window_minutes=%d", row.N, row.WindowMinutes)
	}

	last := "never fired"
	if !row.LastFired.IsZero() {
		outcome := "ok"
		if row.LastError != "" {
			outcome = "error=" + row.LastError
		}
		last = fmt.Sprintf("%s %s", formatRFC3339(row.LastFired), outcome)
	}

	return fmt.Sprintf("#%d %q %s %s%s cooldown=%ds url=%s enabled=%v last_fired=%s",
		row.ID, row.Name, row.Kind, scope, kindParams, row.CooldownSeconds, row.URL, row.Enabled, last)
}

func renderAlertRules(rows []store.AlertRuleRow, projectSlug string) string {
	scope := ""
	if projectSlug != "" {
		scope = " in " + projectSlug
	}
	lines := make([]string, 0, len(rows)+1)
	lines = append(lines, fmt.Sprintf("%d alert rules%s:", len(rows), scope))
	for _, r := range rows {
		lines = append(lines, alertRuleLine(r))
	}
	return strings.Join(lines, "\n")
}

func renderAlertRule(row store.AlertRuleRow) string {
	return "rule " + alertRuleLine(row)
}
