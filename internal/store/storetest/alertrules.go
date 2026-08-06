// Alert rules contract tests. Mirrors the NoiseRules section in suite.go —
// kept in its own file because suite.go is already long.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

func testAlertRulesUpsertInsertReturnsIDAndDefaults(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertAlertRule(ctx, core.AlertRule{
		ProjectID: p.ID,
		Name:      "new issues",
		Kind:      core.AlertNewIssue,
		Service:   "web",
		URL:       "https://example.com/hook",
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertAlertRule: %v", err)
	}
	if row.ID == 0 {
		t.Fatalf("expected non-zero ID")
	}
	if !row.LastFired.IsZero() {
		t.Errorf("LastFired = %v, want zero", row.LastFired)
	}
	if row.LastError != "" {
		t.Errorf("LastError = %q, want empty", row.LastError)
	}
	if row.CreatedAt.IsZero() {
		t.Errorf("expected non-zero CreatedAt")
	}
	if row.ProjectID != p.ID || row.Name != "new issues" || row.Kind != core.AlertNewIssue || row.Service != "web" || row.URL != "https://example.com/hook" || !row.Enabled {
		t.Errorf("row = %+v, unexpected values", row)
	}
	if row.Headers != nil && len(row.Headers) != 0 {
		t.Errorf("Headers = %v, want nil or empty for unset input", row.Headers)
	}
}

func testAlertRulesUpsertUpdateRoundTripsFields(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	inserted, err := s.UpsertAlertRule(ctx, core.AlertRule{
		ProjectID:       p.ID,
		Name:            "threshold rule",
		Kind:            core.AlertThreshold,
		Service:         "web",
		Environment:     "prod",
		MinSeverity:     core.SeverityWarn,
		N:               5,
		WindowMinutes:   10,
		CooldownSeconds: 300,
		URL:             "https://example.com/a",
		Headers:         map[string]string{"X-Token": "abc"},
		Enabled:         true,
	})
	if err != nil {
		t.Fatalf("UpsertAlertRule insert: %v", err)
	}

	updated, err := s.UpsertAlertRule(ctx, core.AlertRule{
		ID:              inserted.ID,
		ProjectID:       p.ID,
		Name:            "regression rule",
		Kind:            core.AlertRegression,
		Service:         "api",
		Environment:     "staging",
		MinSeverity:     core.SeverityError,
		N:               7,
		WindowMinutes:   3,
		CooldownSeconds: 900,
		URL:             "https://example.com/b",
		Headers:         map[string]string{"X-Token": "def", "Y": "z"},
		Enabled:         false,
	})
	if err != nil {
		t.Fatalf("UpsertAlertRule update: %v", err)
	}
	if updated.ID != inserted.ID {
		t.Errorf("ID = %d, want %d (update must not change ID)", updated.ID, inserted.ID)
	}
	if updated.Name != "regression rule" || updated.Kind != core.AlertRegression || updated.Service != "api" ||
		updated.Environment != "staging" || updated.MinSeverity != core.SeverityError ||
		updated.N != 7 || updated.WindowMinutes != 3 ||
		updated.CooldownSeconds != 900 || updated.URL != "https://example.com/b" || updated.Enabled {
		t.Errorf("updated row = %+v, unexpected values", updated)
	}
	if len(updated.Headers) != 2 || updated.Headers["X-Token"] != "def" || updated.Headers["Y"] != "z" {
		t.Errorf("updated Headers = %v, want exact round-trip", updated.Headers)
	}

	rows, err := s.AlertRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rows))
	}
	if rows[0].Name != "regression rule" || rows[0].Kind != core.AlertRegression || rows[0].Enabled {
		t.Errorf("persisted row = %+v, want updated values", rows[0])
	}
	if rows[0].N != 7 || rows[0].WindowMinutes != 3 {
		t.Errorf("persisted row N/WindowMinutes = %d/%d, want 7/3", rows[0].N, rows[0].WindowMinutes)
	}
	if len(rows[0].Headers) != 2 || rows[0].Headers["X-Token"] != "def" {
		t.Errorf("persisted Headers = %v, want exact round-trip", rows[0].Headers)
	}
}

func testAlertRulesUpsertUpdateMissingIDNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	_, err := s.UpsertAlertRule(ctx, core.AlertRule{ID: 999999, ProjectID: p.ID, Kind: core.AlertNewIssue, URL: "https://example.com"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpsertAlertRule(missing ID) err = %v, want ErrNotFound", err)
	}
}

func testAlertRulesUpsertUnknownKindRejected(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	_, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p.ID, Kind: core.AlertRuleKind("bogus")})
	if err == nil {
		t.Fatalf("UpsertAlertRule(unknown kind) err = nil, want error")
	}
	if errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpsertAlertRule(unknown kind) err = ErrNotFound, want a validation error")
	}
}

func testAlertRulesDeleteThenNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p.ID, Kind: core.AlertNewIssue, URL: "https://example.com"})
	if err != nil {
		t.Fatalf("UpsertAlertRule: %v", err)
	}

	if err := s.DeleteAlertRule(ctx, row.ID); err != nil {
		t.Fatalf("DeleteAlertRule: %v", err)
	}

	rows, err := s.AlertRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rules after delete, got %d", len(rows))
	}

	err = s.DeleteAlertRule(ctx, row.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteAlertRule(already deleted) err = %v, want ErrNotFound", err)
	}
}

func testAlertRulesListScopedByProjectAndOrdered(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2, err := s.CreateProject(ctx, "second-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	r1, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p1.ID, Kind: core.AlertNewIssue, URL: "https://example.com/1"})
	if err != nil {
		t.Fatalf("UpsertAlertRule #1: %v", err)
	}
	r2, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p1.ID, Kind: core.AlertRegression, URL: "https://example.com/2"})
	if err != nil {
		t.Fatalf("UpsertAlertRule #2: %v", err)
	}
	if _, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p2.ID, Kind: core.AlertNewIssue, URL: "https://example.com/3"}); err != nil {
		t.Fatalf("UpsertAlertRule #3: %v", err)
	}

	rows, err := s.AlertRules(ctx, p1.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rules for p1, got %d", len(rows))
	}
	if rows[0].ID != r1.ID || rows[1].ID != r2.ID {
		t.Errorf("rows = %+v, want ascending ID order [%d, %d]", rows, r1.ID, r2.ID)
	}
}

func testAlertRulesListAllProjects(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2, err := s.CreateProject(ctx, "second-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	if _, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p1.ID, Kind: core.AlertNewIssue, URL: "https://example.com/1"}); err != nil {
		t.Fatalf("UpsertAlertRule #1: %v", err)
	}
	if _, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p2.ID, Kind: core.AlertNewIssue, URL: "https://example.com/2"}); err != nil {
		t.Fatalf("UpsertAlertRule #2: %v", err)
	}

	rows, err := s.AlertRules(ctx, 0)
	if err != nil {
		t.Fatalf("AlertRules(0): %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rules across all projects, got %d", len(rows))
	}
}

func testAlertRulesRecordAlertResultSetsAndClearsFields(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertAlertRule(ctx, core.AlertRule{ProjectID: p.ID, Kind: core.AlertNewIssue, URL: "https://example.com"})
	if err != nil {
		t.Fatalf("UpsertAlertRule: %v", err)
	}

	firedAt := baseTime.Add(5 * time.Minute)
	if err := s.RecordAlertResult(ctx, row.ID, firedAt, "connection refused"); err != nil {
		t.Fatalf("RecordAlertResult (failure): %v", err)
	}

	rows, err := s.AlertRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rows))
	}
	if !rows[0].LastFired.Equal(firedAt) {
		t.Errorf("LastFired = %v, want %v", rows[0].LastFired, firedAt)
	}
	if rows[0].LastError != "connection refused" {
		t.Errorf("LastError = %q, want %q", rows[0].LastError, "connection refused")
	}

	firedAt2 := baseTime.Add(10 * time.Minute)
	if err := s.RecordAlertResult(ctx, row.ID, firedAt2, ""); err != nil {
		t.Fatalf("RecordAlertResult (success): %v", err)
	}
	rows, err = s.AlertRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if !rows[0].LastFired.Equal(firedAt2) {
		t.Errorf("LastFired = %v, want %v", rows[0].LastFired, firedAt2)
	}
	if rows[0].LastError != "" {
		t.Errorf("LastError = %q, want empty after success", rows[0].LastError)
	}
}

func testAlertRulesRecordAlertResultMissingIDNoOp(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)

	if err := s.RecordAlertResult(ctx, 999999, baseTime, "boom"); err != nil {
		t.Fatalf("RecordAlertResult(missing ID) err = %v, want nil (silent no-op)", err)
	}
}

func testAlertRulesSeverityRoundTripsLowercase(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertAlertRule(ctx, core.AlertRule{
		ProjectID:   p.ID,
		Kind:        core.AlertNewIssue,
		URL:         "https://example.com",
		MinSeverity: core.SeverityWarn,
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("UpsertAlertRule: %v", err)
	}
	if row.MinSeverity != core.SeverityWarn {
		t.Errorf("returned row MinSeverity = %v, want %v", row.MinSeverity, core.SeverityWarn)
	}

	rows, err := s.AlertRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if len(rows) != 1 || rows[0].MinSeverity != core.SeverityWarn {
		t.Fatalf("rows = %+v, want single row with MinSeverity=%v", rows, core.SeverityWarn)
	}
}

func testAlertRulesHeadersNilRoundTrips(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertAlertRule(ctx, core.AlertRule{
		ProjectID: p.ID,
		Kind:      core.AlertNewIssue,
		URL:       "https://example.com",
		Headers:   nil,
	})
	if err != nil {
		t.Fatalf("UpsertAlertRule: %v", err)
	}
	if len(row.Headers) != 0 {
		t.Errorf("Headers = %v, want empty for nil input", row.Headers)
	}

	rows, err := s.AlertRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("AlertRules: %v", err)
	}
	if len(rows) != 1 || len(rows[0].Headers) != 0 {
		t.Fatalf("rows = %+v, want single row with empty Headers", rows)
	}
}
