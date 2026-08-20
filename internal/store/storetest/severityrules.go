// Severity rules contract tests. Mirrors the NoiseRules section in suite.go —
// kept in its own file because suite.go is already long.
//
// The package comment lives on suite.go; this file's leading comment above
// is deliberately separated from the package clause (blank line) so it
// isn't mistaken for a second, malformed package comment.

package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

func testSeverityRulesUpsertInsertReturnsIDAndDefaults(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p.ID,
		Service:   "api",
		Pattern:   `panic|fatal`,
		Severity:  core.SeverityError,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}
	if row.ID == 0 {
		t.Fatalf("expected non-zero ID")
	}
	if row.LiftedCount != 0 {
		t.Errorf("LiftedCount = %d, want 0", row.LiftedCount)
	}
	if row.CreatedAt.IsZero() {
		t.Errorf("expected non-zero CreatedAt")
	}
	if row.ProjectID != p.ID || row.Service != "api" || row.Pattern != `panic|fatal` || row.Severity != core.SeverityError || !row.Enabled {
		t.Errorf("row = %+v, unexpected values", row)
	}
}

func testSeverityRulesUpsertUpdateRoundTripsFields(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	inserted, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule insert: %v", err)
	}

	updated, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ID:        inserted.ID,
		ProjectID: p.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityError,
		Enabled:   false,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule update: %v", err)
	}
	if updated.ID != inserted.ID {
		t.Errorf("ID = %d, want %d (update must not change ID)", updated.ID, inserted.ID)
	}
	if updated.Service != "web" {
		t.Errorf("Service = %q, want %q", updated.Service, "web")
	}
	if updated.Pattern != "timeout" {
		t.Errorf("Pattern = %q, want %q", updated.Pattern, "timeout")
	}
	if updated.Severity != core.SeverityError {
		t.Errorf("Severity = %v, want %v", updated.Severity, core.SeverityError)
	}
	if updated.Enabled {
		t.Errorf("Enabled = true, want false")
	}

	rows, err := s.SeverityRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rows))
	}
	if rows[0].Service != "web" || rows[0].Pattern != "timeout" || rows[0].Severity != core.SeverityError || rows[0].Enabled {
		t.Errorf("persisted row = %+v, want updated values", rows[0])
	}
}

func testSeverityRulesUpsertUpdateMissingIDNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	_, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ID:        999999,
		ProjectID: p.ID,
		Pattern:   `error`,
		Severity:  core.SeverityError,
	})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpsertSeverityRule(missing ID) err = %v, want ErrNotFound", err)
	}
}

func testSeverityRulesDeleteThenNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}

	if err := s.DeleteSeverityRule(ctx, row.ID); err != nil {
		t.Fatalf("DeleteSeverityRule: %v", err)
	}

	err = s.DeleteSeverityRule(ctx, row.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteSeverityRule(missing ID) err = %v, want ErrNotFound", err)
	}

	rows, err := s.SeverityRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rules after delete, got %d", len(rows))
	}
}

func testSeverityRulesListScopedByProjectAndOrdered(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2 := mustProject(ctx, t, s)

	// Add rules to p1
	for i := 0; i < 3; i++ {
		_, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
			ProjectID: p1.ID,
			Service:   "api",
			Pattern:   `error\d+`,
			Severity:  core.SeverityError,
			Enabled:   true,
		})
		if err != nil {
			t.Fatalf("UpsertSeverityRule: %v", err)
		}
	}

	// Add one rule to p2
	_, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p2.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}

	// Query p1 — should get 3 rules ordered by ID
	p1Rows, err := s.SeverityRules(ctx, p1.ID)
	if err != nil {
		t.Fatalf("SeverityRules(p1): %v", err)
	}
	if len(p1Rows) != 3 {
		t.Fatalf("expected 3 rules for p1, got %d", len(p1Rows))
	}
	for i := 0; i < len(p1Rows)-1; i++ {
		if p1Rows[i].ID >= p1Rows[i+1].ID {
			t.Errorf("rules not ordered by ID: %d >= %d", p1Rows[i].ID, p1Rows[i+1].ID)
		}
	}

	// Query p2 — should get 1 rule
	p2Rows, err := s.SeverityRules(ctx, p2.ID)
	if err != nil {
		t.Fatalf("SeverityRules(p2): %v", err)
	}
	if len(p2Rows) != 1 {
		t.Fatalf("expected 1 rule for p2, got %d", len(p2Rows))
	}
	if p2Rows[0].Service != "web" {
		t.Errorf("p2 rule Service = %q, want web", p2Rows[0].Service)
	}
}

func testSeverityRulesListAllProjects(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2 := mustProject(ctx, t, s)

	// Add rules to p1 and p2
	_, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p1.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule(p1): %v", err)
	}

	_, err = s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p2.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule(p2): %v", err)
	}

	// Query with projectID 0 (all projects)
	allRows, err := s.SeverityRules(ctx, 0)
	if err != nil {
		t.Fatalf("SeverityRules(0): %v", err)
	}
	if len(allRows) != 2 {
		t.Fatalf("expected 2 rules from all projects, got %d", len(allRows))
	}
	// Verify they're ordered by ID across projects
	for i := 0; i < len(allRows)-1; i++ {
		if allRows[i].ID >= allRows[i+1].ID {
			t.Errorf("rules not ordered by ID: %d >= %d", allRows[i].ID, allRows[i+1].ID)
		}
	}
}

func testSeverityRulesAddLiftsAccumulatesAndSkipsUnknown(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	// Create two rules
	rule1, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule 1: %v", err)
	}

	rule2, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule 2: %v", err)
	}

	// Add lifts to both rules
	if err := s.AddSeverityLifts(ctx, map[int64]int64{rule1.ID: 5, rule2.ID: 10}); err != nil {
		t.Fatalf("AddSeverityLifts: %v", err)
	}

	rows, err := s.SeverityRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rows))
	}

	// Add more lifts with unknown ID — unknown IDs should be silently skipped
	if err := s.AddSeverityLifts(ctx, map[int64]int64{rule1.ID: 3, rule2.ID: 7, 999999: 100}); err != nil {
		t.Fatalf("AddSeverityLifts with unknown ID: %v", err)
	}

	rows2, err := s.SeverityRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("SeverityRules after 2nd add: %v", err)
	}

	// Verify accumulation
	for _, r := range rows2 {
		if r.ID == rule1.ID && r.LiftedCount != 8 {
			t.Errorf("rule1 LiftedCount = %d, want 8", r.LiftedCount)
		}
		if r.ID == rule2.ID && r.LiftedCount != 17 {
			t.Errorf("rule2 LiftedCount = %d, want 17", r.LiftedCount)
		}
	}
}

func testSeverityRulesSeverityRoundTripsLowercase(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	// Severity is stored as lowercase string and round-trips correctly
	row, err := s.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: p.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}

	rows, err := s.SeverityRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rows))
	}

	// Verify severity round-trips correctly
	if rows[0].Severity != core.SeverityError {
		t.Errorf("Severity = %v, want %v", rows[0].Severity, core.SeverityError)
	}
	if row.Severity != core.SeverityError {
		t.Errorf("inserted Severity = %v, want %v", row.Severity, core.SeverityError)
	}
}
