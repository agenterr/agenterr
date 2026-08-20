package sqlite_test

import (
	"context"
	"strings"
	"testing"

	"github.com/agenterr/agenterr/internal/core"
)

func TestSeverityRulesInvalidRegex(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Try to insert a rule with an invalid regex pattern
	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `[invalid(regex`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	_, err = db.UpsertSeverityRule(ctx, rule)
	if err == nil {
		t.Fatal("UpsertSeverityRule with invalid regex should error")
	}
	// We expect an error about the regex pattern
	if !strings.Contains(err.Error(), "severity rule pattern") {
		t.Fatalf("UpsertSeverityRule invalid regex error = %v, want pattern error", err)
	}
}

func TestSeverityRulesEmptyPattern(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   "",
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	_, err = db.UpsertSeverityRule(ctx, rule)
	if err == nil {
		t.Fatal("UpsertSeverityRule with empty pattern should error")
	}
	if err.Error() != "sqlite: severity rule pattern cannot be empty" {
		t.Fatalf("UpsertSeverityRule empty pattern error = %v, want empty pattern error", err)
	}
}

func TestSeverityRulesSeverityTooLow(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Try to insert a rule with severity <= SeverityInfo
	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityInfo,
		Enabled:   true,
	}
	_, err = db.UpsertSeverityRule(ctx, rule)
	if err == nil {
		t.Fatal("UpsertSeverityRule with severity <= info should error")
	}
	if err.Error() != "sqlite: severity rule must lift above info" {
		t.Fatalf("UpsertSeverityRule low severity error = %v, want lift above info error", err)
	}
}

func TestSeverityRulesSoftCap(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	var lastID int64
	for i := 0; i < 100; i++ {
		row, err := db.UpsertSeverityRule(ctx, core.SeverityRule{
			ProjectID: proj.ID,
			Pattern:   "pattern",
			Severity:  core.SeverityError,
			Enabled:   true,
		})
		if err != nil {
			t.Fatalf("UpsertSeverityRule rule %d: %v", i, err)
		}
		lastID = row.ID
	}

	// 101st new rule (ID==0) is rejected — at the cap.
	_, err = db.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: proj.ID,
		Pattern:   "one too many",
		Severity:  core.SeverityError,
		Enabled:   true,
	})
	if err == nil {
		t.Fatal("UpsertSeverityRule for the 101st new rule should error")
	}
	if err.Error() != "sqlite: project has 100 severity rules — delete unused rules first" {
		t.Fatalf("UpsertSeverityRule cap error = %v, want soft-cap error", err)
	}

	// Updating an existing rule is always allowed, even while at the cap.
	if _, err := db.UpsertSeverityRule(ctx, core.SeverityRule{
		ID:        lastID,
		ProjectID: proj.ID,
		Pattern:   "updated pattern",
		Severity:  core.SeverityWarn,
		Enabled:   true,
	}); err != nil {
		t.Fatalf("UpsertSeverityRule update at cap should not error: %v", err)
	}

	// A different project starts its own count from zero.
	proj2, err := db.CreateProject(ctx, "other-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if _, err := db.UpsertSeverityRule(ctx, core.SeverityRule{
		ProjectID: proj2.ID,
		Pattern:   "fine",
		Severity:  core.SeverityError,
		Enabled:   true,
	}); err != nil {
		t.Fatalf("UpsertSeverityRule for a different project should not error: %v", err)
	}
}
