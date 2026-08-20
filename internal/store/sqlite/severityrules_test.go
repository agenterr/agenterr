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
