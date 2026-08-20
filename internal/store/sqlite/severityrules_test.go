package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

func TestSeverityRulesRoundTrip(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create a project to hold the rules
	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Upsert a new severity rule (ID = 0)
	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `panic|fatal`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	stored, err := db.UpsertSeverityRule(ctx, rule)
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}

	// Verify the stored row has ID, CreatedAt, and LiftedCount set
	if stored.ID == 0 {
		t.Fatal("UpsertSeverityRule returned zero ID for new rule")
	}
	if stored.CreatedAt.IsZero() {
		t.Fatal("UpsertSeverityRule returned zero CreatedAt")
	}
	if stored.LiftedCount != 0 {
		t.Fatalf("new rule LiftedCount = %d, want 0", stored.LiftedCount)
	}

	// Verify the stored values match what was inserted
	if stored.ProjectID != proj.ID {
		t.Fatalf("stored.ProjectID = %d, want %d", stored.ProjectID, proj.ID)
	}
	if stored.Service != rule.Service {
		t.Fatalf("stored.Service = %q, want %q", stored.Service, rule.Service)
	}
	if stored.Pattern != rule.Pattern {
		t.Fatalf("stored.Pattern = %q, want %q", stored.Pattern, rule.Pattern)
	}
	if stored.Severity != rule.Severity {
		t.Fatalf("stored.Severity = %d, want %d", stored.Severity, rule.Severity)
	}
	if !stored.Enabled {
		t.Fatal("stored.Enabled = false, want true")
	}

	// Verify the rule is retrievable by ID
	retrieved, err := db.SeverityRules(ctx, proj.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(retrieved) != 1 {
		t.Fatalf("SeverityRules returned %d rules, want 1", len(retrieved))
	}
	if retrieved[0].ID != stored.ID {
		t.Fatalf("retrieved ID = %d, want %d", retrieved[0].ID, stored.ID)
	}
}

func TestSeverityRulesUpsertUpdate(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Create initial rule
	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	}
	stored, err := db.UpsertSeverityRule(ctx, rule)
	if err != nil {
		t.Fatalf("UpsertSeverityRule (insert): %v", err)
	}
	ruleID := stored.ID

	// Update the rule by ID
	updated := core.SeverityRule{
		ID:        ruleID,
		ProjectID: proj.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityError,
		Enabled:   false,
	}
	updatedStored, err := db.UpsertSeverityRule(ctx, updated)
	if err != nil {
		t.Fatalf("UpsertSeverityRule (update): %v", err)
	}

	// Verify the update took effect
	if updatedStored.Service != "web" {
		t.Fatalf("after update, Service = %q, want web", updatedStored.Service)
	}
	if updatedStored.Pattern != "timeout" {
		t.Fatalf("after update, Pattern = %q, want timeout", updatedStored.Pattern)
	}
	if updatedStored.Severity != core.SeverityError {
		t.Fatalf("after update, Severity = %d, want %d", updatedStored.Severity, core.SeverityError)
	}
	if updatedStored.Enabled {
		t.Fatal("after update, Enabled = true, want false")
	}
}

func TestSeverityRulesDeleteMissing(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	err := db.DeleteSeverityRule(ctx, 99999)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteSeverityRule(missing) err = %v, want store.ErrNotFound", err)
	}
}

func TestSeverityRulesDelete(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	stored, err := db.UpsertSeverityRule(ctx, rule)
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}
	ruleID := stored.ID

	// Delete the rule
	if err := db.DeleteSeverityRule(ctx, ruleID); err != nil {
		t.Fatalf("DeleteSeverityRule: %v", err)
	}

	// Verify it's gone
	retrieved, err := db.SeverityRules(ctx, proj.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(retrieved) != 0 {
		t.Fatalf("after delete, SeverityRules returned %d rules, want 0", len(retrieved))
	}
}

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
	if !contains(err.Error(), "severity rule pattern") {
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

func TestSeverityRulesAddLifts(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Create two rules
	rule1 := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	stored1, err := db.UpsertSeverityRule(ctx, rule1)
	if err != nil {
		t.Fatalf("UpsertSeverityRule 1: %v", err)
	}

	rule2 := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	}
	stored2, err := db.UpsertSeverityRule(ctx, rule2)
	if err != nil {
		t.Fatalf("UpsertSeverityRule 2: %v", err)
	}

	// Add lifts to both rules
	counts := map[int64]int64{
		stored1.ID: 5,
		stored2.ID: 10,
	}
	if err := db.AddSeverityLifts(ctx, counts); err != nil {
		t.Fatalf("AddSeverityLifts: %v", err)
	}

	// Verify the lifts were added
	retrieved, err := db.SeverityRules(ctx, proj.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(retrieved) != 2 {
		t.Fatalf("after AddSeverityLifts, got %d rules, want 2", len(retrieved))
	}

	// Find the rules by ID and verify counts
	for _, r := range retrieved {
		if r.ID == stored1.ID && r.LiftedCount != 5 {
			t.Fatalf("rule 1 LiftedCount = %d, want 5", r.LiftedCount)
		}
		if r.ID == stored2.ID && r.LiftedCount != 10 {
			t.Fatalf("rule 2 LiftedCount = %d, want 10", r.LiftedCount)
		}
	}

	// Add more lifts — they should accumulate
	counts2 := map[int64]int64{
		stored1.ID: 3,
		stored2.ID: 7,
	}
	if err := db.AddSeverityLifts(ctx, counts2); err != nil {
		t.Fatalf("AddSeverityLifts (2nd): %v", err)
	}

	retrieved2, err := db.SeverityRules(ctx, proj.ID)
	if err != nil {
		t.Fatalf("SeverityRules (2nd): %v", err)
	}

	for _, r := range retrieved2 {
		if r.ID == stored1.ID && r.LiftedCount != 8 {
			t.Fatalf("rule 1 LiftedCount after 2nd add = %d, want 8", r.LiftedCount)
		}
		if r.ID == stored2.ID && r.LiftedCount != 17 {
			t.Fatalf("rule 2 LiftedCount after 2nd add = %d, want 17", r.LiftedCount)
		}
	}
}

func TestSeverityRulesAddLiftsUnknownID(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Create one rule
	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	stored, err := db.UpsertSeverityRule(ctx, rule)
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}

	// Try to add lifts for a rule that doesn't exist — should silently skip
	counts := map[int64]int64{
		stored.ID: 5,
		99999:     10, // unknown ID, should be silently skipped
	}
	if err := db.AddSeverityLifts(ctx, counts); err != nil {
		t.Fatalf("AddSeverityLifts with unknown ID: %v", err)
	}

	// Verify the known rule was updated
	retrieved, err := db.SeverityRules(ctx, proj.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(retrieved) != 1 {
		t.Fatalf("got %d rules, want 1", len(retrieved))
	}
	if retrieved[0].LiftedCount != 5 {
		t.Fatalf("rule LiftedCount = %d, want 5", retrieved[0].LiftedCount)
	}
}

func TestSeverityRulesAddLiftsEmptyMap(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// AddSeverityLifts with empty map should be a no-op
	if err := db.AddSeverityLifts(ctx, map[int64]int64{}); err != nil {
		t.Fatalf("AddSeverityLifts(empty): %v", err)
	}
}

func TestSeverityRulesOrderedByID(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// Create multiple rules
	for i := 0; i < 3; i++ {
		rule := core.SeverityRule{
			ProjectID: proj.ID,
			Service:   "api",
			Pattern:   `error\d+`,
			Severity:  core.SeverityError,
			Enabled:   true,
		}
		if _, err := db.UpsertSeverityRule(ctx, rule); err != nil {
			t.Fatalf("UpsertSeverityRule: %v", err)
		}
	}

	// Retrieve rules
	retrieved, err := db.SeverityRules(ctx, proj.ID)
	if err != nil {
		t.Fatalf("SeverityRules: %v", err)
	}
	if len(retrieved) != 3 {
		t.Fatalf("got %d rules, want 3", len(retrieved))
	}

	// Verify they are ordered by ascending ID
	for i := 0; i < len(retrieved)-1; i++ {
		if retrieved[i].ID >= retrieved[i+1].ID {
			t.Fatalf("rules not ordered by ID: %d >= %d", retrieved[i].ID, retrieved[i+1].ID)
		}
	}
}

func TestSeverityRulesAllProjects(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	// Create two projects
	proj1, err := db.CreateProject(ctx, "proj1", 14)
	if err != nil {
		t.Fatalf("CreateProject 1: %v", err)
	}
	proj2, err := db.CreateProject(ctx, "proj2", 14)
	if err != nil {
		t.Fatalf("CreateProject 2: %v", err)
	}

	// Add a rule to each project
	rule1 := core.SeverityRule{
		ProjectID: proj1.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	if _, err := db.UpsertSeverityRule(ctx, rule1); err != nil {
		t.Fatalf("UpsertSeverityRule 1: %v", err)
	}

	rule2 := core.SeverityRule{
		ProjectID: proj2.ID,
		Service:   "web",
		Pattern:   `timeout`,
		Severity:  core.SeverityWarn,
		Enabled:   true,
	}
	if _, err := db.UpsertSeverityRule(ctx, rule2); err != nil {
		t.Fatalf("UpsertSeverityRule 2: %v", err)
	}

	// Query for projectID 0 (all projects)
	all, err := db.SeverityRules(ctx, 0)
	if err != nil {
		t.Fatalf("SeverityRules(0): %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("SeverityRules(0) returned %d rules, want 2", len(all))
	}

	// Query for specific project
	proj1Rules, err := db.SeverityRules(ctx, proj1.ID)
	if err != nil {
		t.Fatalf("SeverityRules(proj1): %v", err)
	}
	if len(proj1Rules) != 1 {
		t.Fatalf("SeverityRules(proj1) returned %d rules, want 1", len(proj1Rules))
	}
}

func TestSeverityRulesTimeStamp(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	proj, err := db.CreateProject(ctx, "test-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	before := time.Now().UTC()
	rule := core.SeverityRule{
		ProjectID: proj.ID,
		Service:   "api",
		Pattern:   `error`,
		Severity:  core.SeverityError,
		Enabled:   true,
	}
	stored, err := db.UpsertSeverityRule(ctx, rule)
	if err != nil {
		t.Fatalf("UpsertSeverityRule: %v", err)
	}
	after := time.Now().UTC()

	// Verify CreatedAt is within the time window
	if stored.CreatedAt.Before(before.Add(-time.Second)) || stored.CreatedAt.After(after.Add(time.Second)) {
		t.Fatalf("CreatedAt = %v, not within [%v, %v]", stored.CreatedAt, before, after)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
