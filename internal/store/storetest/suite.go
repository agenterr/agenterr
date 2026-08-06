// Package storetest defines the behavioral contract every store.Store
// implementation must satisfy. Run it against a fresh store instance from
// each implementation's test package: storetest.Run(t, func(t *testing.T)
// store.Store { ... }).
package storetest

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// baseTime anchors every timestamp used in the suite so ordering and
// day-bucketing assertions are deterministic instead of racing time.Now().
var baseTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// Run executes the full behavioral contract suite against a store built by
// open. open must return a fresh, empty store for every subtest.
func Run(t *testing.T, open func(t *testing.T) store.Store) {
	t.Run("WriteBatch/ReadBack", func(t *testing.T) { testWriteBatchReadBack(t, open) })
	t.Run("WriteBatch/CreatesOpenIssue", func(t *testing.T) { testWriteBatchCreatesOpenIssue(t, open) })
	t.Run("WriteBatch/IncrementsExisting", func(t *testing.T) { testWriteBatchIncrementsExisting(t, open) })
	t.Run("WriteBatch/RegressionReopens", func(t *testing.T) { testWriteBatchRegressionReopens(t, open) })
	t.Run("WriteBatch/EventSampleCap", func(t *testing.T) { testWriteBatchEventSampleCap(t, open) })
	t.Run("Issues/FilterByStatusEnvProject", func(t *testing.T) { testIssuesFilterByStatusEnvProject(t, open) })
	t.Run("Issues/SortedByLastSeenDesc", func(t *testing.T) { testIssuesSortedByLastSeenDesc(t, open) })
	t.Run("Issues/LimitHonored", func(t *testing.T) { testIssuesLimitHonored(t, open) })
	t.Run("Issues/DefaultLimitFifty", func(t *testing.T) { testIssuesDefaultLimitFifty(t, open) })
	t.Run("Issue/NotFound", func(t *testing.T) { testIssueNotFound(t, open) })
	t.Run("SearchLogs/FTSMatchesBody", func(t *testing.T) { testSearchLogsFTSMatchesBody(t, open) })
	t.Run("SearchLogs/MinSeverityAndTimeRange", func(t *testing.T) { testSearchLogsMinSeverityAndTimeRange(t, open) })
	t.Run("LogContext/ReturnsNeighborsSameProjectService", func(t *testing.T) { testLogContextReturnsNeighborsSameProjectService(t, open) })
	t.Run("Stats/CountsAndPerDay", func(t *testing.T) { testStatsCountsAndPerDay(t, open) })
	t.Run("Prune/DeletesOnlyOlderAndOnlyProject", func(t *testing.T) { testPruneDeletesOnlyOlderAndOnlyProject(t, open) })
	t.Run("Admin/ProjectCRUDAndKeys", func(t *testing.T) { testAdminProjectCRUDAndKeys(t, open) })
	t.Run("Admin/InstanceAdminKey", func(t *testing.T) { testAdminInstanceAdminKey(t, open) })
	t.Run("SetIssueStatus/Transitions", func(t *testing.T) { testSetIssueStatusTransitions(t, open) })
	t.Run("NoiseRules/UpsertInsertReturnsIDAndDefaults", func(t *testing.T) { testNoiseRulesUpsertInsertReturnsIDAndDefaults(t, open) })
	t.Run("NoiseRules/UpsertUpdateRoundTripsFields", func(t *testing.T) { testNoiseRulesUpsertUpdateRoundTripsFields(t, open) })
	t.Run("NoiseRules/UpsertUpdateMissingIDNotFound", func(t *testing.T) { testNoiseRulesUpsertUpdateMissingIDNotFound(t, open) })
	t.Run("NoiseRules/UpsertUnknownKindRejected", func(t *testing.T) { testNoiseRulesUpsertUnknownKindRejected(t, open) })
	t.Run("NoiseRules/DeleteThenNotFound", func(t *testing.T) { testNoiseRulesDeleteThenNotFound(t, open) })
	t.Run("NoiseRules/ListScopedByProjectAndOrdered", func(t *testing.T) { testNoiseRulesListScopedByProjectAndOrdered(t, open) })
	t.Run("NoiseRules/ListAllProjects", func(t *testing.T) { testNoiseRulesListAllProjects(t, open) })
	t.Run("NoiseRules/AddNoiseDropsAccumulatesAndSkipsUnknown", func(t *testing.T) { testNoiseRulesAddNoiseDropsAccumulatesAndSkipsUnknown(t, open) })
	t.Run("NoiseRules/SetProjectParseBodies", func(t *testing.T) { testNoiseRulesSetProjectParseBodies(t, open) })
	t.Run("NoiseRules/CreateProjectDefaultsParseBodiesTrue", func(t *testing.T) { testNoiseRulesCreateProjectDefaultsParseBodiesTrue(t, open) })
	t.Run("NoiseRules/SeverityRoundTripsLowercase", func(t *testing.T) { testNoiseRulesSeverityRoundTripsLowercase(t, open) })
	t.Run("AlertRules/UpsertInsertReturnsIDAndDefaults", func(t *testing.T) { testAlertRulesUpsertInsertReturnsIDAndDefaults(t, open) })
	t.Run("AlertRules/UpsertUpdateRoundTripsFields", func(t *testing.T) { testAlertRulesUpsertUpdateRoundTripsFields(t, open) })
	t.Run("AlertRules/UpsertUpdateMissingIDNotFound", func(t *testing.T) { testAlertRulesUpsertUpdateMissingIDNotFound(t, open) })
	t.Run("AlertRules/UpsertUnknownKindRejected", func(t *testing.T) { testAlertRulesUpsertUnknownKindRejected(t, open) })
	t.Run("AlertRules/DeleteThenNotFound", func(t *testing.T) { testAlertRulesDeleteThenNotFound(t, open) })
	t.Run("AlertRules/ListScopedByProjectAndOrdered", func(t *testing.T) { testAlertRulesListScopedByProjectAndOrdered(t, open) })
	t.Run("AlertRules/ListAllProjects", func(t *testing.T) { testAlertRulesListAllProjects(t, open) })
	t.Run("AlertRules/RecordAlertResultSetsAndClearsFields", func(t *testing.T) { testAlertRulesRecordAlertResultSetsAndClearsFields(t, open) })
	t.Run("AlertRules/RecordAlertResultMissingIDNoOp", func(t *testing.T) { testAlertRulesRecordAlertResultMissingIDNoOp(t, open) })
	t.Run("AlertRules/SeverityRoundTripsLowercase", func(t *testing.T) { testAlertRulesSeverityRoundTripsLowercase(t, open) })
	t.Run("AlertRules/HeadersNilRoundTrips", func(t *testing.T) { testAlertRulesHeadersNilRoundTrips(t, open) })
	t.Run("ServiceCounts/GroupsAndOrdersDescending", func(t *testing.T) { testServiceCountsGroupsAndOrdersDescending(t, open) })
}

// --- helpers ---------------------------------------------------------------

// mustProject creates a project via the store's Admin interface and fails
// the test immediately on error.
func mustProject(ctx context.Context, t *testing.T, s store.Store) core.Project {
	t.Helper()
	p, err := s.CreateProject(ctx, "test-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	return p
}

// entry builds a plain (non-event) log entry.
func entry(projectID int64, severity core.Severity, body, service string, at time.Time) store.Entry {
	return store.Entry{
		Log: core.Log{
			ProjectID:   projectID,
			Time:        at,
			Severity:    severity,
			Body:        body,
			Service:     service,
			Environment: "prod",
		},
	}
}

// eventEntry builds an event entry with the given fingerprint, which upserts
// an issue when written.
func eventEntry(projectID int64, severity core.Severity, body, fingerprint, title string, at time.Time) store.Entry {
	return store.Entry{
		Log: core.Log{
			ProjectID:   projectID,
			Time:        at,
			Severity:    severity,
			Body:        body,
			Service:     "svc",
			Environment: "prod",
		},
		IsEvent:     true,
		Fingerprint: fingerprint,
		Title:       title,
	}
}

// findIssue looks up an issue by fingerprint via the Issues listing API and
// fails the test if none matches.
func findIssue(ctx context.Context, t *testing.T, s store.Store, projectID int64, fingerprint string) core.Issue {
	t.Helper()
	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: projectID})
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	for _, iss := range issues {
		if iss.Fingerprint == fingerprint {
			return iss
		}
	}
	t.Fatalf("no issue found with fingerprint %q among %d issues", fingerprint, len(issues))
	return core.Issue{}
}

// --- WriteBatch --------------------------------------------------------

func testWriteBatchReadBack(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	e := entry(p.ID, core.SeverityWarn, "disk usage high", "web", baseTime)
	e.Log.Release = "v1.2.3"
	e.Log.TraceID = "trace-abc"
	e.Log.Attrs = map[string]string{
		"exception.type":    "DiskWarning",
		"exception.message": "usage at 91%",
	}

	if err := s.WriteBatch(ctx, []store.Entry{e}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Limit: 10})
	if err != nil {
		t.Fatalf("SearchLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 log, got %d", len(logs))
	}

	got := logs[0]
	if got.ID == 0 {
		t.Errorf("expected non-zero ID, got 0")
	}
	if got.ProjectID != p.ID {
		t.Errorf("ProjectID = %d, want %d", got.ProjectID, p.ID)
	}
	if !got.Time.Equal(baseTime) {
		t.Errorf("Time = %v, want %v", got.Time, baseTime)
	}
	if got.Severity != core.SeverityWarn {
		t.Errorf("Severity = %v, want %v", got.Severity, core.SeverityWarn)
	}
	if got.Body != "disk usage high" {
		t.Errorf("Body = %q, want %q", got.Body, "disk usage high")
	}
	if got.Service != "web" {
		t.Errorf("Service = %q, want %q", got.Service, "web")
	}
	if got.Environment != "prod" {
		t.Errorf("Environment = %q, want %q", got.Environment, "prod")
	}
	if got.Release != "v1.2.3" {
		t.Errorf("Release = %q, want %q", got.Release, "v1.2.3")
	}
	if got.TraceID != "trace-abc" {
		t.Errorf("TraceID = %q, want %q", got.TraceID, "trace-abc")
	}
	if !maps.Equal(got.Attrs, e.Log.Attrs) {
		t.Errorf("Attrs = %v, want %v", got.Attrs, e.Log.Attrs)
	}
}

func testWriteBatchCreatesOpenIssue(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	e := eventEntry(p.ID, core.SeverityError, "boom", "f1", "boom", baseTime)
	if err := s.WriteBatch(ctx, []store.Entry{e}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID})
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(issues))
	}
	iss := issues[0]
	if iss.Status != core.StatusOpen {
		t.Errorf("Status = %q, want %q", iss.Status, core.StatusOpen)
	}
	if iss.Count != 1 {
		t.Errorf("Count = %d, want 1", iss.Count)
	}
	if iss.Title != "boom" {
		t.Errorf("Title = %q, want %q", iss.Title, "boom")
	}
	if iss.Fingerprint != "f1" {
		t.Errorf("Fingerprint = %q, want %q", iss.Fingerprint, "f1")
	}
	if !iss.FirstSeen.Equal(baseTime) {
		t.Errorf("FirstSeen = %v, want %v", iss.FirstSeen, baseTime)
	}
	if !iss.LastSeen.Equal(baseTime) {
		t.Errorf("LastSeen = %v, want %v", iss.LastSeen, baseTime)
	}
}

func testWriteBatchIncrementsExisting(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	t1 := baseTime
	t2 := baseTime.Add(time.Hour)

	if err := s.WriteBatch(ctx, []store.Entry{eventEntry(p.ID, core.SeverityError, "boom", "f1", "boom", t1)}); err != nil {
		t.Fatalf("WriteBatch #1: %v", err)
	}
	if err := s.WriteBatch(ctx, []store.Entry{eventEntry(p.ID, core.SeverityError, "boom again", "f1", "boom", t2)}); err != nil {
		t.Fatalf("WriteBatch #2: %v", err)
	}

	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID})
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("expected 1 issue after repeat fingerprint, got %d", len(issues))
	}
	iss := issues[0]
	if iss.Count != 2 {
		t.Errorf("Count = %d, want 2", iss.Count)
	}
	if !iss.LastSeen.Equal(t2) {
		t.Errorf("LastSeen = %v, want %v", iss.LastSeen, t2)
	}
	if !iss.FirstSeen.Equal(t1) {
		t.Errorf("FirstSeen = %v, want %v", iss.FirstSeen, t1)
	}
}

func testWriteBatchRegressionReopens(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	if err := s.WriteBatch(ctx, []store.Entry{eventEntry(p.ID, core.SeverityError, "boom", "f1", "boom", baseTime)}); err != nil {
		t.Fatalf("WriteBatch #1: %v", err)
	}
	iss := findIssue(ctx, t, s, p.ID, "f1")

	if err := s.SetIssueStatus(ctx, iss.ID, core.StatusResolved); err != nil {
		t.Fatalf("SetIssueStatus resolved: %v", err)
	}
	resolved, _, err := s.Issue(ctx, iss.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if resolved.Status != core.StatusResolved {
		t.Fatalf("expected resolved status before regression, got %q", resolved.Status)
	}

	if err := s.WriteBatch(ctx, []store.Entry{eventEntry(p.ID, core.SeverityError, "boom again", "f1", "boom", baseTime.Add(time.Hour))}); err != nil {
		t.Fatalf("WriteBatch #2: %v", err)
	}

	reopened, _, err := s.Issue(ctx, iss.ID)
	if err != nil {
		t.Fatalf("Issue after regression: %v", err)
	}
	if reopened.Status != core.StatusOpen {
		t.Errorf("Status after regression = %q, want %q", reopened.Status, core.StatusOpen)
	}
	if reopened.Count != 2 {
		t.Errorf("Count after regression = %d, want 2", reopened.Count)
	}
}

func testWriteBatchEventSampleCap(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	const total = 60
	for i := 0; i < total; i++ {
		at := baseTime.Add(time.Duration(i) * time.Minute)
		e := eventEntry(p.ID, core.SeverityError, "boom", "f1", "boom", at)
		if err := s.WriteBatch(ctx, []store.Entry{e}); err != nil {
			t.Fatalf("WriteBatch #%d: %v", i, err)
		}
	}

	iss := findIssue(ctx, t, s, p.ID, "f1")
	if iss.Count != total {
		t.Fatalf("Count = %d, want %d", iss.Count, total)
	}

	_, events, err := s.Issue(ctx, iss.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if len(events) > 50 {
		t.Fatalf("expected <=50 samples, got %d", len(events))
	}
	if len(events) == 0 {
		t.Fatalf("expected some samples, got 0")
	}

	// The 10 oldest writes (minutes 0..9) must have been evicted; the
	// newest write (minute 59) must be present among the retained samples.
	oldestKept := baseTime.Add(9 * time.Minute)
	newestExpected := baseTime.Add(59 * time.Minute)
	sawOldest := false
	sawNewest := false
	for _, ev := range events {
		if !ev.Time.After(oldestKept) {
			sawOldest = true
		}
		if ev.Time.Equal(newestExpected) {
			sawNewest = true
		}
	}
	if sawOldest {
		t.Errorf("expected the 10 oldest event samples to be evicted, but found one at or before %v", oldestKept)
	}
	if !sawNewest {
		t.Errorf("expected newest event sample %v to be present", newestExpected)
	}
}

// --- Issues --------------------------------------------------------------

func testIssuesFilterByStatusEnvProject(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2, err := s.CreateProject(ctx, "second-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	// p1, prod, open — the target match.
	writeIssue(ctx, t, s, p1.ID, "prod", "f1", baseTime)
	// p1, staging, open — wrong environment.
	writeIssue(ctx, t, s, p1.ID, "staging", "f2", baseTime)
	// p2, prod, open — wrong project.
	writeIssue(ctx, t, s, p2.ID, "prod", "f3", baseTime)
	// p1, prod, resolved — wrong status.
	iss4 := writeIssue(ctx, t, s, p1.ID, "prod", "f4", baseTime)
	if err := s.SetIssueStatus(ctx, iss4.ID, core.StatusResolved); err != nil {
		t.Fatalf("SetIssueStatus: %v", err)
	}

	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p1.ID, Environment: "prod", Status: core.StatusOpen})
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	if len(issues) != 1 {
		t.Fatalf("expected 1 matching issue, got %d: %+v", len(issues), issues)
	}
	if issues[0].Fingerprint != "f1" {
		t.Errorf("Fingerprint = %q, want %q", issues[0].Fingerprint, "f1")
	}
}

// writeIssue writes a single event entry and returns the resulting issue.
func writeIssue(ctx context.Context, t *testing.T, s store.Store, projectID int64, environment, fingerprint string, at time.Time) core.Issue {
	t.Helper()
	e := eventEntry(projectID, core.SeverityError, "boom "+fingerprint, fingerprint, "boom", at)
	e.Log.Environment = environment
	if err := s.WriteBatch(ctx, []store.Entry{e}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	return findIssue(ctx, t, s, projectID, fingerprint)
}

func testIssuesSortedByLastSeenDesc(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	writeIssue(ctx, t, s, p.ID, "prod", "f1", baseTime)
	writeIssue(ctx, t, s, p.ID, "prod", "f2", baseTime.Add(2*time.Hour))
	writeIssue(ctx, t, s, p.ID, "prod", "f3", baseTime.Add(time.Hour))

	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID})
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	if len(issues) != 3 {
		t.Fatalf("expected 3 issues, got %d", len(issues))
	}
	want := []string{"f2", "f3", "f1"} // LastSeen desc: 2h, 1h, 0h
	for i, w := range want {
		if issues[i].Fingerprint != w {
			t.Errorf("issues[%d].Fingerprint = %q, want %q (full order: %v)", i, issues[i].Fingerprint, w, fingerprints(issues))
		}
	}
}

func fingerprints(issues []core.Issue) []string {
	out := make([]string, len(issues))
	for i, iss := range issues {
		out[i] = iss.Fingerprint
	}
	return out
}

func testIssuesLimitHonored(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	writeIssue(ctx, t, s, p.ID, "prod", "f1", baseTime)
	writeIssue(ctx, t, s, p.ID, "prod", "f2", baseTime.Add(2*time.Hour))
	writeIssue(ctx, t, s, p.ID, "prod", "f3", baseTime.Add(time.Hour))

	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID, Limit: 2})
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	if len(issues) != 2 {
		t.Fatalf("expected 2 issues with Limit=2, got %d", len(issues))
	}
	// The 2 most recent by LastSeen: f2 (2h), f3 (1h).
	want := []string{"f2", "f3"}
	for i, w := range want {
		if issues[i].Fingerprint != w {
			t.Errorf("issues[%d].Fingerprint = %q, want %q (full order: %v)", i, issues[i].Fingerprint, w, fingerprints(issues))
		}
	}
}

func testIssuesDefaultLimitFifty(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	const total = 55
	for i := 0; i < total; i++ {
		at := baseTime.Add(time.Duration(i) * time.Minute)
		fp := "f" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if err := s.WriteBatch(ctx, []store.Entry{eventEntry(p.ID, core.SeverityError, "boom", fp, "boom", at)}); err != nil {
			t.Fatalf("WriteBatch #%d: %v", i, err)
		}
	}

	issues, err := s.Issues(ctx, store.IssueFilter{ProjectID: p.ID}) // Limit unset (0)
	if err != nil {
		t.Fatalf("Issues: %v", err)
	}
	if len(issues) != 50 {
		t.Fatalf("expected default limit of 50 issues, got %d (wrote %d)", len(issues), total)
	}
}

func testIssueNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	_ = mustProject(ctx, t, s)

	_, _, err := s.Issue(ctx, 999999)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Issue(unknown) err = %v, want ErrNotFound", err)
	}
}

// --- SearchLogs ------------------------------------------------------------

func testSearchLogsFTSMatchesBody(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	entries := []store.Entry{
		entry(p.ID, core.SeverityError, "connection refused", "svc", baseTime),
		entry(p.ID, core.SeverityError, "disk full", "svc", baseTime.Add(time.Minute)),
		entry(p.ID, core.SeverityInfo, "request completed", "svc", baseTime.Add(2*time.Minute)),
	}
	if err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: "refused"})
	if err != nil {
		t.Fatalf("SearchLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected 1 match for %q, got %d", "refused", len(logs))
	}
	if logs[0].Body != "connection refused" {
		t.Errorf("Body = %q, want %q", logs[0].Body, "connection refused")
	}
}

func testSearchLogsMinSeverityAndTimeRange(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	entries := []store.Entry{
		entry(p.ID, core.SeverityInfo, "info at t0", "svc", baseTime),
		entry(p.ID, core.SeverityError, "error at t1", "svc", baseTime.Add(time.Hour)),
		entry(p.ID, core.SeverityWarn, "warn at t2", "svc", baseTime.Add(2*time.Hour)),
		entry(p.ID, core.SeverityFatal, "fatal at t3, out of range", "svc", baseTime.Add(10*time.Hour)),
	}
	if err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// MinSeverity=Warn should exclude the info-level log.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, MinSeverity: core.SeverityWarn})
	if err != nil {
		t.Fatalf("SearchLogs (MinSeverity): %v", err)
	}
	for _, l := range logs {
		if l.Severity < core.SeverityWarn {
			t.Errorf("SearchLogs with MinSeverity=Warn returned severity %v log %q", l.Severity, l.Body)
		}
	}
	if len(logs) != 3 {
		t.Fatalf("expected 3 logs at Warn+, got %d", len(logs))
	}

	// Since/Until should bound to [t0, t2], excluding the t3 log.
	logs, err = s.SearchLogs(ctx, store.LogFilter{
		ProjectID: p.ID,
		Since:     baseTime,
		Until:     baseTime.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("SearchLogs (time range): %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("expected 3 logs in [t0,t2], got %d", len(logs))
	}
	for _, l := range logs {
		if l.Body == "fatal at t3, out of range" {
			t.Errorf("time range filter leaked out-of-range log: %q", l.Body)
		}
	}
}

// --- LogContext --------------------------------------------------------

func testLogContextReturnsNeighborsSameProjectService(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	// Five same-service logs, 1 minute apart, plus a distractor on a
	// different service sitting right in the middle of the timeline.
	entries := []store.Entry{
		entry(p.ID, core.SeverityInfo, "svc-0", "svc", baseTime),
		entry(p.ID, core.SeverityInfo, "svc-1", "svc", baseTime.Add(time.Minute)),
		entry(p.ID, core.SeverityInfo, "svc-2-target", "svc", baseTime.Add(2*time.Minute)),
		entry(p.ID, core.SeverityInfo, "svc-3", "svc", baseTime.Add(3*time.Minute)),
		entry(p.ID, core.SeverityInfo, "svc-4", "svc", baseTime.Add(4*time.Minute)),
		entry(p.ID, core.SeverityInfo, "other-service", "other-svc", baseTime.Add(2*time.Minute+time.Second)),
	}
	if err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: "svc-2-target"})
	if err != nil {
		t.Fatalf("SearchLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("expected to find target log, got %d results", len(logs))
	}
	targetID := logs[0].ID

	ctxLogs, err := s.LogContext(ctx, targetID, 2)
	if err != nil {
		t.Fatalf("LogContext: %v", err)
	}
	// Exactly 2 before + target + 2 after = 5, no more, no less.
	if len(ctxLogs) != 5 {
		t.Fatalf("expected exactly 5 context entries (2 before + target + 2 after), got %d: bodies=%v", len(ctxLogs), bodiesOf(ctxLogs))
	}

	for _, l := range ctxLogs {
		if l.Service != "svc" {
			t.Errorf("LogContext leaked log from service %q, want %q", l.Service, "svc")
		}
	}
	for i := 1; i < len(ctxLogs); i++ {
		if ctxLogs[i].Time.Before(ctxLogs[i-1].Time) {
			t.Fatalf("LogContext results not ordered by time: %v", timesOf(ctxLogs))
		}
	}

	wantBodies := []string{"svc-0", "svc-1", "svc-2-target", "svc-3", "svc-4"}
	gotBodies := bodiesOf(ctxLogs)
	for i, w := range wantBodies {
		if gotBodies[i] != w {
			t.Errorf("ctxLogs[%d].Body = %q, want %q (full: %v)", i, gotBodies[i], w, gotBodies)
		}
	}

	// Target must sit in the middle of the returned window.
	mid := len(ctxLogs) / 2
	if ctxLogs[mid].ID != targetID {
		t.Errorf("target log not in the middle of context window: got %v at index %d, want ID %d", ctxLogs[mid], mid, targetID)
	}
}

func bodiesOf(logs []core.Log) []string {
	out := make([]string, len(logs))
	for i, l := range logs {
		out[i] = l.Body
	}
	return out
}

func timesOf(logs []core.Log) []time.Time {
	out := make([]time.Time, len(logs))
	for i, l := range logs {
		out[i] = l.Time
	}
	return out
}

// --- Stats -----------------------------------------------------------------

func testStatsCountsAndPerDay(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	day1 := baseTime
	day2 := baseTime.Add(24 * time.Hour)

	entries := []store.Entry{
		entry(p.ID, core.SeverityInfo, "day1 log a", "svc", day1),
		entry(p.ID, core.SeverityInfo, "day1 log b", "svc", day1.Add(time.Minute)),
		eventEntry(p.ID, core.SeverityError, "day1 event", "f1", "day1 event", day1.Add(2*time.Minute)),
		entry(p.ID, core.SeverityInfo, "day2 log", "svc", day2),
		eventEntry(p.ID, core.SeverityError, "day2 event", "f2", "day2 event", day2.Add(time.Minute)),
	}
	if err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	stats, err := s.Stats(ctx, store.StatsFilter{ProjectID: p.ID, Since: day1.Add(-time.Hour)})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Logs != 5 {
		t.Errorf("Logs = %d, want 5", stats.Logs)
	}
	if stats.Events != 2 {
		t.Errorf("Events = %d, want 2", stats.Events)
	}
	if stats.OpenIssues != 2 {
		t.Errorf("OpenIssues = %d, want 2", stats.OpenIssues)
	}
	if len(stats.PerDay) != 2 {
		t.Fatalf("PerDay len = %d, want 2, got %+v", len(stats.PerDay), stats.PerDay)
	}

	byDay := map[string]store.DayCount{}
	for _, d := range stats.PerDay {
		byDay[d.Day] = d
	}
	d1 := byDay[day1.Format("2006-01-02")]
	if d1.Logs != 3 || d1.Events != 1 {
		t.Errorf("day1 bucket = %+v, want Logs=3 Events=1", d1)
	}
	d2 := byDay[day2.Format("2006-01-02")]
	if d2.Logs != 2 || d2.Events != 1 {
		t.Errorf("day2 bucket = %+v, want Logs=2 Events=1", d2)
	}
}

// --- Prune -------------------------------------------------------------

func testPruneDeletesOnlyOlderAndOnlyProject(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2, err := s.CreateProject(ctx, "second-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	cutoff := baseTime.Add(24 * time.Hour)
	oldTime := baseTime
	newTime := baseTime.Add(48 * time.Hour)

	// Include event-linked logs (not just plain logs) on both sides of the
	// cutoff: events.log_id has a foreign key to logs(id), so pruning a log
	// that produced an event must also clean up its event row in the same
	// operation, or the delete fails under foreign_keys=on. This is the
	// retention system's primary real-world case (most retained logs are
	// events), so it must be covered here, not just with plain logs.
	entries := []store.Entry{
		entry(p1.ID, core.SeverityInfo, "p1 old", "svc", oldTime),
		entry(p1.ID, core.SeverityInfo, "p1 new", "svc", newTime),
		entry(p2.ID, core.SeverityInfo, "p2 old", "svc", oldTime),
		eventEntry(p1.ID, core.SeverityError, "p1 old event", "old-event", "old event", oldTime),
		eventEntry(p1.ID, core.SeverityError, "p1 new event", "new-event", "new event", newTime),
	}
	if err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	oldIssue := findIssue(ctx, t, s, p1.ID, "old-event")
	newIssue := findIssue(ctx, t, s, p1.ID, "new-event")

	n, err := s.Prune(ctx, p1.ID, cutoff)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if n != 2 {
		t.Fatalf("Prune returned %d, want 2 (p1 old plain log + p1 old event log)", n)
	}

	p1Logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p1.ID})
	if err != nil {
		t.Fatalf("SearchLogs p1: %v", err)
	}
	if len(p1Logs) != 2 {
		t.Fatalf("p1 logs after prune = %+v, want 2 (p1 new + p1 new event)", p1Logs)
	}
	for _, l := range p1Logs {
		if l.Body == "p1 old" || l.Body == "p1 old event" {
			t.Errorf("prune left an old p1 log behind: %q", l.Body)
		}
	}

	p2Logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p2.ID})
	if err != nil {
		t.Fatalf("SearchLogs p2: %v", err)
	}
	if len(p2Logs) != 1 || p2Logs[0].Body != "p2 old" {
		t.Errorf("p2 logs after prune = %+v, want untouched %q", p2Logs, "p2 old")
	}

	// The issue row itself survives pruning — only its old event log/sample
	// goes. Both issues (old and new) must still be resolvable, with their
	// counts untouched by prune.
	gotOld, oldEvents, err := s.Issue(ctx, oldIssue.ID)
	if err != nil {
		t.Fatalf("Issue(old) after prune: %v", err)
	}
	if gotOld.Count != 1 {
		t.Errorf("old issue Count after prune = %d, want 1 (prune doesn't touch issue counts)", gotOld.Count)
	}
	if len(oldEvents) != 0 {
		t.Errorf("old issue events after prune = %d, want 0 (its only event log was pruned)", len(oldEvents))
	}

	gotNew, newEvents, err := s.Issue(ctx, newIssue.ID)
	if err != nil {
		t.Fatalf("Issue(new) after prune: %v", err)
	}
	if gotNew.Count != 1 {
		t.Errorf("new issue Count after prune = %d, want 1", gotNew.Count)
	}
	if len(newEvents) != 1 {
		t.Errorf("new issue events after prune = %d, want 1 (its event log survives)", len(newEvents))
	}
}

// --- Admin -------------------------------------------------------------

func testAdminProjectCRUDAndKeys(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)

	p, err := s.CreateProject(ctx, "keys-project", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if p.ID == 0 {
		t.Fatalf("expected non-zero project ID")
	}
	if p.RetentionDays != 14 {
		t.Errorf("RetentionDays = %d, want 14", p.RetentionDays)
	}

	projects, err := s.Projects(ctx)
	if err != nil {
		t.Fatalf("Projects: %v", err)
	}
	found := false
	for _, pr := range projects {
		if pr.ID == p.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("Projects() did not include created project %+v", p)
	}

	plaintext1, err := s.MintKey(ctx, p.ID, "ingest")
	if err != nil {
		t.Fatalf("MintKey: %v", err)
	}
	if plaintext1 == "" {
		t.Fatalf("expected non-empty plaintext key")
	}

	gotProjectID, gotKind, err := s.LookupKey(ctx, plaintext1)
	if err != nil {
		t.Fatalf("LookupKey: %v", err)
	}
	if gotProjectID != p.ID {
		t.Errorf("LookupKey projectID = %d, want %d", gotProjectID, p.ID)
	}
	if gotKind != "ingest" {
		t.Errorf("LookupKey kind = %q, want %q", gotKind, "ingest")
	}

	_, _, err = s.LookupKey(ctx, "not-a-real-key")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LookupKey(wrong) err = %v, want ErrNotFound", err)
	}

	plaintext2, err := s.MintKey(ctx, p.ID, "api")
	if err != nil {
		t.Fatalf("MintKey #2: %v", err)
	}
	if plaintext2 == plaintext1 {
		t.Fatalf("expected distinct plaintext keys across mints, got the same value twice")
	}
	gotProjectID2, gotKind2, err := s.LookupKey(ctx, plaintext2)
	if err != nil {
		t.Fatalf("LookupKey #2: %v", err)
	}
	if gotProjectID2 != p.ID || gotKind2 != "api" {
		t.Errorf("LookupKey #2 = (%d, %q), want (%d, %q)", gotProjectID2, gotKind2, p.ID, "api")
	}
}

// testAdminInstanceAdminKey covers the instance-level "admin" key kind: it
// is minted with projectID 0 (not tied to any project) and LookupKey must
// round-trip that as (0, "admin", nil) rather than treating 0 as a real
// project ID.
func testAdminInstanceAdminKey(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)

	plaintext, err := s.MintKey(ctx, 0, "admin")
	if err != nil {
		t.Fatalf("MintKey(admin): %v", err)
	}
	if plaintext == "" {
		t.Fatalf("expected non-empty plaintext admin key")
	}

	gotProjectID, gotKind, err := s.LookupKey(ctx, plaintext)
	if err != nil {
		t.Fatalf("LookupKey(admin): %v", err)
	}
	if gotProjectID != 0 {
		t.Errorf("LookupKey(admin) projectID = %d, want 0", gotProjectID)
	}
	if gotKind != "admin" {
		t.Errorf("LookupKey(admin) kind = %q, want %q", gotKind, "admin")
	}
}

// --- SetIssueStatus ------------------------------------------------------

func testSetIssueStatusTransitions(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	iss := writeIssue(ctx, t, s, p.ID, "prod", "f1", baseTime)
	if iss.Status != core.StatusOpen {
		t.Fatalf("initial status = %q, want %q", iss.Status, core.StatusOpen)
	}

	if err := s.SetIssueStatus(ctx, iss.ID, core.StatusResolved); err != nil {
		t.Fatalf("SetIssueStatus(resolved): %v", err)
	}
	got, _, err := s.Issue(ctx, iss.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if got.Status != core.StatusResolved {
		t.Errorf("Status = %q, want %q", got.Status, core.StatusResolved)
	}

	if err := s.SetIssueStatus(ctx, iss.ID, core.StatusIgnored); err != nil {
		t.Fatalf("SetIssueStatus(ignored): %v", err)
	}
	got, _, err = s.Issue(ctx, iss.ID)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if got.Status != core.StatusIgnored {
		t.Errorf("Status = %q, want %q", got.Status, core.StatusIgnored)
	}

	err = s.SetIssueStatus(ctx, 999999, core.StatusResolved)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("SetIssueStatus(unknown) err = %v, want ErrNotFound", err)
	}
}

// --- NoiseRules ----------------------------------------------------------

func testNoiseRulesUpsertInsertReturnsIDAndDefaults(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertNoiseRule(ctx, core.NoiseRule{
		ProjectID: p.ID,
		Kind:      core.NoiseDropMatch,
		Service:   "traefik",
		Pattern:   "healthcheck",
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertNoiseRule: %v", err)
	}
	if row.ID == 0 {
		t.Fatalf("expected non-zero ID")
	}
	if row.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", row.DroppedCount)
	}
	if row.CreatedAt.IsZero() {
		t.Errorf("expected non-zero CreatedAt")
	}
	if row.ProjectID != p.ID || row.Kind != core.NoiseDropMatch || row.Service != "traefik" || row.Pattern != "healthcheck" || !row.Enabled {
		t.Errorf("row = %+v, unexpected values", row)
	}
}

func testNoiseRulesUpsertUpdateRoundTripsFields(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	inserted, err := s.UpsertNoiseRule(ctx, core.NoiseRule{
		ProjectID: p.ID,
		Kind:      core.NoiseSample,
		Service:   "web",
		Severity:  core.SeverityInfo,
		N:         5,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertNoiseRule insert: %v", err)
	}

	updated, err := s.UpsertNoiseRule(ctx, core.NoiseRule{
		ID:        inserted.ID,
		ProjectID: p.ID,
		Kind:      core.NoiseSeverityFloor,
		Service:   "api",
		Severity:  core.SeverityWarn,
		Pattern:   "",
		N:         0,
		Enabled:   false,
	})
	if err != nil {
		t.Fatalf("UpsertNoiseRule update: %v", err)
	}
	if updated.ID != inserted.ID {
		t.Errorf("ID = %d, want %d (update must not change ID)", updated.ID, inserted.ID)
	}
	if updated.Kind != core.NoiseSeverityFloor {
		t.Errorf("Kind = %q, want %q", updated.Kind, core.NoiseSeverityFloor)
	}
	if updated.Service != "api" {
		t.Errorf("Service = %q, want %q", updated.Service, "api")
	}
	if updated.Severity != core.SeverityWarn {
		t.Errorf("Severity = %v, want %v", updated.Severity, core.SeverityWarn)
	}
	if updated.Enabled {
		t.Errorf("Enabled = true, want false")
	}

	rows, err := s.NoiseRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("NoiseRules: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rows))
	}
	if rows[0].Kind != core.NoiseSeverityFloor || rows[0].Service != "api" || rows[0].Severity != core.SeverityWarn || rows[0].Enabled {
		t.Errorf("persisted row = %+v, want updated values", rows[0])
	}
}

func testNoiseRulesUpsertUpdateMissingIDNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	_, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ID: 999999, ProjectID: p.ID, Kind: core.NoiseDropMatch, Pattern: "x"})
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpsertNoiseRule(missing ID) err = %v, want ErrNotFound", err)
	}
}

func testNoiseRulesUpsertUnknownKindRejected(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	_, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p.ID, Kind: core.NoiseRuleKind("bogus")})
	if err == nil {
		t.Fatalf("UpsertNoiseRule(unknown kind) err = nil, want error")
	}
	if errors.Is(err, store.ErrNotFound) {
		t.Fatalf("UpsertNoiseRule(unknown kind) err = ErrNotFound, want a validation error")
	}
}

func testNoiseRulesDeleteThenNotFound(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p.ID, Kind: core.NoiseDropMatch, Pattern: "x"})
	if err != nil {
		t.Fatalf("UpsertNoiseRule: %v", err)
	}

	if err := s.DeleteNoiseRule(ctx, row.ID); err != nil {
		t.Fatalf("DeleteNoiseRule: %v", err)
	}

	rows, err := s.NoiseRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("NoiseRules: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rules after delete, got %d", len(rows))
	}

	err = s.DeleteNoiseRule(ctx, row.ID)
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("DeleteNoiseRule(already deleted) err = %v, want ErrNotFound", err)
	}
}

func testNoiseRulesListScopedByProjectAndOrdered(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2, err := s.CreateProject(ctx, "second-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	r1, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p1.ID, Kind: core.NoiseDropMatch, Pattern: "a"})
	if err != nil {
		t.Fatalf("UpsertNoiseRule #1: %v", err)
	}
	r2, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p1.ID, Kind: core.NoiseDropMatch, Pattern: "b"})
	if err != nil {
		t.Fatalf("UpsertNoiseRule #2: %v", err)
	}
	if _, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p2.ID, Kind: core.NoiseDropMatch, Pattern: "c"}); err != nil {
		t.Fatalf("UpsertNoiseRule #3: %v", err)
	}

	rows, err := s.NoiseRules(ctx, p1.ID)
	if err != nil {
		t.Fatalf("NoiseRules: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rules for p1, got %d", len(rows))
	}
	if rows[0].ID != r1.ID || rows[1].ID != r2.ID {
		t.Errorf("rows not ordered ascending by ID: got IDs %d, %d, want %d, %d", rows[0].ID, rows[1].ID, r1.ID, r2.ID)
	}
}

func testNoiseRulesListAllProjects(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p1 := mustProject(ctx, t, s)
	p2, err := s.CreateProject(ctx, "second-project", 30)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	if _, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p1.ID, Kind: core.NoiseDropMatch, Pattern: "a"}); err != nil {
		t.Fatalf("UpsertNoiseRule #1: %v", err)
	}
	if _, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p2.ID, Kind: core.NoiseDropMatch, Pattern: "b"}); err != nil {
		t.Fatalf("UpsertNoiseRule #2: %v", err)
	}

	rows, err := s.NoiseRules(ctx, 0)
	if err != nil {
		t.Fatalf("NoiseRules(0): %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rules across all projects, got %d", len(rows))
	}
}

func testNoiseRulesAddNoiseDropsAccumulatesAndSkipsUnknown(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	r1, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p.ID, Kind: core.NoiseDropMatch, Pattern: "a"})
	if err != nil {
		t.Fatalf("UpsertNoiseRule #1: %v", err)
	}
	r2, err := s.UpsertNoiseRule(ctx, core.NoiseRule{ProjectID: p.ID, Kind: core.NoiseDropMatch, Pattern: "b"})
	if err != nil {
		t.Fatalf("UpsertNoiseRule #2: %v", err)
	}

	if err := s.AddNoiseDrops(ctx, map[int64]int64{r1.ID: 3, r2.ID: 1, 999999: 42}); err != nil {
		t.Fatalf("AddNoiseDrops #1: %v", err)
	}
	if err := s.AddNoiseDrops(ctx, map[int64]int64{r1.ID: 2}); err != nil {
		t.Fatalf("AddNoiseDrops #2: %v", err)
	}

	rows, err := s.NoiseRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("NoiseRules: %v", err)
	}
	byID := map[int64]store.NoiseRuleRow{}
	for _, row := range rows {
		byID[row.ID] = row
	}
	if byID[r1.ID].DroppedCount != 5 {
		t.Errorf("r1 DroppedCount = %d, want 5", byID[r1.ID].DroppedCount)
	}
	if byID[r2.ID].DroppedCount != 1 {
		t.Errorf("r2 DroppedCount = %d, want 1", byID[r2.ID].DroppedCount)
	}
}

func testNoiseRulesSetProjectParseBodies(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	if err := s.SetProjectParseBodies(ctx, p.ID, false); err != nil {
		t.Fatalf("SetProjectParseBodies(false): %v", err)
	}

	projects, err := s.Projects(ctx)
	if err != nil {
		t.Fatalf("Projects: %v", err)
	}
	found := false
	for _, pr := range projects {
		if pr.ID == p.ID {
			found = true
			if pr.ParseBodies {
				t.Errorf("ParseBodies = true after SetProjectParseBodies(false)")
			}
		}
	}
	if !found {
		t.Fatalf("project %d not found in Projects()", p.ID)
	}

	if err := s.SetProjectParseBodies(ctx, p.ID, true); err != nil {
		t.Fatalf("SetProjectParseBodies(true): %v", err)
	}
	projects, err = s.Projects(ctx)
	if err != nil {
		t.Fatalf("Projects: %v", err)
	}
	for _, pr := range projects {
		if pr.ID == p.ID && !pr.ParseBodies {
			t.Errorf("ParseBodies = false after SetProjectParseBodies(true)")
		}
	}
}

func testNoiseRulesCreateProjectDefaultsParseBodiesTrue(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	if !p.ParseBodies {
		t.Errorf("CreateProject: ParseBodies = false, want true by default")
	}

	projects, err := s.Projects(ctx)
	if err != nil {
		t.Fatalf("Projects: %v", err)
	}
	for _, pr := range projects {
		if pr.ID == p.ID && !pr.ParseBodies {
			t.Errorf("Projects(): ParseBodies = false, want true by default")
		}
	}
}

func testNoiseRulesSeverityRoundTripsLowercase(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)

	row, err := s.UpsertNoiseRule(ctx, core.NoiseRule{
		ProjectID: p.ID,
		Kind:      core.NoiseSeverityFloor,
		Service:   "traefik",
		Severity:  core.SeverityWarn,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("UpsertNoiseRule: %v", err)
	}
	if row.Severity != core.SeverityWarn {
		t.Errorf("returned row Severity = %v, want %v", row.Severity, core.SeverityWarn)
	}

	rows, err := s.NoiseRules(ctx, p.ID)
	if err != nil {
		t.Fatalf("NoiseRules: %v", err)
	}
	if len(rows) != 1 || rows[0].Severity != core.SeverityWarn {
		t.Fatalf("rows = %+v, want single row with Severity=%v", rows, core.SeverityWarn)
	}
}

func testServiceCountsGroupsAndOrdersDescending(t *testing.T, open func(t *testing.T) store.Store) {
	ctx := context.Background()
	s := open(t)
	p := mustProject(ctx, t, s)
	other := mustProject(ctx, t, s)

	entries := []store.Entry{
		entry(p.ID, core.SeverityInfo, "a1", "svc-a", baseTime),
		entry(p.ID, core.SeverityInfo, "a2", "svc-a", baseTime.Add(time.Minute)),
		entry(p.ID, core.SeverityInfo, "a3", "svc-a", baseTime.Add(2*time.Minute)),
		entry(p.ID, core.SeverityInfo, "b1", "svc-b", baseTime.Add(3*time.Minute)),
		entry(p.ID, core.SeverityInfo, "too-old", "svc-a", baseTime.Add(-time.Hour)),
		entry(other.ID, core.SeverityInfo, "other-project", "svc-a", baseTime),
	}
	if err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	counts, err := s.ServiceCounts(ctx, p.ID, baseTime.Add(-time.Minute))
	if err != nil {
		t.Fatalf("ServiceCounts: %v", err)
	}
	if len(counts) != 2 {
		t.Fatalf("counts = %+v, want 2 services", counts)
	}
	if counts[0].Service != "svc-a" || counts[0].Logs != 3 {
		t.Errorf("counts[0] = %+v, want svc-a with 3 logs", counts[0])
	}
	if counts[1].Service != "svc-b" || counts[1].Logs != 1 {
		t.Errorf("counts[1] = %+v, want svc-b with 1 log", counts[1])
	}
}
