package mcp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/auth/authtest"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// ---- fakeStore: hand-written fake over maps (mirrors internal/api's pattern) ----

type fakeStore struct {
	projects map[int64]core.Project
	keys     map[string]struct {
		projectID int64
		kind      string
	}

	issues      map[int64]core.Issue
	issueEvents map[int64][]core.Event
	issueList   []core.Issue

	logList []core.Log

	stats map[int64]store.Stats // keyed by projectID

	serviceCounts map[int64][]store.ServiceCount // keyed by projectID

	noiseRules      map[int64]store.NoiseRuleRow // keyed by rule ID
	nextNoiseRuleID int64
	lastAddedDrops  map[int64]int64

	lastIssueFilter store.IssueFilter
	lastLogFilter   store.LogFilter
	lastStatsFilter store.StatsFilter
	lastContextID   int64
	lastContextN    int
	contextNotFound bool

	// forcedErr, when set, is returned by Issues in place of the canned
	// issueList — used to simulate a non-ErrNotFound store failure (e.g.
	// a wrapped driver error) and assert it never reaches the client
	// verbatim.
	forcedErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		projects: make(map[int64]core.Project),
		keys: make(map[string]struct {
			projectID int64
			kind      string
		}),
		issues:        make(map[int64]core.Issue),
		issueEvents:   make(map[int64][]core.Event),
		stats:         make(map[int64]store.Stats),
		serviceCounts: make(map[int64][]store.ServiceCount),
		noiseRules:    make(map[int64]store.NoiseRuleRow),
	}
}

func (f *fakeStore) Issues(_ context.Context, filter store.IssueFilter) ([]core.Issue, error) {
	f.lastIssueFilter = filter
	if f.forcedErr != nil {
		return nil, f.forcedErr
	}
	return f.issueList, nil
}

func (f *fakeStore) Issue(_ context.Context, id int64) (core.Issue, []core.Event, error) {
	iss, ok := f.issues[id]
	if !ok {
		return core.Issue{}, nil, store.ErrNotFound
	}
	return iss, f.issueEvents[id], nil
}

func (f *fakeStore) SearchLogs(_ context.Context, filter store.LogFilter) ([]core.Log, error) {
	f.lastLogFilter = filter
	return f.logList, nil
}

func (f *fakeStore) LogContext(_ context.Context, logID int64, n int) ([]core.Log, error) {
	f.lastContextID = logID
	f.lastContextN = n
	if f.contextNotFound {
		return nil, store.ErrNotFound
	}
	return f.logList, nil
}

func (f *fakeStore) Stats(_ context.Context, filter store.StatsFilter) (store.Stats, error) {
	f.lastStatsFilter = filter
	return f.stats[filter.ProjectID], nil
}

func (f *fakeStore) ServiceCounts(_ context.Context, projectID int64, _ time.Time) ([]store.ServiceCount, error) {
	return f.serviceCounts[projectID], nil
}

func (f *fakeStore) CreateProject(_ context.Context, _ string, _ int) (core.Project, error) {
	return core.Project{}, errors.New("not implemented")
}

func (f *fakeStore) Projects(_ context.Context) ([]core.Project, error) {
	out := make([]core.Project, 0, len(f.projects))
	for _, p := range f.projects {
		out = append(out, p)
	}
	return out, nil
}

func (f *fakeStore) SetIssueStatus(_ context.Context, id int64, s core.IssueStatus) error {
	iss, ok := f.issues[id]
	if !ok {
		return store.ErrNotFound
	}
	iss.Status = s
	f.issues[id] = iss
	return nil
}

func (f *fakeStore) MintKey(_ context.Context, _ int64, _ string) (string, error) {
	return "", errors.New("not implemented")
}

func (f *fakeStore) LookupKey(_ context.Context, plaintext string) (int64, string, error) {
	e, ok := f.keys[plaintext]
	if !ok {
		return 0, "", store.ErrNotFound
	}
	return e.projectID, e.kind, nil
}

// ---- store.NoiseRules ----

func (f *fakeStore) NoiseRules(_ context.Context, projectID int64) ([]store.NoiseRuleRow, error) {
	ids := make([]int64, 0, len(f.noiseRules))
	for id, row := range f.noiseRules {
		if projectID == 0 || row.ProjectID == projectID {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]store.NoiseRuleRow, 0, len(ids))
	for _, id := range ids {
		out = append(out, f.noiseRules[id])
	}
	return out, nil
}

func (f *fakeStore) UpsertNoiseRule(_ context.Context, r core.NoiseRule) (store.NoiseRuleRow, error) {
	if r.ID == 0 {
		f.nextNoiseRuleID++
		r.ID = f.nextNoiseRuleID
		row := store.NoiseRuleRow{NoiseRule: r}
		f.noiseRules[r.ID] = row
		return row, nil
	}
	existing, ok := f.noiseRules[r.ID]
	if !ok {
		return store.NoiseRuleRow{}, store.ErrNotFound
	}
	existing.NoiseRule = r
	f.noiseRules[r.ID] = existing
	return existing, nil
}

func (f *fakeStore) DeleteNoiseRule(_ context.Context, id int64) error {
	if _, ok := f.noiseRules[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.noiseRules, id)
	return nil
}

func (f *fakeStore) AddNoiseDrops(_ context.Context, counts map[int64]int64) error {
	f.lastAddedDrops = counts
	return nil
}

func (f *fakeStore) SetProjectParseBodies(_ context.Context, projectID int64, on bool) error {
	p, ok := f.projects[projectID]
	if !ok {
		return store.ErrNotFound
	}
	p.ParseBodies = on
	f.projects[projectID] = p
	return nil
}

// ---- context helpers (bypass HTTP auth for handler-level tests) ----

func apiCtx(projectID int64) context.Context {
	return authtest.Context(projectID, "api")
}

func adminCtx() context.Context {
	return authtest.Context(0, "admin")
}

var fixedNow = time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

func fixedClock() time.Time { return fixedNow }

// ---- golden render tests ----

func TestRenderProjects(t *testing.T) {
	got := renderProjects([]projectRow{
		{ID: 1, Slug: "payment-api", OpenIssues: 3, Logs24h: 1204},
		{ID: 2, Slug: "worker", OpenIssues: 0, Logs24h: 40},
	})
	want := "2 projects:\n" +
		"#1 payment-api — 3 open issues, 1204 logs (24h)\n" +
		"#2 worker — 0 open issues, 40 logs (24h)"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderProjects_Empty(t *testing.T) {
	got := renderProjects(nil)
	want := "0 projects:"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderIssues(t *testing.T) {
	issues := []core.Issue{
		{
			ID: 12, Count: 214, Severity: core.SeverityError, Title: "pg: connection refused",
			FirstSeen: fixedNow.Add(-48 * time.Hour), LastSeen: fixedNow.Add(-2 * time.Minute),
		},
		{
			ID: 9, Count: 12, Severity: core.SeverityError, Title: "job timeout after 30s",
			FirstSeen: fixedNow.Add(-18 * time.Minute), LastSeen: fixedNow.Add(-18 * time.Minute),
		},
	}
	hdr := issueListHeader{Status: "open", ProjectSlug: "payment-api", Environment: "production", SinceLabel: "24h"}
	got := renderIssues(issues, 20, fixedNow, hdr)
	want := "2 open issues in payment-api (production, 24h):\n" +
		"#12 ×214 [ERROR] pg: connection refused (last 2m ago, first 2d ago)\n" +
		"#9 ×12 [ERROR] job timeout after 30s (last 18m ago, first 18m ago)"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderIssues_Truncates(t *testing.T) {
	issues := make([]core.Issue, 0, 25)
	for i := int64(1); i <= 25; i++ {
		issues = append(issues, core.Issue{
			ID: i, Count: 1, Severity: core.SeverityWarn, Title: "x",
			FirstSeen: fixedNow, LastSeen: fixedNow,
		})
	}
	hdr := issueListHeader{Status: "open"}
	got := renderIssues(issues, 20, fixedNow, hdr)
	lines := len(splitLines(got))
	// header + 20 rows + truncation notice
	if lines != 22 {
		t.Fatalf("got %d lines, want 22:\n%s", lines, got)
	}
	last := splitLines(got)[len(splitLines(got))-1]
	if last != "(+5 more — refine filters)" {
		t.Errorf("last line = %q, want %q", last, "(+5 more — refine filters)")
	}
	first := splitLines(got)[0]
	if first != "25 open issues:" {
		t.Errorf("first line = %q, want %q", first, "25 open issues:")
	}
}

func TestRenderIssue(t *testing.T) {
	iss := core.Issue{
		ID: 12, Fingerprint: "fp-abc", Title: "pg: connection refused",
		Severity: core.SeverityError, Status: core.StatusOpen,
		FirstSeen: fixedNow.Add(-48 * time.Hour), LastSeen: fixedNow.Add(-2 * time.Minute),
		Count: 214,
	}
	events := []core.Event{
		{
			LogID: 100, IssueID: 12, Time: fixedNow.Add(-48 * time.Hour),
			Log: core.Log{
				ID: 100, Service: "payment-api", Environment: "production", Release: "v1.0.0",
				Body: "pg: connection refused",
			},
		},
		{
			LogID: 101, IssueID: 12, Time: fixedNow.Add(-2 * time.Minute),
			Log: core.Log{
				ID: 101, Service: "payment-api", Environment: "production", Release: "v1.1.0",
				Body: "pg: connection refused",
				Attrs: map[string]string{
					"exception.type":       "*fiber.Error",
					"exception.stacktrace": "main.go:42\nmain.go:10",
				},
			},
		},
		{
			LogID: 102, IssueID: 12, Time: fixedNow.Add(-3 * 24 * time.Hour), // outside 7d window rows still counted per-event but > 7d ago
			Log: core.Log{ID: 102, Service: "payment-api", Environment: "staging", Release: "v1.0.0", Body: "pg: connection refused"},
		},
	}
	got := renderIssue(iss, events, fixedNow)
	want := "#12 [ERROR] pg: connection refused\n" +
		"status: open  fingerprint: fp-abc\n" +
		"first seen: 2026-08-04T12:00:00Z  last seen: 2026-08-06T11:58:00Z  count: 214\n" +
		"environments: production, staging\n" +
		"releases: v1.0.0, v1.1.0\n" +
		"7d: 0 0 0 1 1 0 1\n" +
		"\n" +
		"newest event (2026-08-06T11:58:00Z):\n" +
		"service: payment-api\n" +
		"pg: connection refused\n" +
		"exception.stacktrace:\n" +
		"main.go:42\n" +
		"main.go:10\n" +
		"exception.type=*fiber.Error"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderIssue_CapsOversizedBodyAndStacktrace(t *testing.T) {
	iss := core.Issue{
		ID: 12, Fingerprint: "fp-abc", Title: "boom",
		Severity: core.SeverityError, Status: core.StatusOpen,
		FirstSeen: fixedNow, LastSeen: fixedNow, Count: 1,
	}
	bigBody := strings.Repeat("b", 2500)
	bigStack := strings.Repeat("s", 5000)
	events := []core.Event{
		{
			LogID: 1, IssueID: 12, Time: fixedNow,
			Log: core.Log{
				ID: 1, Service: "svc", Body: bigBody,
				Attrs: map[string]string{"exception.stacktrace": bigStack},
			},
		},
	}
	got := renderIssue(iss, events, fixedNow)

	wantBody := strings.Repeat("b", maxBodyChars) + "… (truncated, 2500 chars total)"
	wantStack := strings.Repeat("s", maxStacktraceChars) + "… (truncated, 5000 chars total)"

	lines := splitLines(got)
	var gotBody, gotStack string
	for i, l := range lines {
		if l == "service: svc" && i+1 < len(lines) {
			gotBody = lines[i+1]
		}
		if l == "exception.stacktrace:" && i+1 < len(lines) {
			gotStack = lines[i+1]
		}
	}
	if gotBody != wantBody {
		t.Errorf("body line len=%d, want len=%d (mismatch)", len(gotBody), len(wantBody))
	}
	if gotStack != wantStack {
		t.Errorf("stacktrace line len=%d, want len=%d (mismatch)", len(gotStack), len(wantStack))
	}
	if !strings.Contains(gotBody, "(truncated, 2500 chars total)") {
		t.Errorf("body missing truncation marker: %q", gotBody)
	}
	if !strings.Contains(gotStack, "(truncated, 5000 chars total)") {
		t.Errorf("stacktrace missing truncation marker: %q", gotStack)
	}
}

func TestRenderIssue_UnderCap_NotTruncated(t *testing.T) {
	iss := core.Issue{ID: 1, FirstSeen: fixedNow, LastSeen: fixedNow}
	events := []core.Event{{LogID: 1, IssueID: 1, Time: fixedNow, Log: core.Log{ID: 1, Service: "svc", Body: "short body"}}}
	got := renderIssue(iss, events, fixedNow)
	if !strings.Contains(got, "short body") || strings.Contains(got, "truncated") {
		t.Errorf("got:\n%s\nwant untruncated short body", got)
	}
}

func TestTruncateChars(t *testing.T) {
	short := "hello"
	if got := truncateChars(short, 10); got != short {
		t.Errorf("got %q, want unchanged %q", got, short)
	}
	long := strings.Repeat("x", 10)
	got := truncateChars(long, 5)
	want := "xxxxx… (truncated, 10 chars total)"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderLogRow_CapsOversizedFirstLine(t *testing.T) {
	body := strings.Repeat("x", 500) + "\nsecond line"
	l := core.Log{ID: 1, Time: fixedNow, Severity: core.SeverityInfo, Service: "svc", Body: body}
	got := renderLogRow(l, "")
	if strings.Contains(got, "second line") {
		t.Errorf("row leaked past first line: %q", got)
	}
	if !strings.Contains(got, "(truncated, 500 chars total)") {
		t.Errorf("row missing truncation marker: %q", got)
	}
}

func TestRenderLogs(t *testing.T) {
	logs := []core.Log{
		{ID: 5, Time: fixedNow.Add(-5 * time.Minute), Severity: core.SeverityError, Service: "payment-api", Body: "pg: connection refused\nfull trace here"},
	}
	hdr := logListHeader{ProjectSlug: "payment-api", Query: "connection"}
	got := renderLogs(logs, 20, hdr)
	want := "1 logs in payment-api (q=\"connection\"):\n" +
		"2026-08-06T11:55:00Z [ERROR] payment-api pg: connection refused id=5"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderLogContext(t *testing.T) {
	logs := []core.Log{
		{ID: 4, Time: fixedNow.Add(-2 * time.Minute), Severity: core.SeverityInfo, Service: "svc", Body: "before"},
		{ID: 5, Time: fixedNow.Add(-1 * time.Minute), Severity: core.SeverityError, Service: "svc", Body: "target"},
		{ID: 6, Time: fixedNow, Severity: core.SeverityInfo, Service: "svc", Body: "after"},
	}
	got := renderLogContext(logs, 5)
	want := "3 logs:\n" +
		"2026-08-06T11:58:00Z [INFO] svc before id=4\n" +
		"→ 2026-08-06T11:59:00Z [ERROR] svc target id=5\n" +
		"2026-08-06T12:00:00Z [INFO] svc after id=6"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderStats(t *testing.T) {
	st := store.Stats{
		Logs: 100, Events: 10, OpenIssues: 2,
		PerDay: []store.DayCount{
			{Day: "2026-08-05", Logs: 40, Events: 4},
			{Day: "2026-08-06", Logs: 60, Events: 6},
		},
	}
	got := renderStats(st)
	want := "logs: 100  events: 10  open issues: 2\n" +
		"2026-08-05: 40 logs, 4 events\n" +
		"2026-08-06: 60 logs, 6 events"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderNoiseRules(t *testing.T) {
	rows := []store.NoiseRuleRow{
		{NoiseRule: core.NoiseRule{ID: 1, Kind: core.NoiseSeverityFloor, Service: "traefik", Severity: core.SeverityWarn, Enabled: true}, DroppedCount: 412},
		{NoiseRule: core.NoiseRule{ID: 2, Kind: core.NoiseDropMatch, Service: "api", Pattern: "health check ok", Enabled: false}, DroppedCount: 0},
		{NoiseRule: core.NoiseRule{ID: 3, Kind: core.NoiseSample, Service: "", Severity: core.SeverityInfo, N: 10, Enabled: true}, DroppedCount: 87},
	}
	got := renderNoiseRules(rows, "payment-api")
	want := "3 noise rules in payment-api:\n" +
		"#1 severity_floor service=traefik severity=warn enabled=true dropped=412\n" +
		"#2 drop_match service=api pattern=\"health check ok\" enabled=false dropped=0\n" +
		"#3 sample service=any severity=info n=10 enabled=true dropped=87"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderNoiseRules_Empty(t *testing.T) {
	got := renderNoiseRules(nil, "")
	if got != "0 noise rules:" {
		t.Errorf("got %q, want %q", got, "0 noise rules:")
	}
}

func TestRenderNoiseRule(t *testing.T) {
	row := store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 9, Kind: core.NoiseSeverityFloor, Service: "traefik", Severity: core.SeverityWarn, Enabled: true}}
	got := renderNoiseRule(row)
	want := "rule #9 severity_floor service=traefik severity=warn enabled=true dropped=0"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRenderNoiseReport(t *testing.T) {
	services := []store.ServiceCount{
		{Service: "traefik", Logs: 40000},
		{Service: "checkout-api", Logs: 1200},
	}
	rules := []store.NoiseRuleRow{
		{NoiseRule: core.NoiseRule{ID: 1, Kind: core.NoiseSeverityFloor, Service: "traefik", Severity: core.SeverityWarn, Enabled: true}, DroppedCount: 38000},
	}
	got := renderNoiseReport(services, rules, 38000, "24h")
	want := "noise report (24h): 2 services, 1 rules, 38000 total dropped\n" +
		"top services by volume:\n" +
		"traefik: 40000 logs\n" +
		"checkout-api: 1200 logs\n" +
		"rules:\n" +
		"#1 severity_floor service=traefik severity=warn enabled=true dropped=38000"
	if got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	lines = append(lines, s[start:])
	return lines
}

// ---- handler-level tests (call tool handlers directly, not over the wire) ----

func newTestMCPServer() (*Server, *fakeStore) {
	fs := newFakeStore()
	engine := rules.New(fs, fs)
	s := New(fs, fs, fs, engine)
	s.clock = fixedClock
	return s, fs
}

func TestListProjects_ProjectKey_SeesOnlyOwnProject(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.projects[1] = core.Project{ID: 1, Slug: "acme"}
	fs.projects[2] = core.Project{ID: 2, Slug: "other"}
	fs.stats[1] = store.Stats{OpenIssues: 3, Logs: 10}
	fs.stats[2] = store.Stats{OpenIssues: 99, Logs: 999}

	res, _, err := s.listProjects(apiCtx(1), nil, listProjectsInput{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	text := textOf(t, res)
	want := "1 projects:\n#1 acme — 3 open issues, 10 logs (24h)"
	if text != want {
		t.Errorf("got:\n%s\nwant:\n%s", text, want)
	}
}

func TestListProjects_AdminKey_SeesAll(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.projects[1] = core.Project{ID: 1, Slug: "acme"}
	fs.projects[2] = core.Project{ID: 2, Slug: "other"}
	fs.stats[1] = store.Stats{OpenIssues: 3, Logs: 10}
	fs.stats[2] = store.Stats{OpenIssues: 1, Logs: 5}

	res, _, err := s.listProjects(adminCtx(), nil, listProjectsInput{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	text := textOf(t, res)
	if text != "2 projects:\n#1 acme — 3 open issues, 10 logs (24h)\n#2 other — 1 open issues, 5 logs (24h)" {
		t.Errorf("got:\n%s", text)
	}
}

func TestListIssues_ProjectKey_IgnoresProjectInput(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.projects[1] = core.Project{ID: 1, Slug: "acme"}
	fs.issueList = []core.Issue{{ID: 1, ProjectID: 1, Title: "boom", Severity: core.SeverityError, FirstSeen: fixedNow, LastSeen: fixedNow, Count: 1}}

	_, _, err := s.listIssues(apiCtx(1), nil, listIssuesInput{Project: 999})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fs.lastIssueFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1 (caller's own project)", fs.lastIssueFilter.ProjectID)
	}
}

func TestListIssues_AdminKey_UsesProjectInput(t *testing.T) {
	s, fs := newTestMCPServer()
	_, _, err := s.listIssues(adminCtx(), nil, listIssuesInput{Project: 7})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fs.lastIssueFilter.ProjectID != 7 {
		t.Errorf("filter.ProjectID = %d, want 7", fs.lastIssueFilter.ProjectID)
	}
}

func TestListIssues_DefaultStatusOpen(t *testing.T) {
	s, fs := newTestMCPServer()
	_, _, err := s.listIssues(apiCtx(1), nil, listIssuesInput{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fs.lastIssueFilter.Status != core.StatusOpen {
		t.Errorf("filter.Status = %q, want open", fs.lastIssueFilter.Status)
	}
}

func TestListIssues_StoreError_ReturnsGenericInternalError(t *testing.T) {
	s, fs := newTestMCPServer()
	// Simulate a raw, sqlite-flavored driver error — the kind of thing
	// that must never reach an MCP client verbatim.
	fs.forcedErr = fmt.Errorf("querying issues: %w", errors.New("sqlite: SQL logic error near \"WHRE\": syntax error (code:1)"))

	res, _, err := s.listIssues(apiCtx(1), nil, listIssuesInput{})
	if err != nil {
		t.Fatalf("err = %v, want nil (tool error goes in result)", err)
	}
	if !res.IsError {
		t.Fatalf("IsError = false, want true")
	}
	text := textOf(t, res)
	if text != "internal error" {
		t.Errorf("text = %q, want %q", text, "internal error")
	}
	if strings.Contains(text, "sqlite") || strings.Contains(text, "WHRE") {
		t.Errorf("text leaked raw store error detail: %q", text)
	}
}

func TestGetIssue_APIKey_OtherProjectIssue_NotFoundError(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Title: "boom"}

	res, _, err := s.getIssue(apiCtx(1), nil, getIssueInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v, want nil (tool error goes in result)", err)
	}
	if !res.IsError {
		t.Fatalf("IsError = false, want true")
	}
	if textOf(t, res) != "not found" {
		t.Errorf("text = %q, want %q", textOf(t, res), "not found")
	}
}

func TestGetIssue_UnknownID_NotFoundError(t *testing.T) {
	s, _ := newTestMCPServer()
	res, _, err := s.getIssue(apiCtx(1), nil, getIssueInput{ID: 999})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
}

func TestGetIssue_AdminKey_SeesAnyProject(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Title: "boom", FirstSeen: fixedNow, LastSeen: fixedNow}
	res, _, err := s.getIssue(adminCtx(), nil, getIssueInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if res.IsError {
		t.Fatalf("IsError = true, want false: %s", textOf(t, res))
	}
}

func TestResolveIssue_HappyPath(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 1, Status: core.StatusOpen}

	res, _, err := s.resolveIssue(apiCtx(1), nil, resolveIssueInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if textOf(t, res) != "issue #1 resolved" {
		t.Errorf("text = %q", textOf(t, res))
	}
	if fs.issues[1].Status != core.StatusResolved {
		t.Errorf("status = %q, want resolved", fs.issues[1].Status)
	}
}

func TestIgnoreIssue_HappyPath(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 1, Status: core.StatusOpen}

	res, _, err := s.ignoreIssue(apiCtx(1), nil, ignoreIssueInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if textOf(t, res) != "issue #1 ignored" {
		t.Errorf("text = %q", textOf(t, res))
	}
	if fs.issues[1].Status != core.StatusIgnored {
		t.Errorf("status = %q, want ignored", fs.issues[1].Status)
	}
}

func TestResolveIssue_APIKey_OtherProject_NotFoundError(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Status: core.StatusOpen}

	res, _, err := s.resolveIssue(apiCtx(1), nil, resolveIssueInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
	if fs.issues[1].Status != core.StatusOpen {
		t.Errorf("status changed despite scoping failure: %q", fs.issues[1].Status)
	}
}

func TestResolveIssue_UnknownID_NotFoundError(t *testing.T) {
	s, _ := newTestMCPServer()
	res, _, err := s.resolveIssue(apiCtx(1), nil, resolveIssueInput{ID: 999})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
}

func TestSearchLogs_ProjectKey_IgnoresProjectInput(t *testing.T) {
	s, fs := newTestMCPServer()
	_, _, err := s.searchLogs(apiCtx(1), nil, searchLogsInput{Project: 999})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fs.lastLogFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1", fs.lastLogFilter.ProjectID)
	}
}

func TestGetLogContext_APIKey_OtherProjectLog_NotFoundError(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.logList = []core.Log{{ID: 5, ProjectID: 2, Body: "target"}}

	res, _, err := s.getLogContext(apiCtx(1), nil, getLogContextInput{LogID: 5})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
}

func TestGetStats_ProjectKey_IgnoresProjectInput(t *testing.T) {
	s, fs := newTestMCPServer()
	_, _, err := s.getStats(apiCtx(1), nil, getStatsInput{Project: 999})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if fs.lastStatsFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1", fs.lastStatsFilter.ProjectID)
	}
}

// ---- list_noise_rules ----

func TestListNoiseRules_ProjectKey_SeesOnlyOwnProject(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.projects[1] = core.Project{ID: 1, Slug: "acme"}
	fs.noiseRules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseSeverityFloor, Service: "traefik", Severity: core.SeverityWarn, Enabled: true}}
	fs.noiseRules[2] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 2, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true}}

	// Admin-only input, requesting another project's rules — must be
	// ignored for a project-scoped key.
	res, _, err := s.listNoiseRules(apiCtx(1), nil, listNoiseRulesInput{ProjectID: 2})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	text := textOf(t, res)
	want := "1 noise rules in acme:\n" +
		"#1 severity_floor service=traefik severity=warn enabled=true dropped=0"
	if text != want {
		t.Errorf("got:\n%s\nwant:\n%s", text, want)
	}
}

func TestListNoiseRules_AdminKey_NoFilter_SeesAllProjects(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.noiseRules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseSample, Severity: core.SeverityInfo, N: 5, Enabled: true}}
	fs.noiseRules[2] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 2, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true}}

	res, _, err := s.listNoiseRules(adminCtx(), nil, listNoiseRulesInput{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !strings.HasPrefix(textOf(t, res), "2 noise rules:") {
		t.Errorf("got %q, want a 2-rule listing with no project scope", textOf(t, res))
	}
}

// ---- upsert_noise_rule ----

func TestUpsertNoiseRule_ProjectKey_CreatesUnderOwnProject(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.projects[1] = core.Project{ID: 1, Slug: "acme"}

	res, _, err := s.upsertNoiseRule(apiCtx(1), nil, upsertNoiseRuleInput{
		ProjectID: 999, // must be overridden by the caller's own project
		Kind:      string(core.NoiseSeverityFloor),
		Service:   "traefik",
		Severity:  "warn",
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if res.IsError {
		t.Fatalf("IsError = true: %s", textOf(t, res))
	}
	stored, ok := fs.noiseRules[1]
	if !ok {
		t.Fatalf("no rule stored")
	}
	if stored.ProjectID != 1 {
		t.Errorf("stored ProjectID = %d, want 1 (caller's own project, not the input's 999)", stored.ProjectID)
	}
	if stored.Severity != core.SeverityWarn {
		t.Errorf("stored Severity = %v, want SeverityWarn", stored.Severity)
	}
}

func TestUpsertNoiseRule_UnknownKind_ErrorsWithoutStoring(t *testing.T) {
	s, fs := newTestMCPServer()

	res, _, err := s.upsertNoiseRule(apiCtx(1), nil, upsertNoiseRuleInput{Kind: "bogus"})
	if err != nil {
		t.Fatalf("err = %v, want nil (tool error goes in result)", err)
	}
	if !res.IsError {
		t.Fatalf("IsError = false, want true")
	}
	if len(fs.noiseRules) != 0 {
		t.Errorf("a rule was stored despite the invalid kind")
	}
}

func TestUpsertNoiseRule_UnknownSeverity_ErrorsWithoutStoring(t *testing.T) {
	s, fs := newTestMCPServer()

	res, _, err := s.upsertNoiseRule(apiCtx(1), nil, upsertNoiseRuleInput{
		Kind: string(core.NoiseSeverityFloor), Severity: "wrn",
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "severity: unknown value" {
		t.Errorf("got IsError=%v text=%q, want the strict-severity rejection", res.IsError, textOf(t, res))
	}
	if len(fs.noiseRules) != 0 {
		t.Errorf("a rule was stored despite the invalid severity")
	}
}

func TestUpsertNoiseRule_ProjectKey_UpdateCrossProjectRule_NotFoundError(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.noiseRules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true}}

	res, _, err := s.upsertNoiseRule(apiCtx(1), nil, upsertNoiseRuleInput{
		ID: 1, Kind: string(core.NoiseDropMatch), Pattern: "changed",
	})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
	if fs.noiseRules[1].Pattern != "boom" {
		t.Errorf("rule was mutated despite cross-project ownership check failing")
	}
}

// ---- delete_noise_rule ----

func TestDeleteNoiseRule_ProjectKey_OwnRule_Succeeds(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.noiseRules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true}}

	res, _, err := s.deleteNoiseRule(apiCtx(1), nil, deleteNoiseRuleInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if res.IsError {
		t.Fatalf("IsError = true: %s", textOf(t, res))
	}
	if textOf(t, res) != "noise rule #1 deleted" {
		t.Errorf("text = %q", textOf(t, res))
	}
	if _, ok := fs.noiseRules[1]; ok {
		t.Errorf("rule still present after delete")
	}
}

func TestDeleteNoiseRule_ProjectKey_CrossProjectRule_NotFoundError(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.noiseRules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true}}

	res, _, err := s.deleteNoiseRule(apiCtx(1), nil, deleteNoiseRuleInput{ID: 1})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
	if _, ok := fs.noiseRules[1]; !ok {
		t.Errorf("rule was deleted despite belonging to another project")
	}
}

// ---- get_noise_report ----

func TestGetNoiseReport_ProjectKey_IgnoresProjectInput(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.serviceCounts[1] = []store.ServiceCount{{Service: "traefik", Logs: 40000}}
	fs.noiseRules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseSeverityFloor, Service: "traefik", Severity: core.SeverityWarn, Enabled: true}, DroppedCount: 38000}
	fs.noiseRules[2] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 2, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true}, DroppedCount: 500}

	res, _, err := s.getNoiseReport(apiCtx(1), nil, getNoiseReportInput{ProjectID: 999})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if res.IsError {
		t.Fatalf("IsError = true: %s", textOf(t, res))
	}
	text := textOf(t, res)
	if !strings.HasPrefix(text, "noise report (24h): 1 services, 1 rules, 38000 total dropped") {
		t.Errorf("got:\n%s", text)
	}
	if strings.Contains(text, "#2") {
		t.Errorf("report leaked another project's rule: %q", text)
	}
}

func TestGetNoiseReport_AdminKey_MissingProjectID_Errors(t *testing.T) {
	s, _ := newTestMCPServer()

	res, _, err := s.getNoiseReport(adminCtx(), nil, getNoiseReportInput{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "project_id: required for admin keys" {
		t.Errorf("got IsError=%v text=%q, want the required-project_id rejection", res.IsError, textOf(t, res))
	}
}

// ---- set_project_parse ----

func TestSetProjectParse_ProjectKey_TogglesOwnProject(t *testing.T) {
	s, fs := newTestMCPServer()
	fs.projects[1] = core.Project{ID: 1, Slug: "acme", ParseBodies: true}
	fs.projects[999] = core.Project{ID: 999, Slug: "other", ParseBodies: true}

	res, _, err := s.setProjectParse(apiCtx(1), nil, setProjectParseInput{ProjectID: 999, ParseBodies: false})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if res.IsError {
		t.Fatalf("IsError = true: %s", textOf(t, res))
	}
	if fs.projects[1].ParseBodies {
		t.Errorf("project 1's parse_bodies was not flipped")
	}
	if !fs.projects[999].ParseBodies {
		t.Errorf("project_id input (999, not the caller's project) should have been ignored, but project 999 was flipped")
	}
}

func TestSetProjectParse_UnknownProject_NotFoundError(t *testing.T) {
	s, _ := newTestMCPServer()

	res, _, err := s.setProjectParse(adminCtx(), nil, setProjectParseInput{ProjectID: 999, ParseBodies: true})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if !res.IsError || textOf(t, res) != "not found" {
		t.Errorf("got IsError=%v text=%q, want not found error", res.IsError, textOf(t, res))
	}
}

func textOf(t *testing.T, res *mcpsdk.CallToolResult) string {
	t.Helper()
	if len(res.Content) != 1 {
		t.Fatalf("Content len = %d, want 1", len(res.Content))
	}
	tc, ok := res.Content[0].(*mcpsdk.TextContent)
	if !ok {
		t.Fatalf("Content[0] is %T, want *TextContent", res.Content[0])
	}
	return tc.Text
}

// ---- transport-level test: real HTTP handler, real auth, MCP client over the wire ----

func TestMCP_OverTheWire(t *testing.T) {
	fs := newFakeStore()
	fs.keys["agt_api_valid"] = struct {
		projectID int64
		kind      string
	}{projectID: 1, kind: "api"}
	fs.issueList = []core.Issue{{
		ID: 1, ProjectID: 1, Title: "boom", Severity: core.SeverityError,
		FirstSeen: fixedNow, LastSeen: fixedNow, Count: 3,
	}}

	a := auth.New(fs, []byte{})
	engine := rules.New(fs, fs)
	srv := New(fs, fs, fs, engine)
	srv.clock = fixedClock
	mux := http.NewServeMux()
	srv.Mount(mux, a)
	httpSrv := httptest.NewServer(mux)
	defer httpSrv.Close()

	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	transport := &mcpsdk.StreamableClientTransport{
		Endpoint: httpSrv.URL + "/mcp",
		HTTPClient: &http.Client{
			Transport: bearerTransport{key: "agt_api_valid"},
		},
	}
	cs, err := client.Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = cs.Close() }()

	tools, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools.Tools) != 13 {
		names := make([]string, len(tools.Tools))
		for i, tl := range tools.Tools {
			names[i] = tl.Name
		}
		t.Fatalf("got %d tools, want 13: %v", len(tools.Tools), names)
	}

	res, err := cs.CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "list_issues",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	text := textOf(t, res)
	if text == "" {
		t.Fatalf("empty text content")
	}
	// Loose assertion: proves the wire works. Exact rendering is covered by
	// the handler-level golden tests.
	if !contains(text, "#1") || !contains(text, "boom") {
		t.Errorf("text = %q, want to contain #1 and boom", text)
	}
}

type bearerTransport struct {
	key string
}

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.key)
	return http.DefaultTransport.RoundTrip(req)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (func() bool {
		for i := 0; i+len(substr) <= len(s); i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	})()
}
