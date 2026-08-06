package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// fakeStore implements store.Reader and store.Admin over maps. It records
// the last filter/args it received on each method so tests can assert
// query-param -> filter mapping, and returns canned data for happy paths.
type fakeStore struct {
	projects map[int64]core.Project
	nextID   int64
	keys     map[string]struct {
		projectID int64
		kind      string
	}

	issues      map[int64]core.Issue
	issueEvents map[int64][]core.Event
	issueList   []core.Issue

	logList []core.Log

	stats         store.Stats
	serviceCounts []store.ServiceCount

	rules      map[int64]store.NoiseRuleRow
	nextRuleID int64
	dropCalls  []map[int64]int64

	lastIssueFilter store.IssueFilter
	lastLogFilter   store.LogFilter
	lastStatsFilter store.StatsFilter
	lastContextID   int64
	lastContextN    int
	contextNotFound bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		projects: make(map[int64]core.Project),
		keys: make(map[string]struct {
			projectID int64
			kind      string
		}),
		issues:      make(map[int64]core.Issue),
		issueEvents: make(map[int64][]core.Event),
		rules:       make(map[int64]store.NoiseRuleRow),
	}
}

func (f *fakeStore) Issues(_ context.Context, filter store.IssueFilter) ([]core.Issue, error) {
	f.lastIssueFilter = filter
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
	return f.stats, nil
}

func (f *fakeStore) ServiceCounts(_ context.Context, _ int64, _ time.Time) ([]store.ServiceCount, error) {
	return f.serviceCounts, nil
}

func (f *fakeStore) NoiseRules(_ context.Context, projectID int64) ([]store.NoiseRuleRow, error) {
	ids := make([]int64, 0, len(f.rules))
	for id := range f.rules {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []store.NoiseRuleRow
	for _, id := range ids {
		row := f.rules[id]
		if projectID == 0 || row.ProjectID == projectID {
			out = append(out, row)
		}
	}
	return out, nil
}

func (f *fakeStore) UpsertNoiseRule(_ context.Context, r core.NoiseRule) (store.NoiseRuleRow, error) {
	if r.ID == 0 {
		f.nextRuleID++
		r.ID = f.nextRuleID
	} else if _, ok := f.rules[r.ID]; !ok {
		return store.NoiseRuleRow{}, store.ErrNotFound
	}
	row := store.NoiseRuleRow{NoiseRule: r}
	if existing, ok := f.rules[r.ID]; ok {
		row.DroppedCount = existing.DroppedCount
		row.CreatedAt = existing.CreatedAt
	}
	f.rules[r.ID] = row
	return row, nil
}

func (f *fakeStore) DeleteNoiseRule(_ context.Context, id int64) error {
	if _, ok := f.rules[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.rules, id)
	return nil
}

func (f *fakeStore) AddNoiseDrops(_ context.Context, counts map[int64]int64) error {
	cp := make(map[int64]int64, len(counts))
	for id, n := range counts {
		cp[id] = n
		if row, ok := f.rules[id]; ok {
			row.DroppedCount += n
			f.rules[id] = row
		}
	}
	f.dropCalls = append(f.dropCalls, cp)
	return nil
}

func (f *fakeStore) SetProjectParseBodies(_ context.Context, projectID int64, on bool) error {
	p, ok := f.projects[projectID]
	if !ok {
		p = core.Project{ID: projectID}
	}
	p.ParseBodies = on
	f.projects[projectID] = p
	return nil
}

func (f *fakeStore) CreateProject(_ context.Context, name string, retentionDays int) (core.Project, error) {
	f.nextID++
	p := core.Project{ID: f.nextID, Name: name, Slug: "slug-" + name, RetentionDays: retentionDays, CreatedAt: time.Now().UTC()}
	f.projects[p.ID] = p
	return p, nil
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

func (f *fakeStore) MintKey(_ context.Context, projectID int64, kind string) (string, error) {
	if _, ok := f.projects[projectID]; !ok {
		return "", store.ErrNotFound
	}
	plaintext := "agt_minted_" + kind
	f.keys[plaintext] = struct {
		projectID int64
		kind      string
	}{projectID, kind}
	return plaintext, nil
}

func (f *fakeStore) LookupKey(_ context.Context, plaintext string) (int64, string, error) {
	e, ok := f.keys[plaintext]
	if !ok {
		return 0, "", store.ErrNotFound
	}
	return e.projectID, e.kind, nil
}

const (
	validAPIKey = "agt_api_valid"   // project 1
	adminKey    = "agt_admin_valid" // instance-level, unscoped
)

func newTestServer(fs *fakeStore) *httptest.Server {
	fs.keys[validAPIKey] = struct {
		projectID int64
		kind      string
	}{projectID: 1, kind: "api"}
	fs.keys[adminKey] = struct {
		projectID int64
		kind      string
	}{projectID: 0, kind: "admin"}

	a := auth.New(fs, []byte{})
	engine := rules.New(fs, fs)
	api := New(fs, fs, fs, engine)
	mux := http.NewServeMux()
	api.Mount(mux, a)
	return httptest.NewServer(mux)
}

func doReq(t *testing.T, srv *httptest.Server, method, path, key string, body []byte) *http.Response {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, srv.URL+path, reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

func TestProjects_Create_HappyPath(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects", adminKey, []byte(`{"name":"acme","retention_days":30}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var got struct {
		ID            int64  `json:"id"`
		Name          string `json:"name"`
		Slug          string `json:"slug"`
		RetentionDays int    `json:"retention_days"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "acme" || got.RetentionDays != 30 || got.ID == 0 || got.Slug == "" {
		t.Errorf("got %+v, want populated project fields", got)
	}
}

func TestProjects_List_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.projects[1] = core.Project{ID: 1, Name: "acme", Slug: "acme", RetentionDays: 14}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/projects", adminKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got []struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].Name != "acme" {
		t.Errorf("got %+v, want one project named acme", got)
	}
}

func TestProjects_MintKey_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.projects[1] = core.Project{ID: 1, Name: "acme"}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/1/keys", adminKey, []byte(`{"kind":"ingest"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	var got struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Key == "" {
		t.Errorf("got empty key")
	}
}

func TestProjects_MintKey_UnknownProject_Returns404(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/999/keys", adminKey, []byte(`{"kind":"ingest"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestProjects_Create_APIKey_Returns401(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects", validAPIKey, []byte(`{"name":"acme","retention_days":30}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestProjects_List_APIKey_Returns401(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/projects", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestProjects_Create_NonPositiveRetention_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects", adminKey, []byte(`{"name":"acme","retention_days":0}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIssues_List_HappyPath_FilterMapping(t *testing.T) {
	fs := newFakeStore()
	fs.issueList = []core.Issue{{
		ID: 1, ProjectID: 1, Fingerprint: "fp1", Title: "boom",
		Severity: core.SeverityError, Status: core.StatusOpen,
		FirstSeen: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		LastSeen:  time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Count:     5,
	}}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?project=1&env=prod&status=open&since=2026-01-01T00:00:00Z&limit=10", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got []struct {
		ID          int64  `json:"id"`
		ProjectID   int64  `json:"project_id"`
		Fingerprint string `json:"fingerprint"`
		Title       string `json:"title"`
		Severity    string `json:"severity"`
		Status      string `json:"status"`
		FirstSeen   string `json:"first_seen"`
		LastSeen    string `json:"last_seen"`
		Count       int64  `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d issues, want 1", len(got))
	}
	g := got[0]
	if g.ID != 1 || g.ProjectID != 1 || g.Fingerprint != "fp1" || g.Title != "boom" ||
		g.Severity != "ERROR" || g.Status != "open" || g.Count != 5 {
		t.Errorf("got %+v, unexpected fields", g)
	}
	if g.FirstSeen != "2026-01-01T00:00:00Z" {
		t.Errorf("FirstSeen = %q", g.FirstSeen)
	}

	f := fs.lastIssueFilter
	if f.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1", f.ProjectID)
	}
	if f.Environment != "prod" {
		t.Errorf("filter.Environment = %q, want prod", f.Environment)
	}
	if f.Status != core.StatusOpen {
		t.Errorf("filter.Status = %q, want open", f.Status)
	}
	if !f.Since.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("filter.Since = %v", f.Since)
	}
	if f.Limit != 10 {
		t.Errorf("filter.Limit = %d, want 10", f.Limit)
	}
}

func TestIssues_List_BadSince_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?since=not-a-time", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var got struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got.Error == "" {
		t.Errorf("expected non-empty error message")
	}
}

func TestIssues_List_BadLimit_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?limit=abc", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIssues_List_NegativeLimit_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?limit=-1", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var got struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if got.Error == "" {
		t.Errorf("expected error naming the limit param")
	}
}

func TestIssues_List_APIKey_IgnoresProjectParam(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1; ?project=999 must be ignored, not
	// trusted, since an api key is authoritatively scoped to its own
	// project.
	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?project=999", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastIssueFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1 (caller's own project)", fs.lastIssueFilter.ProjectID)
	}
}

func TestIssues_List_AdminKey_UsesProjectParam(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?project=5", adminKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastIssueFilter.ProjectID != 5 {
		t.Errorf("filter.ProjectID = %d, want 5 (admin keys are unscoped, query wins)", fs.lastIssueFilter.ProjectID)
	}
}

func TestIssues_Get_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 1, Fingerprint: "fp1", Title: "boom", Severity: core.SeverityWarn, Status: core.StatusOpen}
	fs.issueEvents[1] = []core.Event{{
		LogID: 10, IssueID: 1, Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Log: core.Log{ID: 10, ProjectID: 1, Body: "oops", Severity: core.SeverityWarn},
	}}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues/1", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Issue struct {
			ID    int64  `json:"id"`
			Title string `json:"title"`
		} `json:"issue"`
		Events []struct {
			LogID int64 `json:"log_id"`
			Log   struct {
				Body string `json:"body"`
			} `json:"log"`
		} `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Issue.ID != 1 || got.Issue.Title != "boom" {
		t.Errorf("issue = %+v", got.Issue)
	}
	if len(got.Events) != 1 || got.Events[0].LogID != 10 || got.Events[0].Log.Body != "oops" {
		t.Errorf("events = %+v", got.Events)
	}
}

func TestIssues_Get_UnknownID_Returns404(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues/999", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var got struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Error != "not found" {
		t.Errorf("error = %q, want %q", got.Error, "not found")
	}
}

func TestIssues_Get_APIKey_OtherProjectIssue_Returns404(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Title: "boom"}
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1; the issue belongs to project 2.
	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues/1", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	var got struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Error != "not found" {
		t.Errorf("error = %q, want %q (must not leak existence)", got.Error, "not found")
	}
}

func TestIssues_Get_AdminKey_SeesAnyProject(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Title: "boom"}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues/1", adminKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestIssues_UpdateStatus_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 1, Status: core.StatusOpen}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/1", validAPIKey, []byte(`{"status":"resolved"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if fs.issues[1].Status != core.StatusResolved {
		t.Errorf("issue status = %q, want resolved", fs.issues[1].Status)
	}
}

func TestIssues_UpdateStatus_BadValue_Returns400(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, Status: core.StatusOpen}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/1", validAPIKey, []byte(`{"status":"bogus"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIssues_UpdateStatus_UnknownID_Returns404(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/999", validAPIKey, []byte(`{"status":"resolved"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestIssues_UpdateStatus_APIKey_OtherProjectIssue_Returns404(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Status: core.StatusOpen}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/1", validAPIKey, []byte(`{"status":"resolved"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if fs.issues[1].Status != core.StatusOpen {
		t.Errorf("issue status = %q, want unchanged (open) — cross-project write must be blocked", fs.issues[1].Status)
	}
}

func TestIssues_UpdateStatus_AdminKey_OtherProjectIssue_Succeeds(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 2, Status: core.StatusOpen}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/1", adminKey, []byte(`{"status":"resolved"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if fs.issues[1].Status != core.StatusResolved {
		t.Errorf("issue status = %q, want resolved", fs.issues[1].Status)
	}
}

func TestIssues_UpdateStatus_UnknownField_Returns400(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, ProjectID: 1, Status: core.StatusOpen}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/1", validAPIKey, []byte(`{"status":"resolved","bogus":true}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestLogs_Search_HappyPath_FilterMapping(t *testing.T) {
	fs := newFakeStore()
	fs.logList = []core.Log{{
		ID: 5, ProjectID: 1, Body: "hello", Severity: core.SeverityError,
		Time: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet,
		"/api/v1/logs?project=1&q=hello&min_severity=error&service=api&env=prod&since=2026-01-01T00:00:00Z&until=2026-01-02T00:00:00Z&limit=25",
		validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got []struct {
		ID       int64  `json:"id"`
		Body     string `json:"body"`
		Severity string `json:"severity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].Body != "hello" || got[0].Severity != "ERROR" {
		t.Errorf("got %+v", got)
	}

	f := fs.lastLogFilter
	if f.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1", f.ProjectID)
	}
	if f.Query != "hello" {
		t.Errorf("filter.Query = %q, want hello", f.Query)
	}
	if f.MinSeverity != core.SeverityError {
		t.Errorf("filter.MinSeverity = %v, want SeverityError", f.MinSeverity)
	}
	if f.Service != "api" {
		t.Errorf("filter.Service = %q, want api", f.Service)
	}
	if f.Environment != "prod" {
		t.Errorf("filter.Environment = %q, want prod", f.Environment)
	}
	if f.Limit != 25 {
		t.Errorf("filter.Limit = %d, want 25", f.Limit)
	}
	if !f.Until.Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("filter.Until = %v", f.Until)
	}
}

func TestLogs_Search_NegativeLimit_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs?limit=-5", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestLogs_Search_APIKey_IgnoresProjectParam(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs?project=999", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastLogFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1 (caller's own project)", fs.lastLogFilter.ProjectID)
	}
}

func TestLogs_Search_AdminKey_UsesProjectParam(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs?project=7", adminKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastLogFilter.ProjectID != 7 {
		t.Errorf("filter.ProjectID = %d, want 7", fs.lastLogFilter.ProjectID)
	}
}

func TestLogs_Context_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.logList = []core.Log{
		{ID: 4, ProjectID: 1, Body: "before"},
		{ID: 5, ProjectID: 1, Body: "target"},
		{ID: 6, ProjectID: 1, Body: "after"},
	}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs/5/context?n=20", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got []struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 3 {
		t.Errorf("got %d logs, want 3", len(got))
	}
	if fs.lastContextID != 5 {
		t.Errorf("lastContextID = %d, want 5", fs.lastContextID)
	}
	if fs.lastContextN != 20 {
		t.Errorf("lastContextN = %d, want 20", fs.lastContextN)
	}
}

func TestLogs_Context_UnknownID_Returns404(t *testing.T) {
	fs := newFakeStore()
	fs.contextNotFound = true
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs/999/context", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestLogs_Context_NegativeN_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs/5/context?n=-1", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestLogs_Context_APIKey_OtherProjectLog_Returns404(t *testing.T) {
	fs := newFakeStore()
	fs.logList = []core.Log{{ID: 5, ProjectID: 2, Body: "target"}}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs/5/context", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestLogs_Context_AdminKey_SeesAnyProject(t *testing.T) {
	fs := newFakeStore()
	fs.logList = []core.Log{{ID: 5, ProjectID: 2, Body: "target"}}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs/5/context", adminKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestStats_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.stats = store.Stats{
		Logs: 100, Events: 10, OpenIssues: 2,
		PerDay: []store.DayCount{{Day: "2026-01-01", Logs: 50, Events: 5}},
	}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/stats?project=1&since=2026-01-01T00:00:00Z", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Logs       int64 `json:"logs"`
		Events     int64 `json:"events"`
		OpenIssues int64 `json:"open_issues"`
		PerDay     []struct {
			Day    string `json:"day"`
			Logs   int64  `json:"logs"`
			Events int64  `json:"events"`
		} `json:"per_day"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Logs != 100 || got.Events != 10 || got.OpenIssues != 2 {
		t.Errorf("got %+v", got)
	}
	if len(got.PerDay) != 1 || got.PerDay[0].Day != "2026-01-01" {
		t.Errorf("PerDay = %+v", got.PerDay)
	}
	if fs.lastStatsFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1", fs.lastStatsFilter.ProjectID)
	}
}

func TestStats_APIKey_IgnoresProjectParam(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/stats?project=999", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastStatsFilter.ProjectID != 1 {
		t.Errorf("filter.ProjectID = %d, want 1 (caller's own project)", fs.lastStatsFilter.ProjectID)
	}
}

func TestStats_AdminKey_UsesProjectParam(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/stats?project=9", adminKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastStatsFilter.ProjectID != 9 {
		t.Errorf("filter.ProjectID = %d, want 9", fs.lastStatsFilter.ProjectID)
	}
}

func TestNoAuth_Returns401(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues", "", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestNoiseRules_CreateListDelete_RoundTrip(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/1/noise-rules", validAPIKey,
		[]byte(`{"kind":"severity_floor","service":"traefik","severity":"warn","enabled":true}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var created struct {
		ID       int64  `json:"id"`
		Kind     string `json:"kind"`
		Severity string `json:"severity"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if created.ID == 0 || created.Kind != "severity_floor" || created.Severity != "warn" {
		t.Errorf("created = %+v", created)
	}

	listResp := doReq(t, srv, http.MethodGet, "/api/v1/projects/1/noise-rules", validAPIKey, nil)
	defer func() { _ = listResp.Body.Close() }()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200", listResp.StatusCode)
	}
	var listed []struct {
		ID           int64 `json:"id"`
		DroppedCount int64 `json:"dropped_count"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("listed = %+v, want single rule with id %d", listed, created.ID)
	}

	delResp := doReq(t, srv, http.MethodDelete, fmt.Sprintf("/api/v1/noise-rules/%d", created.ID), validAPIKey, nil)
	defer func() { _ = delResp.Body.Close() }()
	if delResp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", delResp.StatusCode)
	}

	listResp2 := doReq(t, srv, http.MethodGet, "/api/v1/projects/1/noise-rules", validAPIKey, nil)
	defer func() { _ = listResp2.Body.Close() }()
	var listed2 []struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(listResp2.Body).Decode(&listed2); err != nil {
		t.Fatalf("decode list2: %v", err)
	}
	if len(listed2) != 0 {
		t.Errorf("listed2 = %+v, want empty after delete", listed2)
	}
}

func TestNoiseRules_UnknownKind_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/1/noise-rules", validAPIKey,
		[]byte(`{"kind":"bogus","service":"traefik","enabled":true}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestNoiseRules_BadSeverity_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/1/noise-rules", validAPIKey,
		[]byte(`{"kind":"severity_floor","service":"traefik","severity":"wrn","enabled":true}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestNoiseRules_Delete_CrossProject_Returns404(t *testing.T) {
	fs := newFakeStore()
	fs.nextRuleID = 1
	fs.rules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true}}
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1; the rule belongs to project 2.
	resp := doReq(t, srv, http.MethodDelete, "/api/v1/noise-rules/1", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if _, ok := fs.rules[1]; !ok {
		t.Errorf("rule was deleted despite cross-project caller")
	}
}

func TestNoiseRules_Create_UpdateCrossProjectRule_Returns404(t *testing.T) {
	fs := newFakeStore()
	fs.nextRuleID = 1
	fs.rules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true}}
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1; trying to update rule 1 (project 2's)
	// must not succeed, and must not leak that the rule exists.
	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/1/noise-rules", validAPIKey,
		[]byte(`{"id":1,"kind":"drop_match","pattern":"y","enabled":true}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if fs.rules[1].Pattern != "x" {
		t.Errorf("rule was mutated despite cross-project caller: %+v", fs.rules[1])
	}
}

func TestNoiseRules_List_APIKey_IgnoresPathProject(t *testing.T) {
	fs := newFakeStore()
	fs.nextRuleID = 1
	fs.rules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true}}
	fs.rules[2] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 2, ProjectID: 2, Kind: core.NoiseDropMatch, Pattern: "y", Enabled: true}}
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1; path says project 2, must be ignored.
	resp := doReq(t, srv, http.MethodGet, "/api/v1/projects/2/noise-rules", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got []struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ID != 1 {
		t.Errorf("got = %+v, want only project 1's rule", got)
	}
}

func TestNoiseReport_Shape(t *testing.T) {
	fs := newFakeStore()
	fs.serviceCounts = []store.ServiceCount{{Service: "svc-a", Logs: 10}, {Service: "svc-b", Logs: 3}}
	fs.nextRuleID = 1
	fs.rules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true}, DroppedCount: 7}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/noise-report?project=1&hours=24", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		TopServices []struct {
			Service string `json:"service"`
			Logs    int64  `json:"logs"`
		} `json:"top_services"`
		Rules []struct {
			ID           int64 `json:"id"`
			DroppedCount int64 `json:"dropped_count"`
		} `json:"rules"`
		TotalDropped int64 `json:"total_dropped"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.TopServices) != 2 || got.TopServices[0].Service != "svc-a" {
		t.Errorf("top_services = %+v", got.TopServices)
	}
	if len(got.Rules) != 1 || got.Rules[0].DroppedCount != 7 {
		t.Errorf("rules = %+v", got.Rules)
	}
	if got.TotalDropped != 7 {
		t.Errorf("total_dropped = %d, want 7", got.TotalDropped)
	}
}

func TestStats_IncludesDropped(t *testing.T) {
	fs := newFakeStore()
	fs.nextRuleID = 1
	fs.rules[1] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 1, ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true}, DroppedCount: 5}
	fs.rules[2] = store.NoiseRuleRow{NoiseRule: core.NoiseRule{ID: 2, ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "y", Enabled: true}, DroppedCount: 2}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/stats", validAPIKey, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		Dropped int64 `json:"dropped"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Dropped != 7 {
		t.Errorf("dropped = %d, want 7", got.Dropped)
	}
}

func TestProjects_Patch_ParseBodiesToggle_VisibleInList(t *testing.T) {
	fs := newFakeStore()
	fs.projects[1] = core.Project{ID: 1, Name: "acme", Slug: "acme", RetentionDays: 14, ParseBodies: true}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/projects/1", adminKey, []byte(`{"parse_bodies":false}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	listResp := doReq(t, srv, http.MethodGet, "/api/v1/projects", adminKey, nil)
	defer func() { _ = listResp.Body.Close() }()
	var got []struct {
		ID          int64 `json:"id"`
		ParseBodies bool  `json:"parse_bodies"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ParseBodies {
		t.Errorf("got = %+v, want parse_bodies=false", got)
	}
}

func TestProjects_Patch_APIKey_OwnProject_Succeeds(t *testing.T) {
	fs := newFakeStore()
	fs.projects[1] = core.Project{ID: 1, Name: "acme", Slug: "acme", RetentionDays: 14, ParseBodies: true}
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1.
	resp := doReq(t, srv, http.MethodPatch, "/api/v1/projects/1", validAPIKey, []byte(`{"parse_bodies":false}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if fs.projects[1].ParseBodies {
		t.Errorf("project 1 ParseBodies = true, want false")
	}
}

func TestProjects_Patch_APIKey_OtherPathID_ScopeOverridesToOwnProject(t *testing.T) {
	fs := newFakeStore()
	fs.projects[1] = core.Project{ID: 1, Name: "acme", Slug: "acme", RetentionDays: 14, ParseBodies: true}
	fs.projects[2] = core.Project{ID: 2, Name: "other", Slug: "other", RetentionDays: 14, ParseBodies: true}
	srv := newTestServer(fs)
	defer srv.Close()

	// validAPIKey belongs to project 1; path says project 2. Scope-override
	// (consistent with noise-rule list/create) means this must affect only
	// the caller's own project (1), never project 2.
	resp := doReq(t, srv, http.MethodPatch, "/api/v1/projects/2", validAPIKey, []byte(`{"parse_bodies":false}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	listResp := doReq(t, srv, http.MethodGet, "/api/v1/projects", adminKey, nil)
	defer func() { _ = listResp.Body.Close() }()
	var got []struct {
		ID          int64 `json:"id"`
		ParseBodies bool  `json:"parse_bodies"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	byID := map[int64]bool{}
	for _, p := range got {
		byID[p.ID] = p.ParseBodies
	}
	if byID[1] {
		t.Errorf("project 1 (caller's own) ParseBodies = true, want false")
	}
	if !byID[2] {
		t.Errorf("project 2 (path id, not caller's own) ParseBodies = false, want unchanged (true) — must not leak cross-project write")
	}
}

func TestProjects_Patch_UnknownField_Returns400(t *testing.T) {
	fs := newFakeStore()
	fs.projects[1] = core.Project{ID: 1, Name: "acme"}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/projects/1", adminKey, []byte(`{"parse_bodies":false,"name":"nope"}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestProjects_Patch_UnknownProject_Returns404(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/projects/999", adminKey, []byte(`{"parse_bodies":false}`))
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
