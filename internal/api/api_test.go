package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
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

	stats store.Stats

	lastIssueFilter store.IssueFilter
	lastLogFilter   store.LogFilter
	lastStatsFilter store.StatsFilter
	lastContextID   int64
	lastContextN    int
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
	}
}

func (f *fakeStore) Issues(ctx context.Context, filter store.IssueFilter) ([]core.Issue, error) {
	f.lastIssueFilter = filter
	return f.issueList, nil
}

func (f *fakeStore) Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error) {
	iss, ok := f.issues[id]
	if !ok {
		return core.Issue{}, nil, store.ErrNotFound
	}
	return iss, f.issueEvents[id], nil
}

func (f *fakeStore) SearchLogs(ctx context.Context, filter store.LogFilter) ([]core.Log, error) {
	f.lastLogFilter = filter
	return f.logList, nil
}

func (f *fakeStore) LogContext(ctx context.Context, logID int64, n int) ([]core.Log, error) {
	f.lastContextID = logID
	f.lastContextN = n
	return f.logList, nil
}

func (f *fakeStore) Stats(ctx context.Context, filter store.StatsFilter) (store.Stats, error) {
	f.lastStatsFilter = filter
	return f.stats, nil
}

func (f *fakeStore) CreateProject(ctx context.Context, name string, retentionDays int) (core.Project, error) {
	f.nextID++
	p := core.Project{ID: f.nextID, Name: name, Slug: "slug-" + name, RetentionDays: retentionDays, CreatedAt: time.Now().UTC()}
	f.projects[p.ID] = p
	return p, nil
}

func (f *fakeStore) Projects(ctx context.Context) ([]core.Project, error) {
	out := make([]core.Project, 0, len(f.projects))
	for _, p := range f.projects {
		out = append(out, p)
	}
	return out, nil
}

func (f *fakeStore) SetIssueStatus(ctx context.Context, id int64, s core.IssueStatus) error {
	iss, ok := f.issues[id]
	if !ok {
		return store.ErrNotFound
	}
	iss.Status = s
	f.issues[id] = iss
	return nil
}

func (f *fakeStore) MintKey(ctx context.Context, projectID int64, kind string) (string, error) {
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

func (f *fakeStore) LookupKey(ctx context.Context, plaintext string) (int64, string, error) {
	e, ok := f.keys[plaintext]
	if !ok {
		return 0, "", store.ErrNotFound
	}
	return e.projectID, e.kind, nil
}

const validAPIKey = "agt_api_valid"

func newTestServer(fs *fakeStore) *httptest.Server {
	fs.keys[validAPIKey] = struct {
		projectID int64
		kind      string
	}{projectID: 1, kind: "api"}

	a := auth.New(fs, []byte{})
	api := New(fs, fs)
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

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects", validAPIKey, []byte(`{"name":"acme","retention_days":30}`))
	defer resp.Body.Close()
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

	resp := doReq(t, srv, http.MethodGet, "/api/v1/projects", validAPIKey, nil)
	defer resp.Body.Close()
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

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/1/keys", validAPIKey, []byte(`{"kind":"ingest"}`))
	defer resp.Body.Close()
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

	resp := doReq(t, srv, http.MethodPost, "/api/v1/projects/999/keys", validAPIKey, []byte(`{"kind":"ingest"}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
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

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?project=1&environment=prod&status=open&since=2026-01-01T00:00:00Z&limit=10", validAPIKey, nil)
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var got struct {
		Error string `json:"error"`
	}
	json.NewDecoder(resp.Body).Decode(&got)
	if got.Error == "" {
		t.Errorf("expected non-empty error message")
	}
}

func TestIssues_List_BadLimit_Returns400(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues?limit=abc", validAPIKey, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
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

func TestIssues_UpdateStatus_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.issues[1] = core.Issue{ID: 1, Status: core.StatusOpen}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/1", validAPIKey, []byte(`{"status":"resolved"}`))
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestIssues_UpdateStatus_UnknownID_Returns404(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodPatch, "/api/v1/issues/999", validAPIKey, []byte(`{"status":"resolved"}`))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
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
		"/api/v1/logs?project=1&q=hello&min_severity=error&service=api&environment=prod&since=2026-01-01T00:00:00Z&until=2026-01-02T00:00:00Z&limit=25",
		validAPIKey, nil)
	defer resp.Body.Close()
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

func TestLogs_Context_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.logList = []core.Log{{ID: 4, Body: "before"}, {ID: 5, Body: "target"}, {ID: 6, Body: "after"}}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/logs/5/context?n=20", validAPIKey, nil)
	defer resp.Body.Close()
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

func TestStats_HappyPath(t *testing.T) {
	fs := newFakeStore()
	fs.stats = store.Stats{
		Logs: 100, Events: 10, OpenIssues: 2,
		PerDay: []store.DayCount{{Day: "2026-01-01", Logs: 50, Events: 5}},
	}
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/stats?project=1&since=2026-01-01T00:00:00Z", validAPIKey, nil)
	defer resp.Body.Close()
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

func TestNoAuth_Returns401(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(fs)
	defer srv.Close()

	resp := doReq(t, srv, http.MethodGet, "/api/v1/issues", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}
