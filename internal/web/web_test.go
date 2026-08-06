package web

import (
	"context"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// fakeStore implements store.Reader and store.Admin over maps, mirroring
// the pattern in internal/api/api_test.go. It records the last filter it
// received on each read method so tests can assert query-param -> filter
// mapping.
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
	stats   store.Stats

	lastIssueFilter store.IssueFilter
	lastLogFilter   store.LogFilter
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
	return f.logList, nil
}

func (f *fakeStore) Stats(ctx context.Context, filter store.StatsFilter) (store.Stats, error) {
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

const testPassword = "test"

func newTestServer(t *testing.T, fs *fakeStore) *httptest.Server {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	a := auth.New(fs, hash)
	web := New(fs, fs, a)
	mux := http.NewServeMux()
	web.Mount(mux)
	return httptest.NewServer(mux)
}

// loggedInClient returns an http.Client with cookies enabled that has
// already logged in against srv.
func loggedInClient(t *testing.T, srv *httptest.Server) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar: %v", err)
	}
	client := &http.Client{
		Jar: jar,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.PostForm(srv.URL+"/login", url.Values{"password": {testPassword}})
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("login status = %d, want 303", resp.StatusCode)
	}
	// Restore normal redirect handling for subsequent requests.
	client.CheckRedirect = nil
	return client
}

func TestUnauthenticated_RedirectsToLogin(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(t, fs)
	defer srv.Close()

	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/login" {
		t.Fatalf("Location = %q, want /login", loc)
	}
}

func TestLogin_HappyPath(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(t, fs)
	defer srv.Close()

	client := loggedInClient(t, srv)
	resp, err := client.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestIssuesList_RendersTitlesAndCounts(t *testing.T) {
	fs := newFakeStore()
	fs.issueList = []core.Issue{
		{ID: 1, ProjectID: 1, Title: "nil pointer — worker.go:42", Severity: core.SeverityError, Status: core.StatusOpen, Count: 7, LastSeen: time.Now()},
		{ID: 2, ProjectID: 1, Title: "rate limited — client.go:12", Severity: core.SeverityWarn, Status: core.StatusOpen, Count: 3, LastSeen: time.Now()},
	}
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)

	for _, want := range []string{"nil pointer", "×7", "rate limited", "×3", "ERRO", "WARN"} {
		if !strings.Contains(s, want) {
			t.Errorf("body missing %q\n---\n%s", want, s)
		}
	}
}

func TestIssuesList_StatusFilter(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Get(srv.URL + "/?status=resolved")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.lastIssueFilter.Status != core.StatusResolved {
		t.Fatalf("filter.Status = %q, want resolved", fs.lastIssueFilter.Status)
	}
}

func TestIssuesList_HXRequestRendersFragmentOnly(t *testing.T) {
	fs := newFakeStore()
	fs.issueList = []core.Issue{
		{ID: 1, ProjectID: 1, Title: "boom", Severity: core.SeverityError, Status: core.StatusOpen, Count: 1, LastSeen: time.Now()},
	}
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("HX-Request", "true")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if strings.Contains(s, "<html") {
		t.Fatalf("fragment response contains <html>:\n%s", s)
	}
	if !strings.Contains(s, "boom") {
		t.Fatalf("fragment missing issue title:\n%s", s)
	}
}

func TestIssueDetail_ShowsBodyAndResolveButton(t *testing.T) {
	fs := newFakeStore()
	fs.issues[12] = core.Issue{ID: 12, ProjectID: 1, Title: "boom — a.go:1", Severity: core.SeverityError, Status: core.StatusOpen, Count: 2, FirstSeen: time.Now(), LastSeen: time.Now()}
	fs.issueEvents[12] = []core.Event{
		{LogID: 100, IssueID: 12, Time: time.Now(), Log: core.Log{ID: 100, Body: "panic: nil pointer dereference\ngoroutine 1 [running]:\nmain.boom()"}},
	}
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Get(srv.URL + "/issues/12")
	if err != nil {
		t.Fatalf("GET /issues/12: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "panic: nil pointer dereference") {
		t.Errorf("body missing sample event body:\n%s", s)
	}
	if !strings.Contains(s, `hx-post="/issues/12/resolve"`) {
		t.Errorf("body missing resolve button hx-post:\n%s", s)
	}
}

func TestResolve_UpdatesStatusAndReturnsChip(t *testing.T) {
	fs := newFakeStore()
	fs.issues[12] = core.Issue{ID: 12, ProjectID: 1, Title: "boom", Severity: core.SeverityError, Status: core.StatusOpen, Count: 1}
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Post(srv.URL+"/issues/12/resolve", "application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatalf("POST resolve: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if fs.issues[12].Status != core.StatusResolved {
		t.Fatalf("issue status = %q, want resolved", fs.issues[12].Status)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "resolved") {
		t.Errorf("chip fragment missing 'resolved':\n%s", body)
	}
}

func TestResolve_GETNotAllowed(t *testing.T) {
	fs := newFakeStore()
	fs.issues[12] = core.Issue{ID: 12, ProjectID: 1, Title: "boom"}
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Get(srv.URL + "/issues/12/resolve")
	if err != nil {
		t.Fatalf("GET resolve: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestSearch_RendersMatches(t *testing.T) {
	fs := newFakeStore()
	fs.logList = []core.Log{
		{ID: 1, Body: "connection refused to db", Severity: core.SeverityError, Time: time.Now()},
	}
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Get(srv.URL + "/search?q=refused")
	if err != nil {
		t.Fatalf("GET /search: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "connection refused to db") {
		t.Errorf("body missing matched log:\n%s", s)
	}
	if fs.lastLogFilter.Query != "refused" {
		t.Fatalf("filter.Query = %q, want refused", fs.lastLogFilter.Query)
	}
}

func TestSettings_CreateProjectAndMintKey(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.PostForm(srv.URL+"/settings/projects", url.Values{"name": {"acme"}, "retention_days": {"30"}})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d, want 200:\n%s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "acme") {
		t.Errorf("response missing new project name:\n%s", body)
	}

	var projectID int64
	for id := range fs.projects {
		projectID = id
	}
	if projectID == 0 {
		t.Fatalf("project was not created")
	}

	mintResp, err := client.PostForm(srv.URL+"/settings/projects/"+itoa(projectID)+"/keys", url.Values{"kind": {"ingest"}})
	if err != nil {
		t.Fatalf("mint key: %v", err)
	}
	defer mintResp.Body.Close()
	mintBody, _ := io.ReadAll(mintResp.Body)
	if !strings.Contains(string(mintBody), "agt_minted_ingest") {
		t.Errorf("response missing plaintext key:\n%s", mintBody)
	}
}

func TestSettings_EmptyProjectListShowsCurlSnippet(t *testing.T) {
	fs := newFakeStore()
	srv := newTestServer(t, fs)
	defer srv.Close()
	client := loggedInClient(t, srv)

	resp, err := client.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "curl") {
		t.Errorf("empty state missing curl snippet:\n%s", body)
	}
}

func itoa(id int64) string {
	return (func() string {
		if id == 0 {
			return "0"
		}
		neg := id < 0
		if neg {
			id = -id
		}
		var buf [20]byte
		i := len(buf)
		for id > 0 {
			i--
			buf[i] = byte('0' + id%10)
			id /= 10
		}
		if neg {
			i--
			buf[i] = '-'
		}
		return string(buf[i:])
	})()
}
