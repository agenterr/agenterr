// Package web is Agenterr's minimal htmx UI: admin-session-authenticated
// (cookie, not key), it sees every project on the instance. It's for
// verification and light management — issues, search, settings — not a
// second copy of the REST API. Templates and static assets are embedded
// into the binary at build time, so an Agenterr deployment stays a single
// Go binary with no separate asset pipeline.
package web

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"

	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/web/handlers"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static
var staticFS embed.FS

// Web mounts the web UI's routes.
type Web struct {
	issues   *handlers.Issues
	search   *handlers.Search
	settings *handlers.Settings
	login    *handlers.Login
	auth     auth.SessionAuth
}

// New constructs a Web reading via r, administering via a, and
// authenticating the admin session via s. Templates are parsed once here,
// at startup — not per-request.
func New(r store.Reader, a store.Admin, s auth.SessionAuth) *Web {
	tpl := parseTemplates()
	return &Web{
		issues:   handlers.NewIssues(r, a, tpl.issuesList, tpl.issueDetail),
		search:   handlers.NewSearch(r, tpl.search),
		settings: handlers.NewSettings(a, tpl.settings),
		login:    handlers.NewLogin(s, tpl.login),
		auth:     s,
	}
}

// templates holds the parsed page template set for each screen. Each page
// is its own *template.Template (layout.html + that page's file, parsed
// together) so that every page can define a template named "content"
// without colliding with any other page's "content" definition.
type templates struct {
	issuesList  *template.Template
	issueDetail *template.Template
	search      *template.Template
	settings    *template.Template
	login       *template.Template // standalone: defines its own minimal "layout"
}

func parseTemplates() *templates {
	page := func(name string) *template.Template {
		return template.Must(
			template.New("layout.html").
				Funcs(handlers.FuncMap).
				ParseFS(templatesFS, "templates/layout.html", "templates/"+name),
		)
	}
	login := template.Must(
		template.New("login.html").Funcs(handlers.FuncMap).ParseFS(templatesFS, "templates/login.html"),
	)
	return &templates{
		issuesList:  page("issues.html"),
		issueDetail: page("issue.html"),
		search:      page("search.html"),
		settings:    page("settings.html"),
		login:       login,
	}
}

// Mount registers every web UI route on mux. Every route except /login and
// static assets requires an admin session (auth.SessionAuth.RequireSession
// redirects unauthenticated requests to /login). Issue resolve/ignore are
// POST-only — no state change happens on a GET, per the CSRF posture the
// rest of the app follows (net/http's method-aware ServeMux answers a GET
// to a POST-only pattern with 405 on its own).
func (web *Web) Mount(mux *http.ServeMux) {
	mux.Handle("GET /login", http.HandlerFunc(web.login.Page))
	mux.Handle("POST /login", http.HandlerFunc(web.login.Submit))
	mux.Handle("POST /logout", web.auth.RequireSession(http.HandlerFunc(web.login.Logout)))

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		// static/ is embedded above; a failure here means the embed
		// directive itself is broken, which build would already have
		// caught — this is unreachable in a built binary.
		panic(err)
	}
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))

	protect := web.auth.RequireSession

	// "GET /{$}" (not "GET /") so this matches only the exact root path —
	// a plain "GET /" pattern is a catch-all for any unmatched GET request
	// and would silently swallow method mismatches on other routes (e.g.
	// a GET to the POST-only resolve/ignore routes should 405, not render
	// the issues list).
	mux.Handle("GET /{$}", protect(http.HandlerFunc(web.issues.List)))
	mux.Handle("GET /issues/{id}", protect(http.HandlerFunc(web.issues.Detail)))
	mux.Handle("POST /issues/{id}/resolve", protect(http.HandlerFunc(web.issues.Resolve)))
	mux.Handle("POST /issues/{id}/ignore", protect(http.HandlerFunc(web.issues.Ignore)))

	mux.Handle("GET /search", protect(http.HandlerFunc(web.search.List)))
	mux.Handle("GET /logs/{id}/context", protect(http.HandlerFunc(web.search.Context)))

	mux.Handle("GET /settings", protect(http.HandlerFunc(web.settings.Page)))
	mux.Handle("POST /settings/projects", protect(http.HandlerFunc(web.settings.CreateProject)))
	mux.Handle("POST /settings/projects/{id}/keys", protect(http.HandlerFunc(web.settings.MintKey)))
}
