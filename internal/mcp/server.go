// Package mcp is Agenterr's MCP edge — twenty-one token-frugal tools served
// over Streamable HTTP at /mcp. It reads via store.Reader and administers
// via store.Admin (issue status transitions, project listing),
// store.NoiseRules (rule listing), store.SeverityRules (rule listing), and
// store.AlertRules (rule listing); noise-rule, severity-rule, and
// parse-bodies mutations go through rules.Engine, alert-rule mutations
// (and the synchronous test-fire) go through alerts.Engine. It never
// touches store.Writer.
//
// Every tool renders compact plain text designed for agent consumption:
// lists lead with a count line, rows are one line each, and long lists are
// capped and truncated with a "(+N more — refine filters)" trailer rather
// than dumping everything. This mirrors the scoping rules enforced by the
// REST edge (internal/api/handlers): a project-scoped "api" key sees only
// its own project — every filter is pinned to it and every id-based lookup
// 404s (here: returns a tool error "not found") for rows belonging to
// another project. An "admin" key is unscoped.
package mcp

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
)

// fetchCap bounds how many rows a list tool pulls from the store before
// rendering caps the visible output at the caller's limit. It decouples
// the store-side fetch (bounded, cheap) from the token-frugal render-side
// limit (default 20): the tool reports "(+N more — refine filters)" based
// on what it actually fetched, not a full table count.
const fetchCap = 500

// defaultLimit is applied when a list tool's limit input is unset or <= 0.
const defaultLimit = 20

// defaultContextN is applied when get_log_context's n input is unset.
const defaultContextN = 20

// Server implements Agenterr's MCP edge.
type Server struct {
	reader       store.Reader
	admin        store.Admin
	nr           store.NoiseRules
	sr           store.SeverityRules
	engine       *rules.Engine
	ar           store.AlertRules
	alertsEngine *alerts.Engine
	mcp          *mcpsdk.Server

	// clock is swapped in tests for a fixed time so relative-time renders
	// ("2m ago") are deterministic.
	clock func() time.Time
}

// New constructs a Server reading via r and administering via a. Noise
// rules and severity rules are read straight from nr and sr (plain store
// reads, same as any other list) but always mutated through engine —
// never nr's/sr's write methods directly — so the pipeline's cached view
// of rules stays fresh the moment a tool changes them (mirrors the REST
// edge's rule; see internal/api/handlers/noiserules.go and
// severityrules.go). ar and alertsEngine are the alert-rule analog (see
// internal/alerts.Engine), which additionally backs the synchronous
// test-fire tool.
func New(r store.Reader, a store.Admin, nr store.NoiseRules, sr store.SeverityRules, engine *rules.Engine, ar store.AlertRules, alertsEngine *alerts.Engine) *Server {
	s := &Server{
		reader:       r,
		admin:        a,
		nr:           nr,
		sr:           sr,
		engine:       engine,
		ar:           ar,
		alertsEngine: alertsEngine,
		clock:        time.Now,
	}
	s.mcp = mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "agenterr",
		Version: "0.1.0",
	}, nil)
	s.registerTools()
	return s
}

// Mount registers the Streamable HTTP handler at /mcp, behind key auth.
// The handler serves every method the transport needs (POST, GET, DELETE)
// on the single /mcp endpoint. An "api" key is required — an "admin" key
// also satisfies this per auth.RequireKey's hierarchy — and every tool
// further scopes itself by the caller's project unless the caller is
// admin (see callerScope).
func (s *Server) Mount(mux *http.ServeMux, keys auth.KeyAuth) {
	handler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server {
		return s.mcp
	}, nil)
	mux.Handle("/mcp", keys.RequireKey("api", handler))
}

// callerScope resolves the requesting key's project ID and whether it is
// an instance-level "admin" key, from context values injected by
// auth.RequireKey. Each incoming Streamable HTTP request is re-run through
// that middleware (Mount wraps the whole handler), and the SDK ties the
// context passed to tool handlers back to the originating HTTP request, so
// this is safe to call per tool invocation. Mirrors
// internal/api/handlers.callerScope — same rule, same edge.
func callerScope(ctx context.Context) (projectID int64, isAdmin bool) {
	projectID, _ = auth.ProjectFromContext(ctx)
	kind, _ := auth.KindFromContext(ctx)
	return projectID, kind == "admin"
}

// projectSlug looks up a project's slug by ID, for building list headers
// like "in payment-api". Returns ok=false if the project can't be found.
func (s *Server) projectSlug(ctx context.Context, id int64) (string, bool) {
	projects, err := s.admin.Projects(ctx)
	if err != nil {
		return "", false
	}
	for _, p := range projects {
		if p.ID == id {
			return p.Slug, true
		}
	}
	return "", false
}

// renderLimit normalizes a tool's limit input: <= 0 means "unset", which
// falls back to defaultLimit.
func renderLimit(n int) int {
	if n <= 0 {
		return defaultLimit
	}
	return n
}

func textResult(text string) *mcpsdk.CallToolResult {
	return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: text}}}
}

// errorResult builds a tool-error CallToolResult from err: Content carries
// err.Error() and IsError is set, per the MCP convention that tool-level
// failures (as opposed to protocol failures) are reported in-band so the
// calling agent can see and react to them.
func errorResult(err error) *mcpsdk.CallToolResult {
	res := &mcpsdk.CallToolResult{}
	res.SetError(err)
	return res
}

// errNotFound is the tool-level "not found" error: returned both when a
// row genuinely doesn't exist and when it belongs to another project
// (a project-scoped key must not learn that the row exists elsewhere).
var errNotFound = errors.New("not found")

// errInternal is the tool-level error for any store failure that isn't
// store.ErrNotFound. Its text is deliberately generic.
var errInternal = errors.New("internal error")

// toolErr maps a store error to the error text a client is allowed to
// see. store.ErrNotFound becomes the uniform errNotFound message; any
// other error — a driver error, a wrapped SQL fragment, anything with
// internal detail — must never reach the client over the wire, so it's
// logged server-side and replaced with errInternal. This mirrors the
// REST edge's policy (internal/api/handlers: respondErr(w, 500,
// "internal") on every non-ErrNotFound store error) and is the one
// place all tool handlers route store errors through, so that policy
// can't be missed at a call site.
func toolErr(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return errNotFound
	}
	slog.Error("mcp: store error", "error", err)
	return errInternal
}
