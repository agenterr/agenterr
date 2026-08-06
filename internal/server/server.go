// Package server composes every edge (ingesters, REST API, MCP, web UI)
// plus /healthz onto a single *http.Server: one mux, one middleware chain.
// This is the only place all the edges' Mount methods are called together.
package server

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/agenterr/agenterr/internal/api"
	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/config"
	"github.com/agenterr/agenterr/internal/ingest"
	"github.com/agenterr/agenterr/internal/mcp"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/web"
)

const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 120 * time.Second
)

// Deps holds everything New needs to compose the server. Every field is
// already fully constructed — New only mounts and wires, it builds nothing.
type Deps struct {
	Cfg       config.Config
	Store     store.Store
	Pipe      *pipeline.Pipeline
	Ingesters []ingest.Ingester
	Auth      *auth.Auth
	API       *api.API
	MCP       *mcp.Server
	Web       *web.Web
}

// New composes every edge onto a single http.ServeMux, wraps it with the
// process-wide middleware chain, and returns an *http.Server bound to
// d.Cfg.ListenAddr ready to be Serve'd (or Shutdown, by the caller).
//
// Mount order matters only for web: it registers "GET /{$}" (an
// exact-root match, not a catch-all) plus "GET /static/", so it can be
// mounted anywhere relative to the other edges without risk of shadowing
// their routes — it's mounted last here simply because it "owns" the
// leftover surface (the root UI) after every API-shaped route is claimed.
func New(d Deps) *http.Server {
	mux := http.NewServeMux()

	mux.Handle("GET /healthz", Healthz(d.Store, d.Pipe))

	for _, ing := range d.Ingesters {
		ing.Mount(mux, d.Auth)
	}

	d.API.Mount(mux, d.Auth)
	d.MCP.Mount(mux, d.Auth)
	d.Web.Mount(mux)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	handler := withMiddleware(mux, logger)

	return &http.Server{
		Addr:              d.Cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		// No WriteTimeout: the MCP edge streams over SSE and ingest/API
		// responses must not be cut off mid-write by a blanket deadline.
	}
}
