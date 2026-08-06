// Package app is the only place go.uber.org/fx may be imported (enforced
// by depguard, see .golangci.yml): every other package keeps plain
// constructors, and this package's sole job is wrapping them in
// fx.Provide/fx.Invoke to assemble a runnable process. cmd/agenterr calls
// fx.New(app.Module).Run() and nothing else.
package app

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"
	"golang.org/x/crypto/bcrypt"

	"github.com/agenterr/agenterr/internal/api"
	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/config"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/ingest"
	"github.com/agenterr/agenterr/internal/ingest/jsonapi"
	"github.com/agenterr/agenterr/internal/ingest/otlp"
	"github.com/agenterr/agenterr/internal/mcp"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/server"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/web"
)

// adminPassword is the resolved admin password — either the operator's
// AGENTERR_ADMIN_PASSWORD or, when that's unset, a freshly generated one.
// It flows from newAuth to register purely so the first-run bootstrap
// message can print it; nothing else in the graph depends on it.
type adminPassword string

// Module wires every plain constructor in the codebase into the fx graph
// and registers the process lifecycle (see lifecycle.go). Providers are
// intentionally thin: each one either calls straight through to a plain
// constructor or adapts a concrete type to the narrower interface a
// consumer needs (e.g. *sqlite.DB -> store.Reader).
var Module = fx.Options(
	fx.Provide(
		loadConfig,
		openDB,
		asStore,
		asReader,
		asWriter,
		asAdmin,
		newGrouper,
		newNotifier,
		newPipeline,
		asSink,
		newAuth,
		asSessionAuth,
		fx.Annotate(otlp.New, fx.As(new(ingest.Ingester)), fx.ResultTags(`group:"ingesters"`)),
		fx.Annotate(jsonapi.New, fx.As(new(ingest.Ingester)), fx.ResultTags(`group:"ingesters"`)),
		api.New,
		mcp.New,
		web.New,
		newServer,
	),
	fx.Invoke(register),
)

// loadConfig reads configuration the same way the real binary does: flags
// from the process's own argv, values from the process environment.
func loadConfig() (config.Config, error) {
	return config.Load(os.Args[1:], os.Getenv)
}

// openDB opens the sqlite store at cfg.DBPath. Migrations run inside
// sqlite.Open itself, so there is nothing left to do here beyond
// extracting the path from cfg.
func openDB(cfg config.Config) (*sqlite.DB, error) {
	return sqlite.Open(cfg.DBPath)
}

// asStore, asReader, asWriter, asAdmin each adapt the single *sqlite.DB
// instance to a narrower store interface for consumers that only need
// that slice of behavior. They all close over the same *sqlite.DB — dig
// caches openDB's result, so this does not open the database more than
// once.
func asStore(db *sqlite.DB) store.Store   { return db }
func asReader(db *sqlite.DB) store.Reader { return db }
func asWriter(db *sqlite.DB) store.Writer { return db }
func asAdmin(db *sqlite.DB) store.Admin   { return db }

// asSessionAuth adapts *auth.Auth to the auth.SessionAuth interface
// web.New depends on.
func asSessionAuth(a *auth.Auth) auth.SessionAuth { return a }

func newGrouper() pipeline.Grouper   { return core.DefaultGrouper{} }
func newNotifier() pipeline.Notifier { return pipeline.NopNotifier{} }

func newPipeline(cfg config.Config, w store.Writer, g pipeline.Grouper, n pipeline.Notifier) *pipeline.Pipeline {
	return pipeline.New(w, g, n, pipeline.Options{
		BufferSize: cfg.BufferSize,
		FlushEvery: time.Duration(cfg.FlushEveryMS) * time.Millisecond,
	})
}

// asSink adapts *pipeline.Pipeline to the narrow ingest.Sink interface the
// otlp/jsonapi edges depend on.
func asSink(p *pipeline.Pipeline) ingest.Sink { return p }

// generatedPasswordBytes of crypto/rand output, base64url-encoded without
// padding, yields exactly 24 characters (ceil(18/3)*4).
const generatedPasswordBytes = 18

// newAuth resolves the admin password (operator-supplied or freshly
// generated), hashes it, and constructs *auth.Auth. The plaintext is also
// returned (as adminPassword) purely so register can print it once on
// first run.
func newAuth(cfg config.Config, admin store.Admin) (*auth.Auth, adminPassword, error) {
	pw := cfg.AdminPassword
	if pw == "" {
		generated, err := generatePassword()
		if err != nil {
			return nil, "", fmt.Errorf("app: generate admin password: %w", err)
		}
		pw = generated
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil {
		return nil, "", fmt.Errorf("app: hash admin password: %w", err)
	}

	return auth.New(admin, hash), adminPassword(pw), nil
}

func generatePassword() (string, error) {
	b := make([]byte, generatedPasswordBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("app: read random bytes: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// serverParams collects everything server.New needs via an fx.In struct
// (only possible here in internal/app — server.Deps itself stays a plain
// struct so internal/server never has to know fx exists), including the
// []ingest.Ingester group populated by the otlp/jsonapi providers above.
type serverParams struct {
	fx.In

	Cfg       config.Config
	Store     store.Store
	Pipe      *pipeline.Pipeline
	Ingesters []ingest.Ingester `group:"ingesters"`
	Auth      *auth.Auth
	API       *api.API
	MCP       *mcp.Server
	Web       *web.Web
}

func newServer(p serverParams) *http.Server {
	return server.New(server.Deps{
		Cfg:       p.Cfg,
		Store:     p.Store,
		Pipe:      p.Pipe,
		Ingesters: p.Ingesters,
		Auth:      p.Auth,
		API:       p.API,
		MCP:       p.MCP,
		Web:       p.Web,
	})
}
