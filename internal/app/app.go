// Package app is the only place go.uber.org/fx may be imported (enforced
// by depguard, see .golangci.yml): every other package keeps plain
// constructors, and this package's sole job is wrapping them in
// fx.Provide/fx.Invoke to assemble a runnable process. cmd/agenterr calls
// fx.New(app.Module).Run() and nothing else.
package app

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/fx"
	"golang.org/x/crypto/bcrypt"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/api"
	"github.com/agenterr/agenterr/internal/auth"
	"github.com/agenterr/agenterr/internal/config"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/ingest"
	"github.com/agenterr/agenterr/internal/ingest/jsonapi"
	"github.com/agenterr/agenterr/internal/ingest/otlp"
	"github.com/agenterr/agenterr/internal/mcp"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/server"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/web"
)

// generatedCreds carries the admin password's plaintext from newAuth to
// register, but only when that password was freshly generated this boot
// (Password == ""  otherwise) — the one and only reason register needs
// it is to print it once, on first run. It is never persisted; only its
// bcrypt hash is (see settingAdminPasswordHash).
type generatedCreds struct {
	password string
}

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
		asNoiseRules,
		asAlertRules,
		newGrouper,
		asNotifier,
		rules.New,
		newAlertsEngine,
		asDropper,
		newPipeline,
		asSink,
		newAuth,
		asSessionAuth,
		fx.Annotate(newOTLPIngester, fx.As(new(ingest.Ingester)), fx.ResultTags(`group:"ingesters"`)),
		fx.Annotate(newJSONAPIIngester, fx.As(new(ingest.Ingester)), fx.ResultTags(`group:"ingesters"`)),
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
func asStore(db *sqlite.DB) store.Store           { return db }
func asReader(db *sqlite.DB) store.Reader         { return db }
func asWriter(db *sqlite.DB) store.Writer         { return db }
func asAdmin(db *sqlite.DB) store.Admin           { return db }
func asNoiseRules(db *sqlite.DB) store.NoiseRules { return db }
func asAlertRules(db *sqlite.DB) store.AlertRules { return db }

// asSessionAuth adapts *auth.Auth to the auth.SessionAuth interface
// web.New depends on.
func asSessionAuth(a *auth.Auth) auth.SessionAuth { return a }

func newGrouper() pipeline.Grouper { return core.DefaultGrouper{} }

// asDropper adapts *rules.Engine to the narrower pipeline.Dropper
// interface pipeline.New depends on. pipeline never imports internal/rules
// (see pipeline/ports.go) — rules.Engine satisfies Dropper structurally,
// and this is where that structural fit gets made explicit for fx.
func asDropper(e *rules.Engine) pipeline.Dropper { return e }

// newAlertsEngine constructs the alerts.Engine with a nil *http.Client so
// it falls back to its documented 5s-timeout default — nothing else in the
// graph needs an *http.Client, so there is no reason to provide a real one
// just to satisfy this constructor.
func newAlertsEngine(ar store.AlertRules) *alerts.Engine { return alerts.New(ar, nil) }

// asNotifier adapts *alerts.Engine to the narrower pipeline.Notifier
// interface pipeline.New depends on. pipeline never imports internal/alerts
// (mirrors asDropper/internal/rules above) — alerts.Engine satisfies
// Notifier structurally, and this is where that structural fit gets made
// explicit for fx. pipeline.NopNotifier remains only for pipeline's own
// tests now that the production graph wires the real engine.
func asNotifier(e *alerts.Engine) pipeline.Notifier { return e }

func newPipeline(cfg config.Config, w store.Writer, g pipeline.Grouper, n pipeline.Notifier, d pipeline.Dropper) *pipeline.Pipeline {
	return pipeline.New(w, g, n, d, pipeline.Options{
		BufferSize:       cfg.BufferSize,
		FlushEvery:       time.Duration(cfg.FlushEveryMS) * time.Millisecond,
		DisableBodyParse: !cfg.ParseBodies,
	})
}

// asSink adapts *pipeline.Pipeline to the narrow ingest.Sink interface the
// otlp/jsonapi edges depend on.
func asSink(p *pipeline.Pipeline) ingest.Sink { return p }

// newOTLPIngester and newJSONAPIIngester wire cfg.MaxBodyBytes into each
// ingest edge's constructor rather than letting them fall back to
// ingest.MaxBody unconditionally, so the configured limit (flag/env/
// default, see internal/config) actually governs what the edges accept.
func newOTLPIngester(cfg config.Config, sink ingest.Sink) *otlp.Handler {
	return otlp.New(sink, cfg.MaxBodyBytes)
}

func newJSONAPIIngester(cfg config.Config, sink ingest.Sink) *jsonapi.Handler {
	return jsonapi.New(sink, cfg.MaxBodyBytes)
}

// generatedPasswordBytes of crypto/rand output, base64url-encoded without
// padding, yields exactly 24 characters (ceil(18/3)*4).
const generatedPasswordBytes = 18

// settingAdminPasswordHash is the settings table key under which the
// admin password's bcrypt hash is persisted, so it survives restarts
// without regenerating (and thereby invalidating) the operator's
// password on every boot.
const settingAdminPasswordHash = "admin_password_hash"

// newAuth resolves the admin password's bcrypt hash and constructs
// *auth.Auth. Three cases, in order:
//
//  1. cfg.AdminPassword is set: hash it and use it, every boot, without
//     persisting — the environment variable always wins, which is what
//     makes password rotation possible (set it, restart, unset it again
//     and the persisted hash from case 2 takes back over next boot...
//     except case 2 only ever writes once, see below). The plaintext is
//     already known to the operator, so it is not returned for printing.
//  2. cfg.AdminPassword is empty and a hash is already persisted (every
//     boot after the first): load and use it. Nothing is generated or
//     printed.
//  3. Neither: this is the very first boot. Generate a password, hash
//     it, persist the hash, and return the plaintext so register can
//     print it once.
func newAuth(cfg config.Config, db *sqlite.DB) (*auth.Auth, generatedCreds, error) {
	ctx := context.Background()

	if cfg.AdminPassword != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(cfg.AdminPassword), bcrypt.DefaultCost)
		if err != nil {
			return nil, generatedCreds{}, fmt.Errorf("app: hash admin password: %w", err)
		}
		return auth.New(db, hash), generatedCreds{}, nil
	}

	if hash, err := db.Setting(ctx, settingAdminPasswordHash); err == nil {
		return auth.New(db, []byte(hash)), generatedCreds{}, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return nil, generatedCreds{}, fmt.Errorf("app: load admin password hash: %w", err)
	}

	pw, err := generatePassword()
	if err != nil {
		return nil, generatedCreds{}, fmt.Errorf("app: generate admin password: %w", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil {
		return nil, generatedCreds{}, fmt.Errorf("app: hash admin password: %w", err)
	}
	if err := db.SetSetting(ctx, settingAdminPasswordHash, string(hash)); err != nil {
		return nil, generatedCreds{}, fmt.Errorf("app: persist admin password hash: %w", err)
	}

	return auth.New(db, hash), generatedCreds{password: pw}, nil
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
