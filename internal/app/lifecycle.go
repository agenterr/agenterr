package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/fx"

	"github.com/agenterr/agenterr/internal/config"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/store"
)

// retentionInterval is how often the retention job walks every project
// and prunes logs older than its retention window.
const retentionInterval = time.Hour

// guardrailWindow is how far back the MaxDBBytes guardrail prunes when the
// database file has grown past the configured cap. It is deliberately
// coarse (a whole day at a time) rather than trying to hit the cap
// precisely — this is a last-resort safety valve, not the primary
// retention mechanism.
const guardrailWindow = 24 * time.Hour

// register wires the fx.Lifecycle hooks that turn the constructed graph
// into a running process: OnStart performs first-run bootstrap, then
// starts the pipeline writer loop, the HTTP server, and the retention
// job, each in its own goroutine. OnStop reverses the order — server,
// then pipeline, then store — so nothing is torn down out from under a
// request that's still in flight.
func register(lc fx.Lifecycle, sd fx.Shutdowner, cfg config.Config, st store.Store, pipe *pipeline.Pipeline, srv *http.Server, pw adminPassword) {
	pipeCtx, cancelPipe := context.WithCancel(context.Background())
	retentionCtx, cancelRetention := context.WithCancel(context.Background())

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := bootstrap(ctx, cfg, st, pw); err != nil {
				return fmt.Errorf("app: bootstrap: %w", err)
			}

			go pipe.Run(pipeCtx)

			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.Error("app: server exited", "err", err)
					if shutdownErr := sd.Shutdown(fx.ExitCode(1)); shutdownErr != nil {
						slog.Error("app: shutdown after server error", "err", shutdownErr)
					}
				}
			}()

			go retentionLoop(retentionCtx, cfg, st)

			return nil
		},
		OnStop: func(ctx context.Context) error {
			if err := srv.Shutdown(ctx); err != nil {
				slog.Error("app: server shutdown", "err", err)
			}

			cancelRetention()
			cancelPipe()

			if err := pipe.Drain(ctx); err != nil {
				slog.Error("app: pipeline drain", "err", err)
			}

			if err := st.Close(); err != nil {
				return fmt.Errorf("app: store close: %w", err)
			}
			return nil
		},
	})
}

// bootstrap mints an instance admin key and prints it (plus the admin
// password, if it was freshly generated) to stdout exactly once, the
// first time the process ever starts against cfg.DBPath. Idempotency is
// tracked with a marker file next to the database rather than a store
// query, since store.Admin has no "does an admin key already exist"
// method and adding one would ripple across every store implementation
// and test fake for a check only this bootstrap path needs.
func bootstrap(ctx context.Context, cfg config.Config, st store.Store, pw adminPassword) error {
	marker := cfg.DBPath + ".bootstrap"

	if _, err := os.Stat(marker); err == nil {
		return nil // already bootstrapped against this database
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat bootstrap marker: %w", err)
	}

	key, err := st.MintKey(ctx, 0, "admin")
	if err != nil {
		return fmt.Errorf("mint admin key: %w", err)
	}

	if err := os.WriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o600); err != nil {
		return fmt.Errorf("write bootstrap marker: %w", err)
	}

	printBootstrap(cfg, key, pw)
	return nil
}

func printBootstrap(cfg config.Config, key string, pw adminPassword) {
	var b strings.Builder
	b.WriteString("\n=== Agenterr: first run ===\n")
	if cfg.AdminPassword == "" {
		fmt.Fprintf(&b, "Generated admin password: %s\n", pw)
	}
	fmt.Fprintf(&b, "Admin API key:            %s\n", key)
	fmt.Fprintf(&b, "Setup URL:                %s/\n", setupURL(cfg.ListenAddr))
	b.WriteString("Save these now: the key is shown only once and cannot be recovered.\n")
	b.WriteString("============================\n\n")
	fmt.Print(b.String())
}

// setupURL turns a listen address (which may be host-less, e.g. ":3617")
// into a browsable http:// URL for the printed bootstrap message.
func setupURL(listenAddr string) string {
	if strings.HasPrefix(listenAddr, ":") {
		return "http://localhost" + listenAddr
	}
	return "http://" + listenAddr
}

// retentionLoop runs the retention sweep once an hour until ctx is
// canceled: for every project, prune logs older than its retention
// window, then apply the MaxDBBytes guardrail if configured.
func retentionLoop(ctx context.Context, cfg config.Config, st store.Store) {
	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			retentionTick(ctx, cfg, st)
		}
	}
}

func retentionTick(ctx context.Context, cfg config.Config, st store.Store) {
	projects, err := st.Projects(ctx)
	if err != nil {
		slog.Error("app: retention list projects", "err", err)
		return
	}

	now := time.Now().UTC()
	for _, p := range projects {
		if p.RetentionDays <= 0 {
			continue // 0/negative retention means "keep forever"
		}
		cutoff := now.AddDate(0, 0, -p.RetentionDays)
		if _, err := st.Prune(ctx, p.ID, cutoff); err != nil {
			slog.Error("app: retention prune", "project", p.ID, "err", err)
		}
	}

	if cfg.MaxDBBytes > 0 {
		enforceMaxDBBytes(ctx, cfg, st, projects)
	}
}

// enforceMaxDBBytes is a last-resort guardrail: if the database file has
// grown past cfg.MaxDBBytes despite normal per-project retention, prune
// everything older than a day, across every project, and warn loudly.
// This is a coarse safety valve, not a precise size target — getting the
// file back under the cap exactly would require knowing how much space
// pruning actually frees, which SQLite doesn't reclaim without a VACUUM.
func enforceMaxDBBytes(ctx context.Context, cfg config.Config, st store.Store, projects []core.Project) {
	fi, err := os.Stat(cfg.DBPath)
	if err != nil {
		return // best-effort: nothing to guard if we can't stat the file
	}
	if fi.Size() <= cfg.MaxDBBytes {
		return
	}

	slog.Warn("app: database exceeds max-db-bytes, pruning oldest day across all projects",
		"size_bytes", fi.Size(), "max_bytes", cfg.MaxDBBytes)

	cutoff := time.Now().UTC().Add(-guardrailWindow)
	for _, p := range projects {
		if _, err := st.Prune(ctx, p.ID, cutoff); err != nil {
			slog.Error("app: retention guardrail prune", "project", p.ID, "err", err)
		}
	}
}
