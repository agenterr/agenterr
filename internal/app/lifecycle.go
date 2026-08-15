package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/fx"

	"github.com/agenterr/agenterr/internal/alerts"
	"github.com/agenterr/agenterr/internal/config"
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/pipeline"
	"github.com/agenterr/agenterr/internal/rules"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/enginestore"
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

// shutdownServerBudget bounds how long OnStop gives srv.Shutdown before
// moving on, so a slow/stuck HTTP shutdown can never eat the entire Stop
// budget and starve pipe.Drain of the time it needs to flush — see
// boundedShutdownCtx and its use in OnStop below.
const shutdownServerBudget = 7 * time.Second

// register wires the fx.Lifecycle hooks that turn the constructed graph
// into a running process: OnStart loads the noise-rule and alert-rule
// engines' caches, performs first-run bootstrap, then starts the pipeline
// writer loop, the HTTP server, the retention job, the drop-counter flush
// loop, and the alerts delivery worker, each in its own goroutine. OnStop
// reverses the order — server, then pipeline, then the alerts worker
// (which must not be canceled until pipeline drain has stopped the only
// caller of its IssueEvent, see the OnStop body below), then store — so
// nothing is torn down out from under a request, or a notification, still
// in flight.
func register(lc fx.Lifecycle, sd fx.Shutdowner, cfg config.Config, db *enginestore.Store, engine *rules.Engine, alertsEngine *alerts.Engine, pipe *pipeline.Pipeline, srv *http.Server, creds generatedCreds) {
	pipeCtx, cancelPipe := context.WithCancel(context.Background())
	retentionCtx, cancelRetention := context.WithCancel(context.Background())
	flushCtx, cancelFlush := context.WithCancel(context.Background())
	alertsCtx, cancelAlerts := context.WithCancel(context.Background())
	alertsDone := make(chan struct{})

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := bootstrap(ctx, cfg, db, creds); err != nil {
				return fmt.Errorf("app: bootstrap: %w", err)
			}

			// Load before the pipeline starts accepting: fail-open (an
			// unloaded engine keeps everything) covers the gap, but
			// loading first means rules apply from the very first record
			// instead of racing the pipeline's first Enqueue.
			if err := engine.Load(ctx); err != nil {
				return fmt.Errorf("app: load noise rules: %w", err)
			}

			// Same reasoning as the noise engine above: load the alert-rule
			// cache before the pipeline can call IssueEvent, so rules apply
			// from the first record instead of racing the pipeline's first
			// Enqueue.
			if err := alertsEngine.Load(ctx); err != nil {
				return fmt.Errorf("app: load alert rules: %w", err)
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

			go retentionLoop(retentionCtx, cfg, db)

			go flushDropsLoop(flushCtx, engine, time.Duration(cfg.NoiseFlushMS)*time.Millisecond)

			go func() {
				defer close(alertsDone)
				alertsEngine.Run(alertsCtx)
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			// srv.Shutdown gets its own bounded child context (capped at
			// min(shutdownServerBudget, whatever's left of ctx)) so that
			// even if it hangs — a stuck connection, a slow client — it
			// cannot consume the entire Stop budget and leave pipe.Drain
			// with no time to flush pending writes below. Drain itself
			// still uses the outer ctx, so it gets whatever budget
			// remains after the server phase actually finishes (which is
			// normally almost immediately, well under the cap).
			srvCtx, cancelSrv := boundedShutdownCtx(ctx, shutdownServerBudget)
			err := srv.Shutdown(srvCtx)
			cancelSrv()
			if err != nil {
				slog.Error("app: server shutdown", "err", err)
			}

			cancelRetention()
			cancelPipe()
			cancelFlush()

			if err := pipe.Drain(ctx); err != nil {
				slog.Error("app: pipeline drain", "err", err)
			}

			// Only cancel the alerts worker's ctx once pipe.Drain has
			// returned: pipe.Drain is the last place IssueEvent can still
			// be called (the drain loop runs events through the same
			// process/Decide path as the live loop), and alerts.Run's
			// documented precondition is that callers stop invoking
			// IssueEvent before canceling its ctx. Canceling it stops the
			// worker from accepting new fires but not before it drains
			// whatever's already queued — waiting on alertsDone below
			// blocks until that drain (with its in-flight webhook
			// deliveries) actually finishes, so shutdown never leaves the
			// worker goroutine running past OnStop.
			cancelAlerts()
			// Deliberately unbounded, not tied to ctx: this must wait out
			// whatever alerts.Run's own drain pass is doing so every
			// queued fire gets its delivery attempt and its outcome
			// recorded (no-silent-failure) rather than being abandoned
			// mid-flight. Worst case is the serial worker draining up to
			// queueCap (256) queued fires against a dead webhook at
			// ~20s each (3 attempts x 5s client timeout, plus 1s/4s
			// backoff) — in practice bounded well below that by per-rule
			// cooldowns (at most one queued fire per rule per cooldown
			// window) and the short window between pipe.Drain and here.
			// fx cannot preempt this: if it ever proves too slow in
			// practice, the tracked follow-up is a hard cap here (with
			// whatever's left in the queue counted as dropped) rather
			// than trusting the process orchestrator's SIGKILL.
			<-alertsDone

			// After drain, not before: any drops the pipeline recorded
			// while shutting down (the drain loop goes through the same
			// process/Decide path as the live loop) must be counted in
			// this final flush, not lost to a flush that ran ahead of
			// them.
			if err := engine.FlushDrops(ctx); err != nil {
				slog.Error("app: final noise-drop flush", "err", err)
			}

			if err := db.Close(); err != nil {
				return fmt.Errorf("app: store close: %w", err)
			}
			return nil
		},
	})
}

// boundedShutdownCtx derives a child of parent with a deadline no later
// than budget from now, and no later than parent's own deadline (if it
// has one) — i.e. min(budget, time remaining on parent). If parent has no
// deadline at all, the child is simply capped at budget.
func boundedShutdownCtx(parent context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	if dl, ok := parent.Deadline(); ok {
		if remaining := time.Until(dl); remaining < budget {
			budget = remaining
		}
	}
	return context.WithTimeout(parent, budget)
}

// bootstrap mints an instance admin key and prints it (plus the admin
// password, if it was freshly generated this boot — creds.password is
// only ever non-empty in that case) to stdout exactly once, the first
// time an admin key exists for this database. HasAdminKey is a durable,
// database-backed check (not a marker file living alongside it), so a
// file-only restore of the database — Litestream or a plain copy,
// carrying the existing admin key row with it — correctly prints
// nothing instead of minting and printing a redundant key.
func bootstrap(ctx context.Context, cfg config.Config, db *enginestore.Store, creds generatedCreds) error {
	has, err := db.HasAdminKey(ctx)
	if err != nil {
		return fmt.Errorf("check admin key: %w", err)
	}
	if has {
		return nil
	}

	// Mint before printing, not the other way round: if the process
	// crashes between the two, the worst case here is a minted-but-
	// unprinted admin key sitting in the database — HasAdminKey then
	// reports true on the next boot, nothing is printed, and the
	// operator is left without a usable key (recoverable by minting one
	// by hand against the db). Printing before minting risks the worse
	// failure mode: showing the operator a key that a subsequently
	// failed mint never actually created — a credential that looks
	// valid but silently isn't.
	key, err := db.MintKey(ctx, 0, "admin")
	if err != nil {
		return fmt.Errorf("mint admin key: %w", err)
	}

	printBootstrap(cfg, key, creds)
	return nil
}

func printBootstrap(cfg config.Config, key string, creds generatedCreds) {
	var b strings.Builder
	b.WriteString("\n=== Agenterr: first run ===\n")
	if creds.password != "" {
		fmt.Fprintf(&b, "Generated admin password: %s\n", creds.password)
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

// flushDropsLoop persists the noise-rule engine's in-memory drop counters
// on a cadence of interval (cfg.NoiseFlushMS, config.go's default 30s)
// until ctx is canceled. The final flush after shutdown (see register's
// OnStop) covers whatever accumulates between the last tick and the
// process stopping.
func flushDropsLoop(ctx context.Context, engine *rules.Engine, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := engine.FlushDrops(ctx); err != nil {
				slog.Error("app: periodic noise-drop flush", "err", err)
			}
		}
	}
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

// enforceMaxDBBytes is a last-resort guardrail: if the database file plus
// the engine's segment/WAL directory have together grown past
// cfg.MaxDBBytes despite normal per-project retention, prune everything
// older than a day, across every project, and warn loudly. This is a
// coarse safety valve, not a precise size target — getting the total back
// under the cap exactly would require knowing how much space pruning
// actually frees, which SQLite doesn't reclaim without a VACUUM, and
// segment deletion only frees space once the underlying files are
// removed.
//
// The size checked is cfg.DBPath plus the engine data directory
// (<dir(DBPath)>/engine, matching enginestore.Open's own layout) walked
// recursively: log bodies live in the engine's segments, not in the
// SQLite file, so measuring DBPath alone would silently stop guarding
// against exactly the data this cap exists to bound.
func enforceMaxDBBytes(ctx context.Context, cfg config.Config, st store.Store, projects []core.Project) {
	total, err := dbAndEngineBytes(cfg.DBPath)
	if err != nil {
		return // best-effort: nothing to guard if we can't stat the files
	}
	if total <= cfg.MaxDBBytes {
		return
	}

	slog.Warn("app: database exceeds max-db-bytes, pruning oldest day across all projects",
		"size_bytes", total, "max_bytes", cfg.MaxDBBytes)

	cutoff := time.Now().UTC().Add(-guardrailWindow)
	for _, p := range projects {
		if _, err := st.Prune(ctx, p.ID, cutoff); err != nil {
			slog.Error("app: retention guardrail prune", "project", p.ID, "err", err)
		}
	}
}

// dbAndEngineBytes returns the combined size of the SQLite file at dbPath
// and every file under its sibling engine data directory
// (<dir(dbPath)>/engine — segments and WALs), the same layout
// enginestore.Open constructs. A missing dbPath is an error (nothing to
// guard); a missing or not-yet-created engine directory is not (a
// freshly bootstrapped store may not have written anything yet).
func dbAndEngineBytes(dbPath string) (int64, error) {
	fi, err := os.Stat(dbPath)
	if err != nil {
		return 0, err
	}
	total := fi.Size()

	engineDir := filepath.Join(filepath.Dir(dbPath), "engine")
	err = filepath.WalkDir(engineDir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	return total, nil
}
