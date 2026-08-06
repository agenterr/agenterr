// Command agenterr runs the Agenterr server: ingest edges, REST API, MCP
// server, and web UI on a single port, backed by a local SQLite database.
// It also runs agenterr-ship, the log-tailing sidecar, as a subcommand:
// `agenterr ship ...`. Bare invocation (no subcommand) keeps serving,
// exactly as before ship existed.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/fx"

	"github.com/agenterr/agenterr/internal/app"
	"github.com/agenterr/agenterr/internal/ship"
)

// version is set at build time via -ldflags "-X main.version=...".
// goreleaser injects the tag; a local `go build` keeps the "dev" default.
var version = "dev"

func main() {
	cmd, rest := dispatchTarget(os.Args[1:])
	if cmd == "ship" {
		runShip(rest)
		return
	}

	if len(os.Args) > 1 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Println("agenterr " + version)
		return
	}

	opts := []fx.Option{app.Module}

	// fx's own startup/shutdown logging is noisy and aimed at debugging
	// the graph, not at end users running the binary — keep it out of
	// normal stdout unless explicitly asked for.
	if os.Getenv("AGENTERR_DEBUG") == "" {
		opts = append(opts, fx.NopLogger)
	}

	fx.New(opts...).Run()
}

// dispatchTarget decides which code path main takes for a given
// os.Args[1:] slice: "ship" (with the remaining args, for agenterr-ship)
// or "" (the existing serve path, untouched — including --version and the
// bare invocation). Kept separate from main so it's unit-testable without
// a subprocess: main just needs the args[0] == "ship" check, but pulling it
// out here also means the exact matching rule ("ship" only when it's the
// very first argument) lives in one place.
func dispatchTarget(args []string) (cmd string, rest []string) {
	if len(args) > 0 && args[0] == "ship" {
		return "ship", args[1:]
	}
	return "", args
}

// runShip parses agenterr-ship's own flags from args, runs the pipeline
// until an interrupt/TERM signal arrives, and reports any startup error the
// way an operator running a CLI expects: a message on stderr naming what
// went wrong and a non-zero exit — never a Go panic or stack trace.
func runShip(args []string) {
	cfg, err := ship.Load(args, os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "agenterr ship: "+err.Error())
		os.Exit(2) // usage error, per the ship semantics doc
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := ship.Run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "agenterr ship: "+err.Error())
		os.Exit(1)
	}
}
