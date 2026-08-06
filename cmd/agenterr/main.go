// Command agenterr runs the Agenterr server: ingest edges, REST API, MCP
// server, and web UI on a single port, backed by a local SQLite database.
package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"

	"github.com/agenterr/agenterr/internal/app"
)

// version is set at build time via -ldflags "-X main.version=...".
// goreleaser injects the tag; a local `go build` keeps the "dev" default.
var version = "dev"

func main() {
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
