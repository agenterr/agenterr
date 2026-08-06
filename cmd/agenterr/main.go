// Command agenterr runs the Agenterr server: ingest edges, REST API, MCP
// server, and web UI on a single port, backed by a local SQLite database.
package main

import (
	"os"

	"go.uber.org/fx"

	"github.com/agenterr/agenterr/internal/app"
)

func main() {
	opts := []fx.Option{app.Module}

	// fx's own startup/shutdown logging is noisy and aimed at debugging
	// the graph, not at end users running the binary — keep it out of
	// normal stdout unless explicitly asked for.
	if os.Getenv("AGENTERR_DEBUG") == "" {
		opts = append(opts, fx.NopLogger)
	}

	fx.New(opts...).Run()
}
