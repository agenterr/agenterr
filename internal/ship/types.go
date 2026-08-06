// Package ship holds types shared across the ship subpackages (file,
// docker, process, buffer) to avoid import cycles between them.
package ship

import "github.com/agenterr/agenterr/internal/ship/process"

// Sourced pairs a process.Line with the service name of the source it came
// from, so an orchestrator fanning in multiple tailers can route each line
// to the right per-service process.Joiner.
type Sourced struct {
	Service string
	Line    process.Line
}
