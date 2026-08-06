// Package shared holds types used across the ship subpackages (file,
// docker's consumers, and the root ship orchestrator) to avoid import
// cycles: the root ship package (orchestrator) imports the file and docker
// packages, so a type shared with file cannot itself live in the root
// package — that would make file -> ship -> file a cycle.
package shared

import "github.com/agenterr/agenterr/internal/ship/process"

// Sourced pairs a process.Line with the service name of the source it came
// from, so an orchestrator fanning in multiple tailers can route each line
// to the right per-service process.Joiner.
type Sourced struct {
	Service string
	Line    process.Line
}
