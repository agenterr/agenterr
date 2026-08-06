// Package pipeline is the only path to disk for log writes: edges call
// Enqueue, and handlers never touch the store's Writer directly. It buffers
// incoming logs, annotates them (event detection, fingerprinting, titling)
// on the writer side, and batches them into the store.
package pipeline

import (
	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// Grouper assigns a stable fingerprint to a log, used to group events into
// issues. core.DefaultGrouper satisfies this by delegating to core.Fingerprint.
type Grouper interface {
	Fingerprint(l core.Log) string
}

// Notifier is fired per event entry after a batch is successfully written,
// carrying the store's IssueOutcome for that entry (New/Reopened). Real
// implementations must not block the writer loop — do the actual
// notification work asynchronously (e.g. hand off to a goroutine or a
// buffered channel of their own) and return quickly.
type Notifier interface {
	IssueEvent(e store.Entry, o store.IssueOutcome)
}

// NopNotifier is the v1 no-op Notifier.
type NopNotifier struct{}

// IssueEvent does nothing.
func (NopNotifier) IssueEvent(store.Entry, store.IssueOutcome) {}

// Dropper decides whether an ingested log should be filtered out before
// storage, and whether a project wants body parsing. Defined here rather
// than consumed from internal/rules so the pipeline never imports rules —
// rules.Engine satisfies this interface structurally.
type Dropper interface {
	Decide(l core.Log) (drop bool, ruleID int64)
	ParseBodies(projectID int64) bool
}

// NopDropper keeps everything and parses everything.
type NopDropper struct{}

// Decide never drops.
func (NopDropper) Decide(core.Log) (bool, int64) { return false, 0 }

// ParseBodies always parses.
func (NopDropper) ParseBodies(int64) bool { return true }
