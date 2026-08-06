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

// Notifier is fired per event entry after a batch is successfully written.
// Real implementations must not block the writer loop — do the actual
// notification work asynchronously (e.g. hand off to a goroutine or a
// buffered channel of their own) and return quickly.
type Notifier interface {
	IssueEvent(e store.Entry)
}

// NopNotifier is the v1 no-op Notifier.
type NopNotifier struct{}

// IssueEvent does nothing.
func (NopNotifier) IssueEvent(store.Entry) {}
