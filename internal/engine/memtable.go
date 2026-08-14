package engine

import (
	"sync"

	"github.com/agenterr/agenterr/internal/segment"
)

// Memtable holds rows accepted but not yet flushed to a segment. Reads
// over recent data (the last flush window) come from here. Safe for
// concurrent use; Snapshot returns an independent copy so readers are
// never affected by a concurrent Reset or Append.
type Memtable struct {
	mu   sync.RWMutex
	rows []segment.Row
}

// NewMemtable returns an empty Memtable.
func NewMemtable() *Memtable { return &Memtable{} }

// Append adds rows.
func (m *Memtable) Append(rows []segment.Row) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = append(m.rows, rows...)
}

// Snapshot returns a copy of the current rows.
func (m *Memtable) Snapshot() []segment.Row {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]segment.Row(nil), m.rows...)
}

// Len reports the current row count.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rows)
}

// Reset empties the memtable (after its rows are flushed durably).
func (m *Memtable) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = nil
}
