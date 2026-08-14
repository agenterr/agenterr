package engine

import (
	"sync"
	"testing"

	"github.com/agenterr/agenterr/internal/segment"
)

func TestMemtableSnapshotIsolation(t *testing.T) {
	m := NewMemtable()
	m.Append(walRows(3))
	snap := m.Snapshot()
	if len(snap) != 3 || m.Len() != 3 {
		t.Fatalf("len: snap %d table %d", len(snap), m.Len())
	}
	snap[0].Service = "mutated"
	if m.Snapshot()[0].Service != "api" {
		t.Error("snapshot mutation leaked into memtable")
	}
	m.Reset()
	if m.Len() != 0 || len(snap) != 3 {
		t.Error("reset must not affect prior snapshots")
	}
}

func TestMemtableConcurrent(t *testing.T) {
	m := NewMemtable()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Append([]segment.Row{{LogID: 1, Service: "api"}})
				_ = m.Snapshot()
				_ = m.Len()
			}
		}()
	}
	wg.Wait()
	if m.Len() != 800 {
		t.Errorf("len = %d want 800", m.Len())
	}
}
