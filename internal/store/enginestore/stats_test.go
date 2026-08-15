package enginestore

import (
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/store"
)

func TestEngineStats_FlushedAndUnflushedRows(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{FlushRows: 2})
	p, err := s.CreateProject(ctx, "p", 30)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Two rows flush to a segment (FlushRows: 2); a third stays in the
	// memtable.
	entries := []store.Entry{
		logEntry(p.ID, "row one", "api", at),
		logEntry(p.ID, "row two", "api", at.Add(time.Second)),
	}
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "row three", "api", at.Add(2*time.Second))}); err != nil {
		t.Fatal(err)
	}

	es, err := s.EngineStats(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if es.Segments != 1 {
		t.Errorf("Segments = %d, want 1", es.Segments)
	}
	if es.Rows != 2 {
		t.Errorf("Rows = %d, want 2 (flushed rows only)", es.Rows)
	}
	if es.MemRows != 1 {
		t.Errorf("MemRows = %d, want 1 (unflushed row)", es.MemRows)
	}
}

func TestEngineStats_UnknownProject_ZeroValues(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{})

	es, err := s.EngineStats(ctx, 999)
	if err != nil {
		t.Fatal(err)
	}
	if es != (store.EngineStats{}) {
		t.Errorf("got %+v, want zero value", es)
	}
}
