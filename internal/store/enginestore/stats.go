package enginestore

import (
	"context"

	"github.com/agenterr/agenterr/internal/store"
)

// EngineStats implements store.EngineMetrics: manifest-level segment/row
// totals (via sqlite.DB.EngineTotals, embedded on Store) plus the
// project's live memtable row count. The manifest query and the memtable
// read are taken together under the project's ps.mu, the same coherence
// pattern collectRows uses, so a concurrent flush can't be observed
// mid-flight (e.g. rows double-counted across both the just-written
// segment and the not-yet-reset memtable). A project with no engine
// state yet (readProj returns nil — nothing written since Open) reports
// MemRows 0 rather than creating one.
func (s *Store) EngineStats(ctx context.Context, projectID int64) (store.EngineStats, error) {
	ps := s.readProj(projectID)

	var segments, rows, rawRows, sizeBytes int64
	var memRows int64
	var err error
	if ps == nil {
		segments, rows, rawRows, sizeBytes, err = s.EngineTotals(ctx, projectID)
		if err != nil {
			return store.EngineStats{}, err
		}
	} else {
		ps.mu.Lock()
		segments, rows, rawRows, sizeBytes, err = s.EngineTotals(ctx, projectID)
		if err != nil {
			ps.mu.Unlock()
			return store.EngineStats{}, err
		}
		memRows = int64(ps.mem.Len())
		ps.mu.Unlock()
	}

	return store.EngineStats{
		Segments:  segments,
		Rows:      rows,
		RawRows:   rawRows,
		SizeBytes: sizeBytes,
		MemRows:   memRows,
	}, nil
}
