package enginestore

import (
	"context"

	"github.com/agenterr/agenterr/internal/store"
)

// EngineStats implements store.EngineMetrics: manifest-level segment/row
// totals (via sqlite.DB.EngineTotals, embedded on Store) plus the live
// memtable row count. For a single project (projectID != 0), the manifest
// query and the memtable read are taken together under that project's
// ps.mu, the same coherence pattern collectRows uses, so a concurrent
// flush can't be observed mid-flight (e.g. rows double-counted across
// both the just-written segment and the not-yet-reset memtable). A
// project with no engine state yet (readProj returns nil — nothing
// written since Open) reports MemRows 0 rather than creating one.
//
// projectID == 0 means "all projects" (matching Segments/EngineTotals'
// own projectID==0 convention). There is no single ps.mu to pair the
// manifest read against there, so MemRows is a separate pass: it sums
// Len() across every known project's memtable, each under its own ps.mu,
// rather than reading a single (nonexistent) project-0 projState — which
// would otherwise always report 0 regardless of how much unflushed data
// actually exists. This can race an individual project's concurrent
// flush (its rows momentarily missing from both the manifest total and
// the memtable sum, or double-counted, depending on interleaving), the
// same class of imprecision EngineTotals(0) already accepts by summing
// per-project manifest rows outside any single lock.
func (s *Store) EngineStats(ctx context.Context, projectID int64) (store.EngineStats, error) {
	if projectID == 0 {
		segments, rows, rawRows, sizeBytes, err := s.EngineTotals(ctx, projectID)
		if err != nil {
			return store.EngineStats{}, err
		}
		s.mu.Lock()
		pss := make([]*projState, 0, len(s.projects))
		for _, ps := range s.projects {
			pss = append(pss, ps)
		}
		s.mu.Unlock()
		var memRows int64
		for _, ps := range pss {
			ps.mu.Lock()
			memRows += int64(ps.mem.Len())
			ps.mu.Unlock()
		}
		return store.EngineStats{
			Segments: segments, Rows: rows, RawRows: rawRows,
			SizeBytes: sizeBytes, MemRows: memRows,
		}, nil
	}

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
