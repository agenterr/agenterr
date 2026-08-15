package enginestore

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/segment"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
)

// Prune removes a project's logs older than before, with row precision:
// the project is flushed first (so the WAL and memtable cannot
// resurrect pruned rows), whole-old segments are deleted outright, and
// a segment straddling the cutoff is rewritten without its old rows.
// Event refs are cleaned alongside. Rollups are intentionally retained
// (spec: trend history outlives bodies). Returns removed log count.
//
// The manifest mutations (per-segment insert-then-delete swap) run under
// ps.mu, acquired after flushProject returns (flushProject takes ps.mu
// itself, so this must not nest inside it). This closes the same race
// class flushProject was hardened against: collectRows takes its manifest
// snapshot under ps.mu too, so without this lock a concurrent reader could
// observe both a straddling segment's old file and its "-pruned"
// replacement in the same Segments() call — double-counting the rows that
// survived the rewrite. DeleteIssueEventsBefore is metadata-only (no
// manifest or memtable interaction) and runs after the lock is released.
func (s *Store) Prune(ctx context.Context, projectID int64, before time.Time) (int64, error) {
	if err := s.flushProject(projectID); err != nil {
		return 0, err
	}
	ps, err := s.proj(projectID)
	if err != nil {
		return 0, err
	}
	cutoff := before.UTC().UnixMicro()

	ps.mu.Lock()
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		ps.mu.Unlock()
		return 0, err
	}
	var removed int64
	for _, m := range segs {
		switch {
		case m.MaxTs < cutoff: // entirely old
			if err := s.dropSegment(ctx, m); err != nil {
				ps.mu.Unlock()
				return removed, err
			}
			removed += m.Count
		case m.MinTs < cutoff: // straddles: rewrite without old rows
			n, err := s.rewriteSegment(ctx, m, cutoff)
			if err != nil {
				ps.mu.Unlock()
				return removed, err
			}
			removed += n
		}
	}
	ps.mu.Unlock()

	if err := s.DB.DeleteIssueEventsBefore(ctx, projectID, before); err != nil {
		return removed, err
	}
	return removed, nil
}

// dropSegment deletes a segment's manifest row then its file. Manifest
// first: a file with no manifest row is an ignorable orphan, while a
// manifest row with no file would fail reads.
func (s *Store) dropSegment(ctx context.Context, m sqlitestore.SegmentMeta) error {
	if err := s.DB.DeleteSegment(ctx, m.ID); err != nil {
		return err
	}
	if err := os.Remove(s.segPath(m.Path)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("enginestore: remove segment: %w", err)
	}
	return nil
}

// rewriteSegment replaces m with a copy holding only rows at/after
// cutoff, returning how many rows were dropped. If nothing survives the
// filter the segment is simply dropped.
//
// The manifest swap (delete m's row, insert the "-pruned" replacement's
// row) goes through sqlitestore.SwapSegment — one transaction — instead
// of a separate InsertSegment then DeleteSegment: two separate commits
// left a crash-between-them window where BOTH manifest rows survived
// (double-reading whatever rows the rewrite kept, permanently, since the
// old row was never cleaned up) and wedged retention on retry (see
// SwapSegment's doc comment). Only the old *file* is removed
// post-commit, same as dropSegment: a removal failure there is a
// harmless orphan file with no manifest row pointing at it, not a
// correctness problem.
func (s *Store) rewriteSegment(ctx context.Context, m sqlitestore.SegmentMeta, cutoff int64) (int64, error) {
	_, rows, err := segment.Read(s.segPath(m.Path))
	if err != nil {
		return 0, err
	}
	keep := rows[:0]
	for _, r := range rows {
		if r.TsMicros >= cutoff {
			keep = append(keep, r)
		}
	}
	dropped := int64(len(rows) - len(keep))
	if len(keep) == 0 {
		return dropped, s.dropSegment(ctx, m)
	}
	rel := strings.TrimSuffix(m.Path, ".seg") + "-pruned.seg"
	foot, err := segment.Write(s.segPath(rel), keep)
	if err != nil {
		return 0, err
	}
	meta := sqlitestore.SegmentMeta{
		ProjectID: m.ProjectID, Path: rel,
		MinTs: foot.MinTs, MaxTs: foot.MaxTs,
		MinLogID: foot.MinLogID, MaxLogID: foot.MaxLogID,
		Count: int64(foot.Count), Events: foot.Events, Services: foot.Services,
	}
	if _, err := s.DB.SwapSegment(ctx, m.ID, meta); err != nil {
		_ = os.Remove(s.segPath(rel))
		return 0, err
	}
	if err := os.Remove(s.segPath(m.Path)); err != nil && !os.IsNotExist(err) {
		return dropped, fmt.Errorf("enginestore: remove segment: %w", err)
	}
	return dropped, nil
}
