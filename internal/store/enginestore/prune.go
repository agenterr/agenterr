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
func (s *Store) Prune(ctx context.Context, projectID int64, before time.Time) (int64, error) {
	if err := s.flushProject(projectID); err != nil {
		return 0, err
	}
	cutoff := before.UTC().UnixMicro()
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		return 0, err
	}
	var removed int64
	for _, m := range segs {
		switch {
		case m.MaxTs < cutoff: // entirely old
			if err := s.dropSegment(ctx, m); err != nil {
				return removed, err
			}
			removed += m.Count
		case m.MinTs < cutoff: // straddles: rewrite without old rows
			n, err := s.rewriteSegment(ctx, m, cutoff)
			if err != nil {
				return removed, err
			}
			removed += n
		}
	}
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
	if _, err := s.DB.InsertSegment(ctx, meta); err != nil {
		_ = os.Remove(s.segPath(rel))
		return 0, err
	}
	return dropped, s.dropSegment(ctx, m)
}
