package enginestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/agenterr/agenterr/internal/segment"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
)

// CompactAll merges small flushed segments to bound read amplification:
// per project, segments are bucketed by MinTs — hourly buckets for the
// current UTC day, daily buckets for prior days — and any bucket with
// two or more segments (excluding the still-filling current hour) is
// merged into one. The manifest swap is crash-atomic (ReplaceSegments);
// old files are removed only after commit. Reads stay coherent: the
// swap and removals run under the project's ps.mu, mirroring flush and
// prune. Reading the old (immutable) segments happens outside the lock.
func (s *Store) CompactAll(ctx context.Context) error {
	segs, err := s.Segments(ctx, 0)
	if err != nil {
		return err
	}
	byProject := map[int64][]sqlitestore.SegmentMeta{}
	for _, m := range segs {
		byProject[m.ProjectID] = append(byProject[m.ProjectID], m)
	}
	now := time.Now().UTC()
	var firstErr error
	for pid, pidSegs := range byProject {
		for key, members := range bucketSegments(pidSegs, now) {
			if err := s.compactBucket(ctx, pid, key, members); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("enginestore: compact project %d bucket %s: %w", pid, key, err)
			}
		}
	}
	return firstErr
}

// bucketKey returns the compaction bucket key for a segment's MinTs
// (epoch micros): hourly ("2006-01-02T15") when MinTs falls on now's
// UTC day, daily ("2006-01-02") otherwise.
func bucketKey(minTsMicros int64, now time.Time) string {
	t := time.UnixMicro(minTsMicros).UTC()
	ty, tm, td := t.Date()
	ny, nm, nd := now.Date()
	if ty == ny && tm == nm && td == nd {
		return t.Format("2006-01-02T15")
	}
	return t.Format("2006-01-02")
}

// bucketSegments groups segs by bucketKey, drops the still-filling
// current-hour bucket, and returns only buckets with 2+ members — the
// ones CompactAll will actually merge.
func bucketSegments(segs []sqlitestore.SegmentMeta, now time.Time) map[string][]sqlitestore.SegmentMeta {
	currentHour := now.Format("2006-01-02T15")
	buckets := map[string][]sqlitestore.SegmentMeta{}
	for _, m := range segs {
		k := bucketKey(m.MinTs, now)
		if k == currentHour {
			continue
		}
		buckets[k] = append(buckets[k], m)
	}
	for k, ms := range buckets {
		if len(ms) < 2 {
			delete(buckets, k)
		}
	}
	return buckets
}

// compactBucket merges one project's bucket of segments into a single
// new segment. The old segments are read outside ps.mu (they are
// immutable once flushed); the new file is written outside the lock too.
// Only the manifest swap and old-file removal run under ps.mu, after
// re-verifying every member still exists in the manifest — a concurrent
// Prune may have dropped or rewritten one while this bucket's rows were
// being read. If that happened, the bucket is skipped and the orphan new
// file is removed.
func (s *Store) compactBucket(ctx context.Context, projectID int64, key string, members []sqlitestore.SegmentMeta) error {
	meta, err := s.buildMergedSegment(projectID, key, members)
	if err != nil {
		return err
	}

	ps, err := s.proj(projectID)
	if err != nil {
		_ = os.Remove(s.segPath(meta.Path))
		return err
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	current, err := s.Segments(ctx, projectID)
	if err != nil {
		_ = os.Remove(s.segPath(meta.Path))
		return err
	}
	oldIDs, ok := memberIDsUnchanged(current, members)
	if !ok {
		// A concurrent Prune changed the manifest under us; skip this
		// bucket (it will be retried on the next compaction pass) and
		// clean up the orphan file this attempt produced.
		_ = os.Remove(s.segPath(meta.Path))
		return nil
	}

	if _, err := s.ReplaceSegments(ctx, oldIDs, meta); err != nil {
		_ = os.Remove(s.segPath(meta.Path))
		return err
	}
	for _, m := range members {
		if err := os.Remove(s.segPath(m.Path)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("enginestore: remove old segment %s: %w", m.Path, err)
		}
	}
	return nil
}

// buildMergedSegment reads every member segment (outside any lock — they
// are immutable) and writes their concatenated rows to a new segment
// file at segments/<pid>/c-<bucket>-<minLogID>.seg, returning the
// manifest row ready for ReplaceSegments.
func (s *Store) buildMergedSegment(projectID int64, key string, members []sqlitestore.SegmentMeta) (sqlitestore.SegmentMeta, error) {
	var rows []segment.Row
	var rawRows int64
	minLogID := members[0].MinLogID
	for _, m := range members {
		_, rs, err := segment.Read(s.segPath(m.Path))
		if err != nil {
			return sqlitestore.SegmentMeta{}, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
		}
		rows = append(rows, rs...)
		rawRows += m.RawRows
		if m.MinLogID < minLogID {
			minLogID = m.MinLogID
		}
	}

	rel := filepath.Join("segments", fmt.Sprintf("%d", projectID), fmt.Sprintf("c-%s-%d.seg", key, minLogID))
	abs := s.segPath(rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return sqlitestore.SegmentMeta{}, fmt.Errorf("enginestore: mkdir segment dir: %w", err)
	}
	foot, err := segment.Write(abs, rows)
	if err != nil {
		return sqlitestore.SegmentMeta{}, fmt.Errorf("enginestore: write merged segment: %w", err)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return sqlitestore.SegmentMeta{}, fmt.Errorf("enginestore: stat merged segment: %w", err)
	}
	return sqlitestore.SegmentMeta{
		ProjectID: projectID, Path: rel,
		MinTs: foot.MinTs, MaxTs: foot.MaxTs,
		MinLogID: foot.MinLogID, MaxLogID: foot.MaxLogID,
		Count: int64(foot.Count), Events: foot.Events, Services: foot.Services,
		RawRows: rawRows, SizeBytes: fi.Size(),
	}, nil
}

// memberIDsUnchanged reports whether every segment in members is still
// present, unmodified, in current (matched by ID) — and if so returns
// their IDs for ReplaceSegments's delete list.
func memberIDsUnchanged(current, members []sqlitestore.SegmentMeta) ([]int64, bool) {
	byID := make(map[int64]sqlitestore.SegmentMeta, len(current))
	for _, m := range current {
		byID[m.ID] = m
	}
	ids := make([]int64, 0, len(members))
	for _, want := range members {
		got, ok := byID[want.ID]
		if !ok || got.Path != want.Path {
			return nil, false
		}
		ids = append(ids, want.ID)
	}
	return ids, true
}
