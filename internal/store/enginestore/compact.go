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
// old files are removed only after commit. The manifest swap and old-file
// removal run under the project's ps.mu, mirroring flush and prune — that
// guarantees no reader observes a manifest row with no backing file (the
// row is gone before the file is removed) and no row is double-counted
// or lost across the swap (the whole replace commits in one transaction).
// It does NOT guarantee every reader's file open lands after the swap: a
// reader can snapshot the manifest just before this runs and then try to
// open a member segment just after its file is removed. That ENOENT is
// expected, not corruption — collectRows/logByID handle it by re-fetching
// the manifest (see isSegmentNotExist/freshSegmentByID in read.go): gone
// from the fresh manifest too means legitimately replaced, and since the
// replacement (merged) segment is not part of the snapshot the reader is
// mid-pass on, the reader abandons that pass and restarts from a brand
// new snapshot (readSegmentRowsWithRestart/readSegmentFileWithRestart,
// bounded by maxSegmentSetRestarts) rather than silently returning a
// result missing that segment's rows; still present in the fresh manifest
// means real corruption (propagate). Reading the old (immutable) segments
// happens outside the lock.
//
// CompactAll is serialized on s.compactMu: the compaction loop is a
// single goroutine, but CompactAll is also exported for tests and any
// other caller, and two concurrent runs would race on the same
// candidate buckets (and, worse, could pick colliding output paths).
// Concurrent calls simply run back-to-back — fine at the hourly cadence
// this is meant for.
//
// Between buckets, a non-blocking check of s.stop lets a pass in progress
// on compactLoop's goroutine bail out early (returning nil, not an error —
// stopping mid-pass is normal shutdown, not failure) instead of running to
// completion across every project/bucket first: Close() waits on that
// goroutine via s.wg, and a full pass over many projects could otherwise
// delay shutdown well past Close's caller's patience. Any buckets skipped
// this way are simply picked up by the next scheduled pass.
func (s *Store) CompactAll(ctx context.Context) error {
	s.compactMu.Lock()
	defer s.compactMu.Unlock()
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
			select {
			case <-s.stop:
				return nil
			default:
			}
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

	// s.proj() (not readProj) is deliberate here: compaction is a write-side
	// actor that must take ps.mu around the manifest swap below, including
	// for a segment-only project (all memtable rows already flushed, no
	// projState left) that a pure read path would never touch — the "reads
	// never create engine state" rule (readProj's doc comment) applies to
	// queries, not to this maintenance write.
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
		if m.Path == meta.Path {
			// Should be unreachable now that the output name is unique per
			// generation (see buildMergedSegment), but this is the file the
			// new manifest row points at — removing it would strand the
			// swap we just committed, so skip as a belt-and-braces guard.
			continue
		}
		if err := os.Remove(s.segPath(m.Path)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("enginestore: remove old segment %s: %w", m.Path, err)
		}
	}
	return nil
}

// buildMergedSegment reads every member segment (outside any lock — they
// are immutable) and writes their concatenated rows to a new segment
// file at segments/<pid>/c-<bucket>-<minLogID>-<maxMemberID>.seg,
// returning the manifest row ready for ReplaceSegments.
//
// RawRows and SizeBytes are computed from ground truth here — RawRows by
// counting TemplateID==0 over the concatenated rows actually in memory,
// SizeBytes by os.Stat on the file just written — rather than summed from
// member metadata. A member's own RawRows/SizeBytes can be zero even
// though it holds raw rows (e.g. a straddling segment rewritten by an
// older prune.go that omitted those columns); summing such zeros would
// silently poison every merged segment downstream, so this function never
// trusts member totals for either field.
//
// The name includes both the minimum member LogID and the maximum
// member MANIFEST id (not LogID): bucket+minLogID alone is a pure
// function of the bucket's original contents, so when a later,
// backdated write lands a new segment in an ALREADY-compacted bucket,
// re-compacting it would recompute the very same minLogID (the earlier
// merge's own MinLogID, since it is still the smallest) and collide with
// — i.e. get renamed over — the live, manifest-referenced output of the
// prior generation, outside ps.mu. segment_manifest.id is an
// AUTOINCREMENT primary key (migration 0010): SQLite tracks the
// high-water mark in sqlite_sequence and never reissues an id, even one
// freed by ReplaceSegments deleting the current max row — a plain
// INTEGER PRIMARY KEY rowid alias would not have this guarantee, since
// SQLite would then assign max(existing)+1 and could reuse a deleted
// max id. With AUTOINCREMENT, every re-compaction includes at least one
// member (the earlier merged segment, or the new arrival) with a higher
// id than any previous generation used, so appending the max member id
// makes the name unique per generation and never equal to a current
// member's path.
func (s *Store) buildMergedSegment(projectID int64, key string, members []sqlitestore.SegmentMeta) (sqlitestore.SegmentMeta, error) {
	var rows []segment.Row
	minLogID := members[0].MinLogID
	maxMemberID := members[0].ID
	for _, m := range members {
		_, rs, err := segment.Read(s.segPath(m.Path))
		if err != nil {
			return sqlitestore.SegmentMeta{}, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
		}
		rows = append(rows, rs...)
		if m.MinLogID < minLogID {
			minLogID = m.MinLogID
		}
		if m.ID > maxMemberID {
			maxMemberID = m.ID
		}
	}

	rel := filepath.Join("segments", fmt.Sprintf("%d", projectID), fmt.Sprintf("c-%s-%d-%d.seg", key, minLogID, maxMemberID))
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
	var rawRows int64
	for _, r := range rows {
		if r.TemplateID == 0 {
			rawRows++
		}
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
