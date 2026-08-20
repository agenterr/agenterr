package enginestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
// new snapshot (readSegmentRowsWithRestart/findInSegmentWithRestart,
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
		for key, members := range bucketSegments(pidSegs, now, s.opts.CompactShardRows) {
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
// current-hour bucket, and returns only the buckets CompactAll will
// actually merge: 2+ members AND more members than the minimum shard
// count the bucket's total rows allow under shardRows. The second
// condition is the anti-rechurn guard: a bucket that a prior pass
// already merged into ceil(rows/shardRows) shards is at its floor —
// re-merging it would rewrite the same bytes into the same number of
// segments every pass, forever. A later (backdated) flush adds a member
// above the floor and makes the bucket eligible again.
func bucketSegments(segs []sqlitestore.SegmentMeta, now time.Time, shardRows int) map[string][]sqlitestore.SegmentMeta {
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
		if len(ms) < 2 || len(ms) <= minShardCount(ms, shardRows) {
			delete(buckets, k)
		}
	}
	return buckets
}

// minShardCount is the fewest segments a bucket's rows can occupy under
// the shard cap: ceil(totalRows / shardRows), at least 1.
func minShardCount(members []sqlitestore.SegmentMeta, shardRows int) int {
	var total int64
	for _, m := range members {
		total += m.Count
	}
	n := int((total + int64(shardRows) - 1) / int64(shardRows))
	if n < 1 {
		n = 1
	}
	return n
}

// compactBucket merges one project's bucket of segments into
// ceil(rows/CompactShardRows) ts-ordered shard segments. The old
// segments are read outside ps.mu (they are immutable once flushed); the
// new files are written outside the lock too. Only the manifest swap and
// old-file removal run under ps.mu, after re-verifying every member
// still exists in the manifest — a concurrent Prune may have dropped or
// rewritten one while this bucket's rows were being read. If that
// happened, the bucket is skipped and the orphan new files are removed.
func (s *Store) compactBucket(ctx context.Context, projectID int64, key string, members []sqlitestore.SegmentMeta) error {
	metas, err := s.buildMergedSegments(projectID, key, members)
	if err != nil {
		return err
	}
	removeNew := func() {
		for _, m := range metas {
			_ = os.Remove(s.segPath(m.Path))
		}
	}

	// s.proj() (not readProj) is deliberate here: compaction is a write-side
	// actor that must take ps.mu around the manifest swap below, including
	// for a segment-only project (all memtable rows already flushed, no
	// projState left) that a pure read path would never touch — the "reads
	// never create engine state" rule (readProj's doc comment) applies to
	// queries, not to this maintenance write.
	ps, err := s.proj(projectID)
	if err != nil {
		removeNew()
		return err
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	current, err := s.Segments(ctx, projectID)
	if err != nil {
		removeNew()
		return err
	}
	oldIDs, ok := memberIDsUnchanged(current, members)
	if !ok {
		// A concurrent Prune changed the manifest under us; skip this
		// bucket (it will be retried on the next compaction pass) and
		// clean up the orphan files this attempt produced.
		removeNew()
		return nil
	}

	if _, err := s.ReplaceSegmentsMulti(ctx, oldIDs, metas); err != nil {
		removeNew()
		return err
	}
	newPaths := make(map[string]bool, len(metas))
	for _, m := range metas {
		newPaths[m.Path] = true
	}
	for _, m := range members {
		if newPaths[m.Path] {
			// Should be unreachable now that output names are unique per
			// generation (see buildMergedSegments), but these are the files
			// the new manifest rows point at — removing one would strand
			// the swap we just committed, so skip as a belt-and-braces
			// guard.
			continue
		}
		if err := os.Remove(s.segPath(m.Path)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("enginestore: remove old segment %s: %w", m.Path, err)
		}
	}
	return nil
}

// buildMergedSegments reads every member segment (outside any lock —
// they are immutable), sorts the concatenated rows into one global
// (TsMicros, LogID) order, and writes them as ceil(n/CompactShardRows)
// shard segments at segments/<pid>/c-<bucket>-<minLogID>-<maxMemberID>-<shard>.seg,
// returning the manifest rows ready for ReplaceSegmentsMulti. The global
// sort matters: segment.Write sorts within one file, but shard files are
// cut from the concatenation, so without it shards could overlap in time
// and defeat manifest-level ts pruning. Sharding (rather than one merged
// file) is what lets the parallel search path divide a big bucket's
// column decompression across cores.
//
// RawRows and SizeBytes are computed from ground truth here — RawRows by
// counting TemplateID==0 over the shard's rows actually in memory,
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
// freed by ReplaceSegmentsMulti deleting the current max row — a plain
// INTEGER PRIMARY KEY rowid alias would not have this guarantee, since
// SQLite would then assign max(existing)+1 and could reuse a deleted
// max id. With AUTOINCREMENT, every re-compaction includes at least one
// member (an earlier merged shard, or the new arrival) with a higher
// id than any previous generation used, so appending the max member id
// (plus the shard index) makes every name unique per generation and
// never equal to a current member's path.
func (s *Store) buildMergedSegments(projectID int64, key string, members []sqlitestore.SegmentMeta) ([]sqlitestore.SegmentMeta, error) {
	var rows []segment.Row
	minLogID := members[0].MinLogID
	maxMemberID := members[0].ID
	for _, m := range members {
		_, rs, err := segment.Read(s.segPath(m.Path))
		if err != nil {
			return nil, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
		}
		rows = append(rows, rs...)
		if m.MinLogID < minLogID {
			minLogID = m.MinLogID
		}
		if m.ID > maxMemberID {
			maxMemberID = m.ID
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rowLess(rows[i], rows[j]) })

	nShards := (len(rows) + s.opts.CompactShardRows - 1) / s.opts.CompactShardRows
	perShard := (len(rows) + nShards - 1) / nShards
	metas := make([]sqlitestore.SegmentMeta, 0, nShards)
	for shard := 0; shard < nShards; shard++ {
		lo := shard * perShard
		hi := min(lo+perShard, len(rows))
		rel := filepath.Join("segments", fmt.Sprintf("%d", projectID),
			fmt.Sprintf("c-%s-%d-%d-%d.seg", key, minLogID, maxMemberID, shard))
		meta, err := s.writeShard(projectID, rel, rows[lo:hi])
		if err != nil {
			for _, m := range metas {
				_ = os.Remove(s.segPath(m.Path))
			}
			return nil, err
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

// writeShard writes one shard's rows to rel and returns its manifest row
// with ground-truth RawRows/SizeBytes (see buildMergedSegments).
func (s *Store) writeShard(projectID int64, rel string, rows []segment.Row) (sqlitestore.SegmentMeta, error) {
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
