package enginestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/store"
)

// yesterdayBatches writes 3 separate flushed segments on a past day.
func yesterdayBatches(t *testing.T, s *Store, pid int64) {
	t.Helper()
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour).Add(6 * time.Hour)
	for i := 0; i < 3; i++ {
		if _, err := s.WriteBatch(context.Background(), []store.Entry{
			logEntry(pid, "past row", "api", base.Add(time.Duration(i)*time.Minute)),
		}); err != nil {
			t.Fatal(err)
		}
		if err := s.FlushAll(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactMergesPastDayBuckets(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	yesterdayBatches(t, s, p.ID)
	segsBefore, _ := s.Segments(ctx, p.ID)
	if len(segsBefore) != 3 {
		t.Fatalf("precondition: %d segments", len(segsBefore))
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	segs, _ := s.Segments(ctx, p.ID)
	if len(segs) != 1 || segs[0].Count != 3 {
		t.Fatalf("after compact: %+v", segs)
	}
	// Old files removed, new file present.
	for _, m := range segsBefore {
		if _, err := os.Stat(s.segPath(m.Path)); !os.IsNotExist(err) {
			t.Errorf("old segment file survives: %s", m.Path)
		}
	}
	if _, err := os.Stat(s.segPath(segs[0].Path)); err != nil {
		t.Errorf("merged file missing: %v", err)
	}
	// All rows still readable.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
	if err != nil || len(logs) != 3 {
		t.Fatalf("post-compact read: %d err %v", len(logs), err)
	}
	// Idempotent: nothing left to merge.
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if segs2, _ := s.Segments(ctx, p.ID); len(segs2) != 1 {
		t.Errorf("second compact changed things: %+v", segs2)
	}
}

// TestCompactRecompactionAfterLateArrival is the regression case for the
// filename-collision bug: a past-day bucket compacts once, then a
// late/backdated write lands a NEW segment in that same already-compacted
// bucket, and a second compaction must merge them into a fresh file
// rather than colliding with (and deleting) the live merged segment from
// the first generation. Before the fix, both generations computed the
// same "c-<bucket>-<minLogID>.seg" name (min member LogID is unchanged
// across generations — the first generation's own MinLogID is still the
// smallest), so the second compaction's segment.Write renamed over the
// manifest-referenced file from the first generation, and the post-swap
// removal loop then deleted that very same path out from under the new
// manifest row.
func TestCompactRecompactionAfterLateArrival(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	yesterdayBatches(t, s, p.ID)
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	segs, _ := s.Segments(ctx, p.ID)
	if len(segs) != 1 {
		t.Fatalf("precondition: expected 1 merged segment, got %+v", segs)
	}

	// A late/backdated write lands one more segment in the SAME past day.
	late := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour).Add(6*time.Hour + 30*time.Minute)
	if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "late row", "api", late)}); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}

	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}

	segs, err := s.Segments(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("after re-compaction: expected 1 segment, got %+v", segs)
	}
	if _, err := os.Stat(s.segPath(segs[0].Path)); err != nil {
		t.Errorf("manifest-referenced merged file missing on disk: %v", err)
	}

	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 4 {
		t.Fatalf("post-recompaction read: got %d logs, want 4", len(logs))
	}
	seen := map[int64]bool{}
	for _, l := range logs {
		if seen[l.ID] {
			t.Fatalf("duplicate log %d after re-compaction", l.ID)
		}
		seen[l.ID] = true
	}
}

func TestCompactSkipsCurrentHour(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	now := time.Now().UTC()
	for i := 0; i < 2; i++ {
		if _, err := s.WriteBatch(ctx, []store.Entry{logEntry(p.ID, "now row", "api", now)}); err != nil {
			t.Fatal(err)
		}
		if err := s.FlushAll(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if segs, _ := s.Segments(ctx, p.ID); len(segs) != 2 {
		t.Errorf("current-hour segments must not compact: %+v", segs)
	}
}

// TestCompactAllStopsBetweenBuckets is M5's regression case: CompactAll
// must notice s.stop closing partway through a multi-bucket pass and
// return promptly (nil, not an error) instead of grinding through every
// remaining project/bucket first — otherwise Close() (which waits on the
// compaction goroutine via s.wg) could be delayed by an arbitrarily long
// pass.
func TestCompactAllStopsBetweenBuckets(t *testing.T) {
	// Opened directly (not via openStore/t.Cleanup): this test closes
	// s.stop itself to simulate a shutdown already in progress, and
	// Close() would double-close that same channel and panic. Leaving the
	// store's files open past the end of the test is harmless — they live
	// under t.TempDir() and are never reopened.
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "agenterr.db"), Options{CompactEvery: -1})
	if err != nil {
		t.Fatal(err)
	}
	// Several distinct past-day buckets, each with 2+ segments, so a full
	// pass has multiple buckets to work through.
	for day := 2; day <= 4; day++ {
		p, _ := s.CreateProject(ctx, "p", 30)
		base := time.Now().UTC().AddDate(0, 0, -day).Truncate(24 * time.Hour).Add(6 * time.Hour)
		for i := 0; i < 2; i++ {
			if _, err := s.WriteBatch(context.Background(), []store.Entry{
				logEntry(p.ID, "row", "api", base.Add(time.Duration(i)*time.Minute)),
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.FlushAll(); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Simulate Close() having already signaled shutdown: close s.stop
	// directly (Close itself would also flush/close WALs, which would
	// interfere with the segments this test just wrote).
	close(s.stop)

	if err := s.CompactAll(ctx); err != nil {
		t.Fatalf("CompactAll after stop closed: %v", err)
	}
}

func TestCompactConcurrentWithReadsNoDuplicates(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1})
	p, _ := s.CreateProject(ctx, "p", 30)
	yesterdayBatches(t, s, p.ID)
	stopc := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopc:
				return
			default:
				_ = s.CompactAll(ctx)
			}
		}
	}()
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID})
		if err != nil {
			t.Fatalf("read during compact: %v", err)
		}
		seen := map[int64]bool{}
		for _, l := range logs {
			if seen[l.ID] {
				t.Fatalf("duplicate log %d during compaction", l.ID)
			}
			seen[l.ID] = true
		}
		if len(logs) != 3 {
			t.Fatalf("row count changed during compaction: %d", len(logs))
		}
	}
	close(stopc)
}

// TestCompactShardsLargeBuckets: a bucket whose rows exceed
// CompactShardRows merges into ceil(rows/cap) ts-ordered,
// non-overlapping shards — and a second pass leaves them alone (the
// anti-rechurn guard: the bucket is already at its shard floor).
func TestCompactShardsLargeBuckets(t *testing.T) {
	s := openStore(t, t.TempDir(), Options{CompactEvery: -1, CompactShardRows: 4})
	p, _ := s.CreateProject(ctx, "p", 30)
	base := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour).Add(6 * time.Hour)
	// Five flushed segments of 2 rows each: 10 rows, cap 4 → 3 shards
	// (5 members > ceil(10/4)=3, so the bucket is eligible; merging
	// reduces the segment count while keeping every shard under cap).
	for b := 0; b < 5; b++ {
		var entries []store.Entry
		for i := 0; i < 2; i++ {
			entries = append(entries, logEntry(p.ID, "past row", "api",
				base.Add(time.Duration(b*2+i)*time.Minute)))
		}
		if _, err := s.WriteBatch(context.Background(), entries); err != nil {
			t.Fatal(err)
		}
		if err := s.FlushAll(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	segs, _ := s.Segments(ctx, p.ID)
	if len(segs) != 3 {
		t.Fatalf("want 3 shards, got %d: %+v", len(segs), segs)
	}
	var total int64
	for _, m := range segs {
		total += m.Count
		if m.Count > 4 {
			t.Errorf("shard %s exceeds cap: %d rows", m.Path, m.Count)
		}
	}
	if total != 10 {
		t.Fatalf("rows lost/duplicated across shards: %d", total)
	}
	// Shards must not overlap in time (global sort before cutting).
	for i := 1; i < len(segs); i++ {
		for j := 0; j < i; j++ {
			a, b := segs[i], segs[j]
			if a.MinTs <= b.MaxTs && b.MinTs <= a.MaxTs {
				t.Errorf("shards %s and %s overlap in time", a.Path, b.Path)
			}
		}
	}
	// All rows still readable through search.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Limit: 100})
	if err != nil || len(logs) != 10 {
		t.Fatalf("post-shard read: %d err %v", len(logs), err)
	}
	// Anti-rechurn: a second pass must not rewrite the shards.
	idsBefore := map[int64]bool{}
	for _, m := range segs {
		idsBefore[m.ID] = true
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	segs2, _ := s.Segments(ctx, p.ID)
	if len(segs2) != 3 {
		t.Fatalf("second pass changed shard count: %d", len(segs2))
	}
	for _, m := range segs2 {
		if !idsBefore[m.ID] {
			t.Errorf("second pass rewrote shard %s (new manifest id %d)", m.Path, m.ID)
		}
	}
}
