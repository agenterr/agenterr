package enginestore

import (
	"context"
	"os"
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
