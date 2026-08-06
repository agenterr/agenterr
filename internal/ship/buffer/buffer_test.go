package buffer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const bigCap = 512 * 1024 * 1024 // effectively unbounded for tests that don't exercise eviction

func rec(i int) []byte {
	return []byte(fmt.Sprintf(`{"n":%d}`, i))
}

// --- Behavior 1: Append -> Next -> Ack round-trip; restart resumes exactly ---

func TestAppendNextAckRestart(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := s.Append(rec(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	got, cur, err := s.Next(5)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("Next returned %d records, want 5", len(got))
	}
	for i, r := range got {
		want := string(rec(i))
		if string(r) != want {
			t.Errorf("record %d = %q, want %q", i, r, want)
		}
	}
	if err := s.Ack(cur); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restart.
	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer s2.Close()

	got2, cur2, err := s2.Next(100)
	if err != nil {
		t.Fatalf("Next after restart: %v", err)
	}
	if len(got2) != 5 {
		t.Fatalf("after restart, Next returned %d records, want 5 (records 5..9)", len(got2))
	}
	for i, r := range got2 {
		want := string(rec(i + 5))
		if string(r) != want {
			t.Errorf("record %d after restart = %q, want %q", i, r, want)
		}
	}
	if err := s2.Ack(cur2); err != nil {
		t.Fatalf("Ack after restart: %v", err)
	}

	// Now fully caught up.
	got3, _, err := s2.Next(100)
	if err != nil {
		t.Fatalf("Next when caught up: %v", err)
	}
	if len(got3) != 0 {
		t.Fatalf("Next when caught up returned %d records, want 0", len(got3))
	}
}

// --- Behavior 2: segments roll at 4MB; fully-acked segments are deleted ---

func TestSegmentRollAndAckedSegmentsDeleted(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Each record ~1KB; write enough to roll past 4MB into a second segment.
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = 'x'
	}
	recordJSON := func(i int) []byte {
		b, _ := json.Marshal(map[string]any{"n": i, "pad": string(payload)})
		return b
	}

	const n = 4500 // ~4.5MB of records -> should roll into segment 2
	for i := 0; i < n; i++ {
		if err := s.Append(recordJSON(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	segFiles, err := filepath.Glob(filepath.Join(dir, "spool-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segFiles) < 2 {
		t.Fatalf("expected at least 2 segment files after ~4.5MB of writes, got %d: %v", len(segFiles), segFiles)
	}

	// Drain everything and ack it all.
	for {
		got, cur, err := s.Next(1000)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if len(got) == 0 {
			break
		}
		if err := s.Ack(cur); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}

	// All segments except the current (last) write segment should be gone.
	segFiles, err = filepath.Glob(filepath.Join(dir, "spool-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segFiles) != 1 {
		t.Fatalf("expected exactly 1 segment file left (the live write segment) after full ack, got %d: %v", len(segFiles), segFiles)
	}
}

// --- Behavior 3: cap eviction drops oldest segment, Dropped() counts it, reader survives ---

func TestCapEviction(t *testing.T) {
	dir := t.TempDir()

	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = 'y'
	}
	recordJSON := func(i int) []byte {
		b, _ := json.Marshal(map[string]any{"n": i, "pad": string(payload)})
		return b
	}

	// Cap small enough that writing several MB forces eviction of the
	// oldest of several ~4MB segments.
	const cap = 5 * 1024 * 1024
	s, err := Open(dir, cap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const n = 9000 // ~9MB, should roll into 3 segments and evict at least 1
	for i := 0; i < n; i++ {
		if err := s.Append(recordJSON(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	dropped := s.Dropped()
	if dropped <= 0 {
		t.Fatalf("expected Dropped() > 0 after exceeding cap, got %d", dropped)
	}

	// Reader must survive: Next must not error, and must not return
	// anything from before the surviving oldest segment.
	got, _, err := s.Next(10)
	if err != nil {
		t.Fatalf("Next after eviction: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Next after eviction returned nothing; reader should still see surviving data")
	}
	var first int
	if err := json.Unmarshal(got[0], &struct {
		N *int `json:"n"`
	}{N: &first}); err != nil {
		// fall back to manual decode
	}
	var decoded struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(got[0], &decoded); err != nil {
		t.Fatalf("decode first surviving record: %v", err)
	}
	if decoded.N == 0 {
		t.Fatalf("first surviving record is n=0, expected the oldest segment (containing early records) to have been evicted")
	}
}

// --- Behavior 4: torn tail is truncated at the last complete record on Open ---

func TestTornTailTruncatedOnOpen(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := s.Append(rec(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write: append a partial line (no trailing \n)
	// directly to the current segment file.
	segFiles, err := filepath.Glob(filepath.Join(dir, "spool-*.jsonl"))
	if err != nil || len(segFiles) != 1 {
		t.Fatalf("expected 1 segment file, got %v (err=%v)", segFiles, err)
	}
	f, err := os.OpenFile(segFiles[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(`{"n":5,"garbage`)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen: torn tail should be truncated away, leaving exactly the
	// original 5 complete records readable.
	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open with torn tail: %v", err)
	}
	defer s2.Close()

	got, _, err := s2.Next(100)
	if err != nil {
		t.Fatalf("Next after torn-tail repair: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("Next after torn-tail repair returned %d records, want 5", len(got))
	}
	for i, r := range got {
		want := string(rec(i))
		if string(r) != want {
			t.Errorf("record %d = %q, want %q", i, r, want)
		}
	}

	// Appending after repair must work and produce a well-formed record
	// right after the truncation point.
	if err := s2.Append(rec(99)); err != nil {
		t.Fatalf("Append after repair: %v", err)
	}
	got2, _, err := s2.Next(100)
	if err != nil {
		t.Fatalf("Next after post-repair append: %v", err)
	}
	if len(got2) != 6 || string(got2[5]) != string(rec(99)) {
		t.Fatalf("Next after post-repair append = %v, want 6 records ending in %s", got2, rec(99))
	}
}

// --- Behavior 5: Since round-trips through restart ---

func TestSinceRoundTrip(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ts := time.Date(2026, 8, 6, 12, 34, 56, 789000000, time.UTC)
	if err := s.SetSince("web", ts); err != nil {
		t.Fatalf("SetSince: %v", err)
	}
	if _, ok := s.Since("db"); ok {
		t.Fatal("Since(unset container) should report ok=false")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer s2.Close()

	got, ok := s2.Since("web")
	if !ok {
		t.Fatal("Since(\"web\") after restart: ok=false, want true")
	}
	if !got.Equal(ts) {
		t.Fatalf("Since(\"web\") after restart = %v, want %v", got, ts)
	}
}

// --- Behavior 6: concurrency + conservation under -race ---
//
// This exercises the writer (Append) and reader (Next/Ack) concurrently
// under -race to prove the mutex actually serializes state correctly. To
// keep the conservation check unambiguous, the cap is large enough that no
// eviction happens here — every appended record must be delivered by Next
// exactly once and successfully acked, with none lost or duplicated. The
// separate TestEvictionConservationWithPartialAck test below covers the
// harder three-way split (acked/dropped/pending) that eviction introduces.
func TestConcurrentAppendAckConservation(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	const nAppends = 20000
	var appended int64

	seen := make([]int32, nAppends) // 0 = not delivered, 1 = delivered exactly once
	var duplicates int64

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < nAppends; i++ {
			if err := s.Append(rec(i)); err != nil {
				t.Errorf("Append(%d): %v", i, err)
				return
			}
			atomic.AddInt64(&appended, 1)
		}
	}()

	writerDone := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			got, cur, err := s.Next(50)
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			for _, r := range got {
				var d struct {
					N int `json:"n"`
				}
				if err := json.Unmarshal(r, &d); err != nil {
					t.Errorf("decode record: %v", err)
					continue
				}
				if !atomic.CompareAndSwapInt32(&seen[d.N], 0, 1) {
					atomic.AddInt64(&duplicates, 1)
				}
			}
			if len(got) > 0 {
				if err := s.Ack(cur); err != nil {
					t.Errorf("Ack: %v", err)
					return
				}
				continue
			}
			select {
			case <-writerDone:
				return // caught up and writer finished: done
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}()

	go func() {
		for atomic.LoadInt64(&appended) < nAppends {
			time.Sleep(time.Millisecond)
		}
		close(writerDone)
	}()
	wg.Wait()

	if dropped := s.Dropped(); dropped != 0 {
		t.Fatalf("Dropped() = %d, want 0 (cap was set high enough to avoid eviction)", dropped)
	}
	if duplicates != 0 {
		t.Fatalf("delivered %d duplicate records", duplicates)
	}
	for i, v := range seen {
		if v == 0 {
			t.Fatalf("record %d was never delivered by Next", i)
		}
	}
}

// --- Deterministic eviction + partial-ack conservation ---
//
// Regression test for a specific double-count hazard: the ack checkpoint
// can sit partway inside the oldest surviving segment (some of its records
// acked, the rest not). If cap eviction then evicts that very segment, it
// must count only the still-unacked tail as dropped — counting the whole
// segment would double-count records that are simultaneously "acked" and
// "dropped", breaking appended == acked + dropped + pending.
func TestEvictionConservationWithPartialAck(t *testing.T) {
	dir := t.TempDir()

	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = 'z'
	}
	recordJSON := func(i int) []byte {
		b, _ := json.Marshal(map[string]any{"n": i, "pad": string(payload)})
		return b
	}

	const cap = 5 * 1024 * 1024
	s, err := Open(dir, cap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	var appended, acked int64

	// Write enough to roll into a second segment, then ack only part of
	// the first segment — leaving the ack checkpoint inside segment 1's
	// unacked tail while segment 1 is still the oldest on disk.
	const firstBatch = 4200 // > one 4MB segment's worth at ~1KB/record
	for i := 0; i < firstBatch; i++ {
		if err := s.Append(recordJSON(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		appended++
	}
	got, cur, err := s.Next(1000) // partial ack: well within segment 1
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(got) != 1000 {
		t.Fatalf("Next returned %d records, want 1000", len(got))
	}
	if err := s.Ack(cur); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	acked += int64(len(got))

	// Keep appending to push total size past the cap, forcing eviction of
	// segment 1 (the oldest), which is now known to be partially acked.
	i := firstBatch
	for s.Dropped() == 0 && i < firstBatch+5000 {
		if err := s.Append(recordJSON(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		appended++
		i++
	}
	dropped := s.Dropped()
	if dropped == 0 {
		t.Fatal("expected cap eviction to have triggered")
	}

	// Drain and ack everything that remains.
	for {
		got, cur, err := s.Next(10000)
		if err != nil {
			t.Fatalf("Next (drain): %v", err)
		}
		if len(got) == 0 {
			break
		}
		if err := s.Ack(cur); err != nil {
			t.Fatalf("Ack (drain): %v", err)
		}
		acked += int64(len(got))
	}

	if appended != acked+dropped {
		t.Fatalf("conservation violated: appended=%d, acked=%d, dropped=%d (acked+dropped=%d)",
			appended, acked, dropped, acked+dropped)
	}
}
