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
	defer func() { _ = s2.Close() }()

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
	defer func() { _ = s.Close() }()

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
	const capBytes = 5 * 1024 * 1024
	s, err := Open(dir, capBytes)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

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

// --- Regression: Dropped() must survive a restart, and keep accumulating ---
//
// checkpoint.json persists the dropped count alongside the ack position;
// Open must restore it rather than zero-initializing it, or cap-eviction
// losses go silently unaccounted for across a restart.
func TestDroppedSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = 'w'
	}
	recordJSON := func(i int) []byte {
		b, _ := json.Marshal(map[string]any{"n": i, "pad": string(payload)})
		return b
	}

	const capBytes = 5 * 1024 * 1024
	s, err := Open(dir, capBytes)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; s.Dropped() == 0; i++ {
		if err := s.Append(recordJSON(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if i > 20000 {
			t.Fatal("eviction never triggered")
		}
	}
	firstDropped := s.Dropped()
	if firstDropped == 0 {
		t.Fatal("expected Dropped() > 0 before restart")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, capBytes)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.Dropped(); got != firstDropped {
		t.Fatalf("Dropped() after restart = %d, want %d (the pre-restart value)", got, firstDropped)
	}

	// Force a second eviction post-restart: the count must accumulate on
	// top of the restored value, not reset.
	for i := 30000; s2.Dropped() == firstDropped; i++ {
		if err := s2.Append(recordJSON(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		if i > 60000 {
			t.Fatal("second eviction never triggered")
		}
	}
	if got := s2.Dropped(); got <= firstDropped {
		t.Fatalf("Dropped() after second eviction = %d, want > %d", got, firstDropped)
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
	_ = f.Close()

	// Reopen: torn tail should be truncated away, leaving exactly the
	// original 5 complete records readable.
	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open with torn tail: %v", err)
	}
	defer func() { _ = s2.Close() }()

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

// --- Regression: torn-tail truncation accounting is explicit, not silent ---
//
// A torn tail is by definition an INCOMPLETE trailing line (no newline) —
// it was never a fully-persisted record, so truncating it away must not
// increment Dropped(). This pins that decision down (see Spool.Dropped's
// doc comment) rather than leaving the loss unaccounted for one way or the
// other.
func TestTornTailDoesNotDoubleCountDropped(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Append(rec(0)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if s.Dropped() != 0 {
		t.Fatalf("Dropped() = %d before any tear, want 0", s.Dropped())
	}

	segFiles, err := filepath.Glob(filepath.Join(dir, "spool-*.jsonl"))
	if err != nil || len(segFiles) != 1 {
		t.Fatalf("expected 1 segment file, got %v (err=%v)", segFiles, err)
	}
	// Simulate a crash mid-write directly on the file (bypassing Append,
	// and deliberately WITHOUT closing s first, per the reviewer's request
	// to reopen without a clean Close).
	f, err := os.OpenFile(segFiles[0], os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(`{"n":1,"garbage`)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open with torn tail: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if got := s2.Dropped(); got != 0 {
		t.Fatalf("Dropped() after torn-tail repair = %d, want 0 (an incomplete fragment was never a persisted record)", got)
	}

	got, _, err := s2.Next(10)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(got) != 1 || string(got[0]) != string(rec(0)) {
		t.Fatalf("Next = %v, want exactly [%s]", got, rec(0))
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
	s.SetSince("web", ts)
	if _, ok := s.Since("db"); ok {
		t.Fatal("Since(unset container) should report ok=false")
	}
	// SetSince no longer persists by itself (see its doc comment) — this
	// round-trip goes through the same PersistSince path the orchestrator's
	// periodic timer drives.
	if err := s.PersistSince(); err != nil {
		t.Fatalf("PersistSince: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer func() { _ = s2.Close() }()

	got, ok := s2.Since("web")
	if !ok {
		t.Fatal("Since(\"web\") after restart: ok=false, want true")
	}
	if !got.Equal(ts) {
		t.Fatalf("Since(\"web\") after restart = %v, want %v", got, ts)
	}
}

// --- Regression: SetSince no longer persists per call -----------------------
//
// Finding 2 (ship code review): SetSince used to fsync+rename
// checkpoint.json on every call, capping the whole docker-log pipeline at
// fsync rate. It must now only update memory; persistence is piggybacked on
// Ack's existing checkpoint write, or driven explicitly by PersistSince
// (which the orchestrator calls on a 2s timer — see ship.runSincePersister).
func TestSetSinceDoesNotPersistPerCall(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

	cpPath := checkpointPath(dir)
	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Fatalf("checkpoint.json exists before any persist call: stat err=%v", err)
	}

	for i := 0; i < 500; i++ {
		s.SetSince("web", time.Now())
	}

	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Fatalf("checkpoint.json was written by a tight loop of SetSince alone (stat err=%v) — SetSince must be in-memory only", err)
	}
	if got, ok := s.Since("web"); !ok {
		t.Fatal("Since(\"web\") = ok=false after SetSince, want the in-memory update to still be visible")
	} else {
		_ = got
	}

	// The periodic-timer path still flushes it to disk.
	if err := s.PersistSince(); err != nil {
		t.Fatalf("PersistSince: %v", err)
	}
	if _, err := os.Stat(cpPath); err != nil {
		t.Fatalf("checkpoint.json missing after PersistSince: %v", err)
	}
}

// TestSinceSurvivesRestartViaAck proves the other persistence path: Ack's
// existing checkpoint write piggybacks whatever is currently in the
// in-memory since map, with no separate PersistSince call needed.
func TestSinceSurvivesRestartViaAck(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Append(rec(0)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	ts := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	s.SetSince("web", ts)

	_, cur, err := s.Next(10)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := s.Ack(cur); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	defer func() { _ = s2.Close() }()
	got, ok := s2.Since("web")
	if !ok || !got.Equal(ts) {
		t.Fatalf("Since(\"web\") after restart = %v, %v; want %v, true", got, ok, ts)
	}
}

// --- FOLD-IN: clamp a checkpoint that points past the segment's real size --
//
// Minor 4 (ship code review): checkpoint.json is fsynced+renamed on every
// persist, but segment appends are NOT fsynced per record (see the package
// doc). A power loss can therefore leave a checkpoint offset the segment
// itself never physically reached — the checkpoint durable, the segment
// write lost. Open must clamp that offset rather than trust it blindly:
// otherwise every future append is stranded behind a cursor no data will
// ever reach (readRecords just seeks past EOF forever).
func TestOpenClampsCheckpointOffsetPastSegmentSize(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Append(rec(i)); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segFiles, err := filepath.Glob(filepath.Join(dir, "spool-*.jsonl"))
	if err != nil || len(segFiles) != 1 {
		t.Fatalf("expected 1 segment file, got %v (err=%v)", segFiles, err)
	}
	info, err := os.Stat(segFiles[0])
	if err != nil {
		t.Fatalf("stat segment: %v", err)
	}
	realSize := info.Size()

	// Hand-craft the power-loss state directly: checkpoint.json (which
	// fsyncs) claims an offset well past the segment's real on-disk size
	// (which does not fsync per append).
	cp := checkpointFile{Seq: 1, Offset: realSize + 10_000, Since: map[string]string{}}
	if err := saveCheckpoint(dir, cp); err != nil {
		t.Fatalf("craft checkpoint: %v", err)
	}

	s2, err := Open(dir, bigCap)
	if err != nil {
		t.Fatalf("Open with checkpoint-ahead-of-segment state: %v", err)
	}
	defer func() { _ = s2.Close() }()

	onDisk, err := loadCheckpoint(dir)
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if onDisk.Offset > realSize {
		t.Fatalf("checkpoint.json still claims offset %d past real segment size %d after Open (clamp must persist immediately)", onDisk.Offset, realSize)
	}

	if err := s2.Append(rec(99)); err != nil {
		t.Fatalf("Append after clamp: %v", err)
	}
	got, _, err := s2.Next(100)
	if err != nil {
		t.Fatalf("Next after clamp: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Next returned nothing after clamp — the append got stranded behind a too-high cursor")
	}
	if last := got[len(got)-1]; string(last) != string(rec(99)) {
		t.Fatalf("last record = %q, want %q", last, rec(99))
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
	defer func() { _ = s.Close() }()

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

	const capBytes = 5 * 1024 * 1024
	s, err := Open(dir, capBytes)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = s.Close() }()

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
