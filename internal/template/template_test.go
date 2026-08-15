package template

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// fakeStore is an in-memory template.Store with error injection.
type fakeStore struct {
	rows       map[int64][]Row // projectID → rows
	nextID     int64
	failInsert error
	failLoad   error
	inserts    int
}

func newFakeStore() *fakeStore { return &fakeStore{rows: map[int64][]Row{}, nextID: 0} }

func (f *fakeStore) InsertTemplate(_ context.Context, projectID int64, text string) (int64, error) {
	if f.failInsert != nil {
		return 0, f.failInsert
	}
	f.nextID++
	f.inserts++
	f.rows[projectID] = append(f.rows[projectID], Row{ID: f.nextID, Text: text})
	return f.nextID, nil
}

func (f *fakeStore) LoadTemplates(_ context.Context, projectID int64) ([]Row, error) {
	if f.failLoad != nil {
		return nil, f.failLoad
	}
	return append([]Row(nil), f.rows[projectID]...), nil
}

var ctx = context.Background()

func TestExtractRoundTripAppendOnly(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	lines := []string{
		`104.23.160.143 - - [08/Aug/2026:22:26:49 +0000] "POST /webhooks/daily HTTP/2.0" 401 29 1ms`,
		`108.162.246.84 - - [08/Aug/2026:21:59:01 +0000] "POST /api/webhooks/daily HTTP/2.0" 404 18 1ms`,
		`2026/08/08 22:18:20 repo.go:25 record not found`,
		`2026/08/08 22:18:21 repo.go:22 record not found`,
		`{"level":"ERROR","msg":"request failed","err":"record not found"}`,
	}
	type stored struct {
		body string
		id   int64
		vars []string
	}
	var all []stored
	for pass := 0; pass < 2; pass++ { // second pass hits generalized templates
		for _, l := range lines {
			id, vars, ok, err := e.Extract(ctx, 1, l)
			if err != nil {
				t.Fatalf("extract error: %v", err)
			}
			if !ok {
				t.Fatalf("line went raw: %q", l)
			}
			all = append(all, stored{l, id, vars})
		}
	}
	for _, s := range all { // every triple reconstructs forever (append-only)
		got, ok, err := e.Reconstruct(1, s.id, s.vars)
		if err != nil || !ok || got != s.body {
			t.Errorf("round trip broke:\n got %q\nwant %q err=%v", got, s.body, err)
		}
	}
}

func TestProjectIsolation(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	id1, _, _, _ := e.Extract(ctx, 1, "hello world alpha")
	id2, _, _, _ := e.Extract(ctx, 2, "hello world alpha")
	if id1 == id2 {
		t.Errorf("projects share template ids: %d == %d", id1, id2)
	}
	if _, ok, _ := e.Reconstruct(2, id1, nil); ok && id1 != id2 {
		t.Error("project 2 can reconstruct project 1's template id")
	}
}

func TestCapFallsBackToRaw(t *testing.T) {
	e := NewExtractor(newFakeStore(), 2)
	// Two dissimilar shapes fill the cap.
	if _, _, ok, _ := e.Extract(ctx, 1, "alpha beta gamma"); !ok {
		t.Fatal("first shape should template")
	}
	if _, _, ok, _ := e.Extract(ctx, 1, "one two three four five"); !ok {
		t.Fatal("second shape should template")
	}
	// Third distinct shape must go raw, not mint.
	id, vars, ok, err := e.Extract(ctx, 1, "completely different line here now")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || id != RawID || vars != nil {
		t.Errorf("at cap: want raw fallback, got id=%d ok=%v", id, ok)
	}
	if e.Count(1) != 2 {
		t.Errorf("count = %d, want 2", e.Count(1))
	}
	// Existing templates still work at the cap.
	if _, _, ok, _ := e.Extract(ctx, 1, "alpha beta gamma"); !ok {
		t.Error("existing template must still match at cap")
	}
}

func TestPersistFailurePropagatesAndMintsNothing(t *testing.T) {
	fs := newFakeStore()
	fs.failInsert = errors.New("disk full")
	e := NewExtractor(fs, 0)
	_, _, _, err := e.Extract(ctx, 1, "alpha beta gamma")
	if err == nil {
		t.Fatal("want persistence error surfaced")
	}
	if e.Count(1) != 0 {
		t.Errorf("failed persist must not register in memory, count=%d", e.Count(1))
	}
}

func TestLoadFromStoreSurvivesRestart(t *testing.T) {
	fs := newFakeStore()
	e1 := NewExtractor(fs, 0)
	id, vars, ok, _ := e1.Extract(ctx, 1, "req 123 done in 45ms")
	_, _, _, _ = e1.Extract(ctx, 1, "req 999 done in 7ms") // generalizes
	if !ok {
		t.Fatal("should template")
	}
	// "Restart": fresh extractor over the same store.
	e2 := NewExtractor(fs, 0)
	got, ok2, err2 := e2.Reconstruct(1, id, vars)
	if err2 != nil || !ok2 || got != "req 123 done in 45ms" {
		t.Errorf("after reload: got %q ok=%v err=%v", got, ok2, err2)
	}
	// New extraction reuses loaded templates rather than re-minting.
	before := fs.inserts
	_, _, ok3, _ := e2.Extract(ctx, 1, "req 555 done in 9ms")
	if !ok3 {
		t.Fatal("should template after reload")
	}
	if fs.inserts != before {
		t.Errorf("re-minted %d templates after reload; want reuse", fs.inserts-before)
	}
}

func TestRawFallbacks(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	for _, body := range []string{"", "line one\nline two", "nul\x00byte", longLine(201)} {
		if id, _, ok, err := e.Extract(ctx, 1, body); ok || id != RawID || err != nil {
			t.Errorf("%q: want raw fallback, got id=%d ok=%v err=%v", body, id, ok, err)
		}
	}
}

func TestReconstructSurfacesLoadError(t *testing.T) {
	fs := newFakeStore()
	e1 := NewExtractor(fs, 0)
	id, vars, ok, _ := e1.Extract(ctx, 1, "alpha beta gamma")
	if !ok {
		t.Fatal("should template")
	}
	// Fresh extractor whose store now fails loads: error, not "missing".
	fs.failLoad = errors.New("disk exploded")
	e2 := NewExtractor(fs, 0)
	if _, ok, err := e2.Reconstruct(1, id, vars); ok || err == nil {
		t.Errorf("want load error surfaced, got ok=%v err=%v", ok, err)
	}
	fs.failLoad = nil
	if got, ok, err := e2.Reconstruct(1, id, vars); !ok || err != nil || got != "alpha beta gamma" {
		t.Errorf("after recovery: %q ok=%v err=%v", got, ok, err)
	}
	if _, ok, err := e2.Reconstruct(1, 9999, nil); ok || err != nil {
		t.Errorf("genuinely missing: want (false, nil), got ok=%v err=%v", ok, err)
	}
}

func longLine(tokens int) string {
	s := ""
	for i := 0; i < tokens; i++ {
		s += fmt.Sprintf("t%d ", i)
	}
	return s
}
