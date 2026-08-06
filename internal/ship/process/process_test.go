package process

import (
	"strings"
	"testing"
	"time"
)

// --- Behavior 1: StripANSI -------------------------------------------------

func TestStripANSI(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "SGR reset",
			in:   "hello\x1b[0m",
			want: "hello",
		},
		{
			name: "SGR color and bold wrapping text",
			in:   "\x1b[31;1mERROR\x1b[0m: boom",
			want: "ERROR: boom",
		},
		{
			name: "cursor movement and erase-line codes",
			in:   "Progress\x1b[2K\x1b[1G50%\x1b[0m done",
			want: "Progress50% done",
		},
		{
			name: "plain text unchanged",
			in:   "connection refused to db:5432",
			want: "connection refused to db:5432",
		},
		{
			name: "multibyte UTF-8 preserved alongside SGR codes",
			in:   "\x1b[32m起動しました\x1b[0m 日本語 OK",
			want: "起動しました 日本語 OK",
		},
		{
			name: "lone ESC not followed by CSI is left intact",
			in:   "weird\x1bbyte here",
			want: "weird\x1bbyte here",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StripANSI(tt.in)
			if got != tt.want {
				t.Errorf("StripANSI(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// --- helpers ----------------------------------------------------------------

func mustTime(s string) time.Time {
	tm, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return tm
}

func feedAll(j *Joiner, lines []Line) []Record {
	var out []Record
	for _, l := range lines {
		out = append(out, j.Feed(l)...)
	}
	return out
}

// --- Behavior 2: Go panic joins into one record; two back-to-back = two ----

func TestJoiner_GoPanic(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	lines := []Line{
		{Text: "panic: runtime error: index out of range [3] with length 3", Time: t0},
		{Text: "goroutine 1 [running]:", Time: t0.Add(time.Millisecond)},
		{Text: "\tmain.main()", Time: t0.Add(2 * time.Millisecond)},
		{Text: "\t\t/tmp/main.go:10 +0x1b", Time: t0.Add(3 * time.Millisecond)},
	}

	j := NewJoiner(64 * 1024)
	completed := feedAll(j, lines)
	if len(completed) != 0 {
		t.Fatalf("expected no records completed mid-panic, got %d: %+v", len(completed), completed)
	}

	final := j.Flush()
	if len(final) != 1 {
		t.Fatalf("expected exactly 1 record from Flush, got %d", len(final))
	}
	want := "panic: runtime error: index out of range [3] with length 3\n" +
		"goroutine 1 [running]:\n" +
		"\tmain.main()\n" +
		"\t\t/tmp/main.go:10 +0x1b"
	if final[0].Text != want {
		t.Errorf("joined text = %q, want %q", final[0].Text, want)
	}
	if !final[0].Time.Equal(t0) {
		t.Errorf("record time = %v, want first line's time %v", final[0].Time, t0)
	}
}

func TestJoiner_TwoBackToBackPanics(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	lines := []Line{
		{Text: "panic: first failure", Time: t0},
		{Text: "goroutine 1 [running]:", Time: t0.Add(time.Millisecond)},
		{Text: "panic: second failure", Time: t0.Add(2 * time.Millisecond)},
		{Text: "goroutine 7 [running]:", Time: t0.Add(3 * time.Millisecond)},
	}

	j := NewJoiner(64 * 1024)
	completed := feedAll(j, lines)
	final := j.Flush()
	all := append(completed, final...)

	if len(all) != 2 {
		t.Fatalf("expected 2 records for 2 back-to-back panics, got %d: %+v", len(all), all)
	}
	if !strings.Contains(all[0].Text, "first failure") {
		t.Errorf("record 1 = %q, want to contain %q", all[0].Text, "first failure")
	}
	if !strings.Contains(all[1].Text, "second failure") {
		t.Errorf("record 2 = %q, want to contain %q", all[1].Text, "second failure")
	}
}

// --- Behavior 3: Java-style trace joins -------------------------------------

func TestJoiner_JavaTrace(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	lines := []Line{
		{Text: `Exception in thread "main" java.lang.NullPointerException: boom`, Time: t0},
		{Text: "\tat com.example.Foo.bar(Foo.java:22)", Time: t0.Add(time.Millisecond)},
		{Text: "\tat com.example.Foo.main(Foo.java:10)", Time: t0.Add(2 * time.Millisecond)},
		{Text: "Caused by: java.lang.IllegalStateException: bad state", Time: t0.Add(3 * time.Millisecond)},
		{Text: "\tat com.example.Baz.qux(Baz.java:5)", Time: t0.Add(4 * time.Millisecond)},
		{Text: "\t... 3 more", Time: t0.Add(5 * time.Millisecond)},
	}

	j := NewJoiner(64 * 1024)
	completed := feedAll(j, lines)
	if len(completed) != 0 {
		t.Fatalf("expected no records mid-trace, got %d", len(completed))
	}
	final := j.Flush()
	if len(final) != 1 {
		t.Fatalf("expected 1 joined record, got %d", len(final))
	}
	wantLineCount := 6
	if got := strings.Count(final[0].Text, "\n") + 1; got != wantLineCount {
		t.Errorf("joined record has %d lines, want %d: %q", got, wantLineCount, final[0].Text)
	}
	if !final[0].Time.Equal(t0) {
		t.Errorf("record time = %v, want %v", final[0].Time, t0)
	}
}

// --- Behavior 4: indented continuation (multiline SQL); non-indented flushes

func TestJoiner_IndentedContinuationSQL(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	lines := []Line{
		{Text: "SELECT id, name", Time: t0},
		{Text: "  FROM users", Time: t0.Add(time.Millisecond)},
		{Text: "  WHERE active = true", Time: t0.Add(2 * time.Millisecond)},
	}

	j := NewJoiner(64 * 1024)
	completed := feedAll(j, lines)
	if len(completed) != 0 {
		t.Fatalf("expected no records mid-SQL, got %d", len(completed))
	}

	// A following non-indented line starts a new record and flushes the
	// previous one.
	next := Line{Text: "SELECT 1", Time: t0.Add(3 * time.Millisecond)}
	got := j.Feed(next)
	if len(got) != 1 {
		t.Fatalf("expected the non-indented line to flush the pending record, got %d records", len(got))
	}
	wantText := "SELECT id, name\n  FROM users\n  WHERE active = true"
	if got[0].Text != wantText {
		t.Errorf("flushed text = %q, want %q", got[0].Text, wantText)
	}

	final := j.Flush()
	if len(final) != 1 || final[0].Text != "SELECT 1" {
		t.Fatalf("expected pending 'SELECT 1' record, got %+v", final)
	}
}

// --- Behavior 5: lone goroutine dump does not join without prior panic/fatal

func TestJoiner_LoneGoroutineDumpDoesNotJoin(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	j := NewJoiner(64 * 1024)
	completed := j.Feed(Line{Text: "some ordinary log line", Time: t0})
	if len(completed) != 0 {
		t.Fatalf("expected no records completed yet, got %d", len(completed))
	}

	// This goroutine-dump line follows an ordinary line, not panic/fatal —
	// per rule 5 it must NOT join, so it flushes the pending record and
	// stands alone as its own pending record.
	got := j.Feed(Line{Text: "goroutine 5 [chan receive]:", Time: t0.Add(time.Millisecond)})
	if len(got) != 1 || got[0].Text != "some ordinary log line" {
		t.Fatalf("expected the ordinary line to be flushed alone, got %+v", got)
	}

	final := j.Flush()
	if len(final) != 1 || final[0].Text != "goroutine 5 [chan receive]:" {
		t.Fatalf("expected lone goroutine-dump record, got %+v", final)
	}
}

// --- Behavior 6: joined record keeps first line's timestamp, \n joins ------
// (also exercised throughout the above; this test isolates the timestamp
// rule with a joined record whose lines arrive with distinct times.)

func TestJoiner_KeepsFirstLineTimestampAndNewlineJoins(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")
	t1 := t0.Add(5 * time.Second)
	t2 := t0.Add(10 * time.Second)

	j := NewJoiner(64 * 1024)
	j.Feed(Line{Text: "panic: boom", Time: t0})
	j.Feed(Line{Text: "goroutine 1 [running]:", Time: t1})
	final := j.Feed(Line{Text: "\tframe one", Time: t2})
	_ = final

	got := j.Flush()
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if !got[0].Time.Equal(t0) {
		t.Errorf("record time = %v, want first line's time %v", got[0].Time, t0)
	}
	wantText := "panic: boom\ngoroutine 1 [running]:\n\tframe one"
	if got[0].Text != wantText {
		t.Errorf("text = %q, want %q", got[0].Text, wantText)
	}
}

// --- Behavior 7: byte cap flushes at the cap and starts a new record -------

func TestJoiner_CapFlushesAndCounts(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	// Small cap so the test doesn't need to push real kilobytes of text.
	j := NewJoiner(20)

	first := j.Feed(Line{Text: "SELECT id, name", Time: t0}) // 15 bytes, under cap
	if len(first) != 0 {
		t.Fatalf("expected no record yet, got %+v", first)
	}

	// Joining this continuation line would push size past 20 bytes
	// (15 + 1 + len("  FROM users_table") = 15+1+19 = 35 > 20), so it
	// must force-flush the pending record at the cap and start fresh.
	second := j.Feed(Line{Text: "  FROM users_table", Time: t0.Add(time.Millisecond)})
	if len(second) != 1 {
		t.Fatalf("expected cap to force-flush 1 record, got %d: %+v", len(second), second)
	}
	if second[0].Text != "SELECT id, name" {
		t.Errorf("cap-flushed text = %q, want %q", second[0].Text, "SELECT id, name")
	}
	if j.CapHits() != 1 {
		t.Errorf("CapHits() = %d, want 1", j.CapHits())
	}

	final := j.Flush()
	if len(final) != 1 || final[0].Text != "  FROM users_table" {
		t.Fatalf("expected the overflow line to start a fresh pending record, got %+v", final)
	}
}

// --- Behavior 8: Flush on demand returns pending and resets ----------------

func TestJoiner_FlushReturnsPendingAndResets(t *testing.T) {
	t0 := mustTime("2026-08-06T12:00:00Z")

	j := NewJoiner(64 * 1024)

	// Flush with nothing pending returns nothing.
	if got := j.Flush(); got != nil {
		t.Fatalf("Flush on empty Joiner = %+v, want nil", got)
	}

	j.Feed(Line{Text: "fatal error: out of memory", Time: t0})
	got := j.Flush()
	if len(got) != 1 || got[0].Text != "fatal error: out of memory" {
		t.Fatalf("Flush = %+v, want the pending 'fatal error' record", got)
	}

	// After Flush, state is reset: a goroutine-dump line arriving next has
	// no panic/fatal predecessor (the reset cleared that gate) and must
	// stand alone rather than being treated as a continuation of nothing.
	completed := j.Feed(Line{Text: "goroutine 9 [running]:", Time: t0.Add(time.Second)})
	if len(completed) != 0 {
		t.Fatalf("expected the goroutine line to become the new pending record, got %+v", completed)
	}
	final := j.Flush()
	if len(final) != 1 || final[0].Text != "goroutine 9 [running]:" {
		t.Fatalf("expected lone goroutine record after reset, got %+v", final)
	}
}
