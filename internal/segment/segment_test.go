package segment

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func sampleRows(n int, seed int64) []Row {
	rng := rand.New(rand.NewSource(seed))
	services := []string{"api", "web", "worker"}
	rows := make([]Row, n)
	for i := range rows {
		r := Row{
			LogID:    int64(1000 + i),
			TsMicros: 1755000000000000 + rng.Int63n(86_400_000_000),
			Severity: rng.Intn(22),
			Service:  services[rng.Intn(len(services))],
			Attrs:    `{"host":"box1"}`,
			IsEvent:  rng.Intn(10) == 0,
		}
		if rng.Intn(20) == 0 { // raw fallback rows
			r.TemplateID = 0
			r.Raw = fmt.Sprintf("panic: boom %d\nstack trace here", i)
		} else {
			r.TemplateID = int64(1 + rng.Intn(50))
			r.Vars = []string{fmt.Sprintf("%d", rng.Intn(10_000)), "GET"}
			if rng.Intn(3) == 0 {
				r.Vars = nil // rows with zero vars
			}
			r.Environment = "production"
			r.Release = "abc123"
			r.TraceID = ""
		}
		rows[i] = r
	}
	return rows
}

func TestWriteReadRoundTrip(t *testing.T) {
	rows := sampleRows(500, 1)
	path := filepath.Join(t.TempDir(), "test.seg")
	foot, err := Write(path, rows)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if foot.Count != 500 {
		t.Errorf("footer count = %d", foot.Count)
	}

	// Expected: rows sorted stably by TsMicros.
	want := append([]Row(nil), rows...)
	sort.SliceStable(want, func(i, j int) bool { return want[i].TsMicros < want[j].TsMicros })

	foot2, got, err := Read(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !reflect.DeepEqual(foot, foot2) {
		t.Error("Open/Read footer mismatch with Write's")
	}
	if !reflect.DeepEqual(got, want) {
		for i := range want {
			if i < len(got) && !reflect.DeepEqual(got[i], want[i]) {
				t.Fatalf("row %d:\n got %+v\nwant %+v", i, got[i], want[i])
			}
		}
		t.Fatalf("row count: got %d want %d", len(got), len(want))
	}

	// Footer aggregates.
	if foot.MinTs != want[0].TsMicros || foot.MaxTs != want[len(want)-1].TsMicros {
		t.Error("footer ts range wrong")
	}
	var events int64
	for _, r := range want {
		if r.IsEvent {
			events++
		}
	}
	if foot.Events != events {
		t.Errorf("footer events = %d want %d", foot.Events, events)
	}
}

func TestOpenFooterOnly(t *testing.T) {
	rows := sampleRows(50, 2)
	path := filepath.Join(t.TempDir(), "t.seg")
	wrote, err := Write(path, rows)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !reflect.DeepEqual(wrote, got) {
		t.Error("footer mismatch")
	}
	if len(got.TemplateIDs) == 0 || len(got.Services) == 0 {
		t.Error("footer pruning metadata empty")
	}
}

func TestCorruptionFailsLoudly(t *testing.T) {
	rows := sampleRows(100, 3)
	dir := t.TempDir()
	path := filepath.Join(dir, "c.seg")
	if _, err := Write(path, rows); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)

	// Flip one byte in the middle of the column area.
	bad := append([]byte(nil), data...)
	bad[len(bad)/3] ^= 0xff
	badPath := filepath.Join(dir, "bad.seg")
	if err := os.WriteFile(badPath, bad, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(badPath); err == nil {
		t.Error("corrupted column must fail Read")
	}

	// Truncate the file.
	truncPath := filepath.Join(dir, "trunc.seg")
	if err := os.WriteFile(truncPath, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(truncPath); err == nil {
		t.Error("truncated file must fail Open")
	}

	// Not a segment at all.
	junkPath := filepath.Join(dir, "junk.seg")
	if err := os.WriteFile(junkPath, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(junkPath); err == nil {
		t.Error("junk file must fail Open")
	}
}

func TestWriteEmptyRejected(t *testing.T) {
	if _, err := Write(filepath.Join(t.TempDir(), "e.seg"), nil); err == nil {
		t.Error("empty segment must be rejected")
	}
}
