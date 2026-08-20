package segment

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexStringsRoundTrip(t *testing.T) {
	vals := []string{"", "a", "hello world", "", "x\x00y", "last"}
	b := EncodeStrings(vals)
	spans, err := IndexStrings(b, len(vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != len(vals) {
		t.Fatalf("got %d spans, want %d", len(spans), len(vals))
	}
	for i, v := range vals {
		if got := string(b[spans[i].Start:spans[i].End]); got != v {
			t.Errorf("span %d: got %q, want %q", i, got, v)
		}
	}
}

func TestIndexStringsRejectsCorruption(t *testing.T) {
	b := EncodeStrings([]string{"abc", "def"})
	if _, err := IndexStrings(b, 3); err == nil {
		t.Error("want error for count beyond buffer")
	}
	if _, err := IndexStrings(b, 1); err == nil {
		t.Error("want error for trailing bytes")
	}
	if _, err := IndexStrings(b[:2], 2); err == nil {
		t.Error("want error for truncated buffer")
	}
}

func TestParseFooterMatchesOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.seg")
	rows := []Row{
		{LogID: 1, TsMicros: 100, Severity: 4, TemplateID: 7, Vars: []string{"a"}, Service: "api", Attrs: "{}"},
		{LogID: 2, TsMicros: 200, Severity: 2, TemplateID: 0, Raw: "raw body", Service: "web", Attrs: "{}"},
	}
	if _, err := Write(path, rows); err != nil {
		t.Fatal(err)
	}
	viaOpen, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	viaParse, err := parseFooter(path, data)
	if err != nil {
		t.Fatal(err)
	}
	if viaParse.Count != viaOpen.Count || viaParse.MinTs != viaOpen.MinTs ||
		viaParse.MaxTs != viaOpen.MaxTs || len(viaParse.Columns) != len(viaOpen.Columns) {
		t.Errorf("parseFooter %+v != Open %+v", viaParse, viaOpen)
	}
}

func TestParseFooterRejectsCorruption(t *testing.T) {
	if _, err := parseFooter("x", []byte("short")); err == nil {
		t.Error("want error for tiny file")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "s.seg")
	if _, err := Write(path, []Row{{LogID: 1, TsMicros: 1, Severity: 4, TemplateID: 0, Raw: "r", Attrs: "{}"}}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	bad := append([]byte(nil), data...)
	bad[len(bad)-1] ^= 0xFF // corrupt magic
	if _, err := parseFooter(path, bad); err == nil {
		t.Error("want error for bad magic")
	}
	bad2 := append([]byte(nil), data...)
	bad2[len(bad2)-20] ^= 0xFF // corrupt footer JSON → CRC mismatch
	if _, err := parseFooter(path, bad2); err == nil {
		t.Error("want error for footer CRC mismatch")
	}
}

func writeScanFixture(t *testing.T) (string, []Row) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "s.seg")
	rows := []Row{
		{LogID: 10, TsMicros: 1000, Severity: 4, TemplateID: 7, Vars: []string{"u1", "9ms"}, Service: "api", Environment: "prod", Release: "r1", TraceID: "t1", Attrs: `{"k":"v"}`, IsEvent: true},
		{LogID: 11, TsMicros: 2000, Severity: 2, TemplateID: 0, Raw: "raw body one", Service: "web", Environment: "prod", Attrs: "{}"},
		{LogID: 12, TsMicros: 3000, Severity: 4, TemplateID: 7, Vars: []string{"u2", "3ms"}, Service: "api", Environment: "dev", Attrs: "{}"},
		{LogID: 13, TsMicros: 4000, Severity: 1, TemplateID: 0, Raw: "raw body two", Service: "api", Environment: "prod", Attrs: "{}"},
	}
	if _, err := Write(path, rows); err != nil {
		t.Fatal(err)
	}
	return path, rows
}

func TestScanMatchFilters(t *testing.T) {
	path, _ := writeScanFixture(t)
	cases := []struct {
		name string
		f    ScanFilter
		want []int64 // expected LogIDs in Match order
	}{
		{"zero filter admits all", ScanFilter{}, []int64{10, 11, 12, 13}},
		// Documented ScanFilter contract: (0, 0) means unbounded, not a
		// genuine [0, 0] instant window (no fixture row has ts=0, so this
		// pins "admits all" rather than "admits nothing").
		{"explicit (0,0) normalizes to unbounded", ScanFilter{SinceM: 0, UntilM: 0}, []int64{10, 11, 12, 13}},
		{"time bounds", ScanFilter{SinceM: 2000, UntilM: 3000}, []int64{11, 12}},
		{"service", ScanFilter{SinceM: MinTsBound, UntilM: MaxTsBound, Service: "api"}, []int64{10, 12, 13}},
		{"service absent", ScanFilter{SinceM: MinTsBound, UntilM: MaxTsBound, Service: "nope"}, nil},
		{"environment", ScanFilter{SinceM: MinTsBound, UntilM: MaxTsBound, Environment: "prod"}, []int64{10, 11, 13}},
		{"min severity", ScanFilter{SinceM: MinTsBound, UntilM: MaxTsBound, MinSeverity: 4}, []int64{10, 12}},
		{"combined", ScanFilter{SinceM: 1000, UntilM: 3500, Service: "api", MinSeverity: 3}, []int64{10, 12}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := OpenScan(path, tc.f)
			if err != nil {
				t.Fatal(err)
			}
			var got []int64
			for _, i := range sc.Match {
				got = append(got, sc.LogID(i))
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k := range got {
				if got[k] != tc.want[k] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestScanBodiesAccessors(t *testing.T) {
	path, _ := writeScanFixture(t)
	sc, err := OpenScan(path, ScanFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sc.EnsureBodies(); err != nil {
		t.Fatal(err)
	}
	if got := sc.TemplateID(0); got != 7 {
		t.Errorf("TemplateID(0) = %d, want 7", got)
	}
	if got := sc.TemplateID(1); got != 0 {
		t.Errorf("TemplateID(1) = %d, want 0", got)
	}
	vars := sc.AppendVars(nil, 2)
	if len(vars) != 2 || string(vars[0]) != "u2" || string(vars[1]) != "3ms" {
		t.Errorf("AppendVars(2) = %q", vars)
	}
	if got := sc.AppendVars(nil, 1); len(got) != 0 {
		t.Errorf("AppendVars on raw row = %q, want empty", got)
	}
	if got := string(sc.RawBytes(1)); got != "raw body one" {
		t.Errorf("RawBytes(1) = %q", got)
	}
	if got := string(sc.RawBytes(3)); got != "raw body two" {
		t.Errorf("RawBytes(3) = %q", got)
	}
	if got := sc.RawBytes(0); got != nil {
		t.Errorf("RawBytes on templated row = %q, want nil", got)
	}
}

func TestScanRowsEqualsRead(t *testing.T) {
	path, want := writeScanFixture(t)
	_, readRows, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := OpenScan(path, ScanFilter{})
	if err != nil {
		t.Fatal(err)
	}
	scanRows, err := sc.Rows(sc.Match)
	if err != nil {
		t.Fatal(err)
	}
	if len(readRows) != len(want) || len(scanRows) != len(want) {
		t.Fatalf("row counts: read=%d scan=%d want=%d", len(readRows), len(scanRows), len(want))
	}
	for i := range readRows {
		a, b := readRows[i], scanRows[i]
		if a.LogID != b.LogID || a.TsMicros != b.TsMicros || a.Severity != b.Severity ||
			a.TemplateID != b.TemplateID || a.Raw != b.Raw || a.Service != b.Service ||
			a.Environment != b.Environment || a.Release != b.Release || a.TraceID != b.TraceID ||
			a.Attrs != b.Attrs || a.IsEvent != b.IsEvent || len(a.Vars) != len(b.Vars) {
			t.Errorf("row %d: Read=%+v Scan=%+v", i, a, b)
		}
		for k := range a.Vars {
			if a.Vars[k] != b.Vars[k] {
				t.Errorf("row %d var %d: %q != %q", i, k, a.Vars[k], b.Vars[k])
			}
		}
	}
}

func TestScanColumnCRCFailure(t *testing.T) {
	path, _ := writeScanFixture(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt the first byte of the first column (log_id) — OpenScan
	// decodes it eagerly and must fail on the CRC check.
	data[0] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenScan(path, ScanFilter{}); err == nil {
		t.Error("want CRC error for corrupted column")
	}
}

func TestScanConcurrentMaterialize(t *testing.T) {
	path, _ := writeScanFixture(t)
	sc, err := OpenScan(path, ScanFilter{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func() {
			if err := sc.EnsureBodies(); err != nil {
				done <- err
				return
			}
			for i := 0; i < sc.Len(); i++ {
				if _, err := sc.Row(i); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
	}
	for g := 0; g < 8; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
