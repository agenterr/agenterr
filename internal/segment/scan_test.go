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
