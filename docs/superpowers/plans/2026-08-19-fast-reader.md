# Fast Reader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make substring search beat OpenObserve (scoped ≤50ms vs their ~38ms; unscoped 1-day well under 100ms) by replacing the all-columns-all-rows segment reader with column-selective, zero-allocation, template-classified, parallel scans — reader-only, no format change.

**Architecture:** A new `segment.Scan` type reads a segment file once into memory, decompresses only the columns a query needs (footers already carry per-column offsets), computes matching row indices from the cheap columns (ts/severity/service/env), and materializes full rows only for survivors. Search pushes the substring predicate down: templates whose static text contains the query match without touching variables; other rows are reconstructed into a reusable byte buffer and checked with `bytes.Contains` — no per-row string allocation. Segments and intra-segment chunks scan in parallel.

**Tech Stack:** Go 1.26, stdlib only (no new dependencies). zstd via the existing `internal/segment` Compress/Decompress.

**Spec:** docs/superpowers/specs/2026-08-12-template-storage-engine-design.md (§3 segments, §5 query paths, §7 performance targets). Baseline measurements and root-cause analysis: docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md.

## Global Constraints

- Segment format v1 is UNCHANGED — this is a reader change only. `Write` is not touched; existing segment files stay readable byte-for-byte.
- Every column block actually read must be CRC-verified before decompression, exactly as `Read` does today — a segment never yields wrong data silently.
- Restart-not-skip semantics are preserved verbatim: a read that finds a segment file vanished re-checks the manifest; row legitimately replaced → restart the whole attempt from a fresh snapshot (bounded by `maxSegmentSetRestarts` = 3, then loud error); manifest row present but file missing → loud corruption error naming the path. NEVER silently drop rows.
- The ps.mu coherence discipline is preserved: manifest query + memtable snapshot are taken together under ps.mu; never hold s.mu while taking ps.mu.
- SearchLogs semantics are unchanged: substring match on the reconstructed body, most recent first, ties by descending LogID, capped at Limit (0 → 50). Service/Environment/MinSeverity filters apply to both segment and memtable rows. Byte-for-byte identical result sets to the current implementation.
- Severity is the internal `core.Severity` enum (Error=4), NOT OTLP numerics.
- All tests run with `-race`. Before every commit: `gofmt -l .` empty, `go vet ./...` clean, and `$(go env GOPATH)/bin/golangci-lint run` reports 0 issues (gocyclo ≤ 15, revive doc comments on ALL exported symbols, no variables named `max`/`min`, errcheck).
- No placeholder tests: every test asserts real values.

## File Structure

- `internal/segment/scan.go` (new): `Span`, `IndexStrings`, `dictCol`, `ScanFilter`, `Scan`, `OpenScan`, accessors, `Row`/`Rows` materialization, `parseFooter`.
- `internal/segment/scan_test.go` (new): primitives + Scan behavior tests.
- `internal/segment/segment.go` (modify): `Read` reimplemented over `Scan`; `decodeColumns`/`decodeDictionaryColumn` deleted.
- `internal/template/template.go` (modify): add `Snapshot`, `AppendSubstitute`, `AlwaysContaining`.
- `internal/template/template_test.go` (modify): tests for the three additions.
- `internal/store/enginestore/read.go` (modify): `readSegmentRows*` rebuilt over `openScanWithRestart`; `logByIDOnce` uses `findInSegmentWithRestart`; old `SearchLogs`, `pairedRow`, `collectRowsAllProjects`, `collectPairedRows`, `readSegmentFileWithRestart` deleted.
- `internal/store/enginestore/search.go` (new): pushdown `SearchLogs` (sequential in Task 4, parallelized in Task 5).
- `internal/store/enginestore/search_test.go` (new): search-specific tests.
- `internal/store/enginestore/bench_test.go` (new, Task 5): Go benchmark.

---

### Task 1: Segment scan primitives — footer parsing from bytes, string indexing, selective column loading

**Files:**
- Create: `internal/segment/scan.go`
- Create: `internal/segment/scan_test.go`

**Interfaces:**
- Produces: `type Span struct{ Start, End int }`; `func IndexStrings(b []byte, n int) ([]Span, error)`; `func parseFooter(path string, data []byte) (Footer, error)`; `type dictCol struct { dict []string; refs []uint64 }` with `func (d *dictCol) at(i int) string`. Task 2 builds `Scan` on these in the same file.

- [ ] **Step 1: Write the failing tests**

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/segment/ -run 'TestIndexStrings|TestParseFooter' -v`
Expected: FAIL — `IndexStrings`, `parseFooter` undefined.

- [ ] **Step 3: Implement the primitives**

Create `internal/segment/scan.go`:

```go
package segment

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// Span locates one string value's payload inside a decoded string-column
// buffer.
type Span struct{ Start, End int }

// IndexStrings walks a string column (uvarint length + payload,
// concatenated — EncodeStrings's format) and returns each value's
// payload span, allocating no strings. Like DecodeStrings, it rejects
// short buffers and trailing bytes so corruption fails loudly.
func IndexStrings(b []byte, n int) ([]Span, error) {
	spans := make([]Span, n)
	off := 0
	for i := 0; i < n; i++ {
		l, w := binary.Uvarint(b[off:])
		if w <= 0 || uint64(len(b)-off-w) < l {
			return nil, fmt.Errorf("segment: short string column at value %d/%d", i, n)
		}
		start := off + w
		spans[i] = Span{Start: start, End: start + int(l)}
		off = start + int(l)
	}
	if off != len(b) {
		return nil, fmt.Errorf("segment: %d trailing bytes in string column", len(b)-off)
	}
	return spans, nil
}

// dictCol is a decoded dictionary column: per-row refs into a small
// dict of unique values.
type dictCol struct {
	dict []string
	refs []uint64
}

// at returns row i's value. Callers guarantee i is in range and refs
// were validated against dict at build time.
func (d *dictCol) at(i int) string { return d.dict[d.refs[i]] }

// parseFooter validates and parses the footer of a fully-read segment
// file, mirroring Open's checks (magic, footer CRC, version) without
// re-reading from disk. path appears only in error messages.
func parseFooter(path string, data []byte) (Footer, error) {
	if len(data) < 16 {
		return Footer{}, fmt.Errorf("segment: %s too small (%d bytes)", path, len(data))
	}
	tail := data[len(data)-16:]
	if string(tail[8:]) != magic {
		return Footer{}, fmt.Errorf("segment: %s: bad magic", path)
	}
	flen := int(binary.LittleEndian.Uint32(tail[:4]))
	fcrc := binary.LittleEndian.Uint32(tail[4:8])
	if len(data) < 16+flen {
		return Footer{}, fmt.Errorf("segment: %s: footer length %d exceeds file", path, flen)
	}
	fj := data[len(data)-16-flen : len(data)-16]
	if crc32.ChecksumIEEE(fj) != fcrc {
		return Footer{}, fmt.Errorf("segment: %s: footer CRC mismatch", path)
	}
	var foot Footer
	if err := json.Unmarshal(fj, &foot); err != nil {
		return Footer{}, fmt.Errorf("segment: parse footer: %w", err)
	}
	if foot.Version != 1 {
		return Footer{}, fmt.Errorf("segment: %s: unsupported version %d", path, foot.Version)
	}
	return foot, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/segment/ -race -v`
Expected: PASS (all, including existing tests).

- [ ] **Step 5: Lint and commit**

Run: `gofmt -l . && go vet ./... && $(go env GOPATH)/bin/golangci-lint run`
Expected: no output / 0 issues.

```bash
git add internal/segment/scan.go internal/segment/scan_test.go
git commit -m "feat(segment): scan primitives — byte-level footer parse, zero-alloc string indexing"
```

---

### Task 2: `segment.Scan` — filtered, column-selective, lazily-materializing reads; `Read` rebuilt on top

**Files:**
- Modify: `internal/segment/scan.go`
- Modify: `internal/segment/scan_test.go`
- Modify: `internal/segment/segment.go` (replace `Read`; delete `decodeColumns`, `decodeDictionaryColumn`)

**Interfaces:**
- Consumes: Task 1's `Span`, `IndexStrings`, `dictCol`, `parseFooter`.
- Produces (used verbatim by Tasks 4–5):
  - `const MinTsBound int64 = -1 << 62` / `const MaxTsBound int64 = 1 << 62`
  - `type ScanFilter struct { SinceM, UntilM int64; Service, Environment string; MinSeverity int }` — zero value means "no constraints".
  - `func OpenScan(path string, f ScanFilter) (*Scan, error)`
  - `func (sc *Scan) Len() int`, `Ts(i int) int64`, `LogID(i int) int64`, `SeverityAt(i int) int` — always available, lock-free.
  - `func (sc *Scan) EnsureBodies() error` — after it returns nil, `TemplateID(i int) int64`, `AppendVars(dst [][]byte, i int) [][]byte`, `RawBytes(i int) []byte` are lock-free and never fail. Calling them before EnsureBodies is a programming error (panics on nil slices).
  - `func (sc *Scan) Row(i int) (Row, error)` / `func (sc *Scan) Rows(idx []int) ([]Row, error)` — full materialization, safe for concurrent use.
  - `sc.Match []int` — ascending indices of rows admitted by the filter; `sc.Foot Footer`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/segment/scan_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/segment/ -run TestScan -v`
Expected: FAIL — `ScanFilter`, `OpenScan` undefined.

- [ ] **Step 3: Implement Scan**

Append to `internal/segment/scan.go` (add `"os"`, `"sync"` to imports):

```go
// MinTsBound and MaxTsBound are the explicit "unbounded" values for
// ScanFilter's inclusive time range, matching the sentinels enginestore
// uses.
const (
	MinTsBound int64 = -1 << 62
	MaxTsBound int64 = 1 << 62
)

// ScanFilter restricts which rows OpenScan admits into Match. The zero
// value admits every row: a SinceM/UntilM pair of (0, 0) is normalized
// to (MinTsBound, MaxTsBound), empty Service/Environment match any, and
// MinSeverity 0 admits all severities.
type ScanFilter struct {
	SinceM, UntilM int64
	Service        string
	Environment    string
	MinSeverity    int
}

// Scan is a column-selective view of one segment file. OpenScan reads
// the whole (compressed) file into memory once, eagerly decodes only
// the cheap ordering/filter columns (log_id, ts, severity, plus the
// service/env dictionaries when the filter needs them), and computes
// Match. Everything else — template ids, vars, raw bodies, the
// remaining dictionaries — decompresses lazily, at most once, on first
// use. After OpenScan returns, no further file I/O happens, so a
// concurrently deleted file cannot fail a Scan mid-query.
//
// Concurrency: Row/Rows and EnsureBodies are safe for concurrent use.
// The lock-free accessors (Ts, LogID, SeverityAt; TemplateID,
// AppendVars, RawBytes after EnsureBodies) are safe for concurrent use
// once their data is built.
type Scan struct {
	// Foot is the segment's footer, CRC-verified at open.
	Foot Footer
	// Match holds the ascending row indices admitted by the ScanFilter.
	Match []int

	data []byte // entire file: compressed columns + footer

	// Eagerly decoded (always available, immutable after OpenScan).
	ts     []int64
	logIDs []int64
	sevs   []byte

	colMu sync.Mutex
	cols  map[string][]byte

	bodiesOnce sync.Once
	bodiesErr  error
	tmplIDs    []uint64
	varStart   []int // len n+1 prefix sums: row i's vars are ordinals varStart[i]..varStart[i+1]
	rawOrd     []int32
	varsBuf    []byte
	varSpans   []Span
	rawBuf     []byte
	rawSpans   []Span

	metaOnce   sync.Once
	metaErr    error
	svc        *dictCol
	env        *dictCol
	rel        *dictCol
	attrs      *dictCol
	traceBuf   []byte
	traceSpans []Span
	isEvent    []byte
}

// OpenScan opens path, verifies its footer, decodes the filter columns,
// and computes Match per f. See Scan for the laziness and concurrency
// contract.
func OpenScan(path string, f ScanFilter) (*Scan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open: %w", err)
	}
	foot, err := parseFooter(path, data)
	if err != nil {
		return nil, err
	}
	sc := &Scan{Foot: foot, data: data, cols: map[string][]byte{}}
	if sc.logIDs, err = sc.deltaCol("log_id"); err != nil {
		return nil, err
	}
	if sc.ts, err = sc.deltaCol("ts"); err != nil {
		return nil, err
	}
	if sc.sevs, err = sc.fixedCol("severity"); err != nil {
		return nil, err
	}
	return sc, sc.computeMatch(f)
}

// col returns the named column's decompressed bytes, CRC-verifying and
// decompressing at most once. Safe for concurrent use.
func (sc *Scan) col(name string) ([]byte, error) {
	sc.colMu.Lock()
	defer sc.colMu.Unlock()
	if b, ok := sc.cols[name]; ok {
		return b, nil
	}
	for _, c := range sc.Foot.Columns {
		if c.Name != name {
			continue
		}
		comp := sc.data[c.Offset : c.Offset+c.CompLen]
		if crc32.ChecksumIEEE(comp) != c.CRC {
			return nil, fmt.Errorf("segment: column %s: CRC mismatch", name)
		}
		raw, err := Decompress(comp, int(c.RawLen))
		if err != nil {
			return nil, fmt.Errorf("segment: column %s: %w", name, err)
		}
		sc.cols[name] = raw
		return raw, nil
	}
	return nil, fmt.Errorf("segment: column %s not found", name)
}

// deltaCol decodes a delta-int64 column with Foot.Count values.
func (sc *Scan) deltaCol(name string) ([]int64, error) {
	b, err := sc.col(name)
	if err != nil {
		return nil, err
	}
	return DecodeDeltaInt64(b, sc.Foot.Count)
}

// fixedCol returns a one-byte-per-row column, validating its length.
func (sc *Scan) fixedCol(name string) ([]byte, error) {
	b, err := sc.col(name)
	if err != nil {
		return nil, err
	}
	if len(b) != sc.Foot.Count {
		return nil, fmt.Errorf("segment: column %s: %d bytes for %d rows", name, len(b), sc.Foot.Count)
	}
	return b, nil
}

// dict decodes a dictionary column pair with Foot.Count refs.
func (sc *Scan) dict(dictName, refName string) (*dictCol, error) {
	refBytes, err := sc.col(refName)
	if err != nil {
		return nil, err
	}
	refs, err := DecodeUvarints(refBytes, sc.Foot.Count)
	if err != nil {
		return nil, err
	}
	maxRef := uint64(0)
	for _, r := range refs {
		if r > maxRef {
			maxRef = r
		}
	}
	dictBytes, err := sc.col(dictName)
	if err != nil {
		return nil, err
	}
	vals, err := DecodeStrings(dictBytes, int(maxRef)+1)
	if err != nil {
		return nil, err
	}
	return &dictCol{dict: vals, refs: refs}, nil
}

// dictOrdinal resolves value to its ordinal in d's dict; ok=false means
// the value does not occur in this segment at all.
func dictOrdinal(d *dictCol, value string) (uint64, bool) {
	for i, v := range d.dict {
		if v == value {
			return uint64(i), true
		}
	}
	return 0, false
}

// computeMatch fills sc.Match with the rows f admits, comparing
// dictionary REFS (small ints), never per-row strings.
func (sc *Scan) computeMatch(f ScanFilter) error {
	if f.SinceM == 0 && f.UntilM == 0 {
		f.SinceM, f.UntilM = MinTsBound, MaxTsBound
	}
	var svcRef, envRef uint64
	if f.Service != "" {
		var err error
		if sc.svc == nil {
			if sc.svc, err = sc.dict("service_dict", "service_refs"); err != nil {
				return err
			}
		}
		var ok bool
		if svcRef, ok = dictOrdinal(sc.svc, f.Service); !ok {
			return nil // service absent from segment: Match stays empty
		}
	}
	if f.Environment != "" {
		var err error
		if sc.env == nil {
			if sc.env, err = sc.dict("env_dict", "env_refs"); err != nil {
				return err
			}
		}
		var ok bool
		if envRef, ok = dictOrdinal(sc.env, f.Environment); !ok {
			return nil
		}
	}
	for i := range sc.ts {
		if sc.ts[i] < f.SinceM || sc.ts[i] > f.UntilM {
			continue
		}
		if int(sc.sevs[i]) < f.MinSeverity {
			continue
		}
		if f.Service != "" && sc.svc.refs[i] != svcRef {
			continue
		}
		if f.Environment != "" && sc.env.refs[i] != envRef {
			continue
		}
		sc.Match = append(sc.Match, i)
	}
	return nil
}

// Len returns the segment's row count.
func (sc *Scan) Len() int { return sc.Foot.Count }

// Ts returns row i's timestamp in epoch micros.
func (sc *Scan) Ts(i int) int64 { return sc.ts[i] }

// LogID returns row i's log id.
func (sc *Scan) LogID(i int) int64 { return sc.logIDs[i] }

// SeverityAt returns row i's severity.
func (sc *Scan) SeverityAt(i int) int { return int(sc.sevs[i]) }

// EnsureBodies decodes the body columns (template ids, vars, raw
// fallbacks) exactly once. After it returns nil, TemplateID,
// AppendVars, and RawBytes are lock-free and never fail.
func (sc *Scan) EnsureBodies() error {
	sc.bodiesOnce.Do(sc.buildBodies)
	return sc.bodiesErr
}

func (sc *Scan) buildBodies() {
	n := sc.Foot.Count
	fail := func(err error) { sc.bodiesErr = err }
	tb, err := sc.col("template_id")
	if err != nil {
		fail(err)
		return
	}
	if sc.tmplIDs, err = DecodeUvarints(tb, n); err != nil {
		fail(err)
		return
	}
	nb, err := sc.col("nvars")
	if err != nil {
		fail(err)
		return
	}
	nvars, err := DecodeUvarints(nb, n)
	if err != nil {
		fail(err)
		return
	}
	sc.varStart = make([]int, n+1)
	sc.rawOrd = make([]int32, n)
	nRaw := 0
	for i := 0; i < n; i++ {
		sc.varStart[i+1] = sc.varStart[i] + int(nvars[i])
		if sc.tmplIDs[i] == 0 {
			sc.rawOrd[i] = int32(nRaw)
			nRaw++
		} else {
			sc.rawOrd[i] = -1
		}
	}
	if sc.varsBuf, err = sc.col("vars"); err != nil {
		fail(err)
		return
	}
	if sc.varSpans, err = IndexStrings(sc.varsBuf, sc.varStart[n]); err != nil {
		fail(err)
		return
	}
	if sc.rawBuf, err = sc.col("raw"); err != nil {
		fail(err)
		return
	}
	sc.rawSpans, err = IndexStrings(sc.rawBuf, nRaw)
	if err != nil {
		fail(err)
	}
}

// TemplateID returns row i's template id (0 = raw fallback). Requires a
// prior successful EnsureBodies.
func (sc *Scan) TemplateID(i int) int64 { return int64(sc.tmplIDs[i]) }

// AppendVars appends row i's variable values to dst as zero-copy
// sub-slices of the vars column buffer — the returned slices must be
// treated as read-only. Requires a prior successful EnsureBodies.
func (sc *Scan) AppendVars(dst [][]byte, i int) [][]byte {
	for v := sc.varStart[i]; v < sc.varStart[i+1]; v++ {
		sp := sc.varSpans[v]
		dst = append(dst, sc.varsBuf[sp.Start:sp.End])
	}
	return dst
}

// RawBytes returns row i's raw body as a zero-copy sub-slice (read-only),
// or nil when the row is templated. Requires a prior successful
// EnsureBodies.
func (sc *Scan) RawBytes(i int) []byte {
	o := sc.rawOrd[i]
	if o < 0 {
		return nil
	}
	sp := sc.rawSpans[o]
	return sc.rawBuf[sp.Start:sp.End]
}

// ensureMeta decodes the metadata columns Row needs beyond the body
// columns (dictionaries, trace ids, attrs, is_event) exactly once.
func (sc *Scan) ensureMeta() error {
	sc.metaOnce.Do(sc.buildMeta)
	return sc.metaErr
}

func (sc *Scan) buildMeta() {
	n := sc.Foot.Count
	var err error
	if sc.svc == nil {
		if sc.svc, err = sc.dict("service_dict", "service_refs"); err != nil {
			sc.metaErr = err
			return
		}
	}
	if sc.env == nil {
		if sc.env, err = sc.dict("env_dict", "env_refs"); err != nil {
			sc.metaErr = err
			return
		}
	}
	if sc.rel, err = sc.dict("release_dict", "release_refs"); err != nil {
		sc.metaErr = err
		return
	}
	if sc.attrs, err = sc.dict("attrs_dict", "attrs_refs"); err != nil {
		sc.metaErr = err
		return
	}
	if sc.traceBuf, err = sc.col("trace_id"); err != nil {
		sc.metaErr = err
		return
	}
	if sc.traceSpans, err = IndexStrings(sc.traceBuf, n); err != nil {
		sc.metaErr = err
		return
	}
	sc.isEvent, err = sc.fixedCol("is_event")
	sc.metaErr = err
}

// Row fully materializes row i, allocating strings for that row only.
// Safe for concurrent use.
func (sc *Scan) Row(i int) (Row, error) {
	if err := sc.EnsureBodies(); err != nil {
		return Row{}, err
	}
	if err := sc.ensureMeta(); err != nil {
		return Row{}, err
	}
	r := Row{
		LogID: sc.logIDs[i], TsMicros: sc.ts[i], Severity: int(sc.sevs[i]),
		TemplateID: int64(sc.tmplIDs[i]),
		Service:    sc.svc.at(i), Environment: sc.env.at(i), Release: sc.rel.at(i),
		Attrs:   sc.attrs.at(i),
		IsEvent: sc.isEvent[i] == 1,
	}
	tsp := sc.traceSpans[i]
	r.TraceID = string(sc.traceBuf[tsp.Start:tsp.End])
	if nv := sc.varStart[i+1] - sc.varStart[i]; nv > 0 {
		r.Vars = make([]string, nv)
		for k := 0; k < nv; k++ {
			sp := sc.varSpans[sc.varStart[i]+k]
			r.Vars[k] = string(sc.varsBuf[sp.Start:sp.End])
		}
	}
	if o := sc.rawOrd[i]; o >= 0 {
		sp := sc.rawSpans[o]
		r.Raw = string(sc.rawBuf[sp.Start:sp.End])
	}
	return r, nil
}

// Rows materializes the given row indices in order.
func (sc *Scan) Rows(idx []int) ([]Row, error) {
	out := make([]Row, 0, len(idx))
	for _, i := range idx {
		r, err := sc.Row(i)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}
```

- [ ] **Step 4: Rebuild `Read` over `Scan`**

In `internal/segment/segment.go`: delete `decodeDictionaryColumn` and `decodeColumns` entirely, and replace the body of `Read` with:

```go
// Read fully decodes a segment. Every column block is CRC-verified;
// any mismatch or decode inconsistency is an error — a segment never
// yields wrong data silently. Implemented over Scan: full
// materialization touches every column, preserving the verify-everything
// guarantee.
func Read(path string) (Footer, []Row, error) {
	sc, err := OpenScan(path, ScanFilter{})
	if err != nil {
		return Footer{}, nil, err
	}
	rows, err := sc.Rows(sc.Match)
	if err != nil {
		return Footer{}, nil, err
	}
	return sc.Foot, rows, nil
}
```

Remove imports `segment.go` no longer needs (likely `crc32` stays for Write/Open — verify with `go vet`).

- [ ] **Step 5: Run the full package tests**

Run: `go test ./internal/segment/ -race -v && go test ./internal/store/... -race`
Expected: PASS — including all pre-existing segment round-trip and corruption tests, now exercising the Scan-backed Read.

- [ ] **Step 6: Lint and commit**

Run: `gofmt -l . && go vet ./... && $(go env GOPATH)/bin/golangci-lint run`
Expected: 0 issues.

```bash
git add internal/segment/
git commit -m "feat(segment): column-selective filtered Scan; Read rebuilt on top"
```

---

### Task 3: Template snapshot, zero-alloc substitution, and static-run query classification

**Files:**
- Modify: `internal/template/template.go`
- Modify: `internal/template/template_test.go`

**Interfaces:**
- Produces (used verbatim by Task 4):
  - `func (e *Extractor) Snapshot(projectID int64) (map[int64][]string, error)`
  - `func AppendSubstitute(dst []byte, tokens []string, vars [][]byte) ([]byte, bool)`
  - `func AlwaysContaining(tmpls map[int64][]string, q string) map[int64]bool`

- [ ] **Step 1: Write the failing tests**

Append to `internal/template/template_test.go` (it already has a fake store type; reuse it — if its constructor differs, adapt only the setup lines, not the assertions):

```go
func TestSnapshotAndAppendSubstitute(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	ctx := context.Background()
	id, vars, ok, err := e.Extract(ctx, 1, "user 42 logged in from 1.2.3.4")
	if err != nil || !ok {
		t.Fatalf("extract: ok=%v err=%v", ok, err)
	}
	snap, err := e.Snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	toks, found := snap[id]
	if !found {
		t.Fatalf("snapshot missing template %d", id)
	}
	bvars := make([][]byte, len(vars))
	for i, v := range vars {
		bvars[i] = []byte(v)
	}
	body, ok := AppendSubstitute(nil, toks, bvars)
	if !ok {
		t.Fatal("AppendSubstitute var count mismatch")
	}
	if string(body) != "user 42 logged in from 1.2.3.4" {
		t.Errorf("got %q", body)
	}
	// Appends to existing dst without clobbering it.
	pre := []byte("x")
	body2, ok := AppendSubstitute(pre, toks, bvars)
	if !ok || string(body2[:1]) != "x" || string(body2[1:]) != "user 42 logged in from 1.2.3.4" {
		t.Errorf("append-to-dst got %q", body2)
	}
	// Wrong var count → ok=false, dst unchanged.
	if _, ok := AppendSubstitute(nil, toks, bvars[:0]); ok && len(bvars) > 0 {
		t.Error("want ok=false on var count mismatch")
	}
}

func TestAlwaysContaining(t *testing.T) {
	// Template: "record not found for user <?> (took <?>" style —
	// tokens with Wild slots.
	tmpls := map[int64][]string{
		5: {"record", "not", "found", "for", "user", Wild},
		6: {Wild, "level=info", "msg=ok"},
	}
	cases := []struct {
		q    string
		want map[int64]bool
	}{
		{"record not found", map[int64]bool{5: true}},
		{"not found for user", map[int64]bool{5: true}},
		{"level=info msg=ok", map[int64]bool{6: true}},
		// Straddles the wild slot of 5 — cannot be guaranteed.
		{"user 42", map[int64]bool{}},
		// Empty query classifies nothing.
		{"", map[int64]bool{}},
		// Matches neither statically.
		{"zzz", map[int64]bool{}},
	}
	for _, tc := range cases {
		got := AlwaysContaining(tmpls, tc.q)
		if len(got) != len(tc.want) {
			t.Errorf("q=%q: got %v, want %v", tc.q, got, tc.want)
			continue
		}
		for id := range tc.want {
			if !got[id] {
				t.Errorf("q=%q: missing id %d", tc.q, id)
			}
		}
	}
}

func TestSnapshotIsolatedFromLaterMints(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	ctx := context.Background()
	if _, _, _, err := e.Extract(ctx, 1, "alpha beta 1"); err != nil {
		t.Fatal(err)
	}
	snap, err := e.Snapshot(1)
	if err != nil {
		t.Fatal(err)
	}
	before := len(snap)
	// A structurally different body mints a new template; the old
	// snapshot map must not grow (no shared mutable map).
	if _, _, _, err := e.Extract(ctx, 1, "totally different shape with many tokens here"); err != nil {
		t.Fatal(err)
	}
	if len(snap) != before {
		t.Errorf("snapshot grew from %d to %d after later mint", before, len(snap))
	}
}
```

If the existing test file's fake store is named differently than `newFakeStore()`, use the existing name; the test bodies stay as written.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/template/ -run 'TestSnapshot|TestAlwaysContaining' -v`
Expected: FAIL — `Snapshot`, `AppendSubstitute`, `AlwaysContaining` undefined.

- [ ] **Step 3: Implement**

Append to `internal/template/template.go`:

```go
// Snapshot returns a copy of projectID's current id→tokens table for
// lock-free read use (search matching). The token slices are shared
// with the extractor and must be treated as read-only — safe because
// templates are append-only and a minted template's tokens never
// mutate. The map itself is a copy, so later mints do not race with
// readers of the snapshot.
func (e *Extractor) Snapshot(projectID int64) (map[int64][]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.projects[projectID]
	if !ok {
		var err error
		p, err = e.load(context.Background(), projectID)
		if err != nil {
			return nil, err
		}
	}
	out := make(map[int64][]string, len(p.byID))
	for id, t := range p.byID {
		out[id] = t.tokens
	}
	return out, nil
}

// AppendSubstitute appends tokens joined by single spaces, with Wild
// slots filled from vars in order, to dst — the zero-allocation
// counterpart of Reconstruct for callers that already hold the token
// list. ok=false (dst returned unchanged) when the var count does not
// match the token list's Wild count.
func AppendSubstitute(dst []byte, tokens []string, vars [][]byte) ([]byte, bool) {
	wilds := 0
	for _, tok := range tokens {
		if tok == Wild {
			wilds++
		}
	}
	if wilds != len(vars) {
		return dst, false
	}
	out := dst
	vi := 0
	for i, tok := range tokens {
		if i > 0 {
			out = append(out, ' ')
		}
		if tok == Wild {
			out = append(out, vars[vi]...)
			vi++
		} else {
			out = append(out, tok...)
		}
	}
	return out, true
}

// AlwaysContaining reports which templates in tmpls are guaranteed to
// contain q in every possible reconstruction: q lies wholly inside one
// of the template's static runs (the Wild-free stretches of its text).
// Sound but deliberately not complete — an id absent from the result
// can still match via variable values or across a run boundary, and
// callers must verify those rows by reconstruction.
func AlwaysContaining(tmpls map[int64][]string, q string) map[int64]bool {
	out := map[int64]bool{}
	if q == "" {
		return out
	}
	for id, tokens := range tmpls {
		text := strings.Join(tokens, " ")
		for _, run := range strings.Split(text, Wild) {
			if strings.Contains(run, q) {
				out[id] = true
				break
			}
		}
	}
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/template/ -race -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

Run: `gofmt -l . && go vet ./... && $(go env GOPATH)/bin/golangci-lint run`
Expected: 0 issues.

```bash
git add internal/template/
git commit -m "feat(template): snapshot, zero-alloc substitution, static-run query classification"
```

---

### Task 4: Engine read-path rewiring — Scan-backed collect/lookup, pushdown SearchLogs (sequential)

**Files:**
- Modify: `internal/store/enginestore/read.go`
- Create: `internal/store/enginestore/search.go`
- Create: `internal/store/enginestore/search_test.go`

**Interfaces:**
- Consumes: Task 2's `segment.OpenScan/ScanFilter/Scan` (accessor contract as specified there), Task 3's `Snapshot`/`AppendSubstitute`/`AlwaysContaining`.
- Produces: `openScanWithRestart` and `searchScanRange` (Task 5 parallelizes around these — signatures below are load-bearing).

- [ ] **Step 1: Write the failing tests**

Create `internal/store/enginestore/search_test.go`. Look at existing tests in this package for the store-setup helper pattern (there is an `Open` + `CreateProject` + `WriteBatch` idiom in enginestore tests — reuse it; the entries below are the data contract):

```go
package enginestore

import (
	"context"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// searchFixture writes a mixed corpus (templated repeats, a raw
// oddball, two services, two severities), flushes so rows live in a
// real segment, then writes two more unflushed rows so the memtable
// path is exercised too.
func searchFixture(t *testing.T) (*Store, int64) {
	t.Helper()
	s := openTestStore(t) // use this package's existing test-open helper
	ctx := context.Background()
	p, err := s.CreateProject(ctx, "search", 30)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	var entries []store.Entry
	add := func(off int, sev core.Severity, svc, body string) {
		entries = append(entries, store.Entry{Log: core.Log{
			ProjectID: p.ID, Time: base.Add(time.Duration(off) * time.Second),
			Severity: sev, Service: svc, Body: body,
		}})
	}
	for i := 0; i < 50; i++ {
		add(i, core.SeverityError, "api", "record not found for user 42")
	}
	add(60, core.SeverityInfo, "api", "user 99 logged in ok")
	add(61, core.SeverityError, "web", "record not found for user 7")
	add(62, core.SeverityError, "api", "!!raw@@line##with War saw inside") // ANSI-free but non-templatable? if it templates, fine — the assertions below don't depend on it
	if _, err := s.WriteBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	// Unflushed memtable rows.
	var late []store.Entry
	late = append(late, store.Entry{Log: core.Log{ProjectID: p.ID, Time: base.Add(2 * time.Minute), Severity: core.SeverityError, Service: "api", Body: "record not found for user 555"}})
	late = append(late, store.Entry{Log: core.Log{ProjectID: p.ID, Time: base.Add(3 * time.Minute), Severity: core.SeverityWarn, Service: "web", Body: "cache warm done"}})
	if _, err := s.WriteBatch(ctx, late); err != nil {
		t.Fatal(err)
	}
	return s, p.ID
}

func TestSearchSubstringAcrossSegmentAndMemtable(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Query: "record not found"})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 50 { // default limit caps 52 matches at 50
		t.Fatalf("got %d, want 50", len(logs))
	}
	// Most recent first: the memtable row leads.
	if logs[0].Body != "record not found for user 555" {
		t.Errorf("first = %q", logs[0].Body)
	}
	for i := 1; i < len(logs); i++ {
		a, b := logs[i-1], logs[i]
		if a.Time.Before(b.Time) || (a.Time.Equal(b.Time) && a.ID < b.ID) {
			t.Fatalf("order violated at %d: %v/%d then %v/%d", i, a.Time, a.ID, b.Time, b.ID)
		}
	}
}

func TestSearchQueryStraddlesVarBoundary(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	// "user 42" spans static text and a variable — the always-match
	// classification must NOT claim it, and reconstruction must find it.
	logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Query: "for user 42", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 50 {
		t.Fatalf("got %d, want 50", len(logs))
	}
	for _, l := range logs {
		if l.Body != "record not found for user 42" {
			t.Errorf("unexpected body %q", l.Body)
		}
	}
}

func TestSearchFiltersComposeWithQuery(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	logs, err := s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "record not found", Service: "web", Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].Body != "record not found for user 7" {
		t.Fatalf("service+query: got %+v", logs)
	}
	logs, err = s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "logged in", MinSeverity: core.SeverityError, Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("severity should exclude the info row, got %d", len(logs))
	}
	logs, err = s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "zzz-not-present", Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("no-hit query returned %d", len(logs))
	}
}

func TestSearchTimeWindow(t *testing.T) {
	s, pid := searchFixture(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	logs, err := s.SearchLogs(ctx, store.LogFilter{
		ProjectID: pid, Query: "record not found",
		Since: base.Add(30 * time.Second), Until: base.Add(70 * time.Second),
		Limit: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 21 { // offsets 30..49 templated + offset 61 web row
		t.Fatalf("got %d, want 21", len(logs))
	}
}
```

Adapt ONLY the store-open helper call (`openTestStore(t)`) to whatever this package's existing tests actually use (grep for `enginestore.Open(` or a helper in `enginestore_test.go`); everything else stays as written. If `core.SeverityWarn`/`core.SeverityInfo` differ in name, check `internal/core/types.go` and use the real identifiers.

- [ ] **Step 2: Run tests to verify they fail correctly**

Run: `go test ./internal/store/enginestore/ -run TestSearch -v`
Expected: compile or assertion behavior — these tests should PASS against the old SearchLogs too (they are behavior-pinning tests). Run them BEFORE the rewrite to confirm they pass, so any later failure is a regression introduced by the rewrite, not a bad test. If any fails against the old implementation, fix the test's expectation to match old behavior — the old behavior is the contract.

- [ ] **Step 3: Rewire read.go's segment reads onto Scan**

In `internal/store/enginestore/read.go`:

3a. Add `openScanWithRestart` (this replaces the role of both old restart wrappers — delete `readSegmentRowsWithRestart`'s body-era sibling `readSegmentFileWithRestart` and the standalone `readSegmentRows`):

```go
// openScanWithRestart opens m as a filtered Scan, mapping a vanished
// file to the restart/corruption split this engine's reads use
// everywhere: row gone from a fresh manifest → the segment was
// legitimately replaced (compacted or pruned) and the caller must
// restart its whole attempt from a fresh snapshot; row still present
// but file missing → real corruption, loud error naming the path. After
// OpenScan returns, the scan holds the file's bytes in memory, so no
// later stage of a query can hit ENOENT.
func (s *Store) openScanWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, f segment.ScanFilter) (*segment.Scan, bool, error) {
	sc, err := segment.OpenScan(s.segPath(m.Path), f)
	if err == nil {
		return sc, false, nil
	}
	if !isSegmentNotExist(err) {
		return nil, false, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
	}
	fresh, found, ferr := s.freshSegmentByID(ctx, projectID, m.ID)
	if ferr != nil {
		return nil, false, ferr
	}
	if !found {
		return nil, true, nil
	}
	sc, err = segment.OpenScan(s.segPath(fresh.Path), f)
	if err != nil {
		if isSegmentNotExist(err) {
			return nil, false, fmt.Errorf("enginestore: segment %s missing but manifest row %d still present: %w", fresh.Path, fresh.ID, err)
		}
		return nil, false, fmt.Errorf("enginestore: read segment %s: %w", fresh.Path, err)
	}
	return sc, false, nil
}
```

3b. Rebuild `readSegmentRowsWithRestart` over it (keeping its exact signature — `collectRowsOnce` keeps compiling unchanged) and delete `readSegmentRows`:

```go
// readSegmentRowsWithRestart returns m's rows within [sinceM, untilM]
// (optionally service-filtered), skipping the file entirely when the
// manifest row already rules it out, with the standard
// restart-on-replacement discipline (see openScanWithRestart).
func (s *Store) readSegmentRowsWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, sinceM, untilM int64, service string) ([]segment.Row, bool, error) {
	if m.MaxTs < sinceM || m.MinTs > untilM {
		return nil, false, nil
	}
	if service != "" && !contains(m.Services, service) {
		return nil, false, nil
	}
	sc, restart, err := s.openScanWithRestart(ctx, projectID, m, segment.ScanFilter{SinceM: sinceM, UntilM: untilM, Service: service})
	if err != nil || restart {
		return nil, restart, err
	}
	rows, err := sc.Rows(sc.Match)
	return rows, false, err
}
```

3c. Replace `readSegmentFileWithRestart` with a lookup that materializes at most one row, and update `logByIDOnce` to call it:

```go
// findInSegmentWithRestart looks up logID in segment m without
// materializing any other row: only the cheap id column is decoded
// unless the id is found. Same restart/corruption discipline as
// openScanWithRestart.
func (s *Store) findInSegmentWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, logID int64) (segment.Row, bool, bool, error) {
	sc, restart, err := s.openScanWithRestart(ctx, projectID, m, segment.ScanFilter{})
	if err != nil || restart {
		return segment.Row{}, false, restart, err
	}
	for i := 0; i < sc.Len(); i++ {
		if sc.LogID(i) == logID {
			r, err := sc.Row(i)
			if err != nil {
				return segment.Row{}, false, false, err
			}
			return r, true, false, nil
		}
	}
	return segment.Row{}, false, false, nil
}
```

In `logByIDOnce`, the segment loop becomes:

```go
	for _, m := range segs {
		if logID < m.MinLogID || logID > m.MaxLogID {
			continue
		}
		r, ok, restart, err := s.findInSegmentWithRestart(ctx, projectID, m, logID)
		if err != nil {
			return segment.Row{}, false, false, err
		}
		if restart {
			return segment.Row{}, false, true, nil
		}
		if ok {
			return r, true, false, nil
		}
	}
```

3d. Delete from read.go: the old `SearchLogs`, `pairedRow`, `collectRowsAllProjects`, `collectPairedRows` (their only consumer was SearchLogs). Keep `collectRows`/`collectRowsOnce`, `filterRowsByTime` (memtable filtering still uses it), `rowLess`, `rowToLog`, `logByID`, `LogContext`, `findLog`, and everything below them unchanged.

- [ ] **Step 4: Implement pushdown SearchLogs**

Create `internal/store/enginestore/search.go`:

```go
package enginestore

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/template"
)

// matchRef points at one row that satisfied a search's filters and
// query. Segment rows stay unmaterialized (scan + index) until the
// final limit cut; memtable rows are carried by value.
type matchRef struct {
	ts, logID int64
	projectID int64
	sc        *segment.Scan // nil → memRow is set
	idx       int
	memRow    segment.Row
}

// SearchLogs returns logs matching f, most recent first (ties broken by
// descending id), capped at f.Limit (0 → 50). Query is a SUBSTRING
// match on the reconstructed body — no tokenizer exists anywhere in
// this engine (spec §5).
//
// The predicate is pushed down: segments decode only the cheap filter
// columns for rows that never match; rows whose template's static text
// already guarantees a hit skip the byte scan entirely; the rest are
// reconstructed into a reusable buffer and checked with bytes.Contains
// — no per-row string allocation. Only the final ≤limit rows are fully
// materialized.
func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}
	var pids []int64
	if f.ProjectID != 0 {
		pids = []int64{f.ProjectID}
	} else {
		s.mu.Lock()
		for pid := range s.projects {
			pids = append(pids, pid)
		}
		s.mu.Unlock()
	}
	var all []matchRef
	for _, pid := range pids {
		ms, err := s.searchProject(ctx, pid, f, limit)
		if err != nil {
			return nil, err
		}
		all = append(all, ms...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].ts != all[j].ts {
			return all[i].ts > all[j].ts
		}
		return all[i].logID > all[j].logID
	})
	if len(all) > limit {
		all = all[:limit]
	}
	out := make([]core.Log, 0, len(all))
	for _, m := range all {
		r := m.memRow
		if m.sc != nil {
			var err error
			r, err = m.sc.Row(m.idx)
			if err != nil {
				return nil, err
			}
		}
		l, err := s.rowToLog(m.projectID, r)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// searchProject runs searchProjectOnce with the standard bounded
// restart loop (see collectRows for the discipline).
func (s *Store) searchProject(ctx context.Context, projectID int64, f store.LogFilter, limit int) ([]matchRef, error) {
	for attempt := 1; attempt <= maxSegmentSetRestarts; attempt++ {
		ms, restart, err := s.searchProjectOnce(ctx, projectID, f, limit)
		if err != nil {
			return nil, err
		}
		if !restart {
			return ms, nil
		}
	}
	return nil, fmt.Errorf("enginestore: segment set changed repeatedly during read (project %d)", projectID)
}

// searchProjectOnce is one attempt: a coherent manifest+memtable
// snapshot (under ps.mu — see collectRowsOnce), then a filtered,
// classified match pass over each candidate segment and the memtable.
func (s *Store) searchProjectOnce(ctx context.Context, projectID int64, f store.LogFilter, limit int) ([]matchRef, bool, error) {
	sinceM, untilM := boundsMicros(f.Since, f.Until)
	segs, memRows, err := s.snapshotProject(ctx, projectID)
	if err != nil {
		return nil, false, err
	}
	tmpls, err := s.ex.Snapshot(projectID)
	if err != nil {
		return nil, false, err
	}
	always := template.AlwaysContaining(tmpls, f.Query)
	filter := segment.ScanFilter{
		SinceM: sinceM, UntilM: untilM,
		Service: f.Service, Environment: f.Environment,
		MinSeverity: int(f.MinSeverity),
	}
	var out []matchRef
	for _, m := range segs {
		if m.MaxTs < sinceM || m.MinTs > untilM {
			continue
		}
		if f.Service != "" && !contains(m.Services, f.Service) {
			continue
		}
		sc, restart, err := s.openScanWithRestart(ctx, projectID, m, filter)
		if err != nil {
			return nil, false, err
		}
		if restart {
			return nil, true, nil
		}
		ms, err := searchScanRange(sc, projectID, f.Query, always, tmpls, limit, 0, len(sc.Match))
		if err != nil {
			return nil, false, err
		}
		out = append(out, ms...)
	}
	mm, err := s.searchMemRows(projectID, memRows, f, sinceM, untilM)
	if err != nil {
		return nil, false, err
	}
	return append(out, mm...), false, nil
}

// snapshotProject takes the manifest and memtable snapshot together
// under ps.mu — the same coherence rule collectRowsOnce documents.
func (s *Store) snapshotProject(ctx context.Context, projectID int64) ([]sqlitestore.SegmentMeta, []segment.Row, error) {
	ps := s.readProj(projectID)
	if ps == nil {
		// No projState: no flush can be running for this project, so the
		// manifest query needs no ps.mu coherence lock.
		segs, err := s.Segments(ctx, projectID)
		return segs, nil, err
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	segs, err := s.Segments(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}
	return segs, ps.mem.Snapshot(), nil
}

// searchScanRange matches sc.Match[lo:hi] against q in reverse
// ((ts, id)-descending within a segment, since segment rows are
// ts-ascending), collecting matchRefs until limit is reached — then
// continuing only while candidates tie the boundary timestamp, so the
// caller's global (ts, id) sort can cut exactly. Requires nothing
// pre-ensured; it calls EnsureBodies itself when a query is present.
func searchScanRange(sc *segment.Scan, projectID int64, q string, always map[int64]bool, tmpls map[int64][]string, limit, lo, hi int) ([]matchRef, error) {
	qb := []byte(q)
	if len(qb) > 0 {
		if err := sc.EnsureBodies(); err != nil {
			return nil, err
		}
	}
	var out []matchRef
	var body []byte
	var vars [][]byte
	for k := hi - 1; k >= lo; k-- {
		i := sc.Match[k]
		if len(out) >= limit && sc.Ts(i) != out[len(out)-1].ts {
			break
		}
		ok, err := rowMatches(sc, i, qb, always, tmpls, &body, &vars)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, matchRef{ts: sc.Ts(i), logID: sc.LogID(i), projectID: projectID, sc: sc, idx: i})
		}
	}
	return out, nil
}

// rowMatches reports whether row i's body contains qb, reusing the
// caller's scratch buffers. An empty query matches everything. Raw rows
// scan their stored bytes directly; templated rows either match by
// static classification (always) or are reconstructed into the scratch
// buffer and scanned.
func rowMatches(sc *segment.Scan, i int, qb []byte, always map[int64]bool, tmpls map[int64][]string, body *[]byte, vars *[][]byte) (bool, error) {
	if len(qb) == 0 {
		return true, nil
	}
	tid := sc.TemplateID(i)
	if tid == 0 {
		return bytes.Contains(sc.RawBytes(i), qb), nil
	}
	if always[tid] {
		return true, nil
	}
	toks, ok := tmpls[tid]
	if !ok {
		return false, fmt.Errorf("enginestore: template %d missing for log %d", tid, sc.LogID(i))
	}
	*vars = sc.AppendVars((*vars)[:0], i)
	b, ok := template.AppendSubstitute((*body)[:0], toks, *vars)
	*body = b
	if !ok {
		return false, fmt.Errorf("enginestore: template %d var count mismatch for log %d", tid, sc.LogID(i))
	}
	return bytes.Contains(b, qb), nil
}

// searchMemRows applies the full filter set plus the substring query to
// unflushed memtable rows. The memtable is small (flushes cap it), so
// this path reconstructs through rowToLog per candidate.
func (s *Store) searchMemRows(projectID int64, memRows []segment.Row, f store.LogFilter, sinceM, untilM int64) ([]matchRef, error) {
	var out []matchRef
	for _, r := range memRows {
		if r.TsMicros < sinceM || r.TsMicros > untilM {
			continue
		}
		if f.Service != "" && r.Service != f.Service {
			continue
		}
		if f.Environment != "" && r.Environment != f.Environment {
			continue
		}
		if r.Severity < int(f.MinSeverity) {
			continue
		}
		if f.Query != "" {
			l, err := s.rowToLog(projectID, r)
			if err != nil {
				return nil, err
			}
			if !strings.Contains(l.Body, f.Query) {
				continue
			}
		}
		out = append(out, matchRef{ts: r.TsMicros, logID: r.LogID, projectID: projectID, memRow: r})
	}
	return out, nil
}
```

Note: `collectRowsOnce` in read.go must be updated to use `snapshotProject` too if that removes duplication cleanly, or left as-is — implementer's choice; do NOT change its behavior.

- [ ] **Step 5: Run the full test suite**

Run: `go test ./... -race 2>&1 | tail -35`
Expected: PASS everywhere — enginestore's existing storetest conformance suite plus the new search tests. Any storetest failure means the pushdown changed semantics: fix the implementation, not the test.

- [ ] **Step 6: Lint and commit**

Run: `gofmt -l . && go vet ./... && $(go env GOPATH)/bin/golangci-lint run`
Expected: 0 issues.

```bash
git add internal/store/enginestore/
git commit -m "feat(enginestore): scan-backed reads and pushdown substring search"
```

---

### Task 5: Parallel scans and chunked matching + benchmark

**Files:**
- Modify: `internal/store/enginestore/search.go`
- Create: `internal/store/enginestore/bench_test.go`

**Interfaces:**
- Consumes: Task 4's `searchScanRange` (its lo/hi range parameters exist exactly for this task) and `openScanWithRestart`.

- [ ] **Step 1: Write the benchmark (also the correctness harness for parallelism)**

Create `internal/store/enginestore/bench_test.go`:

```go
package enginestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// benchStore builds a store with ~200k templated rows across 10
// services in one compacted segment — a miniature of the real bench
// corpus. Shared via package-level cache so multiple benchmarks reuse
// one build.
func benchStore(b *testing.B) (*Store, int64) {
	b.Helper()
	s := openTestStore(b) // same helper the tests use; if it takes *testing.T, generalize it to testing.TB in this task
	ctx := context.Background()
	p, err := s.CreateProject(ctx, "bench", 30)
	if err != nil {
		b.Fatal(err)
	}
	base := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	const n = 200_000
	batch := make([]store.Entry, 0, 1000)
	for i := 0; i < n; i++ {
		svc := fmt.Sprintf("svc%d", i%10)
		body := fmt.Sprintf("request handled path=/api/v1/items/%d status=200 dur=%dms", i, i%97)
		if i%1000 == 999 {
			body = fmt.Sprintf("record not found for id %d", i)
		}
		batch = append(batch, store.Entry{Log: core.Log{
			ProjectID: p.ID, Time: base.Add(time.Duration(i) * 400 * time.Millisecond),
			Severity: core.SeverityError, Service: svc, Body: body,
		}})
		if len(batch) == 1000 {
			if _, err := s.WriteBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err := s.FlushAll(); err != nil {
		b.Fatal(err)
	}
	if err := s.CompactAll(ctx); err != nil {
		b.Fatal(err)
	}
	return s, p.ID
}

func BenchmarkSearchScoped(b *testing.B) {
	s, pid := benchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Service: "svc3", Query: "record not found"})
		if err != nil {
			b.Fatal(err)
		}
		if len(logs) == 0 {
			b.Fatal("expected hits")
		}
	}
}

func BenchmarkSearchUnscopedNoHit(b *testing.B) {
	s, pid := benchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: pid, Query: "zzz never present"})
		if err != nil {
			b.Fatal(err)
		}
		if len(logs) != 0 {
			b.Fatal("expected no hits")
		}
	}
}
```

Record the sequential numbers first: `go test ./internal/store/enginestore/ -bench Search -run '^$' -benchtime 5x` — paste the output into the commit message of Step 3.

- [ ] **Step 2: Parallelize**

In `internal/store/enginestore/search.go`, replace `searchProjectOnce`'s sequential segment loop with a bounded-concurrency fan-out, and route large Match sets through chunked matching (add `"runtime"` and `"sync"` imports):

```go
// searchChunkRows is the Match-count above which one segment's match
// pass is split into parallel chunks. Chunks are independent because
// searchScanRange takes an explicit [lo, hi) range and per-chunk caps
// are safe: the global cut keeps at most `limit` rows, and any row a
// chunk's cap discards is outranked by `limit` rows from that same
// chunk.
const searchChunkRows = 32_768

// segSearchResult carries one segment's outcome across the fan-out.
type segSearchResult struct {
	ms      []matchRef
	restart bool
	err     error
}

// searchSegments scans and matches every candidate segment with bounded
// parallelism (one goroutine per segment, GOMAXPROCS at a time), each
// segment further chunk-parallelized by searchScanChunked.
func (s *Store) searchSegments(ctx context.Context, projectID int64, segs []sqlitestore.SegmentMeta, filter segment.ScanFilter, q string, always map[int64]bool, tmpls map[int64][]string, limit int) ([]matchRef, bool, error) {
	results := make([]segSearchResult, len(segs))
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for si := range segs {
		wg.Add(1)
		go func(si int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			sc, restart, err := s.openScanWithRestart(ctx, projectID, segs[si], filter)
			if err != nil || restart {
				results[si] = segSearchResult{restart: restart, err: err}
				return
			}
			ms, err := searchScanChunked(sc, projectID, q, always, tmpls, limit)
			results[si] = segSearchResult{ms: ms, err: err}
		}(si)
	}
	wg.Wait()
	var out []matchRef
	for _, r := range results {
		if r.err != nil {
			return nil, false, r.err
		}
		if r.restart {
			return nil, true, nil
		}
		out = append(out, r.ms...)
	}
	return out, false, nil
}

// searchScanChunked splits one segment's Match set into contiguous
// chunks matched in parallel, then merges the per-chunk results in
// global descending order, applying the same limit+boundary-tie rule
// searchScanRange uses within a chunk.
func searchScanChunked(sc *segment.Scan, projectID int64, q string, always map[int64]bool, tmpls map[int64][]string, limit int) ([]matchRef, error) {
	n := len(sc.Match)
	if n <= searchChunkRows {
		return searchScanRange(sc, projectID, q, always, tmpls, limit, 0, n)
	}
	if q != "" {
		if err := sc.EnsureBodies(); err != nil {
			return nil, err
		}
	}
	nChunks := (n + searchChunkRows - 1) / searchChunkRows
	chunks := make([][]matchRef, nChunks)
	errs := make([]error, nChunks)
	var wg sync.WaitGroup
	for c := 0; c < nChunks; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			lo := c * searchChunkRows
			hi := lo + searchChunkRows
			if hi > n {
				hi = n
			}
			chunks[c], errs[c] = searchScanRange(sc, projectID, q, always, tmpls, limit, lo, hi)
		}(c)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	// Chunks cover ascending index ranges; each chunk's matches are
	// (ts, id)-descending within it. Consuming chunks from last to
	// first yields global descending order (segment rows are
	// ts-ascending by construction), so apply the limit+tie rule on
	// the concatenation.
	var out []matchRef
	for c := nChunks - 1; c >= 0; c-- {
		for _, m := range chunks[c] {
			if len(out) >= limit && m.ts != out[len(out)-1].ts {
				return out, nil
			}
			out = append(out, m)
		}
	}
	return out, nil
}
```

In `searchProjectOnce`, replace the `for _, m := range segs { ... }` loop: first collect the candidate segments (footer pruning by time/service as before) into a slice, then one call:

```go
	var cands []sqlitestore.SegmentMeta
	for _, m := range segs {
		if m.MaxTs < sinceM || m.MinTs > untilM {
			continue
		}
		if f.Service != "" && !contains(m.Services, f.Service) {
			continue
		}
		cands = append(cands, m)
	}
	out, restart, err := s.searchSegments(ctx, projectID, cands, filter, f.Query, always, tmpls, limit)
	if err != nil || restart {
		return nil, restart, err
	}
```

Chunk-boundary tie caveat the reviewer must check: within one chunk, matches at the SAME timestamp are collected in ascending-id order (reverse index order), and the boundary-tie rule keys off `out[len(out)-1].ts`. That is the same rule Task 4's sequential path uses, and the global sort in SearchLogs re-orders ties — correctness only requires that no globally-top-limit row is dropped, which the tie-continuation guarantees on both chunk and segment level.

- [ ] **Step 3: Run tests + benchmarks**

Run: `go test ./... -race 2>&1 | tail -25`
Expected: PASS everywhere (the -race run over the new parallel paths is the point).

Run: `go test ./internal/store/enginestore/ -bench Search -run '^$' -benchtime 5x`
Expected: both benchmarks meaningfully faster than the Step-1 numbers; paste before/after into the commit message.

- [ ] **Step 4: Lint and commit**

Run: `gofmt -l . && go vet ./... && $(go env GOPATH)/bin/golangci-lint run`
Expected: 0 issues.

```bash
git add internal/store/enginestore/
git commit -m "perf(enginestore): parallel segment scans and chunked substring matching

BenchmarkSearchScoped:        <before> -> <after>
BenchmarkSearchUnscopedNoHit: <before> -> <after>"
```

---

### Task 6 (CONTROLLER-RUN — do not dispatch a subagent): Real head-to-head re-run and report update

The controller (session owner) runs this task directly: it needs Docker, the confidential local corpus (`~/tmp-agenterr-corpus-day.json`, NEVER committed), and the preserved o2 container.

- [ ] Rebuild and run the agenterr side: `go run ./cmd/benchvso2 -mode agenterr -corpus ~/tmp-agenterr-corpus-day.json -dir <fresh tmp dir>`
- [ ] Restart o2 (`docker start bench-o2`) and re-time its queries: `go run ./cmd/benchvso2 -mode o2-query` (data persists in the bench-o2-data volume; no re-ingest)
- [ ] Update `docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md` with the new numbers and a fresh §7 verdict table
- [ ] Commit the report update

---

## Self-Review Notes

- Spec coverage: §5 (query paths — search pushdown, substring semantics preserved), §3 (format untouched), §7 (targets re-measured in Task 6). Severity rules (§1 item 4) are deliberately out of scope — next plan.
- Type consistency verified: `searchScanRange`'s `(sc, projectID, q, always, tmpls, limit, lo, hi)` signature is defined in Task 4 and consumed by Task 5's `searchScanChunked`; `ScanFilter`/accessor contracts defined in Task 2 are consumed verbatim in Tasks 4–5; `Snapshot`/`AppendSubstitute`/`AlwaysContaining` defined in Task 3, consumed in Task 4.
- The one intentional deviation from strict TDD: Task 4 Step 2 runs the new tests against the OLD implementation first (they are behavior-pinning tests, expected to pass both before and after). This is deliberate — the contract is "identical results, faster."
- Deleted symbols each have their only consumers updated in the same task (verified by grep during planning): `decodeColumns`/`decodeDictionaryColumn` (only `Read`), `pairedRow`/`collectRowsAllProjects`/`collectPairedRows` (only old `SearchLogs`), `readSegmentRows`/`readSegmentFileWithRestart` (only the rewired wrappers/`logByIDOnce`).
