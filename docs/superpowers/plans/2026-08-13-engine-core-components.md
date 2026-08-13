# Template Engine Core Components Implementation Plan (Engine Plan A of B)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the four tested core libraries of the template storage engine — production template extractor, columnar encoders, segment file format, and WAL + memtable — as standalone packages ready for assembly.

**Architecture:** Spec §2–§4 of `docs/superpowers/specs/2026-08-12-template-storage-engine-design.md`. This plan produces libraries with no wiring into the running server: `internal/template` (lossless, append-only, capped, persistence-backed extraction), `internal/segment` (column encoding + immutable zstd segment files with CRC'd footers), `internal/engine` (WAL with torn-tail-tolerant replay + memtable). **Plan B** (written after this plan lands) assembles them into a `store.Store` implementation passing `storetest`, adds the manifest/flush/rollups, and swaps the app over.

**Tech Stack:** Go, `github.com/klauspost/compress/zstd` (already a dependency), stdlib only otherwise.

**Validated groundwork:** the prototype in `cmd/tmplproto` (Step-0 gate report: 99.3% rate, 8.8 B/rec) — Task 1 ports its Drain algorithm; adversarial testing proved the template cap is load-bearing (50k-templates-for-50k-lines explosion on high-entropy data).

## Global Constraints

- Pure Go, no cgo (spec: build story untouched).
- Templates are **append-only**: an existing template's tokens never change; generalization mints a NEW id (spec §2).
- Round-trip invariant `Reconstruct(Extract(b)) == b` byte-for-byte, verified at extract time; any failure → raw fallback (`template_id = 0`), never a corrupted body (spec §2).
- Per-project template cap, default **100_000**; at the cap new shapes go raw (spec §2 guardrail — validated by the synth-entropy adversarial test).
- The wildcard sentinel is `"\x00"` (NUL); bodies containing NUL or newline always go raw.
- Segments are immutable once written; every block CRC-checked; a corrupted file must fail loudly on read, never return wrong data (spec §3).
- WAL: acked data survives crash up to the fsync window; replay tolerates a torn tail (spec §4).
- All work on branch `feat/engine-core` off main: `git checkout -b feat/engine-core` before Task 1.
- CI runs gofmt + golangci-lint (gocyclo limit 15): run `gofmt -l .` and keep functions small before every commit.

## File Structure

```
internal/template/template.go        extractor: Extract/Reconstruct/Count, per-project trees
internal/template/template_test.go
internal/segment/colenc.go           varint/delta/string/dict encoders
internal/segment/colenc_test.go
internal/segment/zstd.go             shared zstd Compress/Decompress
internal/segment/segment.go          Row, Footer, Write/Open/Read
internal/segment/segment_test.go
internal/engine/wal.go               WAL append/sync/replay/reset
internal/engine/wal_test.go
internal/engine/memtable.go          in-memory recent rows
internal/engine/memtable_test.go
```

---

### Task 1: `internal/template` — production extractor

**Files:**
- Create: `internal/template/template.go`
- Test: `internal/template/template_test.go`

**Interfaces:**
- Consumes: nothing (stdlib). The algorithm is a port of `cmd/tmplproto/drain.go` (read it for reference) with: `int64` ids from a persistence hook, per-project isolation, the cap, and lazy load.
- Produces (Plan B's engine and its SQLite template table depend on these exact signatures):
  - `const Wild = "\x00"`, `const RawID int64 = 0`
  - `type Row struct { ID int64; Text string }` — Text is tokens joined with `" "`, wildcards as `Wild`.
  - `type Store interface { InsertTemplate(ctx context.Context, projectID int64, text string) (int64, error); LoadTemplates(ctx context.Context, projectID int64) ([]Row, error) }`
  - `func NewExtractor(s Store, capPerProject int) *Extractor` (cap ≤ 0 → 100_000)
  - `func (e *Extractor) Extract(ctx context.Context, projectID int64, body string) (id int64, vars []string, ok bool, err error)` — `ok=false, err=nil` means raw fallback; `err != nil` means persistence failed (caller decides).
  - `func (e *Extractor) Reconstruct(projectID, id int64, vars []string) (string, bool)`
  - `func (e *Extractor) Count(projectID int64) int` (loads project if needed is NOT required — returns in-memory count; 0 for never-touched projects)

- [ ] **Step 1: Write the failing test**

```go
package template

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// fakeStore is an in-memory template.Store with error injection.
type fakeStore struct {
	rows   map[int64][]Row // projectID → rows
	nextID int64
	failInsert error
	inserts int
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
	return append([]Row(nil), f.rows[projectID]...), nil
}

var ctx = context.Background()

func TestExtractRoundTripAppendOnly(t *testing.T) {
	e := NewExtractor(newFakeStore(), 0)
	lines := []string{
		`203.0.113.10 - - [08/Aug/2026:22:26:49 +0000] "POST /webhooks/daily HTTP/2.0" 401 29 1ms`,
		`203.0.113.11 - - [08/Aug/2026:21:59:01 +0000] "POST /api/webhooks/daily HTTP/2.0" 404 18 1ms`,
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
		got, ok := e.Reconstruct(1, s.id, s.vars)
		if !ok || got != s.body {
			t.Errorf("round trip broke:\n got %q\nwant %q", got, s.body)
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
	if _, ok := e.Reconstruct(2, id1, nil); ok && id1 != id2 {
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
	got, ok2 := e2.Reconstruct(1, id, vars)
	if !ok2 || got != "req 123 done in 45ms" {
		t.Errorf("after reload: got %q ok=%v", got, ok2)
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

func longLine(tokens int) string {
	s := ""
	for i := 0; i < tokens; i++ {
		s += fmt.Sprintf("t%d ", i)
	}
	return s
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/template/ -v`
Expected: FAIL (package does not exist).

- [ ] **Step 3: Write the implementation**

```go
// Package template implements lossless, append-only log template
// extraction (spec §2) — the storage engine's core primitive. A log body
// resolves to (templateID, vars) such that substituting vars back into
// the template reproduces the exact original bytes; that invariant is
// verified per extraction, and any failure (or any body that cannot
// tokenize: empty, multiline, NUL bytes, >200 tokens) falls back to
// RawID. Templates never mutate after creation: generalizing an existing
// template mints a NEW id, so any previously returned (id, vars) pair
// reconstructs forever. A per-project cap bounds the template table —
// high-entropy bodies (validated adversarially during Step-0) would
// otherwise mint one template per line.
package template

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Wild marks a variable slot inside a stored template text. NUL cannot
// occur in template-able bodies (they go raw), so it is unambiguous.
const Wild = "\x00"

// RawID is the reserved template id for bodies stored verbatim.
const RawID int64 = 0

// Row is a persisted template.
type Row struct {
	ID   int64
	Text string // tokens joined with " ", variable slots as Wild
}

// Store persists templates. Implementations must return ids that are
// unique per project and stable across restarts. Calls happen under the
// Extractor's lock — implementations should be fast.
type Store interface {
	InsertTemplate(ctx context.Context, projectID int64, text string) (int64, error)
	LoadTemplates(ctx context.Context, projectID int64) ([]Row, error)
}

type tmpl struct {
	id     int64
	tokens []string
}

type project struct {
	groups map[string][]*tmpl // groupKey → candidates
	byID   map[int64]*tmpl
}

// Extractor is safe for concurrent use.
type Extractor struct {
	mu        sync.Mutex
	store     Store
	cap       int
	simThresh float64
	projects  map[int64]*project
}

// NewExtractor returns an Extractor persisting through s. capPerProject
// ≤ 0 selects the default of 100_000 (spec §2).
func NewExtractor(s Store, capPerProject int) *Extractor {
	if capPerProject <= 0 {
		capPerProject = 100_000
	}
	return &Extractor{store: s, cap: capPerProject, simThresh: 0.5, projects: map[int64]*project{}}
}

// Extract resolves body to (id, vars, true) or reports a raw fallback
// (RawID, nil, false, nil). A non-nil error means template persistence
// failed and nothing was minted.
func (e *Extractor) Extract(ctx context.Context, projectID int64, body string) (int64, []string, bool, error) {
	if body == "" || strings.ContainsAny(body, "\n"+Wild) {
		return RawID, nil, false, nil
	}
	tokens := strings.Split(body, " ")
	if len(tokens) > 200 {
		return RawID, nil, false, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	p, err := e.load(ctx, projectID)
	if err != nil {
		return 0, nil, false, err
	}

	key := groupKey(tokens)
	target, mintTokens := resolve(p.groups[key], tokens, e.simThresh)

	tks := mintTokens
	if target != nil {
		tks = target.tokens
	}
	vars := varsFor(tks, tokens)
	if substitute(tks, vars) != body { // invariant check BEFORE any mint
		return RawID, nil, false, nil
	}

	if target == nil {
		if len(p.byID) >= e.cap {
			return RawID, nil, false, nil
		}
		id, err := e.store.InsertTemplate(ctx, projectID, strings.Join(mintTokens, " "))
		if err != nil {
			return 0, nil, false, fmt.Errorf("template: persist: %w", err)
		}
		target = &tmpl{id: id, tokens: mintTokens}
		p.groups[key] = append(p.groups[key], target)
		p.byID[id] = target
	}
	return target.id, vars, true, nil
}

// Reconstruct rebuilds the original body from a (projectID, id, vars)
// triple previously returned by Extract on this or any prior process
// over the same Store.
func (e *Extractor) Reconstruct(projectID, id int64, vars []string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.projects[projectID]
	if !ok {
		// Not loaded this process; load lazily with a background ctx —
		// reads must not depend on the caller having extracted first.
		var err error
		p, err = e.load(context.Background(), projectID)
		if err != nil {
			return "", false
		}
	}
	t, ok := p.byID[id]
	if !ok {
		return "", false
	}
	vi := 0
	for _, tok := range t.tokens {
		if tok == Wild {
			vi++
		}
	}
	if vi != len(vars) {
		return "", false
	}
	return substitute(t.tokens, vars), true
}

// Count reports the in-memory template count for a project (0 when the
// project has not been touched this process).
func (e *Extractor) Count(projectID int64) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.projects[projectID]; ok {
		return len(p.byID)
	}
	return 0
}

// load returns the project's in-memory state, loading persisted
// templates on first touch. Caller holds e.mu.
func (e *Extractor) load(ctx context.Context, projectID int64) (*project, error) {
	if p, ok := e.projects[projectID]; ok {
		return p, nil
	}
	rows, err := e.store.LoadTemplates(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("template: load project %d: %w", projectID, err)
	}
	p := &project{groups: map[string][]*tmpl{}, byID: map[int64]*tmpl{}}
	for _, r := range rows {
		t := &tmpl{id: r.ID, tokens: strings.Split(r.Text, " ")}
		k := groupKey(t.tokens)
		p.groups[k] = append(p.groups[k], t)
		p.byID[t.id] = t
	}
	e.projects[projectID] = p
	return p, nil
}

func groupKey(tokens []string) string {
	first := tokens[0]
	if strings.ContainsAny(first, "0123456789") {
		first = Wild // digit-bearing first tokens (timestamps, IPs) group together
	}
	return fmt.Sprintf("%d|%s", len(tokens), first)
}

// resolve picks the template for tokens: (existing, nil) when one
// already covers the line, (nil, tokensToMint) when a new template —
// exact or generalized under a NEW id (append-only) — is needed.
func resolve(candidates []*tmpl, tokens []string, simThresh float64) (*tmpl, []string) {
	var best *tmpl
	bestSim := 0.0
	for _, t := range candidates {
		if s := similarity(t.tokens, tokens); s > bestSim {
			best, bestSim = t, s
		}
	}
	if best == nil || bestSim < simThresh {
		return nil, append([]string(nil), tokens...)
	}
	if covers(best.tokens, tokens) {
		return best, nil
	}
	merged := append([]string(nil), best.tokens...)
	for i, tok := range tokens {
		if merged[i] != Wild && merged[i] != tok {
			merged[i] = Wild
		}
	}
	return nil, merged
}

// similarity is the fraction of positions where the template matches
// exactly or is already a wildcard. Callers guarantee equal lengths
// (groupKey buckets by token count).
func similarity(ttoks, tokens []string) float64 {
	same := 0
	for i, tok := range tokens {
		if ttoks[i] == Wild || ttoks[i] == tok {
			same++
		}
	}
	return float64(same) / float64(len(tokens))
}

func covers(ttoks, tokens []string) bool {
	for i, tok := range tokens {
		if ttoks[i] != Wild && ttoks[i] != tok {
			return false
		}
	}
	return true
}

func varsFor(ttoks, tokens []string) []string {
	var vars []string
	for i, tok := range ttoks {
		if tok == Wild {
			vars = append(vars, tokens[i])
		}
	}
	return vars
}

func substitute(ttoks, vars []string) string {
	out := make([]string, len(ttoks))
	vi := 0
	for i, tok := range ttoks {
		if tok == Wild {
			if vi >= len(vars) {
				return ""
			}
			out[i] = vars[vi]
			vi++
		} else {
			out[i] = tok
		}
	}
	return strings.Join(out, " ")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/template/ -v`
Expected: PASS, all 6 tests.

- [ ] **Step 5: gofmt, vet, full suite, commit**

Run: `gofmt -l . && go vet ./internal/template/ && go test ./...`
Expected: no gofmt output, vet clean, all green.

```bash
git add internal/template/
git commit -m "feat(template): lossless append-only extractor with cap and persistence"
```

---

### Task 2: `internal/segment` — column encoders + zstd helpers

**Files:**
- Create: `internal/segment/colenc.go`, `internal/segment/zstd.go`
- Test: `internal/segment/colenc_test.go`

**Interfaces:**
- Consumes: `github.com/klauspost/compress/zstd` (existing dep).
- Produces (Task 3 depends on these exact signatures):
  - `func EncodeDeltaInt64(vals []int64) []byte` / `func DecodeDeltaInt64(b []byte, n int) ([]int64, error)` — zigzag varint deltas (handles non-monotonic input).
  - `func EncodeUvarints(vals []uint64) []byte` / `func DecodeUvarints(b []byte, n int) ([]uint64, error)`
  - `func EncodeStrings(vals []string) []byte` / `func DecodeStrings(b []byte, n int) ([]string, error)` — uvarint length-prefixed concat.
  - `func BuildDict(vals []string) (dict []string, refs []uint64)` — first-seen order; `func ApplyDict(dict []string, refs []uint64) ([]string, error)` (out-of-range ref → error).
  - `func Compress(b []byte) []byte` / `func Decompress(b []byte, rawLen int) ([]byte, error)` (shared zstd encoder/decoder, decoded length must equal rawLen).

- [ ] **Step 1: Write the failing test**

```go
package segment

import (
	"math"
	"reflect"
	"testing"
)

func TestDeltaInt64RoundTrip(t *testing.T) {
	cases := [][]int64{
		{},
		{0},
		{5, 5, 5},
		{100, 90, 105, -3, math.MaxInt64, math.MinInt64 + 1},
		{1755000000000000, 1755000000000123, 1755000000001000}, // epoch micros
	}
	for _, vals := range cases {
		got, err := DecodeDeltaInt64(EncodeDeltaInt64(vals), len(vals))
		if err != nil {
			t.Fatalf("%v: %v", vals, err)
		}
		if len(vals) == 0 && len(got) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, vals) {
			t.Errorf("round trip: got %v want %v", got, vals)
		}
	}
}

func TestDeltaInt64Corruption(t *testing.T) {
	enc := EncodeDeltaInt64([]int64{1, 2, 3})
	if _, err := DecodeDeltaInt64(enc[:len(enc)-1], 3); err == nil {
		t.Error("truncated column must error")
	}
	if _, err := DecodeDeltaInt64(append(enc, 0), 3); err == nil {
		t.Error("trailing bytes must error")
	}
}

func TestUvarintsAndStrings(t *testing.T) {
	u := []uint64{0, 1, math.MaxUint64, 300}
	gotU, err := DecodeUvarints(EncodeUvarints(u), len(u))
	if err != nil || !reflect.DeepEqual(gotU, u) {
		t.Errorf("uvarints: got %v err %v", gotU, err)
	}
	s := []string{"", "hello world", "nul\x00ok", "ünïcode…"}
	gotS, err := DecodeStrings(EncodeStrings(s), len(s))
	if err != nil || !reflect.DeepEqual(gotS, s) {
		t.Errorf("strings: got %q err %v", gotS, err)
	}
	if _, err := DecodeStrings([]byte{5, 'a'}, 1); err == nil {
		t.Error("short string data must error")
	}
}

func TestDict(t *testing.T) {
	vals := []string{"api", "web", "api", "api", "db", "web"}
	dict, refs := BuildDict(vals)
	if !reflect.DeepEqual(dict, []string{"api", "web", "db"}) {
		t.Errorf("dict = %v", dict)
	}
	back, err := ApplyDict(dict, refs)
	if err != nil || !reflect.DeepEqual(back, vals) {
		t.Errorf("apply: %v err %v", back, err)
	}
	if _, err := ApplyDict(dict, []uint64{99}); err == nil {
		t.Error("out-of-range ref must error")
	}
}

func TestZstdRoundTrip(t *testing.T) {
	raw := []byte("aaaaaaaaaabbbbbbbbbbaaaaaaaaaa repetitive log-ish content")
	comp := Compress(raw)
	back, err := Decompress(comp, len(raw))
	if err != nil || string(back) != string(raw) {
		t.Fatalf("round trip: err %v", err)
	}
	if _, err := Decompress(comp, len(raw)+1); err == nil {
		t.Error("wrong rawLen must error")
	}
	if _, err := Decompress([]byte("not zstd"), 10); err == nil {
		t.Error("garbage input must error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/segment/ -v`
Expected: FAIL (package does not exist).

- [ ] **Step 3: Write the implementations**

`internal/segment/colenc.go`:

```go
// Package segment implements the engine's immutable columnar log
// storage (spec §3): column encoders here, the on-disk file format in
// segment.go. Encoders are exact-inverse pairs; decoders validate
// length and reject trailing bytes so corruption fails loudly.
package segment

import (
	"encoding/binary"
	"fmt"
)

func zigzag(v int64) uint64  { return uint64((v << 1) ^ (v >> 63)) }
func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

func appendUvarint(dst []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	return append(dst, tmp[:binary.PutUvarint(tmp[:], v)]...)
}

// EncodeDeltaInt64 encodes vals as zigzag-varint deltas from the
// previous value (first value deltas from 0). Zigzag keeps decreasing
// sequences compact and correct.
func EncodeDeltaInt64(vals []int64) []byte {
	var out []byte
	prev := int64(0)
	for _, v := range vals {
		out = appendUvarint(out, zigzag(v-prev))
		prev = v
	}
	return out
}

func DecodeDeltaInt64(b []byte, n int) ([]int64, error) {
	out := make([]int64, 0, n)
	prev := int64(0)
	for i := 0; i < n; i++ {
		u, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("segment: short delta column at row %d/%d", i, n)
		}
		b = b[w:]
		prev += unzigzag(u)
		out = append(out, prev)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("segment: %d trailing bytes in delta column", len(b))
	}
	return out, nil
}

func EncodeUvarints(vals []uint64) []byte {
	var out []byte
	for _, v := range vals {
		out = appendUvarint(out, v)
	}
	return out
}

func DecodeUvarints(b []byte, n int) ([]uint64, error) {
	out := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		u, w := binary.Uvarint(b)
		if w <= 0 {
			return nil, fmt.Errorf("segment: short uvarint column at row %d/%d", i, n)
		}
		b = b[w:]
		out = append(out, u)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("segment: %d trailing bytes in uvarint column", len(b))
	}
	return out, nil
}

// EncodeStrings packs vals as uvarint(len) + bytes, concatenated.
func EncodeStrings(vals []string) []byte {
	var out []byte
	for _, s := range vals {
		out = appendUvarint(out, uint64(len(s)))
		out = append(out, s...)
	}
	return out
}

func DecodeStrings(b []byte, n int) ([]string, error) {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		l, w := binary.Uvarint(b)
		if w <= 0 || uint64(len(b)-w) < l {
			return nil, fmt.Errorf("segment: short string column at row %d/%d", i, n)
		}
		out = append(out, string(b[w:w+int(l)]))
		b = b[w+int(l):]
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("segment: %d trailing bytes in string column", len(b))
	}
	return out, nil
}

// BuildDict returns the unique values of vals in first-seen order plus
// one dict reference per input value.
func BuildDict(vals []string) ([]string, []uint64) {
	idx := map[string]uint64{}
	var dict []string
	refs := make([]uint64, 0, len(vals))
	for _, v := range vals {
		i, ok := idx[v]
		if !ok {
			i = uint64(len(dict))
			idx[v] = i
			dict = append(dict, v)
		}
		refs = append(refs, i)
	}
	return dict, refs
}

func ApplyDict(dict []string, refs []uint64) ([]string, error) {
	out := make([]string, 0, len(refs))
	for i, r := range refs {
		if r >= uint64(len(dict)) {
			return nil, fmt.Errorf("segment: dict ref %d out of range (dict size %d) at row %d", r, len(dict), i)
		}
		out = append(out, dict[r])
	}
	return out, nil
}
```

`internal/segment/zstd.go`:

```go
package segment

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Shared long-lived codec instances: zstd encoders are expensive to
// construct and safe for concurrent EncodeAll/DecodeAll use.
var (
	zenc *zstd.Encoder
	zdec *zstd.Decoder
)

func init() {
	var err error
	zenc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(fmt.Sprintf("segment: zstd encoder: %v", err))
	}
	zdec, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd decoder: %v", err))
	}
}

// Compress returns b zstd-compressed.
func Compress(b []byte) []byte { return zenc.EncodeAll(b, nil) }

// Decompress inflates b and verifies the decoded length equals rawLen —
// a mismatch means the block or its metadata is corrupt.
func Decompress(b []byte, rawLen int) ([]byte, error) {
	out, err := zdec.DecodeAll(b, make([]byte, 0, rawLen))
	if err != nil {
		return nil, fmt.Errorf("segment: zstd: %w", err)
	}
	if len(out) != rawLen {
		return nil, fmt.Errorf("segment: decompressed %d bytes, expected %d", len(out), rawLen)
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/segment/ -v`
Expected: PASS, all 5 tests.

- [ ] **Step 5: gofmt, vet, full suite, commit**

Run: `gofmt -l . && go vet ./internal/segment/ && go test ./...`

```bash
git add internal/segment/
git commit -m "feat(segment): column encoders and zstd helpers"
```

---

### Task 3: `internal/segment` — segment file writer/reader with footer + CRC

**Files:**
- Create: `internal/segment/segment.go`
- Test: `internal/segment/segment_test.go`

**Interfaces:**
- Consumes: Task 2's encoders (`EncodeDeltaInt64`, `EncodeUvarints`, `EncodeStrings`, `BuildDict`, `Compress` and inverses).
- Produces (Plan B's engine depends on these exact signatures):
  - `type Row struct { LogID, TsMicros int64; Severity int; TemplateID int64; Vars []string; Raw string; Service, Environment, Release, TraceID, Attrs string; IsEvent bool }` — `Raw` is set iff `TemplateID == 0`.
  - `type ColMeta struct { Name string; Offset, CompLen, RawLen int64; CRC uint32 }`
  - `type Footer struct { Version, Count int; MinTs, MaxTs, MinLogID, MaxLogID int64; TemplateIDs []int64; Services []string; Events int64; SeverityCounts map[string]int64; Columns []ColMeta }`
  - `func Write(path string, rows []Row) (Footer, error)` — sorts by TsMicros (stable), writes temp file, fsyncs, renames into place. Empty rows → error.
  - `func Open(path string) (Footer, error)` — footer only, CRC-verified.
  - `func Read(path string) (Footer, []Row, error)` — full decode, every column CRC-verified; corruption → error, never wrong data.

File layout (Version 1): `[column blocks…][footer JSON][uint32 footerLen][uint32 footerCRC][8-byte magic "AGSEG001"]`, all fixed-width integers little-endian. Column CRCs are IEEE CRC-32 over the *compressed* bytes.

- [ ] **Step 1: Write the failing test**

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/segment/ -run 'TestWrite|TestOpen|TestCorruption' -v`
Expected: FAIL (Write/Open/Read undefined).

- [ ] **Step 3: Write the implementation**

```go
package segment

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

// Magic trailer identifying a version-1 agenterr segment file.
const magic = "AGSEG001"

// Row is one log as stored in a segment. Raw is set iff TemplateID is
// the raw fallback (template.RawID == 0).
type Row struct {
	LogID      int64
	TsMicros   int64
	Severity   int
	TemplateID int64
	Vars       []string
	Raw        string
	Service    string
	Environment string
	Release    string
	TraceID    string
	Attrs      string // canonical JSON, dictionary-encoded on disk
	IsEvent    bool
}

// ColMeta locates and authenticates one compressed column block.
type ColMeta struct {
	Name    string
	Offset  int64
	CompLen int64
	RawLen  int64
	CRC     uint32 // IEEE CRC-32 over the compressed bytes
}

// Footer is the query planner's whole view of a segment (spec §3):
// pruning metadata plus column locations. Serialized as JSON before the
// fixed trailer.
type Footer struct {
	Version        int
	Count          int
	MinTs, MaxTs   int64
	MinLogID       int64
	MaxLogID       int64
	TemplateIDs    []int64
	Services       []string
	Events         int64
	SeverityCounts map[string]int64
	Columns        []ColMeta
}

// Write sorts rows stably by TsMicros, encodes the columns, and writes
// an immutable segment at path via temp-file + fsync + rename. The
// returned Footer is exactly what Open will read back.
func Write(path string, rows []Row) (Footer, error) {
	if len(rows) == 0 {
		return Footer{}, fmt.Errorf("segment: refusing to write empty segment")
	}
	sorted := append([]Row(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].TsMicros < sorted[j].TsMicros })

	foot := Footer{Version: 1, Count: len(sorted), SeverityCounts: map[string]int64{}}
	foot.MinTs, foot.MaxTs = sorted[0].TsMicros, sorted[len(sorted)-1].TsMicros
	foot.MinLogID, foot.MaxLogID = sorted[0].LogID, sorted[0].LogID

	n := len(sorted)
	logIDs := make([]int64, n)
	ts := make([]int64, n)
	sevs := make([]byte, n)
	tmplIDs := make([]uint64, n)
	nvars := make([]uint64, n)
	var vars []string
	var raws []string
	services := make([]string, n)
	envs := make([]string, n)
	rels := make([]string, n)
	traces := make([]string, n)
	attrs := make([]string, n)
	isEvent := make([]byte, n)

	tset := map[int64]bool{}
	sset := map[string]bool{}
	for i, r := range sorted {
		logIDs[i], ts[i] = r.LogID, r.TsMicros
		if r.LogID < foot.MinLogID {
			foot.MinLogID = r.LogID
		}
		if r.LogID > foot.MaxLogID {
			foot.MaxLogID = r.LogID
		}
		sevs[i] = byte(r.Severity)
		foot.SeverityCounts[fmt.Sprintf("%d", r.Severity)]++
		tmplIDs[i] = uint64(r.TemplateID)
		if r.TemplateID == 0 {
			raws = append(raws, r.Raw)
		} else if !tset[r.TemplateID] {
			tset[r.TemplateID] = true
			foot.TemplateIDs = append(foot.TemplateIDs, r.TemplateID)
		}
		nvars[i] = uint64(len(r.Vars))
		vars = append(vars, r.Vars...)
		services[i], envs[i], rels[i], traces[i], attrs[i] = r.Service, r.Environment, r.Release, r.TraceID, r.Attrs
		if !sset[r.Service] {
			sset[r.Service] = true
			foot.Services = append(foot.Services, r.Service)
		}
		if r.IsEvent {
			isEvent[i] = 1
			foot.Events++
		}
	}
	sort.Slice(foot.TemplateIDs, func(i, j int) bool { return foot.TemplateIDs[i] < foot.TemplateIDs[j] })
	sort.Strings(foot.Services)

	svcDict, svcRefs := BuildDict(services)
	envDict, envRefs := BuildDict(envs)
	relDict, relRefs := BuildDict(rels)
	attrDict, attrRefs := BuildDict(attrs)

	cols := []struct {
		name string
		raw  []byte
	}{
		{"log_id", EncodeDeltaInt64(logIDs)},
		{"ts", EncodeDeltaInt64(ts)},
		{"severity", sevs},
		{"template_id", EncodeUvarints(tmplIDs)},
		{"nvars", EncodeUvarints(nvars)},
		{"vars", EncodeStrings(vars)},
		{"raw", EncodeStrings(raws)},
		{"service_dict", EncodeStrings(svcDict)},
		{"service_refs", EncodeUvarints(svcRefs)},
		{"env_dict", EncodeStrings(envDict)},
		{"env_refs", EncodeUvarints(envRefs)},
		{"release_dict", EncodeStrings(relDict)},
		{"release_refs", EncodeUvarints(relRefs)},
		{"trace_id", EncodeStrings(traces)},
		{"attrs_dict", EncodeStrings(attrDict)},
		{"attrs_refs", EncodeUvarints(attrRefs)},
		{"is_event", isEvent},
	}

	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return Footer{}, fmt.Errorf("segment: create: %w", err)
	}
	defer func() { _ = f.Close(); _ = os.Remove(tmp) }()

	offset := int64(0)
	for _, c := range cols {
		comp := Compress(c.raw)
		if _, err := f.Write(comp); err != nil {
			return Footer{}, fmt.Errorf("segment: write column %s: %w", c.name, err)
		}
		foot.Columns = append(foot.Columns, ColMeta{
			Name: c.name, Offset: offset,
			CompLen: int64(len(comp)), RawLen: int64(len(c.raw)),
			CRC: crc32.ChecksumIEEE(comp),
		})
		offset += int64(len(comp))
	}

	fj, err := json.Marshal(foot)
	if err != nil {
		return Footer{}, fmt.Errorf("segment: marshal footer: %w", err)
	}
	trailer := make([]byte, 0, len(fj)+16)
	trailer = append(trailer, fj...)
	trailer = binary.LittleEndian.AppendUint32(trailer, uint32(len(fj)))
	trailer = binary.LittleEndian.AppendUint32(trailer, crc32.ChecksumIEEE(fj))
	trailer = append(trailer, magic...)
	if _, err := f.Write(trailer); err != nil {
		return Footer{}, fmt.Errorf("segment: write footer: %w", err)
	}
	if err := f.Sync(); err != nil {
		return Footer{}, fmt.Errorf("segment: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return Footer{}, fmt.Errorf("segment: close: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return Footer{}, fmt.Errorf("segment: rename: %w", err)
	}
	return foot, nil
}

// Open reads and CRC-verifies a segment's footer without touching the
// column data.
func Open(path string) (Footer, error) {
	f, err := os.Open(path)
	if err != nil {
		return Footer{}, fmt.Errorf("segment: open: %w", err)
	}
	defer func() { _ = f.Close() }()

	st, err := f.Stat()
	if err != nil {
		return Footer{}, fmt.Errorf("segment: stat: %w", err)
	}
	if st.Size() < 16 {
		return Footer{}, fmt.Errorf("segment: %s too small (%d bytes)", path, st.Size())
	}
	tail := make([]byte, 16)
	if _, err := f.ReadAt(tail, st.Size()-16); err != nil {
		return Footer{}, fmt.Errorf("segment: read trailer: %w", err)
	}
	if string(tail[8:]) != magic {
		return Footer{}, fmt.Errorf("segment: %s: bad magic", path)
	}
	flen := int64(binary.LittleEndian.Uint32(tail[:4]))
	fcrc := binary.LittleEndian.Uint32(tail[4:8])
	if st.Size() < 16+flen {
		return Footer{}, fmt.Errorf("segment: %s: footer length %d exceeds file", path, flen)
	}
	fj := make([]byte, flen)
	if _, err := f.ReadAt(fj, st.Size()-16-flen); err != nil {
		return Footer{}, fmt.Errorf("segment: read footer: %w", err)
	}
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

// Read fully decodes a segment. Every column block is CRC-verified;
// any mismatch or decode inconsistency is an error — a segment never
// yields wrong data silently.
func Read(path string) (Footer, []Row, error) {
	foot, err := Open(path)
	if err != nil {
		return Footer{}, nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return Footer{}, nil, fmt.Errorf("segment: open: %w", err)
	}
	defer func() { _ = f.Close() }()

	raw := map[string][]byte{}
	for _, c := range foot.Columns {
		comp := make([]byte, c.CompLen)
		if _, err := f.ReadAt(comp, c.Offset); err != nil {
			return Footer{}, nil, fmt.Errorf("segment: read column %s: %w", c.Name, err)
		}
		if crc32.ChecksumIEEE(comp) != c.CRC {
			return Footer{}, nil, fmt.Errorf("segment: column %s: CRC mismatch", c.Name)
		}
		raw[c.Name], err = Decompress(comp, int(c.RawLen))
		if err != nil {
			return Footer{}, nil, fmt.Errorf("segment: column %s: %w", c.Name, err)
		}
	}

	n := foot.Count
	logIDs, err := DecodeDeltaInt64(raw["log_id"], n)
	if err != nil {
		return Footer{}, nil, err
	}
	ts, err := DecodeDeltaInt64(raw["ts"], n)
	if err != nil {
		return Footer{}, nil, err
	}
	sevs := raw["severity"]
	isEvent := raw["is_event"]
	if len(sevs) != n || len(isEvent) != n {
		return Footer{}, nil, fmt.Errorf("segment: severity/is_event length mismatch")
	}
	tmplIDs, err := DecodeUvarints(raw["template_id"], n)
	if err != nil {
		return Footer{}, nil, err
	}
	nvars, err := DecodeUvarints(raw["nvars"], n)
	if err != nil {
		return Footer{}, nil, err
	}
	totalVars := 0
	nraw := 0
	for i := 0; i < n; i++ {
		totalVars += int(nvars[i])
		if tmplIDs[i] == 0 {
			nraw++
		}
	}
	vars, err := DecodeStrings(raw["vars"], totalVars)
	if err != nil {
		return Footer{}, nil, err
	}
	raws, err := DecodeStrings(raw["raw"], nraw)
	if err != nil {
		return Footer{}, nil, err
	}
	decodeDicted := func(dictCol, refCol string) ([]string, error) {
		refs, err := DecodeUvarints(raw[refCol], n)
		if err != nil {
			return nil, err
		}
		max := uint64(0)
		for _, r := range refs {
			if r > max {
				max = r
			}
		}
		dict, err := DecodeStrings(raw[dictCol], int(max)+1)
		if err != nil {
			return nil, err
		}
		return ApplyDict(dict, refs)
	}
	services, err := decodeDicted("service_dict", "service_refs")
	if err != nil {
		return Footer{}, nil, err
	}
	envs, err := decodeDicted("env_dict", "env_refs")
	if err != nil {
		return Footer{}, nil, err
	}
	rels, err := decodeDicted("release_dict", "release_refs")
	if err != nil {
		return Footer{}, nil, err
	}
	attrs, err := decodeDicted("attrs_dict", "attrs_refs")
	if err != nil {
		return Footer{}, nil, err
	}
	traces, err := DecodeStrings(raw["trace_id"], n)
	if err != nil {
		return Footer{}, nil, err
	}

	rows := make([]Row, n)
	vi, ri := 0, 0
	for i := 0; i < n; i++ {
		r := Row{
			LogID: logIDs[i], TsMicros: ts[i], Severity: int(sevs[i]),
			TemplateID: int64(tmplIDs[i]),
			Service:    services[i], Environment: envs[i], Release: rels[i],
			TraceID: traces[i], Attrs: attrs[i], IsEvent: isEvent[i] == 1,
		}
		if nv := int(nvars[i]); nv > 0 {
			r.Vars = append([]string(nil), vars[vi:vi+nv]...)
			vi += nv
		}
		if r.TemplateID == 0 {
			r.Raw = raws[ri]
			ri++
		}
		rows[i] = r
	}
	return foot, rows, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/segment/ -v`
Expected: PASS, all tests (including Task 2's).

- [ ] **Step 5: gofmt, gocyclo check, full suite, commit**

`Read` is near the gocyclo limit — if golangci-lint (or a local run of it) flags it, extract the column-decode section into an unexported helper before committing.

Run: `gofmt -l . && go vet ./internal/segment/ && go test ./...`

```bash
git add internal/segment/
git commit -m "feat(segment): immutable columnar segment files with CRC'd footer"
```

---

### Task 4: `internal/engine` — WAL + memtable

**Files:**
- Create: `internal/engine/wal.go`, `internal/engine/memtable.go`
- Test: `internal/engine/wal_test.go`, `internal/engine/memtable_test.go`

**Interfaces:**
- Consumes: `segment.Row` from Task 3.
- Produces (Plan B's flush loop and recovery depend on these exact signatures):
  - `func OpenWAL(path string) (*WAL, error)` — creates or appends.
  - `func (w *WAL) Append(rows []segment.Row) error` — buffered write, no fsync.
  - `func (w *WAL) Sync() error` — fsync (Plan B batches this on the ~100 ms window, spec §4).
  - `func (w *WAL) Reset() error` — truncate to empty after a successful flush + manifest commit.
  - `func (w *WAL) Close() error`
  - `func ReplayWAL(path string) ([]segment.Row, error)` — returns all intact records; a torn tail (partial record from a crash) ends replay WITHOUT error; a missing file returns `(nil, nil)`.
  - `type Memtable struct` with `NewMemtable() *Memtable`, `Append(rows []segment.Row)`, `Snapshot() []segment.Row` (copy, safe for concurrent readers), `Len() int`, `Reset()`.

WAL record layout: `[uint32 payloadLen][uint32 payloadCRC][payload JSON of segment.Row]`, little-endian, one record per row. JSON keeps replay debuggable (`jq` over the raw file works when lengths are stripped); volume is bounded by the flush window so encoding overhead is irrelevant here.

- [ ] **Step 1: Write the failing tests**

`internal/engine/wal_test.go`:

```go
package engine

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agenterr/agenterr/internal/segment"
)

func walRows(n int) []segment.Row {
	rows := make([]segment.Row, n)
	for i := range rows {
		rows[i] = segment.Row{
			LogID: int64(i + 1), TsMicros: 1755000000000000 + int64(i),
			Severity: 9, TemplateID: 3, Vars: []string{"a", "b"},
			Service: "api", Attrs: "{}",
		}
	}
	return rows
}

func TestWALAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := walRows(10)
	if err := w.Append(rows[:6]); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(rows[6:]); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Errorf("replay mismatch: got %d rows", len(got))
	}
}

func TestWALTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := OpenWAL(path)
	rows := walRows(5)
	if err := w.Append(rows); err != nil {
		t.Fatal(err)
	}
	_ = w.Sync()
	_ = w.Close()

	data, _ := os.ReadFile(path)
	// Chop mid-record: drop the last 3 bytes (inside record 5's payload).
	if err := os.WriteFile(path, data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("torn tail must not error: %v", err)
	}
	if !reflect.DeepEqual(got, rows[:4]) {
		t.Errorf("want first 4 intact rows, got %d", len(got))
	}

	// Corrupt CRC mid-file: also ends replay at the last good record.
	data2 := append([]byte(nil), data...)
	data2[10] ^= 0xff // inside record 1
	if err := os.WriteFile(path, data2, 0o644); err != nil {
		t.Fatal(err)
	}
	got2, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("corrupt record must not error: %v", err)
	}
	if len(got2) != 0 {
		t.Errorf("corrupt first record: want 0 rows, got %d", len(got2))
	}
}

func TestWALMissingFileAndReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	if got, err := ReplayWAL(path); err != nil || got != nil {
		t.Errorf("missing wal: got %v, %v", got, err)
	}
	w, _ := OpenWAL(path)
	_ = w.Append(walRows(3))
	_ = w.Sync()
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	if got, _ := ReplayWAL(path); len(got) != 0 {
		t.Errorf("after reset: want empty, got %d rows", len(got))
	}
}
```

`internal/engine/memtable_test.go`:

```go
package engine

import (
	"sync"
	"testing"

	"github.com/agenterr/agenterr/internal/segment"
)

func TestMemtableSnapshotIsolation(t *testing.T) {
	m := NewMemtable()
	m.Append(walRows(3))
	snap := m.Snapshot()
	if len(snap) != 3 || m.Len() != 3 {
		t.Fatalf("len: snap %d table %d", len(snap), m.Len())
	}
	snap[0].Service = "mutated"
	if m.Snapshot()[0].Service != "api" {
		t.Error("snapshot mutation leaked into memtable")
	}
	m.Reset()
	if m.Len() != 0 || len(snap) != 3 {
		t.Error("reset must not affect prior snapshots")
	}
}

func TestMemtableConcurrent(t *testing.T) {
	m := NewMemtable()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				m.Append([]segment.Row{{LogID: 1, Service: "api"}})
				_ = m.Snapshot()
				_ = m.Len()
			}
		}()
	}
	wg.Wait()
	if m.Len() != 800 {
		t.Errorf("len = %d want 800", m.Len())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/ -v`
Expected: FAIL (package does not exist).

- [ ] **Step 3: Write the implementations**

`internal/engine/wal.go`:

```go
// Package engine holds the template storage engine's write path
// primitives (spec §4): the WAL that makes acked logs crash-durable and
// the memtable that serves reads for not-yet-flushed rows. Plan B
// assembles these with the segment writer and manifest into the full
// store implementation.
package engine

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/agenterr/agenterr/internal/segment"
)

// WAL is an append-only crash log of rows accepted but not yet flushed
// into a segment. Records are length-prefixed, CRC'd JSON — one per
// row. Sync policy belongs to the caller (the engine batches fsyncs on
// its flush window); Append never fsyncs by itself.
type WAL struct {
	f  *os.File
	bw *bufio.Writer
}

// OpenWAL opens (creating if needed) the WAL at path for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}
	return &WAL{f: f, bw: bufio.NewWriter(f)}, nil
}

// Append writes one record per row to the WAL buffer.
func (w *WAL) Append(rows []segment.Row) error {
	for _, r := range rows {
		payload, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("engine: wal marshal: %w", err)
		}
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
		if _, err := w.bw.Write(hdr[:]); err != nil {
			return fmt.Errorf("engine: wal write: %w", err)
		}
		if _, err := w.bw.Write(payload); err != nil {
			return fmt.Errorf("engine: wal write: %w", err)
		}
	}
	return nil
}

// Sync flushes the buffer and fsyncs — after it returns, every appended
// record survives a crash.
func (w *WAL) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("engine: wal flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("engine: wal fsync: %w", err)
	}
	return nil
}

// Reset empties the WAL. Call only after its rows are durable in a
// segment AND the manifest records that segment.
func (w *WAL) Reset() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("engine: wal flush: %w", err)
	}
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("engine: wal truncate: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("engine: wal seek: %w", err)
	}
	return w.f.Sync()
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("engine: wal flush: %w", err)
	}
	return w.f.Close()
}

// ReplayWAL returns every intact record in the WAL at path. A torn tail
// — a partial record from a crash mid-write — ends replay silently:
// those bytes were never acked as durable. A missing file is an empty
// WAL. Only I/O failures on an existing file return an error.
func ReplayWAL(path string) ([]segment.Row, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("engine: open wal for replay: %w", err)
	}
	defer func() { _ = f.Close() }()

	br := bufio.NewReader(f)
	var rows []segment.Row
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			return rows, nil // clean EOF or torn header — replay ends
		}
		plen := binary.LittleEndian.Uint32(hdr[:4])
		want := binary.LittleEndian.Uint32(hdr[4:])
		payload := make([]byte, plen)
		if _, err := io.ReadFull(br, payload); err != nil {
			return rows, nil // torn payload
		}
		if crc32.ChecksumIEEE(payload) != want {
			return rows, nil // corrupt record — stop at last good one
		}
		var r segment.Row
		if err := json.Unmarshal(payload, &r); err != nil {
			return rows, nil // undecodable — treat as torn
		}
		rows = append(rows, r)
	}
}
```

`internal/engine/memtable.go`:

```go
package engine

import (
	"sync"

	"github.com/agenterr/agenterr/internal/segment"
)

// Memtable holds rows accepted but not yet flushed to a segment. Reads
// over recent data (the last flush window) come from here. Safe for
// concurrent use; Snapshot returns an independent copy so readers are
// never affected by a concurrent Reset or Append.
type Memtable struct {
	mu   sync.RWMutex
	rows []segment.Row
}

// NewMemtable returns an empty Memtable.
func NewMemtable() *Memtable { return &Memtable{} }

// Append adds rows.
func (m *Memtable) Append(rows []segment.Row) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = append(m.rows, rows...)
}

// Snapshot returns a copy of the current rows.
func (m *Memtable) Snapshot() []segment.Row {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]segment.Row(nil), m.rows...)
}

// Len reports the current row count.
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rows)
}

// Reset empties the memtable (after its rows are flushed durably).
func (m *Memtable) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows = nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/engine/ -v -race`
Expected: PASS including the concurrent memtable test under `-race`.

- [ ] **Step 5: gofmt, vet, full suite, commit**

Run: `gofmt -l . && go vet ./internal/engine/ && go test ./...`

```bash
git add internal/engine/
git commit -m "feat(engine): WAL with torn-tail-tolerant replay, concurrent memtable"
```

---

## After this plan

Plan B ("engine assembly", written once this plan lands) covers: SQLite migration for `templates` + `segment manifest` + `rollups` tables, the flush loop (64k rows / 5 min / fsync window), engine recovery on startup, the `store.Store` implementation (WriteBatch/SearchLogs/LogContext/Stats/ServiceCounts/Prune over memtable + segments, metadata delegated to the existing sqlite package), `storetest` green against the assembled engine, app wiring, and deletion of the `logs` table + FTS5. The Snapshot-mutation and torn-tail semantics established here are relied on there — do not weaken them.

## Self-review notes

- **Spec coverage (this plan's slice):** §2 fully (lossless extract, append-only, cap, raw fallback, verified round trip); §3's format (columns, dict/delta encodings, footer with pruning metadata, CRC everywhere, immutable temp+rename writes); §4's primitives (WAL semantics, memtable) — the flush *loop* and recovery are §4 items deliberately deferred to Plan B where the components meet.
- **Type consistency:** `segment.Row` field set matches every `core.Log` field the store must reproduce (ID, Time→TsMicros, Severity, Body→TemplateID/Vars/Raw, Service, Environment, Release, TraceID, Attrs) plus `IsEvent` for footer stats; `template.Store` names match the Plan-B SQLite table task; WAL/memtable use `segment.Row` throughout.
- **Placeholder scan:** clean — every step carries complete code.
