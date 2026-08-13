# Normalize Stage + Template Prototype (Step-0 Gate) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the normalize stage (ANSI stripping + red hint) into the live pipeline, and build the Step-0 Drain prototype that measures templating rate and bytes/record on real trial logs — the gate for the template storage engine build.

**Architecture:** A new `internal/normalize` package strips ANSI escapes at the top of `Pipeline.process`, before body parsing and severity detection. A throwaway-but-tested Drain extractor lives in `cmd/tmplproto` with an append-only template invariant (templates never mutate; generalization mints a new ID) so reconstruction is valid forever — the same invariant the production engine will use. The CLI replays an exported day of trial logs through normalize + extract, simulates the spec's column encodings, zstd-compresses them, and prints pass/fail against the spec §7/§8 gates.

**Tech Stack:** Go (existing repo at `agenterr/`), `github.com/klauspost/compress/zstd` (new dep, prototype only for now — the engine will reuse it).

**Spec:** `docs/superpowers/specs/2026-08-12-template-storage-engine-design.md`. This plan covers spec §1 (normalize, minus severity rules) and §8 Step-0. Severity rules, the engine, query layer, and bench suite are later plans, written only after this plan's gate passes.

## Global Constraints

- Pure Go, no cgo (spec: build story is untouched).
- ANSI codes are stripped, never preserved; red hint (SGR 31/91) recorded as attr `ansi.red = "true"` (spec §1).
- Template extraction must satisfy `Reconstruct(Extract(b)) == b` byte-for-byte; failures fall back to raw (spec §2).
- Templates are append-only: an existing template's tokens never change after creation (spec §2 — required for reconstruction of already-stored logs).
- Gate thresholds (spec §8): templating rate ≥ 90% of corpus lines AND simulated storage ≤ 100 B/record. Below either → stop, report, redesign.
- All commits on a feature branch off main: `git checkout -b feat/normalize-and-tmplproto` before Task 1.

---

### Task 1: `internal/normalize` — StripANSI

**Files:**
- Create: `internal/normalize/normalize.go`
- Test: `internal/normalize/normalize_test.go`

**Interfaces:**
- Consumes: nothing (stdlib only).
- Produces: `func StripANSI(s string) (clean string, red bool)` — removes all CSI escape sequences (`ESC [ … final-byte`); `red` is true iff any SGR (`m`-final) sequence carried parameter `31` or `91` (optionally with modifiers, e.g. `31;1`). Non-CSI escapes (bare ESC + one byte, e.g. `ESC c`) are also removed. Tasks 2 and 4 call exactly this.

- [ ] **Step 1: Write the failing test**

```go
package normalize

import "testing"

func TestStripANSI(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  string
		red   bool
	}{
		{"no escapes fast path", "plain log line", "plain log line", false},
		// Real GORM line from the the-trial-customer trial (runbook, 2026-08-08).
		{"gorm red line",
			"2026/08/08 22:18:20 \x1b[31;1mgithub.com/acme/orders-api/internal/repositories/billing/invoice/repo.go:22 \x1b[35;1mrecord not found",
			"2026/08/08 22:18:20 github.com/acme/orders-api/internal/repositories/billing/invoice/repo.go:22 record not found",
			true},
		{"bright red 91", "\x1b[91merror\x1b[0m done", "error done", true},
		{"magenta only is not red", "\x1b[35;1mrecord not found", "record not found", false},
		{"31 must be a full param, not a substring", "\x1b[131mx\x1b[315my", "xy", false},
		{"non-SGR CSI stripped, no red", "\x1b[2Jcleared\x1b[10;20H", "cleared", false},
		{"unterminated escape at end dropped", "tail\x1b[31;1", "tail", false},
		{"bare ESC pair removed", "a\x1bcb", "ab", false},
		{"lone ESC at end removed", "x\x1b", "x", false},
		{"empty", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, red := StripANSI(tt.in)
			if got != tt.want {
				t.Errorf("clean = %q, want %q", got, tt.want)
			}
			if red != tt.red {
				t.Errorf("red = %v, want %v", red, tt.red)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/normalize/ -run TestStripANSI -v`
Expected: FAIL (compile error: `StripANSI` undefined).

- [ ] **Step 3: Write the implementation**

```go
// Package normalize cleans raw log bodies before anything downstream —
// body parsing, severity detection, fingerprinting, storage, search —
// ever sees them. Stripping happens exactly once, at the top of the
// pipeline, so every consumer works with the same clean bytes.
package normalize

import "strings"

// StripANSI removes ANSI escape sequences from s and reports whether any
// SGR sequence set the red or bright-red foreground (parameter 31 or 91).
// The red hint exists for the optional severity heuristic (spec §1);
// stripping itself is unconditional. CSI sequences (ESC '[' params
// final-byte in 0x40..0x7E) are removed whole; a bare ESC followed by a
// single non-'[' byte is removed as a pair; a trailing lone or
// unterminated escape is dropped to end of string. The escape bytes are
// deliberately not preserved anywhere — this is the system's one
// intentional data loss.
func StripANSI(s string) (string, bool) {
	if !strings.ContainsRune(s, 0x1b) {
		return s, false
	}
	var b strings.Builder
	b.Grow(len(s))
	red := false
	for i := 0; i < len(s); {
		if s[i] != 0x1b {
			b.WriteByte(s[i])
			i++
			continue
		}
		if i+1 >= len(s) { // lone ESC at end
			break
		}
		if s[i+1] != '[' { // bare ESC pair (e.g. ESC c)
			i += 2
			continue
		}
		j := i + 2
		for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
			j++
		}
		if j >= len(s) { // unterminated CSI: drop the rest
			break
		}
		if s[j] == 'm' && hasRedParam(s[i+2:j]) {
			red = true
		}
		i = j + 1
	}
	return b.String(), red
}

func hasRedParam(params string) bool {
	for _, p := range strings.Split(params, ";") {
		if p == "31" || p == "91" {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/normalize/ -v`
Expected: PASS, all cases.

- [ ] **Step 5: Commit**

```bash
git add internal/normalize/
git commit -m "feat(normalize): StripANSI with red-SGR hint"
```

---

### Task 2: Wire normalize into the pipeline

**Files:**
- Modify: `internal/pipeline/pipeline.go:234-244` (the `process` method)
- Test: `internal/pipeline/pipeline_test.go` (append new test; reuse existing `fakeWriter`, `eventually` helpers already defined at lines 17 and 98)

**Interfaces:**
- Consumes: `normalize.StripANSI` from Task 1.
- Produces: pipeline behavior — every stored log body is ANSI-free; logs whose body carried red SGR gain attr `ansi.red = "true"`. No signature changes.

- [ ] **Step 1: Write the failing test**

Append to `internal/pipeline/pipeline_test.go`:

```go
func TestProcessStripsANSI(t *testing.T) {
	w := &fakeWriter{}
	p := New(w, core.DefaultGrouper{}, NopNotifier{}, NopDropper{}, Options{FlushEvery: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	logs := []core.Log{
		{ProjectID: 1, Time: time.Now(), Severity: core.SeverityInfo,
			Body: "2026/08/08 22:18:20 \x1b[31;1mrepo.go:22 \x1b[35;1mrecord not found"},
		// Panic lift must fire even when ANSI precedes the prefix —
		// this is the case DetectPanicSeverity missed before normalize.
		{ProjectID: 1, Time: time.Now(), Severity: core.SeverityInfo,
			Body: "\x1b[31mpanic: boom"},
	}
	if err := p.Enqueue(logs); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	eventually(t, time.Second, func() bool { return w.totalEntries() == 2 })

	var entries []store.Entry
	for _, b := range w.snapshot() {
		entries = append(entries, b...)
	}
	if got := entries[0].Log.Body; got != "2026/08/08 22:18:20 repo.go:22 record not found" {
		t.Errorf("body not stripped: %q", got)
	}
	if entries[0].Log.Attrs["ansi.red"] != "true" {
		t.Errorf("ansi.red hint missing, attrs = %v", entries[0].Log.Attrs)
	}
	if entries[1].Log.Body != "panic: boom" {
		t.Errorf("panic body not stripped: %q", entries[1].Log.Body)
	}
	if entries[1].Log.Severity != core.SeverityFatal {
		t.Errorf("panic behind ANSI not lifted to FATAL, got %v", entries[1].Log.Severity)
	}
	if !entries[1].IsEvent {
		t.Error("stripped panic should be an event")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/pipeline/ -run TestProcessStripsANSI -v`
Expected: FAIL — body still contains `\x1b[31;1m`, severity stays INFO.

- [ ] **Step 3: Implement — normalize at the top of `process`**

In `internal/pipeline/pipeline.go`, add import `"github.com/agenterr/agenterr/internal/normalize"` and modify `process` (line 234) so stripping precedes `ParseStructuredBody` and `DetectPanicSeverity`:

```go
func (p *Pipeline) process(l core.Log) (store.Entry, bool) {
	// Normalize first: parsing, severity detection, rules, and
	// fingerprinting must all see clean bytes (spec §1). The red-SGR
	// hint is recorded for the future off-by-default severity
	// heuristic; it changes nothing today.
	if body, red := normalize.StripANSI(l.Body); body != l.Body {
		l.Body = body
		if red {
			if l.Attrs == nil {
				l.Attrs = map[string]string{}
			}
			l.Attrs["ansi.red"] = "true"
		}
	}
	if !p.o.DisableBodyParse && p.d.ParseBodies(l.ProjectID) {
		l = core.ParseStructuredBody(l)
	}
	l = core.DetectPanicSeverity(l)
	if drop, _ := p.d.Decide(l); drop {
		atomic.AddInt64(&p.unflushed, -1)
		return store.Entry{}, false
	}
	return p.annotate(l), true
}
```

- [ ] **Step 4: Run the full pipeline suite**

Run: `go test ./internal/pipeline/ -v`
Expected: PASS including all pre-existing tests (stripping is a no-op on ANSI-free bodies, so nothing else moves).

- [ ] **Step 5: Run the whole repo's tests**

Run: `go test ./...`
Expected: PASS. If any ingest/api test asserted a body containing ANSI bytes round-tripping unchanged, that test's expectation changes to the stripped form — that is the intended behavior change, update the expectation.

- [ ] **Step 6: Commit**

```bash
git add internal/pipeline/
git commit -m "feat(pipeline): strip ANSI escapes before parse/severity/fingerprint"
```

---

### Task 3: Drain extractor for the prototype

**Files:**
- Create: `cmd/tmplproto/drain.go`
- Test: `cmd/tmplproto/drain_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces (used by Task 4's `main.go`):
  - `func newDrain() *drain`
  - `func (d *drain) Extract(body string) (id int, vars []string, ok bool)` — `ok=false` means "store raw" (multiline, >200 tokens, or round-trip verification failed).
  - `func (d *drain) Reconstruct(id int, vars []string) (string, bool)`
  - `func (d *drain) TemplateCount() int`
  - `func (d *drain) TemplateBytes() int` — total bytes of all template texts (for the storage sim).

- [ ] **Step 1: Write the failing test**

```go
package main

import "testing"

// Real line shapes from the the-trial-customer trial corpus (runbook).
var sampleLines = []string{
	`203.0.113.10 - - [08/Aug/2026:22:26:49 +0000] "POST /webhooks/daily HTTP/2.0" 401 29 "-" "-" 87794 "orders-api@swarm" "http://10.0.0.10:8080" 1ms`,
	`203.0.113.11 - - [08/Aug/2026:21:59:01 +0000] "POST /api/webhooks/daily HTTP/2.0" 404 18 "-" "-" 87050 "orders-api@swarm" "http://10.0.0.11:8080" 1ms`,
	`2026/08/08 22:18:20 repo.go:25 record not found`,
	`2026/08/08 22:18:21 repo.go:22 record not found`,
	`{"time":"2026-08-08T22:03:37Z","level":"ERROR","msg":"request failed","err":"record not found"}`,
}

func TestExtractRoundTrip(t *testing.T) {
	d := newDrain()
	type stored struct {
		body string
		id   int
		vars []string
	}
	var all []stored
	// Feed everything twice so templates generalize (second pass hits
	// merged/wildcarded templates), then verify EVERY stored triple —
	// including ones extracted before later generalization — still
	// reconstructs byte-for-byte. This is the append-only invariant.
	for pass := 0; pass < 2; pass++ {
		for _, line := range sampleLines {
			id, vars, ok := d.Extract(line)
			if !ok {
				t.Fatalf("line failed to template: %q", line)
			}
			all = append(all, stored{line, id, vars})
		}
	}
	for _, s := range all {
		got, ok := d.Reconstruct(s.id, s.vars)
		if !ok || got != s.body {
			t.Errorf("round trip broke:\n got %q\nwant %q", got, s.body)
		}
	}
}

func TestSimilarLinesShareTemplate(t *testing.T) {
	d := newDrain()
	id1, _, _ := d.Extract(`2026/08/08 22:18:20 repo.go:25 record not found`)
	_, _, _ = d.Extract(`2026/08/08 22:18:21 repo.go:22 record not found`)
	// Third similar line must land on an existing (possibly newly
	// generalized) template rather than minting yet another one forever.
	id3, vars, ok := d.Extract(`2026/08/09 01:00:00 repo.go:99 record not found`)
	if !ok {
		t.Fatal("third line failed to template")
	}
	if id3 == 0 || len(vars) == 0 {
		t.Errorf("expected variable extraction, got id=%d vars=%v", id3, vars)
	}
	_ = id1
	if n := d.TemplateCount(); n > 3 {
		t.Errorf("template explosion: %d templates for 3 near-identical lines", n)
	}
}

func TestMultilineFallsBackToRaw(t *testing.T) {
	d := newDrain()
	if _, _, ok := d.Extract("panic: boom\ngoroutine 1 [running]:\nmain.main()"); ok {
		t.Error("multiline body must fall back to raw (ok=false)")
	}
}

func TestDoubleSpaceRoundTripsOrFallsBack(t *testing.T) {
	d := newDrain()
	body := "aligned    columns  here"
	id, vars, ok := d.Extract(body)
	if ok {
		if got, _ := d.Reconstruct(id, vars); got != body {
			t.Errorf("claimed ok but broke round trip: %q", got)
		}
	}
	// ok=false is acceptable — falling back to raw is correct, silently
	// corrupting spacing is not.
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/tmplproto/ -v`
Expected: FAIL (nothing defined yet).

- [ ] **Step 3: Implement the Drain extractor**

```go
// Drain-style online template extractor — prototype for the Step-0 gate.
// Deliberately simple: space tokenization, similarity grouping by
// (token count, first token), and APPEND-ONLY templates: generalizing an
// existing template mints a new template ID instead of mutating tokens
// in place, so any previously returned (id, vars) reconstructs forever.
// The production engine keeps this invariant (spec §2).
package main

import (
	"fmt"
	"strings"
)

const wild = "\x00" // wildcard slot marker; NUL cannot survive tokenization of sane logs

type tmpl struct {
	id     int
	tokens []string
}

type drain struct {
	groups    map[string][]*tmpl
	templates []*tmpl // index = id-1
	simThresh float64
}

func newDrain() *drain {
	return &drain{groups: map[string][]*tmpl{}, simThresh: 0.5}
}

func groupKey(tokens []string) string {
	first := tokens[0]
	if strings.ContainsAny(first, "0123456789") {
		first = wild // digit-bearing first tokens (timestamps, IPs) all group together
	}
	return fmt.Sprintf("%d|%s", len(tokens), first)
}

// similarity is the fraction of positions where the template token
// matches exactly or is already a wildcard.
func similarity(t *tmpl, tokens []string) float64 {
	same := 0
	for i, tok := range tokens {
		if t.tokens[i] == wild || t.tokens[i] == tok {
			same++
		}
	}
	return float64(same) / float64(len(tokens))
}

func (d *drain) newTemplate(tokens []string, key string) *tmpl {
	t := &tmpl{id: len(d.templates) + 1, tokens: append([]string(nil), tokens...)}
	d.templates = append(d.templates, t)
	d.groups[key] = append(d.groups[key], t)
	return t
}

func (d *drain) Extract(body string) (int, []string, bool) {
	if strings.ContainsRune(body, '\n') || strings.ContainsRune(body, '\x00') || body == "" {
		return 0, nil, false
	}
	tokens := strings.Split(body, " ")
	if len(tokens) > 200 {
		return 0, nil, false
	}
	key := groupKey(tokens)

	var best *tmpl
	bestSim := 0.0
	for _, t := range d.groups[key] {
		if s := similarity(t, tokens); s > bestSim {
			best, bestSim = t, s
		}
	}

	var target *tmpl
	switch {
	case best == nil || bestSim < d.simThresh:
		target = d.newTemplate(tokens, key) // exact template, zero vars
	default:
		mutate := false
		for i, tok := range tokens {
			if best.tokens[i] != wild && best.tokens[i] != tok {
				mutate = true
				break
			}
		}
		if !mutate {
			target = best
		} else {
			// Append-only: mint the generalized template as a NEW id.
			merged := append([]string(nil), best.tokens...)
			for i, tok := range tokens {
				if merged[i] != wild && merged[i] != tok {
					merged[i] = wild
				}
			}
			target = d.newTemplate(merged, key)
		}
	}

	var vars []string
	for i, tok := range target.tokens {
		if tok == wild {
			vars = append(vars, tokens[i])
		}
	}
	// Verify the invariant at extract time; a failed round trip means
	// tokenization lost information (e.g. double spaces) → raw fallback.
	if got, ok := d.Reconstruct(target.id, vars); !ok || got != body {
		return 0, nil, false
	}
	return target.id, vars, true
}

func (d *drain) Reconstruct(id int, vars []string) (string, bool) {
	if id < 1 || id > len(d.templates) {
		return "", false
	}
	t := d.templates[id-1]
	out := make([]string, len(t.tokens))
	vi := 0
	for i, tok := range t.tokens {
		if tok == wild {
			if vi >= len(vars) {
				return "", false
			}
			out[i] = vars[vi]
			vi++
		} else {
			out[i] = tok
		}
	}
	if vi != len(vars) {
		return "", false
	}
	return strings.Join(out, " "), true
}

func (d *drain) TemplateCount() int { return len(d.templates) }

func (d *drain) TemplateBytes() int {
	n := 0
	for _, t := range d.templates {
		n += len(strings.Join(t.tokens, " "))
	}
	return n
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/tmplproto/ -v`
Expected: PASS. Note `TestDoubleSpaceRoundTripsOrFallsBack` passes either way by design — `strings.Split(body, " ")` on `"aligned    columns"` produces empty-string tokens which rejoin losslessly, so it will likely template fine; the test guards the invariant, not the mechanism.

- [ ] **Step 5: Commit**

```bash
git add cmd/tmplproto/
git commit -m "feat(tmplproto): append-only Drain extractor with verified round trip"
```

---

### Task 4: Prototype CLI — corpus replay + storage simulation

**Files:**
- Create: `cmd/tmplproto/main.go`
- Modify: `go.mod` (add `github.com/klauspost/compress`)
- Test: covered by Task 3's tests plus a smoke run in Task 5 (the CLI is measurement scaffolding; its correctness-critical parts — StripANSI, Extract/Reconstruct — are already under test).

**Interfaces:**
- Consumes: `normalize.StripANSI` (Task 1), `drain` (Task 3).
- Produces: `go run ./cmd/tmplproto -corpus <file.json>` printing a gate report. Corpus file = JSON **array** of objects `{"ts": "...", "severity": N, "service": "...", "body": "...", "attrs": "..."}` — exactly what `sqlite3 -json` emits for the export query in Task 5.

- [ ] **Step 1: Add the zstd dependency**

```bash
go get github.com/klauspost/compress@latest
```

- [ ] **Step 2: Write the CLI**

```go
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/agenterr/agenterr/internal/normalize"
)

type rec struct {
	Ts       string `json:"ts"`
	Severity int    `json:"severity"`
	Service  string `json:"service"`
	Body     string `json:"body"`
	Attrs    string `json:"attrs"`
}

func main() {
	corpus := flag.String("corpus", "", "path to sqlite3 -json export of one day of logs")
	flag.Parse()
	if *corpus == "" {
		log.Fatal("usage: tmplproto -corpus day.json")
	}

	data, err := os.ReadFile(*corpus)
	if err != nil {
		log.Fatal(err)
	}
	var recs []rec
	if err := json.Unmarshal(data, &recs); err != nil {
		log.Fatalf("parse corpus: %v", err)
	}
	if len(recs) == 0 {
		log.Fatal("empty corpus")
	}

	d := newDrain()

	// Column buffers, mirroring the spec §3 segment layout.
	var (
		tsCol, idCol, svcCol, sevCol []byte
		varCol, rawCol               []byte
		attrRef                      []byte
		attrDict                     = map[string]int{}
		attrDictBuf                  []byte
		svcDict                      = map[string]int{}
		lastMicros                   int64
		templated, raw               int
	)
	appendUvarint := func(dst []byte, v uint64) []byte {
		var tmp [binary.MaxVarintLen64]byte
		return append(dst, tmp[:binary.PutUvarint(tmp[:], v)]...)
	}
	appendStr := func(dst []byte, s string) []byte {
		dst = appendUvarint(dst, uint64(len(s)))
		return append(dst, s...)
	}

	for _, r := range recs {
		body, _ := normalize.StripANSI(r.Body)

		t, err := time.Parse(time.RFC3339Nano, r.Ts)
		if err != nil {
			t = time.Unix(0, 0)
		}
		micros := t.UnixMicro()
		tsCol = appendUvarint(tsCol, uint64(micros-lastMicros)) // corpus is ts-ordered; deltas ≥ 0
		lastMicros = micros

		if _, ok := svcDict[r.Service]; !ok {
			svcDict[r.Service] = len(svcDict)
		}
		svcCol = appendUvarint(svcCol, uint64(svcDict[r.Service]))
		sevCol = append(sevCol, byte(r.Severity))

		if _, ok := attrDict[r.Attrs]; !ok {
			attrDict[r.Attrs] = len(attrDict)
			attrDictBuf = appendStr(attrDictBuf, r.Attrs)
		}
		attrRef = appendUvarint(attrRef, uint64(attrDict[r.Attrs]))

		if id, vars, ok := d.Extract(body); ok {
			templated++
			idCol = appendUvarint(idCol, uint64(id))
			varCol = appendUvarint(varCol, uint64(len(vars)))
			for _, v := range vars {
				varCol = appendStr(varCol, v)
			}
		} else {
			raw++
			idCol = appendUvarint(idCol, 0) // template_id 0 = raw
			rawCol = appendStr(rawCol, body)
		}
	}

	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	z := func(b []byte) int { return len(enc.EncodeAll(b, nil)) }

	n := len(recs)
	cols := []struct {
		name string
		raw  int
		zip  int
	}{
		{"ts (delta varint)", len(tsCol), z(tsCol)},
		{"template_id", len(idCol), z(idCol)},
		{"service", len(svcCol), z(svcCol)},
		{"severity", len(sevCol), z(sevCol)},
		{"variables", len(varCol), z(varCol)},
		{"raw bodies", len(rawCol), z(rawCol)},
		{"attrs refs", len(attrRef), z(attrRef)},
		{"attrs dict", len(attrDictBuf), z(attrDictBuf)},
	}
	total := d.TemplateBytes() // template table, stored once, uncompressed est.
	fmt.Printf("corpus: %d logs\n\n%-22s %12s %12s\n", n, "column", "raw B", "zstd B")
	for _, c := range cols {
		fmt.Printf("%-22s %12d %12d\n", c.name, c.raw, c.zip)
		total += c.zip
	}
	fmt.Printf("%-22s %12s %12d\n\n", "template table", "-", d.TemplateBytes())

	rate := 100 * float64(templated) / float64(n)
	bpr := float64(total) / float64(n)
	fmt.Printf("templates minted:      %d\n", d.TemplateCount())
	fmt.Printf("templating rate:       %.1f%%  (%d templated / %d raw)\n", rate, templated, raw)
	fmt.Printf("simulated storage:     %.1f B/record (%d B total)\n\n", bpr, total)

	pass := rate >= 90 && bpr <= 100
	fmt.Printf("GATE (spec §8): rate ≥ 90%% && B/rec ≤ 100 → %v\n", map[bool]string{true: "PASS", false: "FAIL"}[pass])
	if !pass {
		os.Exit(1)
	}
}
```

- [ ] **Step 3: Verify it builds and the package tests still pass**

Run: `go build ./cmd/tmplproto/ && go test ./cmd/tmplproto/ -v && go test ./...`
Expected: builds clean, all PASS.

- [ ] **Step 4: Smoke-test on a tiny synthetic corpus**

```bash
cat > /tmp/smoke.json <<'EOF'
[{"ts":"2026-08-11T00:00:01Z","severity":9,"service":"connect_api","body":"2026/08/08 22:18:20 [31;1mrepo.go:25 [35;1mrecord not found","attrs":"{}"},
 {"ts":"2026-08-11T00:00:02Z","severity":9,"service":"connect_api","body":"2026/08/08 22:18:21 [31;1mrepo.go:22 [35;1mrecord not found","attrs":"{}"}]
EOF
go run ./cmd/tmplproto -corpus /tmp/smoke.json
```

Expected: report prints, 2 logs, templating rate 100%, exit status irrelevant here (2-log B/rec is meaningless — the gate only counts on the real corpus).

- [ ] **Step 5: Commit**

```bash
git add cmd/tmplproto/main.go go.mod go.sum
git commit -m "feat(tmplproto): corpus replay CLI with column/zstd storage simulation"
```

---

### Task 5: Export the real corpus, run the gate, record the verdict

**Files:**
- Create: `docs/superpowers/specs/2026-08-12-step0-gate-report.md`

**Interfaces:**
- Consumes: the CLI from Task 4; the trial deployment's `agenterr.db` on the the-trial-customer box (tailnet).
- Produces: the gate verdict that unblocks (or stops) the engine plans.

- [ ] **Step 1: Export one full day of logs from the box**

On the box (via ssh; sqlite3 may need `sudo apt-get install -y sqlite3`):

```bash
sudo sqlite3 -json /var/lib/docker/volumes/agenterr_agenterr-data/_data/agenterr.db \
  "SELECT ts, severity, service, body, attrs FROM logs
   WHERE ts >= '2026-08-11T00:00:00' AND ts < '2026-08-12T00:00:00'
   ORDER BY ts" > /tmp/corpus-day.json
ls -la /tmp/corpus-day.json   # expect ~150-250 MB for ~330k logs
```

Reading the live WAL-mode DB concurrently with agenterr is safe (readers don't block the writer); run it once, not in a loop. Then from the workstation:

```bash
scp <box>:/tmp/corpus-day.json "$HOME/tmp-agenterr-corpus-day.json"
```

Do NOT commit the corpus — it is production log data. Keep it out of the repo tree.

- [ ] **Step 2: Run the gate**

```bash
go run ./cmd/tmplproto -corpus "$HOME/tmp-agenterr-corpus-day.json" | tee /tmp/step0-report.txt
```

- [ ] **Step 3: Write the gate report**

Create `docs/superpowers/specs/2026-08-12-step0-gate-report.md` containing: the run date, corpus window and log count, the full CLI output table verbatim, the two gate numbers against their thresholds, and the verdict line. Template:

```markdown
# Step-0 Gate Report — Template Prototype

**Run:** <date> against corpus <window>, <N> logs (the-trial-customer trial box).
**Spec:** 2026-08-12-template-storage-engine-design.md §8.

## CLI output

<paste verbatim>

## Verdict

- Templating rate: <X>% (threshold ≥ 90%) → <pass/fail>
- Simulated storage: <Y> B/record (threshold ≤ 100; o2 baseline 106) → <pass/fail>
- Templates minted: <N> (sanity: expected thousands, not tens of thousands)

**GATE: <PASS — proceed to engine plans / FAIL — stop, findings below>**

## Notes

<anything surprising: which services fell to raw, template counts per
service if investigated, double-space fallout, etc.>
```

- [ ] **Step 4: Commit the report**

```bash
git add docs/superpowers/specs/2026-08-12-step0-gate-report.md
git commit -m "docs: step-0 gate report (template prototype vs real trial corpus)"
```

- [ ] **Step 5: Act on the verdict**

- **PASS:** merge the branch to main (Tasks 1–2 are shippable product improvements regardless of the engine). Next: write the engine implementation plan (spec §2–§4), then query layer (§5–§6), then bench suite (§7 speed tests) — each via superpowers:writing-plans.
- **FAIL:** do not write engine plans. Record the dominant failure mode in the report (rate too low → which services; B/rec too high → which column dominates) and return to brainstorming with data. Tasks 1–2 still merge — ANSI stripping is correct independent of the verdict.

---

## Self-review notes

- **Spec coverage:** this plan deliberately covers spec §1 (minus severity rules — later plan, called out in header) and §8 Step-0 only; §2–§7 are explicitly deferred behind the gate. No silent gaps.
- **Type consistency:** `StripANSI` signature identical in Tasks 1, 2, 4; `drain` API identical in Tasks 3, 4; `ansi.red` attr key identical in Task 2 test and implementation.
- **Known simplification:** the storage simulation compresses whole-corpus columns rather than 64k-row blocks; real per-block compression will be slightly worse (~10–20%), which is why the gate threshold (100) keeps margin below the ~20–50 target band.
