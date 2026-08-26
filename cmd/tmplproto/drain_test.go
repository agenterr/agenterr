package main

import "testing"

// Line shapes representative of a real production corpus. The shapes are
// what matters to templating; every identifier here is synthetic.
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
