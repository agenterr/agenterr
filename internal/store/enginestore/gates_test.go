//go:build benchgates

package enginestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// TestSpecGates is the automated form of spec §7's performance targets. It
// builds a synthetic corpus that mirrors the real corpus's shape (~99%
// templated rows, ~0.7% rare marker rows, ~0.3% raw-fallback oddballs) and
// asserts each gate. The real production corpus is confidential and
// local-only, so this test never touches it — it exists to catch
// regressions in the fast-reader path, not to reproduce the exact real
// numbers (see docs/superpowers/specs/2026-08-16-bench-vs-o2-report.md for
// those; cmd/benchvso2 runs the real head-to-head manually).
func TestSpecGates(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, Options{CompactEvery: -1})
	ctx := context.Background()

	p, err := s.CreateProject(ctx, "gates", 30)
	if err != nil {
		t.Fatal(err)
	}

	const n = 300_000
	base := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	spacing := 24 * time.Hour / time.Duration(n)

	// Rows are one of exactly three kinds, chosen by index; kind alone
	// decides severity, so it's obvious which rows are Error vs Info:
	//   - marker rows (i%139==0, ~0.7%):   always Error
	//   - oddball rows (i%333==0, ~0.3%):  always Info
	//   - templated rows (the rest, ~99%): Error every 7th row, else Info
	// genBody returns the exact body the fidelity check below searches for
	// later, so it can assert byte-identical round-trips through SearchLogs.
	genBody := func(i int) string {
		switch {
		case i%139 == 0:
			// ~0.7% rare marker rows, scattered across services via a
			// modulus (139) coprime with the 10-service cycle so every
			// service sees hits.
			return fmt.Sprintf("record not found for id %d", i)
		case i%333 == 0:
			// ~0.3% raw-ish oddballs: multiline bodies that force the raw
			// fallback path instead of the templated fast path.
			return fmt.Sprintf("line1\nline2 trace=%d", i)
		default:
			// ~99% templated rows from ~15 distinct format strings, varying
			// a numeric id and a duration/status var per row.
			switch i % 15 {
			case 0:
				return fmt.Sprintf("request handled path=/api/v1/items/%d status=200 dur=%dms", i, i%97)
			case 1:
				return fmt.Sprintf("request handled path=/api/v1/orders/%d status=201 dur=%dms", i, i%83)
			case 2:
				return fmt.Sprintf("request handled path=/api/v1/users/%d status=200 dur=%dms", i, i%71)
			case 3:
				return fmt.Sprintf("db query rows=%d elapsed=%dms table=items", i%500, i%59)
			case 4:
				return fmt.Sprintf("db query rows=%d elapsed=%dms table=orders", i%500, i%61)
			case 5:
				return fmt.Sprintf("cache hit key=item:%d ttl=%ds", i, i%43)
			case 6:
				return fmt.Sprintf("cache miss key=item:%d lookup=%dms", i, i%37)
			case 7:
				return fmt.Sprintf("queue message id=%d processed in %dms", i, i%29)
			case 8:
				return fmt.Sprintf("http client call target=payments status=200 dur=%dms attempt=%d", i%53, i%3)
			case 9:
				return fmt.Sprintf("http client call target=inventory status=200 dur=%dms attempt=%d", i%47, i%3)
			case 10:
				return fmt.Sprintf("worker job id=%d completed dur=%dms", i, i%89)
			case 11:
				return fmt.Sprintf("auth token validated user=%d latency=%dms", i, i%19)
			case 12:
				return fmt.Sprintf("rate limit check key=%d remaining=%d", i, i%1000)
			case 13:
				return fmt.Sprintf("session refresh user=%d expires_in=%ds", i, i%3600)
			default:
				return fmt.Sprintf("metric emitted name=requests_total value=%d service_idx=%d", i, i%10)
			}
		}
	}
	genSeverity := func(i int) core.Severity {
		switch {
		case i%139 == 0:
			return core.SeverityError // marker rows: always Error
		case i%333 == 0:
			return core.SeverityInfo // oddball rows: always Info
		case i%7 == 0:
			return core.SeverityError // templated rows: Error every 7th
		default:
			return core.SeverityInfo // templated rows: Info otherwise
		}
	}

	batch := make([]store.Entry, 0, 1000)
	for i := 0; i < n; i++ {
		svc := fmt.Sprintf("svc%d", i%10)
		at := base.Add(time.Duration(i) * spacing)
		batch = append(batch, store.Entry{Log: core.Log{
			ProjectID: p.ID, Time: at, Severity: genSeverity(i), Body: genBody(i), Service: svc,
		}})
		if len(batch) == 1000 {
			if _, err := s.WriteBatch(ctx, batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := s.WriteBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}

	since := base.Add(-time.Hour)
	until := base.Add(25 * time.Hour)

	// --- Fidelity: byte-exact round-trips through search, and proof the
	// raw-fallback path was actually exercised (not just claimed by the
	// generator's ~0.3% oddball share). This is a correctness gate, not a
	// performance one: the storage gate below only proxies that template
	// extraction happened at all, not that reads reproduce bytes exactly. ---
	fidelityCases := []struct {
		name string
		i    int
	}{
		{"templated", 15}, // i%15==0, not a marker (i%139) or oddball (i%333)
		{"marker", 139},   // i%139==0
		{"oddball", 333},  // i%333==0, multiline body forces raw fallback
	}
	for _, c := range fidelityCases {
		want := genBody(c.i)
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: want, Since: since, Until: until, Limit: 50})
		if err != nil {
			t.Fatalf("fidelity %s: search error: %v", c.name, err)
		}
		found := false
		for _, l := range logs {
			if l.Body == want {
				found = true
				break
			}
		}
		if !found {
			got := "<no results>"
			if len(logs) > 0 {
				got = logs[0].Body
			}
			t.Errorf("fidelity %s: no byte-identical round-trip; got body = %q, want = %q", c.name, got, want)
		}
	}

	var rawRows int64
	segs, err := s.Segments(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		rawRows += seg.RawRows
	}
	t.Logf("raw rows across segments = %d", rawRows)
	if rawRows == 0 {
		t.Errorf("raw-fallback path not exercised: segments report 0 raw rows, want > 0")
	}

	// --- Storage: bytes/record over the whole store dir. ---
	total, err := dirSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	bytesPerRec := float64(total) / float64(n)
	t.Logf("storage = %.2f B/rec (gate <= 100)", bytesPerRec)
	if bytesPerRec > 100 {
		t.Errorf("storage = %.2f B/rec exceeds 100 B/rec gate", bytesPerRec)
	}

	// --- Aggregate by service. ---
	aggFn := func() time.Duration {
		start := time.Now()
		if _, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: p.ID, Since: since, Until: until, GroupBy: "service"}); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	aggFn() // warm-up: not timed
	aggMs := p50(9, aggFn)
	t.Logf("aggregate p50 = %s (gate <= 5ms)", aggMs)
	if aggMs > 5*time.Millisecond {
		t.Errorf("aggregate p50 = %s exceeds 5ms gate", aggMs)
	}

	// --- Scoped substring search (one service, full day). ---
	scopedFn := func() time.Duration {
		start := time.Now()
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Service: "svc3", Query: "record not found", Since: since, Until: until})
		if err != nil {
			t.Fatal(err)
		}
		if len(logs) == 0 {
			t.Fatal("scoped search: expected hits")
		}
		return time.Since(start)
	}
	scopedFn() // warm-up: not timed
	scopedMs := p50(9, scopedFn)
	t.Logf("scoped search p50 = %s (gate <= 50ms)", scopedMs)
	if scopedMs > 50*time.Millisecond {
		t.Errorf("scoped search p50 = %s exceeds 50ms gate", scopedMs)
	}

	// --- Unscoped substring search (no service, full day, scaled gate). ---
	unscopedFn := func() time.Duration {
		start := time.Now()
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: p.ID, Query: "zzz never present", Since: since, Until: until})
		if err != nil {
			t.Fatal(err)
		}
		if len(logs) != 0 {
			t.Fatal("unscoped search: expected no hits")
		}
		return time.Since(start)
	}
	unscopedFn() // warm-up: not timed
	unscopedMs := p50(9, unscopedFn)
	t.Logf("unscoped search p50 = %s (gate <= 500ms)", unscopedMs)
	if unscopedMs > 500*time.Millisecond {
		t.Errorf("unscoped search p50 = %s exceeds 500ms gate", unscopedMs)
	}
}

// p50 runs fn n times and returns the median duration (n must be odd).
func p50(n int, fn func() time.Duration) time.Duration {
	durs := make([]time.Duration, n)
	for i := 0; i < n; i++ {
		durs[i] = fn()
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	return durs[n/2]
}

// dirSize sums the size of every regular file under root, recursively.
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}
