// Command benchvso2 is the head-to-head harness behind the "vs
// OpenObserve" numbers (spec §7): it ingests the SAME corpus into
// agenterr's real storage engine (in-process) and into a local
// OpenObserve (HTTP), then times the same three queries on both sides.
//
// Modes:
//
//	-mode agenterr  -corpus F -dir D      ingest+flush+compact, sizes, query timings
//	-mode o2-ingest -corpus F -o2 URL     batch-ingest corpus into o2 stream "bench"
//	-mode o2-query  -o2 URL               query timings against o2
//
// Fairness note: agenterr is measured through in-process store calls,
// o2 through localhost HTTP (its only interface); localhost HTTP adds
// well under 1ms, negligible at the magnitudes reported.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/normalize"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/enginestore"
)

type rec struct {
	Ts       string `json:"ts"`
	Severity int    `json:"severity"`
	Service  string `json:"service"`
	Body     string `json:"body"`
	Attrs    string `json:"attrs"`
}

// The corpus day (UTC) — queries on both sides use this window.
var (
	dayStart = time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	dayEnd   = time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
)

const reps = 20

func main() {
	mode := flag.String("mode", "", "agenterr | o2-ingest | o2-query")
	corpus := flag.String("corpus", "", "corpus JSON (sqlite3 -json export)")
	dir := flag.String("dir", "", "agenterr work dir")
	o2 := flag.String("o2", "http://localhost:5080", "o2 base URL")
	flag.Parse()
	switch *mode {
	case "agenterr":
		runAgenterr(*corpus, *dir)
	case "o2-ingest":
		runO2Ingest(*corpus, *o2)
	case "o2-query":
		runO2Query(*o2)
	default:
		log.Fatal("unknown -mode")
	}
}

func loadCorpus(path string) []rec {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	var recs []rec
	if err := json.Unmarshal(data, &recs); err != nil {
		log.Fatal(err)
	}
	return recs
}

func timeIt(name string, f func() int) {
	f() // warm
	durs := make([]time.Duration, 0, reps)
	n := 0
	for i := 0; i < reps; i++ {
		t0 := time.Now()
		n = f()
		durs = append(durs, time.Since(t0))
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	fmt.Printf("%-28s p50=%-10s min=%-10s (result rows/groups: %d)\n",
		name, durs[reps/2].Round(10*time.Microsecond), durs[0].Round(10*time.Microsecond), n)
}

// ---- agenterr side ----

func runAgenterr(corpus, dir string) {
	recs := loadCorpus(corpus)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	s, err := enginestore.Open(filepath.Join(dir, "agenterr.db"), enginestore.Options{CompactEvery: -1})
	if err != nil {
		log.Fatal(err)
	}
	p, err := s.CreateProject(ctx, "bench", 30)
	if err != nil {
		log.Fatal(err)
	}

	ingestAgenterr(ctx, s, p.ID, recs)
	queryAgenterr(ctx, s, p.ID)

	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
	reportAgenterrDisk(dir, len(recs))
}

func ingestAgenterr(ctx context.Context, s *enginestore.Store, projectID int64, recs []rec) {
	t0 := time.Now()
	batch := make([]store.Entry, 0, 500)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if _, err := s.WriteBatch(ctx, batch); err != nil {
			log.Fatal(err)
		}
		batch = batch[:0]
	}
	for _, r := range recs {
		t, err := time.Parse(time.RFC3339Nano, r.Ts)
		if err != nil {
			t = time.Unix(0, 0)
		}
		var attrs map[string]string
		_ = json.Unmarshal([]byte(r.Attrs), &attrs)
		body, _ := normalize.StripANSI(r.Body)
		l := core.Log{ProjectID: projectID, Time: t, Severity: core.Severity(r.Severity),
			Body: body, Service: r.Service, Attrs: attrs}
		e := store.Entry{Log: l}
		if core.IsEvent(l) {
			e.IsEvent = true
			e.Fingerprint = core.Fingerprint(l)
			e.Title = core.Title(l)
		}
		batch = append(batch, e)
		if len(batch) == 500 {
			flush()
		}
	}
	flush()
	ingest := time.Since(t0)
	if err := s.FlushAll(); err != nil {
		log.Fatal(err)
	}
	segsBefore, _ := s.Segments(ctx, projectID)
	tC := time.Now()
	if err := s.CompactAll(ctx); err != nil {
		log.Fatal(err)
	}
	compactDur := time.Since(tC)
	segsAfter, _ := s.Segments(ctx, projectID)

	fmt.Printf("== agenterr (real engine, in-process) ==\n")
	fmt.Printf("ingest: %d logs in %s (%.0f logs/s)\n", len(recs), ingest.Round(time.Millisecond), float64(len(recs))/ingest.Seconds())
	fmt.Printf("segments: %d -> %d after compaction (%s)\n", len(segsBefore), len(segsAfter), compactDur.Round(time.Millisecond))
}

func queryAgenterr(ctx context.Context, s *enginestore.Store, projectID int64) {
	timeIt("Q1 scoped substring search", func() int {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: projectID, Service: "connect_api",
			Query: "record not found", Since: dayStart, Until: dayEnd, Limit: 50})
		if err != nil {
			log.Fatal(err)
		}
		return len(logs)
	})
	timeIt("Q2 unscoped substring search", func() int {
		logs, err := s.SearchLogs(ctx, store.LogFilter{ProjectID: projectID,
			Query: "invalid signature", Since: dayStart, Until: dayEnd, Limit: 50})
		if err != nil {
			log.Fatal(err)
		}
		return len(logs)
	})
	timeIt("Q3 aggregate by service", func() int {
		rows, err := s.Aggregate(ctx, store.AggregateFilter{ProjectID: projectID,
			Since: dayStart, Until: dayEnd, GroupBy: "service"})
		if err != nil {
			log.Fatal(err)
		}
		return len(rows)
	})
}

func reportAgenterrDisk(dir string, nrecs int) {
	var db, seg int64
	sum := func(root string, out *int64) {
		_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return nil
			}
			if i, err := d.Info(); err == nil {
				*out += i.Size()
			}
			return nil
		})
	}
	sum(filepath.Join(dir, "engine"), &seg)
	for _, suf := range []string{"", "-wal", "-shm"} {
		if st, err := os.Stat(filepath.Join(dir, "agenterr.db"+suf)); err == nil {
			db += st.Size()
		}
	}
	total := db + seg
	fmt.Printf("disk: engine=%d B, metadata=%d B, TOTAL=%d B -> %.1f B/record\n",
		seg, db, total, float64(total)/float64(nrecs))
}

// ---- o2 side ----

func o2req(method, url, user, pass string, body []byte) []byte {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		log.Fatal(err)
	}
	req.SetBasicAuth(user, pass)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		log.Fatalf("%s %s -> %d: %s", method, url, resp.StatusCode, out[:min(len(out), 300)])
	}
	return out
}

const (
	o2User = "bench@bench.local"
	o2Pass = "BenchPass123!"
)

func runO2Ingest(corpus, base string) {
	recs := loadCorpus(corpus)
	t0 := time.Now()
	const bs = 5000
	for i := 0; i < len(recs); i += bs {
		end := min(i+bs, len(recs))
		rows := make([]map[string]any, 0, bs)
		for _, r := range recs[i:end] {
			t, err := time.Parse(time.RFC3339Nano, r.Ts)
			if err != nil {
				t = time.Unix(0, 0)
			}
			body, _ := normalize.StripANSI(r.Body) // same normalization both sides
			rows = append(rows, map[string]any{
				"_timestamp": t.UnixMicro(), "service": r.Service,
				"severity": r.Severity, "body": body,
			})
		}
		payload, _ := json.Marshal(rows)
		o2req("POST", base+"/api/default/bench/_json", o2User, o2Pass, payload)
	}
	fmt.Printf("o2 ingest: %d logs in %s (%.0f logs/s)\n",
		len(recs), time.Since(t0).Round(time.Millisecond), float64(len(recs))/time.Since(t0).Seconds())
}

func o2search(base, sql string) int {
	q := map[string]any{"query": map[string]any{
		"sql": sql, "start_time": dayStart.UnixMicro(), "end_time": dayEnd.UnixMicro(),
		"from": 0, "size": 50,
	}}
	payload, _ := json.Marshal(q)
	// use_cache=false: o2's result cache otherwise flatters repeated
	// identical queries ~4-10x — the fairness note in the report depends
	// on this being enforced here, not remembered manually.
	out := o2req("POST", base+"/api/default/_search?use_cache=false", o2User, o2Pass, payload)
	var resp struct {
		Hits []any `json:"hits"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		log.Fatalf("o2 search parse: %v: %s", err, out[:min(len(out), 200)])
	}
	return len(resp.Hits)
}

func runO2Query(base string) {
	fmt.Printf("== OpenObserve (docker, localhost HTTP) ==\n")
	timeIt("Q1 scoped substring search", func() int {
		return o2search(base, `SELECT body FROM "bench" WHERE service = 'connect_api' AND body LIKE '%record not found%'`)
	})
	timeIt("Q2 unscoped substring search", func() int {
		return o2search(base, `SELECT body FROM "bench" WHERE body LIKE '%invalid signature%'`)
	})
	timeIt("Q3 aggregate by service", func() int {
		return o2search(base, `SELECT service, COUNT(*) as c FROM "bench" GROUP BY service`)
	})
}
