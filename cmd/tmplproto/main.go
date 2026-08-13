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
