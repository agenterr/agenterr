package segment

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
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
			return nil, fmt.Errorf("segment: short string column at row %d/%d", i, n)
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
func (d *dictCol) at(i int) string {
	return d.dict[d.refs[i]]
}

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
	flen := int64(binary.LittleEndian.Uint32(tail[:4]))
	fcrc := binary.LittleEndian.Uint32(tail[4:8])
	if int64(len(data)) < 16+flen {
		return Footer{}, fmt.Errorf("segment: %s: footer length %d exceeds file", path, flen)
	}
	fj := data[len(data)-16-int(flen) : len(data)-16]
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
func (sc *Scan) computeMatch(f ScanFilter) error { //nolint:gocyclo
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
