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
	LogID       int64
	TsMicros    int64
	Severity    int
	TemplateID  int64
	Vars        []string
	Raw         string
	Service     string
	Environment string
	Release     string
	TraceID     string
	Attrs       string // canonical JSON, dictionary-encoded on disk
	IsEvent     bool
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

// encodeRowsToColumns processes rows and produces column-encoded data and footer metadata.
func encodeRowsToColumns(sorted []Row, foot *Footer) (
	logIDs []int64, ts []int64, sevs []byte, tmplIDs []uint64, nvars []uint64,
	vars []string, raws []string,
	services []string, envs []string, rels []string, traces []string, attrs []string,
	isEvent []byte,
) {
	n := len(sorted)
	logIDs = make([]int64, n)
	ts = make([]int64, n)
	sevs = make([]byte, n)
	tmplIDs = make([]uint64, n)
	nvars = make([]uint64, n)
	services = make([]string, n)
	envs = make([]string, n)
	rels = make([]string, n)
	traces = make([]string, n)
	attrs = make([]string, n)
	isEvent = make([]byte, n)

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
	return
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

	// Validate that all severities are in byte range before encoding.
	for i, r := range sorted {
		if r.Severity < 0 || r.Severity > 255 {
			return Footer{}, fmt.Errorf("segment: row %d: severity %d out of byte range", i, r.Severity)
		}
	}

	logIDs, ts, sevs, tmplIDs, nvars, vars, raws, services, envs, rels, traces, attrs, isEvent := encodeRowsToColumns(sorted, &foot)

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

// decodeDictionaryColumn decodes a column that was dictionary-encoded.
func decodeDictionaryColumn(raw map[string][]byte, n int, dictCol, refCol string) ([]string, error) {
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

// decodeColumns decodes all column data from raw bytes. It returns the
// decoded column values in the order needed to reconstruct rows.
func decodeColumns(raw map[string][]byte, n int) (
	logIDs []int64, ts []int64, sevs, isEvent []byte, tmplIDs, nvars []uint64,
	vars, raws []string, services, envs, rels, attrs, traces []string, err error,
) {
	logIDs, err = DecodeDeltaInt64(raw["log_id"], n)
	if err != nil {
		return
	}
	ts, err = DecodeDeltaInt64(raw["ts"], n)
	if err != nil {
		return
	}
	sevs = raw["severity"]
	isEvent = raw["is_event"]
	if len(sevs) != n || len(isEvent) != n {
		err = fmt.Errorf("segment: severity/is_event length mismatch")
		return
	}
	tmplIDs, err = DecodeUvarints(raw["template_id"], n)
	if err != nil {
		return
	}
	nvars, err = DecodeUvarints(raw["nvars"], n)
	if err != nil {
		return
	}
	totalVars := 0
	nraw := 0
	for i := 0; i < n; i++ {
		totalVars += int(nvars[i])
		if tmplIDs[i] == 0 {
			nraw++
		}
	}
	vars, err = DecodeStrings(raw["vars"], totalVars)
	if err != nil {
		return
	}
	raws, err = DecodeStrings(raw["raw"], nraw)
	if err != nil {
		return
	}
	services, err = decodeDictionaryColumn(raw, n, "service_dict", "service_refs")
	if err != nil {
		return
	}
	envs, err = decodeDictionaryColumn(raw, n, "env_dict", "env_refs")
	if err != nil {
		return
	}
	rels, err = decodeDictionaryColumn(raw, n, "release_dict", "release_refs")
	if err != nil {
		return
	}
	attrs, err = decodeDictionaryColumn(raw, n, "attrs_dict", "attrs_refs")
	if err != nil {
		return
	}
	traces, err = DecodeStrings(raw["trace_id"], n)
	return
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
	logIDs, ts, sevs, isEvent, tmplIDs, nvars, vars, raws, services, envs, rels, attrs, traces, err := decodeColumns(raw, n)
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
