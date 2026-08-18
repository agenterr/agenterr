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
type dictCol struct { //nolint:unused
	dict []string
	refs []uint64
}

// at returns row i's value. Callers guarantee i is in range and refs
// were validated against dict at build time.
func (d *dictCol) at(i int) string { //nolint:unused
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
