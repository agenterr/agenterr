// Package segment implements the engine's immutable columnar log
// storage (spec §3): column encoders here, the on-disk file format in
// segment.go. Encoders are exact-inverse pairs; decoders validate
// length and reject trailing bytes so corruption fails loudly.
package segment

import (
	"encoding/binary"
	"fmt"
)

func zigzag(v int64) uint64   { return uint64((v << 1) ^ (v >> 63)) }
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

// DecodeDeltaInt64 reverses EncodeDeltaInt64, decoding n values.
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

// EncodeUvarints encodes vals as uvarints, concatenated.
func EncodeUvarints(vals []uint64) []byte {
	var out []byte
	for _, v := range vals {
		out = appendUvarint(out, v)
	}
	return out
}

// DecodeUvarints reverses EncodeUvarints, decoding n values.
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

// DecodeStrings reverses EncodeStrings, decoding n values.
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

// ApplyDict expands refs using dict, validating all refs are in range.
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
