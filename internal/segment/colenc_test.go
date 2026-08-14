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
