package segment

import (
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Shared long-lived codec instances: zstd encoders are expensive to
// construct and safe for concurrent EncodeAll/DecodeAll use.
var (
	zenc *zstd.Encoder
	zdec *zstd.Decoder
)

func init() {
	var err error
	zenc, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(fmt.Sprintf("segment: zstd encoder: %v", err))
	}
	zdec, err = zstd.NewReader(nil)
	if err != nil {
		panic(fmt.Sprintf("segment: zstd decoder: %v", err))
	}
}

// Compress returns b zstd-compressed.
func Compress(b []byte) []byte { return zenc.EncodeAll(b, nil) }

// Decompress inflates b and verifies the decoded length equals rawLen —
// a mismatch means the block or its metadata is corrupt.
func Decompress(b []byte, rawLen int) ([]byte, error) {
	out, err := zdec.DecodeAll(b, make([]byte, 0, rawLen))
	if err != nil {
		return nil, fmt.Errorf("segment: zstd: %w", err)
	}
	if len(out) != rawLen {
		return nil, fmt.Errorf("segment: decompressed %d bytes, expected %d", len(out), rawLen)
	}
	return out, nil
}
