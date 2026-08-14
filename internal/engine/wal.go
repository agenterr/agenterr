// Package engine holds the template storage engine's write path
// primitives (spec §4): the WAL that makes acked logs crash-durable and
// the memtable that serves reads for not-yet-flushed rows. Plan B
// assembles these with the segment writer and manifest into the full
// store implementation.
package engine

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/agenterr/agenterr/internal/segment"
)

// WAL is an append-only crash log of rows accepted but not yet flushed
// into a segment. Records are length-prefixed, CRC'd JSON — one per
// row. Sync policy belongs to the caller (the engine batches fsyncs on
// its flush window); Append never fsyncs by itself.
type WAL struct {
	f  *os.File
	bw *bufio.Writer
}

// OpenWAL opens (creating if needed) the WAL at path for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}
	return &WAL{f: f, bw: bufio.NewWriter(f)}, nil
}

// Append writes one record per row to the WAL buffer.
func (w *WAL) Append(rows []segment.Row) error {
	for _, r := range rows {
		payload, err := json.Marshal(r)
		if err != nil {
			return fmt.Errorf("engine: wal marshal: %w", err)
		}
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[:4], uint32(len(payload)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
		if _, err := w.bw.Write(hdr[:]); err != nil {
			return fmt.Errorf("engine: wal write: %w", err)
		}
		if _, err := w.bw.Write(payload); err != nil {
			return fmt.Errorf("engine: wal write: %w", err)
		}
	}
	return nil
}

// Sync flushes the buffer and fsyncs — after it returns, every appended
// record survives a crash.
func (w *WAL) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("engine: wal flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("engine: wal fsync: %w", err)
	}
	return nil
}

// Reset empties the WAL. Call only after its rows are durable in a
// segment AND the manifest records that segment.
func (w *WAL) Reset() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("engine: wal flush: %w", err)
	}
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("engine: wal truncate: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("engine: wal seek: %w", err)
	}
	return w.f.Sync()
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("engine: wal flush: %w", err)
	}
	return w.f.Close()
}

// replayRecords scans records from r and returns all intact ones. A torn tail
// — a partial record from a crash mid-write — ends replay silently: those bytes
// were never acked as durable. Genuine I/O errors are wrapped and returned.
func replayRecords(r io.Reader) ([]segment.Row, error) {
	br := bufio.NewReader(r)
	var rows []segment.Row
	for {
		var hdr [8]byte
		_, err := io.ReadFull(br, hdr[:])
		if err != nil {
			// Only EOF and ErrUnexpectedEOF indicate torn tails/clean ends.
			// Genuine I/O errors must be surfaced.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return rows, nil // clean EOF or torn header — replay ends
			}
			return nil, fmt.Errorf("engine: wal replay read header: %w", err)
		}
		plen := binary.LittleEndian.Uint32(hdr[:4])
		want := binary.LittleEndian.Uint32(hdr[4:])
		payload := make([]byte, plen)
		_, err = io.ReadFull(br, payload)
		if err != nil {
			// Only EOF and ErrUnexpectedEOF indicate torn payloads.
			// Genuine I/O errors must be surfaced.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return rows, nil // torn payload
			}
			return nil, fmt.Errorf("engine: wal replay read payload: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != want {
			return rows, nil // corrupt record — stop at last good one
		}
		var r segment.Row
		if err := json.Unmarshal(payload, &r); err != nil {
			return rows, nil // undecodable — treat as torn
		}
		rows = append(rows, r)
	}
}

// ReplayWAL returns every intact record in the WAL at path. A torn tail
// — a partial record from a crash mid-write — ends replay silently:
// those bytes were never acked as durable. A missing file is an empty
// WAL. Only I/O failures on an existing file return an error.
func ReplayWAL(path string) ([]segment.Row, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("engine: open wal for replay: %w", err)
	}
	defer func() { _ = f.Close() }()

	return replayRecords(f)
}
