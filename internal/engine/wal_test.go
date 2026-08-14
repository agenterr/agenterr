package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agenterr/agenterr/internal/segment"
)

func walRows(n int) []segment.Row {
	rows := make([]segment.Row, n)
	for i := range rows {
		rows[i] = segment.Row{
			LogID: int64(i + 1), TsMicros: 1755000000000000 + int64(i),
			Severity: 9, TemplateID: 3, Vars: []string{"a", "b"},
			Service: "api", Attrs: "{}",
		}
	}
	return rows
}

func TestWALAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := walRows(10)
	if err := w.Append(rows[:6]); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(rows[6:]); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Errorf("replay mismatch: got %d rows", len(got))
	}
}

func TestWALTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _ := OpenWAL(path)
	rows := walRows(5)
	if err := w.Append(rows); err != nil {
		t.Fatal(err)
	}
	_ = w.Sync()
	_ = w.Close()

	data, _ := os.ReadFile(path)
	// Chop mid-record: drop the last 3 bytes (inside record 5's payload).
	if err := os.WriteFile(path, data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("torn tail must not error: %v", err)
	}
	if !reflect.DeepEqual(got, rows[:4]) {
		t.Errorf("want first 4 intact rows, got %d", len(got))
	}

	// Corrupt CRC mid-file: also ends replay at the last good record.
	data2 := append([]byte(nil), data...)
	data2[10] ^= 0xff // inside record 1
	if err := os.WriteFile(path, data2, 0o644); err != nil {
		t.Fatal(err)
	}
	got2, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("corrupt record must not error: %v", err)
	}
	if len(got2) != 0 {
		t.Errorf("corrupt first record: want 0 rows, got %d", len(got2))
	}
}

func TestWALMissingFileAndReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	if got, err := ReplayWAL(path); err != nil || got != nil {
		t.Errorf("missing wal: got %v, %v", got, err)
	}
	w, _ := OpenWAL(path)
	_ = w.Append(walRows(3))
	_ = w.Sync()
	if err := w.Reset(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	if got, _ := ReplayWAL(path); len(got) != 0 {
		t.Errorf("after reset: want empty, got %d rows", len(got))
	}
}

type failingReader struct {
	exhausted bool
	failErr   error
}

func (fr *failingReader) Read(p []byte) (int, error) {
	if fr.exhausted {
		return 0, fr.failErr
	}
	// First read returns minimal data to represent an incomplete header
	// that will trigger an I/O error when trying to read more.
	fr.exhausted = true
	// Return 4 bytes (half a header), so ReadFull will need to call Read again.
	if len(p) >= 4 {
		return 4, nil
	}
	return len(p), nil
}

func TestReplayRecordsIOError(t *testing.T) {
	// Test that genuine I/O errors (not EOF/ErrUnexpectedEOF) are surfaced.
	// We use a reader that returns partial data and then fails.
	testErr := errors.New("simulated disk read error")
	failReader := &failingReader{failErr: testErr}

	// Should encounter an I/O error when trying to read the header.
	rows, err := replayRecords(failReader)
	if err == nil {
		t.Fatalf("expected I/O error but got none; rows=%d", len(rows))
	}
	if !errors.Is(err, testErr) {
		t.Errorf("expected error matching %v but got %v", testErr, err)
	}
}

func TestWALCorruptHeaderOOM(t *testing.T) {
	// Test that a corrupt WAL header claiming a huge length is treated as torn tail
	// and does not attempt allocation that could cause OOM.
	path := filepath.Join(t.TempDir(), "wal")
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	// Write a few valid records first.
	rows := walRows(3)
	if err := w.Append(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a corrupt header claiming 0xFFFFFFFF (4 GiB) followed by any CRC.
	data, _ := os.ReadFile(path)
	buf := bytes.NewBuffer(data)
	// Write huge length: 0xFFFFFFFF
	binary.Write(buf, binary.LittleEndian, uint32(0xFFFFFFFF))
	// Write dummy CRC
	binary.Write(buf, binary.LittleEndian, uint32(0x12345678))

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	// Replay must return the 3 valid rows and no error, without attempting allocation.
	got, err := ReplayWAL(path)
	if err != nil {
		t.Fatalf("replay must not error on corrupt header: %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Errorf("want 3 valid rows, got %d", len(got))
	}
}
