package buffer

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// rollBytes is the fixed per-segment size cap. Segments roll (a new file
// starts) once the current one reaches this size; it is not configurable
// per the ship semantics doc ("segments roll at 4MB").
const rollBytes = 4 * 1024 * 1024

// segMeta tracks one on-disk segment file: its sequence number (segments
// are read/written oldest-to-newest by ascending seq), path, and current
// size in bytes. Segments other than the last are immutable and complete
// by construction — only the last (current write target) ever grows or
// gets torn-tail-truncated.
type segMeta struct {
	seq  int64
	path string
	size int64
}

// segmentPath returns the path for segment seq within dir.
func segmentPath(dir string, seq int64) string {
	return filepath.Join(dir, fmt.Sprintf("spool-%010d.jsonl", seq))
}

// segmentSeqFromName extracts the sequence number from a "spool-NNNN.jsonl"
// basename; ok is false for anything that doesn't match (e.g. checkpoint
// files, temp files, stray junk in the spool dir).
func segmentSeqFromName(name string) (int64, bool) {
	const prefix, suffix = "spool-", ".jsonl"
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	numStr := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	var seq int64
	if _, err := fmt.Sscanf(numStr, "%d", &seq); err != nil {
		return 0, false
	}
	return seq, true
}

// scanSegments lists dir for spool-*.jsonl files and returns their metadata
// sorted ascending by sequence number.
func scanSegments(dir string) ([]*segMeta, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []*segMeta
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := segmentSeqFromName(e.Name())
		if !ok {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, err
		}
		segs = append(segs, &segMeta{seq: seq, path: filepath.Join(dir, e.Name()), size: info.Size()})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	return segs, nil
}

// countRecords returns the number of complete (newline-terminated) records
// in the file at path. Used only for segments being evicted or torn-tail
// truncated, which are small/bounded (at most rollBytes) so a full read is
// cheap and infrequent.
func countRecords(path string) (int64, error) {
	return countRecordsFrom(path, 0)
}

// countRecordsFrom returns the number of complete (newline-terminated)
// records starting at byte offset from. Used at cap-eviction time to count
// only the still-unacked tail of a segment that has already been partially
// acked (the ack checkpoint can sit inside the oldest surviving segment) —
// counting from 0 there would double-count records already reflected in
// the ack count.
func countRecordsFrom(path string, from int64) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return 0, err
	}
	var count int64
	r := bufio.NewReader(f)
	for {
		_, err := r.ReadBytes('\n')
		if err == nil {
			count++
			continue
		}
		if err == io.EOF {
			return count, nil
		}
		return count, err
	}
}

// truncateTornTail scans the last segment starting at byte offset from and
// truncates the file to drop a final line that has no trailing newline
// (evidence of a write that was in progress when the process died). It
// returns the file's complete-record-terminated size after truncation and
// the number of complete records found from `from` onward.
func truncateTornTail(path string, from int64) (newSize int64, recordsFromOffset int64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return 0, 0, err
	}
	r := bufio.NewReader(f)
	pos := from
	lastGoodEnd := from
	var count int64
	for {
		line, rerr := r.ReadBytes('\n')
		if rerr == nil {
			pos += int64(len(line))
			lastGoodEnd = pos
			count++
			continue
		}
		if rerr == io.EOF {
			if len(line) > 0 {
				// Partial trailing line with no newline: torn write. Drop it.
				if terr := f.Truncate(lastGoodEnd); terr != nil {
					return 0, 0, terr
				}
			}
			return lastGoodEnd, count, nil
		}
		return 0, 0, rerr
	}
}

// readRecords reads up to maxRecords complete (newline-terminated) records
// from path starting at byte offset from. It returns the records (without
// their trailing newline), the byte offset just past the last record
// returned, and whether the segment is exhausted (no more complete records
// available past the returned offset — either true EOF or a trailing
// partial line).
func readRecords(path string, from int64, maxRecords int) (records [][]byte, newOffset int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, from, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return nil, from, err
	}
	r := bufio.NewReader(f)
	pos := from
	for len(records) < maxRecords {
		line, rerr := r.ReadBytes('\n')
		if rerr == nil {
			pos += int64(len(line))
			records = append(records, line[:len(line)-1]) // strip trailing \n
			continue
		}
		if rerr == io.EOF {
			// Partial trailing line (no \n yet) is left unread; it isn't a
			// complete record. Non-last segments never have this (they're
			// immutable by construction), but guard anyway.
			break
		}
		return records, pos, rerr
	}
	return records, pos, nil
}
