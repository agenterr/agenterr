// Package buffer implements the disk spool for agenterr-ship: a crash-safe,
// capped, ack-checkpointed FIFO of wire records. Records are appended to
// JSONL segment files (spool-<seq>.jsonl, rolling at 4MB); an ack
// checkpoint (segment + byte offset of the last acked record, plus
// per-container "since" timestamps) is persisted atomically to
// checkpoint.json. Segment appends are NOT fsynced per record for
// performance — the checkpoint only ever points at acked data, so a hard
// crash can lose the unfsynced tail beyond it, which the at-least-once
// contract absorbs: on restart the tail is re-read, or truncated at the
// last complete record if it was torn mid-write.
//
// Stdlib only, per the ship package's dependency constraint.
package buffer

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Cursor identifies a position in the spool: the segment sequence number
// and the byte offset within it, always pointing just past the last
// record returned by Next (or just past the last acked record).
type Cursor struct {
	Seq    int64
	Offset int64
}

// Spool is a disk-backed FIFO of wire records with an ack checkpoint. One
// writer goroutine is expected to call Append, one reader goroutine to call
// Next/Ack — the orchestrator guarantees this — but Spool serializes all
// operations behind a mutex anyway: it's cheap, and checkpoint writes
// (triggered by Ack, SetSince, and cap eviction) would otherwise race
// Append's segment-roll bookkeeping.
type Spool struct {
	mu       sync.Mutex
	dir      string
	maxBytes int64

	segments []*segMeta // ascending by seq; live (non-evicted, non-deleted) segments
	curFile  *os.File   // open handle for the last segment in segments

	ackedSeq    int64
	ackedOffset int64
	since       map[string]time.Time

	dropped int64
}

// Open opens (or creates) a spool rooted at dir, capped at maxBytes total
// on-disk bytes across all live segments. It resumes from checkpoint.json
// if present, and repairs a torn tail on the last segment (a final line
// with no trailing newline, evidence of a write in progress when the
// process died).
func Open(dir string, maxBytes int64) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("buffer: create dir: %w", err)
	}

	cp, err := loadCheckpoint(dir)
	if err != nil {
		return nil, fmt.Errorf("buffer: load checkpoint: %w", err)
	}

	segs, err := scanSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("buffer: scan segments: %w", err)
	}
	if len(segs) == 0 {
		segs = []*segMeta{{seq: 1, path: segmentPath(dir, 1), size: 0}}
		f, err := os.Create(segs[0].path)
		if err != nil {
			return nil, fmt.Errorf("buffer: create initial segment: %w", err)
		}
		f.Close()
	}

	last := segs[len(segs)-1]

	// Repair a torn tail on the last segment only: earlier segments are
	// immutable and complete by construction (they were rolled, meaning a
	// clean close happened before the next segment started).
	tornFrom := int64(0)
	if cp.Seq == last.seq {
		tornFrom = cp.Offset
	}
	newSize, _, err := truncateTornTail(last.path, tornFrom)
	if err != nil {
		return nil, fmt.Errorf("buffer: repair torn tail: %w", err)
	}
	if newSize != last.size {
		log.Printf("buffer: WARN torn tail truncated in %s: %d -> %d bytes", last.path, last.size, newSize)
	}
	last.size = newSize

	f, err := os.OpenFile(last.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("buffer: open last segment for append: %w", err)
	}

	since := make(map[string]time.Time, len(cp.Since))
	for k := range cp.Since {
		if t, ok := sinceToTime(cp.Since, k); ok {
			since[k] = t
		}
	}

	s := &Spool{
		dir:         dir,
		maxBytes:    maxBytes,
		segments:    segs,
		curFile:     f,
		ackedSeq:    cp.Seq,
		ackedOffset: cp.Offset,
		since:       since,
	}
	if s.ackedSeq == 0 {
		// Fresh spool, nothing acked yet: the checkpoint starts at the
		// first live segment.
		s.ackedSeq = segs[0].seq
		s.ackedOffset = 0
	}
	s.normalizeCheckpointLocked()
	return s, nil
}

// Append writes one JSON record (no embedded newlines) to the spool,
// rolling to a new segment if this write would push the current one past
// the 4MB roll size, and evicting the oldest live segment(s) if the total
// on-disk size then exceeds maxBytes.
func (s *Spool) Append(rec []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	line := make([]byte, 0, len(rec)+1)
	line = append(line, rec...)
	line = append(line, '\n')

	cur := s.segments[len(s.segments)-1]
	if cur.size > 0 && cur.size+int64(len(line)) > rollBytes {
		if err := s.rollLocked(); err != nil {
			return err
		}
		cur = s.segments[len(s.segments)-1]
	}

	n, err := s.curFile.Write(line)
	if err != nil {
		return fmt.Errorf("buffer: append: %w", err)
	}
	cur.size += int64(n)

	return s.enforceCapLocked()
}

// rollLocked closes the current segment and starts a new one with the next
// sequence number. Caller holds s.mu.
func (s *Spool) rollLocked() error {
	if err := s.curFile.Close(); err != nil {
		return fmt.Errorf("buffer: close segment on roll: %w", err)
	}
	next := s.segments[len(s.segments)-1].seq + 1
	path := segmentPath(s.dir, next)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("buffer: create rolled segment: %w", err)
	}
	s.segments = append(s.segments, &segMeta{seq: next, path: path, size: 0})
	s.curFile = f
	return nil
}

// enforceCapLocked evicts the oldest live segment(s), oldest-first, while
// the total on-disk size exceeds maxBytes. The current (last, being
// written) segment is never evicted — there must always be somewhere to
// write. Caller holds s.mu.
func (s *Spool) enforceCapLocked() error {
	evicted := false
	for s.totalBytesLocked() > s.maxBytes && len(s.segments) > 1 {
		evicted = true
		victim := s.segments[0]

		// Only records not yet acked count as dropped. The ack checkpoint
		// can already sit inside (or past) this segment — e.g. it was
		// fully acked and normalized onto the next segment's start, but
		// physically deleted only on the next explicit Ack call — in
		// which case none of it is a loss.
		var n int64
		var err error
		switch {
		case victim.seq < s.ackedSeq:
			n = 0
		case victim.seq == s.ackedSeq:
			n, err = countRecordsFrom(victim.path, s.ackedOffset)
		default:
			n, err = countRecords(victim.path)
		}
		if err != nil {
			return fmt.Errorf("buffer: count records in evicted segment: %w", err)
		}
		if err := os.Remove(victim.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("buffer: remove evicted segment: %w", err)
		}
		s.segments = s.segments[1:]
		s.dropped += n
		log.Printf("buffer: WARN cap exceeded, evicted %s (%d records dropped, %d bytes)", victim.path, n, victim.size)

		// If the checkpoint pointed into (or before) the evicted segment,
		// snap it forward to the start of the new oldest surviving one —
		// the reader can never resume from data that no longer exists.
		if s.ackedSeq <= victim.seq {
			s.ackedSeq = s.segments[0].seq
			s.ackedOffset = 0
		}
	}
	if evicted {
		return s.persistCheckpointLocked()
	}
	return nil
}

func (s *Spool) totalBytesLocked() int64 {
	var total int64
	for _, seg := range s.segments {
		total += seg.size
	}
	return total
}

// normalizeCheckpointLocked advances the checkpoint past the end of a
// fully-drained segment onto the start of the next one, so callers never
// have to special-case "offset == segment size" as a distinct state.
func (s *Spool) normalizeCheckpointLocked() {
	for {
		idx := s.segIndexLocked(s.ackedSeq)
		if idx < 0 {
			// Points at an evicted/unknown segment: snap to the oldest live one.
			s.ackedSeq = s.segments[0].seq
			s.ackedOffset = 0
			return
		}
		seg := s.segments[idx]
		if s.ackedOffset < seg.size || idx == len(s.segments)-1 {
			return
		}
		s.ackedSeq = s.segments[idx+1].seq
		s.ackedOffset = 0
	}
}

func (s *Spool) segIndexLocked(seq int64) int {
	for i, seg := range s.segments {
		if seg.seq == seq {
			return i
		}
	}
	return -1
}

// Next returns up to max unacked records starting from the ack checkpoint,
// plus the Cursor positioned just past the last record returned. An empty
// slice means the reader is caught up with the writer.
func (s *Spool) Next(max int) ([][]byte, Cursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.normalizeCheckpointLocked()
	seq, offset := s.ackedSeq, s.ackedOffset

	var out [][]byte
	idx := s.segIndexLocked(seq)
	for idx >= 0 && idx < len(s.segments) && len(out) < max {
		seg := s.segments[idx]
		recs, newOffset, err := readRecords(seg.path, offset, max-len(out))
		if err != nil {
			return out, Cursor{Seq: seq, Offset: offset}, fmt.Errorf("buffer: read segment %d: %w", seg.seq, err)
		}
		out = append(out, recs...)
		seq, offset = seg.seq, newOffset

		if len(out) >= max {
			break
		}
		// This segment is drained. Move on to the next one, if any — but
		// only cross into it if it's not the live write segment sitting at
		// exactly its on-disk size (i.e. genuinely nothing more to read).
		if idx == len(s.segments)-1 {
			break
		}
		idx++
		offset = 0
		seq = s.segments[idx].seq
	}

	return out, Cursor{Seq: seq, Offset: offset}, nil
}

// Ack persists the checkpoint through c: everything up to and including
// the record ending at c is considered delivered. Segments that end up
// strictly before c's segment are now fully acked and are deleted from
// disk. A stale cursor (behind the current checkpoint — e.g. raced by a
// cap eviction that has already snapped the checkpoint forward) is a
// silent no-op rather than moving the checkpoint backward.
func (s *Spool) Ack(c Cursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c.Seq < s.ackedSeq || (c.Seq == s.ackedSeq && c.Offset < s.ackedOffset) {
		return nil
	}

	s.ackedSeq, s.ackedOffset = c.Seq, c.Offset

	for len(s.segments) > 0 && s.segments[0].seq < s.ackedSeq {
		victim := s.segments[0]
		if err := os.Remove(victim.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("buffer: remove acked segment: %w", err)
		}
		s.segments = s.segments[1:]
	}

	return s.persistCheckpointLocked()
}

// SetSince records the resume timestamp for container, persisted with the
// checkpoint.
func (s *Spool) SetSince(container string, t time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.since == nil {
		s.since = make(map[string]time.Time)
	}
	s.since[container] = t
	return s.persistCheckpointLocked()
}

// Since returns the persisted resume timestamp for container, if any.
func (s *Spool) Since(container string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.since[container]
	return t, ok
}

// Dropped returns the cumulative count of records lost to cap eviction.
func (s *Spool) Dropped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// Close closes the current segment's file handle. It does not persist
// anything further — the checkpoint is kept current on every Ack,
// SetSince, and cap eviction.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.curFile.Close()
}

// persistCheckpointLocked writes the current ack position and since map to
// checkpoint.json. Caller holds s.mu.
func (s *Spool) persistCheckpointLocked() error {
	sinceStr := make(map[string]string, len(s.since))
	for k, t := range s.since {
		sinceStr[k] = t.Format(time.RFC3339Nano)
	}
	cp := checkpointFile{Seq: s.ackedSeq, Offset: s.ackedOffset, Since: sinceStr}
	if err := saveCheckpoint(s.dir, cp); err != nil {
		return fmt.Errorf("buffer: persist checkpoint: %w", err)
	}
	return nil
}
