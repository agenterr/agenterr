package enginestore

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/template"
)

// matchRef points at one row that satisfied a search's filters and
// query. Segment rows stay unmaterialized (scan + index) until the
// final limit cut; memtable rows are carried by value.
type matchRef struct {
	ts, logID int64
	projectID int64
	sc        *segment.Scan // nil → memRow is set
	idx       int
	memRow    segment.Row
}

// SearchLogs returns logs matching f, most recent first (ties broken by
// descending id), capped at f.Limit (0 → 50). Query is a SUBSTRING
// match on the reconstructed body — no tokenizer exists anywhere in
// this engine (spec §5).
//
// The predicate is pushed down: segments decode only the cheap filter
// columns for rows that never match; rows whose template's static text
// already guarantees a hit skip the byte scan entirely; the rest are
// reconstructed into a reusable buffer and checked with bytes.Contains
// — no per-row string allocation. Only the final ≤limit rows are fully
// materialized.
func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}
	var pids []int64
	if f.ProjectID != 0 {
		pids = []int64{f.ProjectID}
	} else {
		s.mu.Lock()
		for pid := range s.projects {
			pids = append(pids, pid)
		}
		s.mu.Unlock()
	}
	var all []matchRef
	for _, pid := range pids {
		ms, err := s.searchProject(ctx, pid, f, limit)
		if err != nil {
			return nil, err
		}
		all = append(all, ms...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].ts != all[j].ts {
			return all[i].ts > all[j].ts
		}
		return all[i].logID > all[j].logID
	})
	if len(all) > limit {
		all = all[:limit]
	}
	out := make([]core.Log, 0, len(all))
	for _, m := range all {
		r := m.memRow
		if m.sc != nil {
			var err error
			r, err = m.sc.Row(m.idx)
			if err != nil {
				return nil, err
			}
		}
		l, err := s.rowToLog(m.projectID, r)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// searchProject runs searchProjectOnce with the standard bounded
// restart loop (see collectRows for the discipline).
func (s *Store) searchProject(ctx context.Context, projectID int64, f store.LogFilter, limit int) ([]matchRef, error) {
	for attempt := 1; attempt <= maxSegmentSetRestarts; attempt++ {
		ms, restart, err := s.searchProjectOnce(ctx, projectID, f, limit)
		if err != nil {
			return nil, err
		}
		if !restart {
			return ms, nil
		}
	}
	return nil, fmt.Errorf("enginestore: segment set changed repeatedly during read (project %d)", projectID)
}

// searchProjectOnce is one attempt: a coherent manifest+memtable
// snapshot (under ps.mu — see collectRowsOnce), then a filtered,
// classified match pass over each candidate segment and the memtable.
func (s *Store) searchProjectOnce(ctx context.Context, projectID int64, f store.LogFilter, limit int) ([]matchRef, bool, error) {
	sinceM, untilM := boundsMicros(f.Since, f.Until)
	segs, memRows, err := s.snapshotProject(ctx, projectID)
	if err != nil {
		return nil, false, err
	}
	tmpls, err := s.ex.Snapshot(projectID)
	if err != nil {
		return nil, false, err
	}
	always := template.AlwaysContaining(tmpls, f.Query)
	filter := segment.ScanFilter{
		SinceM: sinceM, UntilM: untilM,
		Service: f.Service, Environment: f.Environment,
		MinSeverity: int(f.MinSeverity),
	}
	var cands []sqlitestore.SegmentMeta
	for _, m := range segs {
		if m.MaxTs < sinceM || m.MinTs > untilM {
			continue
		}
		if f.Service != "" && !contains(m.Services, f.Service) {
			continue
		}
		cands = append(cands, m)
	}
	out, restart, err := s.searchSegments(ctx, projectID, cands, filter, f.Query, always, tmpls, limit)
	if err != nil || restart {
		return nil, restart, err
	}
	mm, err := s.searchMemRows(projectID, memRows, f, sinceM, untilM)
	if err != nil {
		return nil, false, err
	}
	return append(out, mm...), false, nil
}

// snapshotProject takes the manifest and memtable snapshot together
// under ps.mu — the same coherence rule collectRowsOnce documents.
func (s *Store) snapshotProject(ctx context.Context, projectID int64) ([]sqlitestore.SegmentMeta, []segment.Row, error) {
	ps := s.readProj(projectID)
	if ps == nil {
		// No projState: no flush can be running for this project, so the
		// manifest query needs no ps.mu coherence lock.
		segs, err := s.Segments(ctx, projectID)
		return segs, nil, err
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	segs, err := s.Segments(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}
	return segs, ps.mem.Snapshot(), nil
}

// searchChunkRows is the Match-count above which one segment's match
// pass is split into parallel chunks. Chunks are independent because
// searchScanRange takes an explicit [lo, hi) range and per-chunk caps
// are safe: the global cut keeps at most `limit` rows, and any row a
// chunk's cap discards is outranked by `limit` rows from that same
// chunk.
const searchChunkRows = 32_768

// segSearchResult carries one segment's outcome across the fan-out.
type segSearchResult struct {
	ms      []matchRef
	restart bool
	err     error
}

// searchSegments scans and matches every candidate segment with bounded
// parallelism (one goroutine per segment, GOMAXPROCS at a time), each
// segment further chunk-parallelized by searchScanChunked.
func (s *Store) searchSegments(ctx context.Context, projectID int64, segs []sqlitestore.SegmentMeta, filter segment.ScanFilter, q string, always map[int64]bool, tmpls map[int64][]string, limit int) ([]matchRef, bool, error) {
	results := make([]segSearchResult, len(segs))
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for si := range segs {
		wg.Add(1)
		go func(si int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			sc, restart, err := s.openScanWithRestart(ctx, projectID, segs[si], filter)
			if err != nil || restart {
				results[si] = segSearchResult{restart: restart, err: err}
				return
			}
			ms, err := searchScanChunked(sc, projectID, q, always, tmpls, limit)
			results[si] = segSearchResult{ms: ms, err: err}
		}(si)
	}
	wg.Wait()
	var out []matchRef
	for _, r := range results {
		if r.err != nil {
			return nil, false, r.err
		}
		if r.restart {
			return nil, true, nil
		}
		out = append(out, r.ms...)
	}
	return out, false, nil
}

// searchScanChunked splits one segment's Match set into contiguous
// chunks matched in parallel, then merges the per-chunk results in
// global descending order, applying the same limit+boundary-tie rule
// searchScanRange uses within a chunk.
func searchScanChunked(sc *segment.Scan, projectID int64, q string, always map[int64]bool, tmpls map[int64][]string, limit int) ([]matchRef, error) {
	n := len(sc.Match)
	if n <= searchChunkRows {
		return searchScanRange(sc, projectID, q, always, tmpls, limit, 0, n)
	}
	if q != "" {
		if err := sc.EnsureBodies(); err != nil {
			return nil, err
		}
	}
	nChunks := (n + searchChunkRows - 1) / searchChunkRows
	chunks := make([][]matchRef, nChunks)
	errs := make([]error, nChunks)
	var wg sync.WaitGroup
	for c := 0; c < nChunks; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			lo := c * searchChunkRows
			hi := lo + searchChunkRows
			if hi > n {
				hi = n
			}
			chunks[c], errs[c] = searchScanRange(sc, projectID, q, always, tmpls, limit, lo, hi)
		}(c)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	// Chunks cover ascending index ranges; each chunk's matches are
	// (ts, id)-descending within it. Consuming chunks from last to
	// first yields global descending order (segment rows are
	// ts-ascending by construction), so apply the limit+tie rule on
	// the concatenation.
	var out []matchRef
	for c := nChunks - 1; c >= 0; c-- {
		for _, m := range chunks[c] {
			if len(out) >= limit && m.ts != out[len(out)-1].ts {
				return out, nil
			}
			out = append(out, m)
		}
	}
	return out, nil
}

// searchScanRange matches sc.Match[lo:hi] against q in reverse
// ((ts, id)-descending within a segment, since segment rows are
// ts-ascending), collecting matchRefs until limit is reached — then
// continuing only while candidates tie the boundary timestamp, so the
// caller's global (ts, id) sort can cut exactly. Requires nothing
// pre-ensured; it calls EnsureBodies itself when a query is present.
func searchScanRange(sc *segment.Scan, projectID int64, q string, always map[int64]bool, tmpls map[int64][]string, limit, lo, hi int) ([]matchRef, error) {
	qb := []byte(q)
	if len(qb) > 0 {
		if err := sc.EnsureBodies(); err != nil {
			return nil, err
		}
	}
	var out []matchRef
	var body []byte
	var vars [][]byte
	for k := hi - 1; k >= lo; k-- {
		i := sc.Match[k]
		if len(out) >= limit && sc.Ts(i) != out[len(out)-1].ts {
			break
		}
		ok, err := rowMatches(sc, i, qb, always, tmpls, &body, &vars)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, matchRef{ts: sc.Ts(i), logID: sc.LogID(i), projectID: projectID, sc: sc, idx: i})
		}
	}
	return out, nil
}

// rowMatches reports whether row i's body contains qb, reusing the
// caller's scratch buffers. An empty query matches everything. Raw rows
// scan their stored bytes directly; templated rows either match by
// static classification (always) or are reconstructed into the scratch
// buffer and scanned.
func rowMatches(sc *segment.Scan, i int, qb []byte, always map[int64]bool, tmpls map[int64][]string, body *[]byte, vars *[][]byte) (bool, error) {
	if len(qb) == 0 {
		return true, nil
	}
	tid := sc.TemplateID(i)
	if tid == 0 {
		return bytes.Contains(sc.RawBytes(i), qb), nil
	}
	if always[tid] {
		return true, nil
	}
	toks, ok := tmpls[tid]
	if !ok {
		return false, fmt.Errorf("enginestore: template %d missing for log %d", tid, sc.LogID(i))
	}
	*vars = sc.AppendVars((*vars)[:0], i)
	b, ok := template.AppendSubstitute((*body)[:0], toks, *vars)
	*body = b
	if !ok {
		return false, fmt.Errorf("enginestore: template %d var count mismatch for log %d", tid, sc.LogID(i))
	}
	return bytes.Contains(b, qb), nil
}

// searchMemRows applies the full filter set plus the substring query to
// unflushed memtable rows. The memtable is small (flushes cap it), so
// this path reconstructs through rowToLog per candidate.
func (s *Store) searchMemRows(projectID int64, memRows []segment.Row, f store.LogFilter, sinceM, untilM int64) ([]matchRef, error) {
	var out []matchRef
	for _, r := range memRows {
		if r.TsMicros < sinceM || r.TsMicros > untilM {
			continue
		}
		if f.Service != "" && r.Service != f.Service {
			continue
		}
		if f.Environment != "" && r.Environment != f.Environment {
			continue
		}
		if r.Severity < int(f.MinSeverity) {
			continue
		}
		if f.Query != "" {
			l, err := s.rowToLog(projectID, r)
			if err != nil {
				return nil, err
			}
			if !strings.Contains(l.Body, f.Query) {
				continue
			}
		}
		out = append(out, matchRef{ts: r.TsMicros, logID: r.LogID, projectID: projectID, memRow: r})
	}
	return out, nil
}
