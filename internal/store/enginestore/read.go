package enginestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
)

// segPath resolves a manifest-relative segment path (as stored in
// sqlite.SegmentMeta.Path) to an absolute path under the engine data
// directory. Every manifest path open goes through this helper.
func (s *Store) segPath(rel string) string {
	return filepath.Join(s.dir, rel)
}

// readProj returns the project's engine state if it exists, or nil. Reads
// must never create WAL files or projStates for projects they merely
// query — segments are still served via the manifest. Only proj()
// (WriteBatch/flushProject/Prune/recover) may create projState.
func (s *Store) readProj(projectID int64) *projState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.projects[projectID]
}

// maxSegmentSetRestarts bounds how many times collectRows/logByID will
// re-snapshot the manifest and start over after finding a segment they
// were mid-pass on has been legitimately replaced (compacted or pruned
// away). Exceeding it means the segment set is being rewritten faster
// than a single query can complete — treated as a loud error, never a
// silent partial result.
const maxSegmentSetRestarts = 3

// collectRows returns all rows for a project across the memtable and
// every manifest segment overlapping [since, until] (zero times = no
// bound), optionally filtered by service via segment footers.
//
// Each attempt (collectRowsOnce) takes the manifest query and the
// memtable snapshot together under ps.mu so a concurrent flushProject
// (which holds ps.mu across InsertSegment → wal.Reset → mem.Reset) cannot
// be observed mid-flight: callers either see the pre-flush state (new
// segment absent, memtable still holding its rows) or the post-flush
// state (new segment present, memtable already reset) — never a mix that
// duplicates or drops rows.
//
// A single attempt can still find, after that snapshot, that one of its
// member segments has been compacted or pruned away entirely (both
// remove the manifest row before the file) between the snapshot and this
// attempt's file open. The replacement segment (a merged segment covering
// the same rows, or nothing if pruned) is NOT in the snapshot currently
// being iterated — returning the partial result built so far would
// silently under-report rows that legitimately still exist. Instead,
// collectRowsOnce reports restart=true and collectRows abandons the
// attempt and re-snapshots from scratch (bounded by
// maxSegmentSetRestarts), so the replacement segment is read normally on
// the next attempt.
func (s *Store) collectRows(ctx context.Context, projectID int64, since, until time.Time, service string) ([]segment.Row, error) {
	sinceM, untilM := boundsMicros(since, until)
	for attempt := 1; attempt <= maxSegmentSetRestarts; attempt++ {
		rows, restart, err := s.collectRowsOnce(ctx, projectID, sinceM, untilM, service)
		if err != nil {
			return nil, err
		}
		if !restart {
			return rows, nil
		}
	}
	return nil, fmt.Errorf("enginestore: segment set changed repeatedly during read (project %d)", projectID)
}

// collectRowsOnce is one attempt of collectRows: a fresh manifest+memtable
// snapshot, followed by a single pass over every member segment. See
// collectRows for what restart=true means and why a partial result is
// never returned in that case.
func (s *Store) collectRowsOnce(ctx context.Context, projectID int64, sinceM, untilM int64, service string) ([]segment.Row, bool, error) {
	ps := s.readProj(projectID)

	var segs []sqlitestore.SegmentMeta
	var memRows []segment.Row
	var err error
	if ps == nil {
		// No projState for this project: no flush can be running for it
		// (flushProject only ever runs against a projState reached via
		// proj()), so the manifest query needs no ps.mu coherence lock —
		// there is nothing racing it.
		segs, err = s.Segments(ctx, projectID)
		if err != nil {
			return nil, false, err
		}
	} else {
		ps.mu.Lock()
		segs, err = s.Segments(ctx, projectID)
		if err != nil {
			ps.mu.Unlock()
			return nil, false, err
		}
		memRows = ps.mem.Snapshot()
		ps.mu.Unlock()
	}

	var out []segment.Row
	for _, m := range segs {
		rows, restart, err := s.readSegmentRowsWithRestart(ctx, projectID, m, sinceM, untilM, service)
		if err != nil {
			return nil, false, err
		}
		if restart {
			return nil, true, nil
		}
		out = append(out, rows...)
	}
	out = append(out, filterRowsByTime(memRows, sinceM, untilM)...)
	return out, false, nil
}

// isSegmentNotExist reports whether err (from segment.Open/Read, or the
// enginestore wrappers around them) stems from the underlying file being
// absent, unwrapping the fmt.Errorf("%w", ...) chains those functions use
// around os.Open/os.ReadAt.
func isSegmentNotExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}

// freshSegmentByID re-fetches projectID's manifest — under ps.mu when a
// projState exists, matching the coherence discipline collectRows and
// logByID already use — and reports whether segID is still present,
// returning its current row. Read-only: it never calls proj(), so it
// cannot mint a projState/WAL for a project that only has segments (that
// would violate readProj's "reads never create engine state" rule); if no
// projState exists there is nothing a concurrent flush/compact could be
// racing against for THIS query's own earlier snapshot anyway, so an
// unlocked query is safe here for the same reason it is in collectRows.
func (s *Store) freshSegmentByID(ctx context.Context, projectID, segID int64) (sqlitestore.SegmentMeta, bool, error) {
	ps := s.readProj(projectID)
	var segs []sqlitestore.SegmentMeta
	var err error
	if ps == nil {
		segs, err = s.Segments(ctx, projectID)
	} else {
		ps.mu.Lock()
		segs, err = s.Segments(ctx, projectID)
		ps.mu.Unlock()
	}
	if err != nil {
		return sqlitestore.SegmentMeta{}, false, err
	}
	for _, m := range segs {
		if m.ID == segID {
			return m, true, nil
		}
	}
	return sqlitestore.SegmentMeta{}, false, nil
}

// readSegmentRowsWithRestart is collectRowsOnce's ENOENT handler: m's
// file disappeared (compaction or prune removed it) between the manifest
// snapshot that produced m and this read. It re-fetches the manifest via
// freshSegmentByID to distinguish the two ways that can happen:
//
//   - Row gone from the fresh manifest too: the segment was legitimately
//     replaced — compaction folded it into a merged segment covering the
//     same rows (which is NOT among the segments this attempt is
//     iterating), or prune dropped it outright. Either way, this attempt
//     cannot see the true current state and must not return a partial
//     result: restart=true, err=nil, telling the caller to abandon this
//     pass and re-snapshot from scratch.
//   - Row still present in the fresh manifest but the file is still
//     missing: that combination is not explainable by compaction or
//     prune (both remove the manifest row before the file), so it is real
//     corruption and must propagate as an error naming the path.
func (s *Store) readSegmentRowsWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, sinceM, untilM int64, service string) (rows []segment.Row, restart bool, err error) {
	rows, err = s.readSegmentRows(m, sinceM, untilM, service)
	if err == nil {
		return rows, false, nil
	}
	if !isSegmentNotExist(err) {
		return nil, false, err
	}
	fresh, found, ferr := s.freshSegmentByID(ctx, projectID, m.ID)
	if ferr != nil {
		return nil, false, ferr
	}
	if !found {
		return nil, true, nil
	}
	rows, err = s.readSegmentRows(fresh, sinceM, untilM, service)
	if err != nil {
		if isSegmentNotExist(err) {
			return nil, false, fmt.Errorf("enginestore: segment %s missing but manifest row %d still present: %w", fresh.Path, fresh.ID, err)
		}
		return nil, false, err
	}
	return rows, false, nil
}

// readSegmentFileWithRestart is logByIDOnce's counterpart to
// readSegmentRowsWithRestart: it fully decodes segment m (no time/service
// filtering — logByID scans every row for a matching LogID), applying the
// same restart-on-legitimate-replacement, error-on-corruption discipline.
// See readSegmentRowsWithRestart for what restart=true vs an error mean.
func (s *Store) readSegmentFileWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta) (rows []segment.Row, restart bool, err error) {
	_, rows, err = segment.Read(s.segPath(m.Path))
	if err == nil {
		return rows, false, nil
	}
	if !isSegmentNotExist(err) {
		return nil, false, err
	}
	fresh, found, ferr := s.freshSegmentByID(ctx, projectID, m.ID)
	if ferr != nil {
		return nil, false, ferr
	}
	if !found {
		return nil, true, nil
	}
	_, rows, err = segment.Read(s.segPath(fresh.Path))
	if err != nil {
		if isSegmentNotExist(err) {
			return nil, false, fmt.Errorf("enginestore: segment %s missing but manifest row %d still present: %w", fresh.Path, fresh.ID, err)
		}
		return nil, false, err
	}
	return rows, false, nil
}

// boundsMicros converts a [since, until] time range (a zero Time meaning
// unbounded on that side) to inclusive epoch-micro bounds.
func boundsMicros(since, until time.Time) (sinceM, untilM int64) {
	sinceM, untilM = int64(-1<<62), int64(1<<62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	if !until.IsZero() {
		untilM = until.UTC().UnixMicro()
	}
	return sinceM, untilM
}

// readSegmentRows reads one manifest segment's rows and returns those
// within [sinceM, untilM]. It skips the file read entirely (returning nil,
// nil) when the segment's own time range or recorded service set already
// rules it out.
func (s *Store) readSegmentRows(m sqlitestore.SegmentMeta, sinceM, untilM int64, service string) ([]segment.Row, error) {
	if m.MaxTs < sinceM || m.MinTs > untilM {
		return nil, nil
	}
	if service != "" && !contains(m.Services, service) {
		return nil, nil
	}
	_, rows, err := segment.Read(s.segPath(m.Path))
	if err != nil {
		return nil, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
	}
	return filterRowsByTime(rows, sinceM, untilM), nil
}

// filterRowsByTime returns the subset of rows with TsMicros within
// [sinceM, untilM].
func filterRowsByTime(rows []segment.Row, sinceM, untilM int64) []segment.Row {
	var out []segment.Row
	for _, r := range rows {
		if r.TsMicros >= sinceM && r.TsMicros <= untilM {
			out = append(out, r)
		}
	}
	return out
}

// pairedRow bundles a row with the project it belongs to. Needed only
// when scanning across every project (ProjectID == 0 — "all projects",
// used by the web /search page, the admin-key /api/v1/logs endpoint, and
// the MCP search_logs tool): rowToLog/Reconstruct both key off the row's
// true project, which a single filter value cannot supply once rows from
// multiple projects are merged into one slice.
type pairedRow struct {
	ProjectID int64
	Row       segment.Row
}

// collectRowsAllProjects merges collectRows across every project this
// Store has ever opened a projState for. It deliberately does not call
// proj() for a new id purely to satisfy a read — s.mu is only used to
// enumerate the existing s.projects map, matching findLog's convention
// elsewhere in this file. This is complete: every project with a
// manifest segment has one because flushProject can only ever produce a
// segment via proj(), and proj() opens (and never removes) that
// project's WAL file, which recover() globs and replays for every
// project on every Open — so any project with segments or WAL rows
// already has a projState by the time this runs.
//
// Each project's (segments, memtable) pair is still read through
// collectRows, so it keeps that function's ps.mu coherence guarantee
// against a concurrent flushProject on that same project — this only
// adds a loop across projects, not a new locking discipline.
func (s *Store) collectRowsAllProjects(ctx context.Context, since, until time.Time, service string) ([]pairedRow, error) {
	s.mu.Lock()
	pids := make([]int64, 0, len(s.projects))
	for pid := range s.projects {
		pids = append(pids, pid)
	}
	s.mu.Unlock()

	var out []pairedRow
	for _, pid := range pids {
		rows, err := s.collectRows(ctx, pid, since, until, service)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			out = append(out, pairedRow{ProjectID: pid, Row: r})
		}
	}
	return out, nil
}

// collectPairedRows is collectRows tagged with each row's project: for a
// single project (projectID != 0) it just wraps collectRows's result; for
// projectID == 0 ("all projects") it delegates to collectRowsAllProjects.
// SearchLogs is the only caller — factored out mainly to keep that
// function's branching count down.
func (s *Store) collectPairedRows(ctx context.Context, projectID int64, since, until time.Time, service string) ([]pairedRow, error) {
	if projectID == 0 {
		return s.collectRowsAllProjects(ctx, since, until, service)
	}
	single, err := s.collectRows(ctx, projectID, since, until, service)
	if err != nil {
		return nil, err
	}
	out := make([]pairedRow, len(single))
	for i, r := range single {
		out[i] = pairedRow{ProjectID: projectID, Row: r}
	}
	return out, nil
}

// rowLess reports whether a sorts strictly before b in the (TsMicros,
// LogID) total order — LogIDs are unique and monotonic, so this breaks
// exact-timestamp ties deterministically (unlike comparing TsMicros
// alone, which is not a total order over rows sharing an instant).
func rowLess(a, b segment.Row) bool {
	if a.TsMicros != b.TsMicros {
		return a.TsMicros < b.TsMicros
	}
	return a.LogID < b.LogID
}

// contains reports whether ss contains s.
func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// rowToLog reconstructs the core.Log a row represents.
func (s *Store) rowToLog(projectID int64, r segment.Row) (core.Log, error) {
	body := r.Raw
	if r.TemplateID != 0 {
		var ok bool
		var err error
		body, ok, err = s.ex.Reconstruct(projectID, r.TemplateID, r.Vars)
		if err != nil {
			return core.Log{}, fmt.Errorf("enginestore: reconstruct template %d for log %d: %w", r.TemplateID, r.LogID, err)
		}
		if !ok {
			return core.Log{}, fmt.Errorf("enginestore: template %d missing for log %d", r.TemplateID, r.LogID)
		}
	}
	var attrs map[string]string
	if err := json.Unmarshal([]byte(r.Attrs), &attrs); err != nil {
		return core.Log{}, fmt.Errorf("enginestore: attrs for log %d: %w", r.LogID, err)
	}
	return core.Log{
		ID: r.LogID, ProjectID: projectID, Time: time.UnixMicro(r.TsMicros).UTC(),
		Severity: core.Severity(r.Severity), Body: body,
		Service: r.Service, Environment: r.Environment,
		Release: r.Release, TraceID: r.TraceID, Attrs: attrs,
	}, nil
}

// SearchLogs returns logs matching f, most recent first (ties broken by
// descending id, matching the legacy store), capped at f.Limit (0 → 50).
// Query is a SUBSTRING match on the reconstructed body — no tokenizer
// exists anywhere in this engine (spec §5).
func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}
	rows, err := s.collectPairedRows(ctx, f.ProjectID, f.Since, f.Until, f.Service)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Row.TsMicros != rows[j].Row.TsMicros {
			return rows[i].Row.TsMicros > rows[j].Row.TsMicros
		}
		return rows[i].Row.LogID > rows[j].Row.LogID
	})
	out := make([]core.Log, 0, limit)
	for _, pr := range rows {
		r := pr.Row
		if f.Service != "" && r.Service != f.Service {
			continue
		}
		if f.Environment != "" && r.Environment != f.Environment {
			continue
		}
		if r.Severity < int(f.MinSeverity) {
			continue
		}
		l, err := s.rowToLog(pr.ProjectID, r)
		if err != nil {
			return nil, err
		}
		if f.Query != "" && !strings.Contains(l.Body, f.Query) {
			continue
		}
		out = append(out, l)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// logByID locates one row by log id within a project (memtable first,
// then manifest segments whose id range covers it). Each attempt
// (logByIDOnce) takes the memtable snapshot and the manifest query
// together under ps.mu — see collectRows for why that is required to
// avoid racing flushProject — and, like collectRows, restarts from a
// fresh snapshot (bounded by maxSegmentSetRestarts) rather than reporting
// "not found" if a member segment it needed turns out to have been
// legitimately compacted or pruned away mid-lookup: the id could live in
// the replacement segment, which this attempt's snapshot does not contain.
func (s *Store) logByID(ctx context.Context, projectID, logID int64) (segment.Row, bool, error) {
	for attempt := 1; attempt <= maxSegmentSetRestarts; attempt++ {
		row, found, restart, err := s.logByIDOnce(ctx, projectID, logID)
		if err != nil {
			return segment.Row{}, false, err
		}
		if !restart {
			return row, found, nil
		}
	}
	return segment.Row{}, false, fmt.Errorf("enginestore: segment set changed repeatedly during read (project %d)", projectID)
}

// logByIDOnce is one attempt of logByID. See logByID for what restart
// means and why "not found" is never returned in that case.
func (s *Store) logByIDOnce(ctx context.Context, projectID, logID int64) (row segment.Row, found, restart bool, err error) {
	ps := s.readProj(projectID)

	var memRows []segment.Row
	var segs []sqlitestore.SegmentMeta
	if ps == nil {
		// No projState: no flush can be running for this project, so the
		// manifest query needs no ps.mu coherence lock.
		segs, err = s.Segments(ctx, projectID)
		if err != nil {
			return segment.Row{}, false, false, err
		}
	} else {
		ps.mu.Lock()
		memRows = ps.mem.Snapshot()
		segs, err = s.Segments(ctx, projectID)
		ps.mu.Unlock()
		if err != nil {
			return segment.Row{}, false, false, err
		}
	}
	for _, r := range memRows {
		if r.LogID == logID {
			return r, true, false, nil
		}
	}
	for _, m := range segs {
		if logID < m.MinLogID || logID > m.MaxLogID {
			continue
		}
		rows, restart, err := s.readSegmentFileWithRestart(ctx, projectID, m)
		if err != nil {
			return segment.Row{}, false, false, err
		}
		if restart {
			return segment.Row{}, false, true, nil
		}
		for _, r := range rows {
			if r.LogID == logID {
				return r, true, false, nil
			}
		}
	}
	return segment.Row{}, false, false, nil
}

// LogContext returns up to n logs at-or-before the target (inclusive)
// and n after it, same project and service, ascending in time.
//
// "At-or-before"/"after" and each half's ordering use the total order
// (TsMicros, LogID) rather than TsMicros alone: with ties on timestamp,
// TsMicros-only ordering is not stable across rows sharing the target's
// exact instant, and could truncate the target itself out of `before`
// before take-n. Under (ts, id) the target is always the maximum
// element of `before`, so it survives any n >= 1.
func (s *Store) LogContext(ctx context.Context, logID int64, n int) ([]core.Log, error) {
	target, projectID, err := s.findLog(ctx, logID)
	if err != nil {
		return nil, err
	}
	rows, err := s.collectRows(ctx, projectID, time.Time{}, time.Time{}, target.Service)
	if err != nil {
		return nil, err
	}
	var before, after []segment.Row
	for _, r := range rows {
		if r.Service != target.Service {
			continue
		}
		if rowLess(target, r) {
			after = append(after, r)
		} else {
			before = append(before, r)
		}
	}
	// before: (ts, id) DESC — target is the max element under this
	// total order, so it is always index 0 and survives take-n below.
	sort.Slice(before, func(i, j int) bool { return rowLess(before[j], before[i]) })
	// after: (ts, id) ASC.
	sort.Slice(after, func(i, j int) bool { return rowLess(after[i], after[j]) })
	if len(before) > n+1 { // +1: the target itself occupies before[0]
		before = before[:n+1]
	}
	if len(after) > n {
		after = after[:n]
	}
	// Merge ascending: reversed(before) then after.
	out := make([]core.Log, 0, len(before)+len(after))
	for i := len(before) - 1; i >= 0; i-- {
		l, err := s.rowToLog(projectID, before[i])
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	for _, r := range after {
		l, err := s.rowToLog(projectID, r)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// findLog resolves a bare log id to its row and project by consulting
// every known project (memtables) and the manifest.
func (s *Store) findLog(ctx context.Context, logID int64) (segment.Row, int64, error) {
	s.mu.Lock()
	pids := make([]int64, 0, len(s.projects))
	for pid := range s.projects {
		pids = append(pids, pid)
	}
	s.mu.Unlock()
	for _, pid := range pids {
		if r, ok, err := s.logByID(ctx, pid, logID); err != nil {
			return segment.Row{}, 0, err
		} else if ok {
			return r, pid, nil
		}
	}
	segs, err := s.Segments(ctx, 0)
	if err != nil {
		return segment.Row{}, 0, err
	}
	for _, m := range segs {
		if logID >= m.MinLogID && logID <= m.MaxLogID {
			if r, ok, err := s.logByID(ctx, m.ProjectID, logID); err != nil {
				return segment.Row{}, 0, err
			} else if ok {
				return r, m.ProjectID, nil
			}
		}
	}
	return segment.Row{}, 0, store.ErrNotFound
}

// Stats merges flushed rollups with unflushed memtable rows, so counts
// are exact and immediate. OpenIssues comes from the metadata DB.
func (s *Store) Stats(ctx context.Context, f store.StatsFilter) (store.Stats, error) {
	logs, events, perDay, err := s.RollupStats(ctx, f.ProjectID, f.Since)
	if err != nil {
		return store.Stats{}, err
	}
	sinceM := int64(-1 << 62)
	if !f.Since.IsZero() {
		sinceM = f.Since.UTC().UnixMicro()
	}
	if ps := s.readProj(f.ProjectID); ps != nil {
		for _, r := range ps.mem.Snapshot() {
			if r.TsMicros < sinceM {
				continue
			}
			logs++
			day := time.UnixMicro(r.TsMicros).UTC().Format("2006-01-02")
			d := perDay[day]
			d.Day = day
			d.Logs++
			if r.IsEvent {
				events++
				d.Events++
			}
			perDay[day] = d
		}
	}
	open, err := s.OpenIssueCount(ctx, f.ProjectID)
	if err != nil {
		return store.Stats{}, err
	}
	days := make([]store.DayCount, 0, len(perDay))
	for _, d := range perDay {
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Day < days[j].Day })
	return store.Stats{Logs: logs, Events: events, OpenIssues: open, PerDay: days}, nil
}

// ServiceCounts merges rollups with the memtable and returns the top 20
// services by log count (descending, ties by ascending name).
func (s *Store) ServiceCounts(ctx context.Context, projectID int64, since time.Time) ([]store.ServiceCount, error) {
	counts, err := s.RollupServiceCounts(ctx, projectID, since)
	if err != nil {
		return nil, err
	}
	sinceM := int64(-1 << 62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	if ps := s.readProj(projectID); ps != nil {
		for _, r := range ps.mem.Snapshot() {
			if r.TsMicros >= sinceM {
				counts[r.Service]++
			}
		}
	}
	out := make([]store.ServiceCount, 0, len(counts))
	for svc, n := range counts {
		out = append(out, store.ServiceCount{Service: svc, Logs: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Logs != out[j].Logs {
			return out[i].Logs > out[j].Logs
		}
		return out[i].Service < out[j].Service
	})
	if len(out) > 20 {
		out = out[:20]
	}
	return out, nil
}

// Issues returns issues matching f, sorted by last_seen descending. Every
// filter but Environment is delegated to the embedded SQLite store
// unchanged; Environment is handled here via issue_events instead, since
// the embedded implementation's environment filter matches against the
// legacy logs table, which the engine write path never populates.
func (s *Store) Issues(ctx context.Context, f store.IssueFilter) ([]core.Issue, error) {
	env := f.Environment
	if env == "" {
		return s.DB.Issues(ctx, f)
	}
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}
	unfiltered := f
	unfiltered.Environment = ""
	unfiltered.Limit = maxIssueScan
	issues, err := s.DB.Issues(ctx, unfiltered)
	if err != nil {
		return nil, err
	}
	ids, err := s.IssueIDsInEnvironment(ctx, f.ProjectID, env)
	if err != nil {
		return nil, err
	}
	out := make([]core.Issue, 0, limit)
	for _, iss := range issues {
		if !ids[iss.ID] {
			continue
		}
		out = append(out, iss)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// Aggregate groups a project's log volume by service, severity, hour, or
// day — flushed rollups plus the unflushed memtable, so results are
// immediate. Windows are hour-granular: Since/Until are truncated to the
// hour (Until inclusive of its full hour) — the rollup bucket size — so
// the memtable scan and the rollup query agree at any bound, flushed or
// not. ProjectID must be non-zero.
func (s *Store) Aggregate(ctx context.Context, f store.AggregateFilter) ([]store.AggregateRow, error) {
	if f.ProjectID == 0 {
		return nil, fmt.Errorf("enginestore: aggregate requires a project")
	}
	agg, err := s.RollupAggregate(ctx, f.ProjectID, f.Since, f.Until, f.GroupBy)
	if err != nil {
		return nil, err
	}
	buckets := map[string]sqlitestore.RollupAgg{}
	for k, v := range agg {
		buckets[k] = v
	}
	if ps := s.readProj(f.ProjectID); ps != nil {
		sinceM, untilM := aggregateBoundsMicros(f.Since, f.Until)
		for _, r := range ps.mem.Snapshot() {
			if r.TsMicros < sinceM || r.TsMicros > untilM {
				continue
			}
			k, err := memKey(f.GroupBy, r)
			if err != nil {
				return nil, err
			}
			b := buckets[k]
			b.Logs++
			if r.IsEvent {
				b.Events++
			}
			buckets[k] = b
		}
	}
	return orderAggregate(f.GroupBy, buckets), nil
}

// aggregateBoundsMicros converts [since, until] to hour-granular inclusive
// epoch-micro bounds matching RollupAggregate's SQL truncation: since is
// truncated down to the hour, and until (when set) is truncated down to
// the hour and then extended to that hour's end (inclusive) — otherwise a
// row at, say, 10:30 with until=10:00 would be counted by the rollup path
// (whose hour bucket "10:00" covers the full hour) but dropped by a
// micro-exact memtable filter, making Aggregate's result depend on
// whether that row had been flushed yet.
func aggregateBoundsMicros(since, until time.Time) (sinceM, untilM int64) {
	sinceM, untilM = int64(-1<<62), int64(1<<62)
	if !since.IsZero() {
		sinceM = since.UTC().Truncate(time.Hour).UnixMicro()
	}
	if !until.IsZero() {
		untilM = until.UTC().Truncate(time.Hour).Add(time.Hour).UnixMicro() - 1
	}
	return sinceM, untilM
}

// memKey computes the same bucket key RollupAggregate's SQL uses, for a
// single unflushed memtable row — service name, decimal severity, or an
// hour/day formatted from the row's UTC timestamp.
func memKey(groupBy string, r segment.Row) (string, error) {
	switch groupBy {
	case "service":
		return r.Service, nil
	case "severity":
		return strconv.Itoa(r.Severity), nil
	case "hour":
		return time.UnixMicro(r.TsMicros).UTC().Format("2006-01-02T15"), nil
	case "day":
		return time.UnixMicro(r.TsMicros).UTC().Format("2006-01-02"), nil
	default:
		return "", fmt.Errorf("enginestore: unknown aggregate groupBy %q", groupBy)
	}
}

// orderAggregate flattens buckets into rows in the ordering Aggregate
// promises: service by Logs descending (ties by Key ascending); severity
// by numeric Key descending (most severe first); hour/day by Key
// ascending.
func orderAggregate(groupBy string, buckets map[string]sqlitestore.RollupAgg) []store.AggregateRow {
	out := make([]store.AggregateRow, 0, len(buckets))
	for k, v := range buckets {
		out = append(out, store.AggregateRow{Key: k, Logs: v.Logs, Events: v.Events})
	}
	switch groupBy {
	case "service":
		sort.Slice(out, func(i, j int) bool {
			if out[i].Logs != out[j].Logs {
				return out[i].Logs > out[j].Logs
			}
			return out[i].Key < out[j].Key
		})
	case "severity":
		sort.Slice(out, func(i, j int) bool {
			a, _ := strconv.Atoi(out[i].Key)
			b, _ := strconv.Atoi(out[j].Key)
			return a > b
		})
	default: // "hour", "day"
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	}
	return out
}

// maxIssueScan bounds the unfiltered fetch Issues performs before
// applying its own Environment filter — generous enough that no
// realistic project truncates before the filter runs.
const maxIssueScan = 1_000_000

// Issue returns the issue plus its retained events, newest first, with
// each event's log resolved from the engine. A ref whose log has been
// pruned yields the event with only its LogID populated.
func (s *Store) Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error) {
	iss, refs, err := s.IssueRefs(ctx, id)
	if err != nil {
		return core.Issue{}, nil, err
	}
	events := make([]core.Event, 0, len(refs))
	for _, ref := range refs {
		ev := core.Event{LogID: ref.LogID, IssueID: ref.IssueID, Time: ref.Ts, Log: core.Log{ID: ref.LogID}}
		if r, ok, err := s.logByID(ctx, iss.ProjectID, ref.LogID); err != nil {
			return core.Issue{}, nil, err
		} else if ok {
			l, err := s.rowToLog(iss.ProjectID, r)
			if err != nil {
				return core.Issue{}, nil, err
			}
			ev.Log = l
		}
		events = append(events, ev)
	}
	return iss, events, nil
}
