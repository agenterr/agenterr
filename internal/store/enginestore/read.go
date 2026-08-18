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
	segs, memRows, err := s.snapshotProject(ctx, projectID)
	if err != nil {
		return nil, false, err
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

// openScanWithRestart opens m as a filtered Scan, mapping a vanished
// file to the restart/corruption split this engine's reads use
// everywhere: row gone from a fresh manifest → the segment was
// legitimately replaced (compacted or pruned) and the caller must
// restart its whole attempt from a fresh snapshot; row still present
// but file missing → real corruption, loud error naming the path. After
// OpenScan returns, the scan holds the file's bytes in memory, so no
// later stage of a query can hit ENOENT.
func (s *Store) openScanWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, f segment.ScanFilter) (*segment.Scan, bool, error) {
	sc, err := segment.OpenScan(s.segPath(m.Path), f)
	if err == nil {
		return sc, false, nil
	}
	if !isSegmentNotExist(err) {
		return nil, false, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
	}
	fresh, found, ferr := s.freshSegmentByID(ctx, projectID, m.ID)
	if ferr != nil {
		return nil, false, ferr
	}
	if !found {
		return nil, true, nil
	}
	sc, err = segment.OpenScan(s.segPath(fresh.Path), f)
	if err != nil {
		if isSegmentNotExist(err) {
			return nil, false, fmt.Errorf("enginestore: segment %s missing but manifest row %d still present: %w", fresh.Path, fresh.ID, err)
		}
		return nil, false, fmt.Errorf("enginestore: read segment %s: %w", fresh.Path, err)
	}
	return sc, false, nil
}

// readSegmentRowsWithRestart returns m's rows within [sinceM, untilM]
// (optionally service-filtered), skipping the file entirely when the
// manifest row already rules it out, with the standard
// restart-on-replacement discipline (see openScanWithRestart).
func (s *Store) readSegmentRowsWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, sinceM, untilM int64, service string) ([]segment.Row, bool, error) {
	if m.MaxTs < sinceM || m.MinTs > untilM {
		return nil, false, nil
	}
	if service != "" && !contains(m.Services, service) {
		return nil, false, nil
	}
	sc, restart, err := s.openScanWithRestart(ctx, projectID, m, segment.ScanFilter{SinceM: sinceM, UntilM: untilM, Service: service})
	if err != nil || restart {
		return nil, restart, err
	}
	rows, err := sc.Rows(sc.Match)
	return rows, false, err
}

// findInSegmentWithRestart looks up logID in segment m without
// materializing any other row: only the cheap id column is decoded
// unless the id is found. Same restart/corruption discipline as
// openScanWithRestart.
func (s *Store) findInSegmentWithRestart(ctx context.Context, projectID int64, m sqlitestore.SegmentMeta, logID int64) (segment.Row, bool, bool, error) {
	sc, restart, err := s.openScanWithRestart(ctx, projectID, m, segment.ScanFilter{})
	if err != nil || restart {
		return segment.Row{}, false, restart, err
	}
	for i := 0; i < sc.Len(); i++ {
		if sc.LogID(i) == logID {
			r, err := sc.Row(i)
			if err != nil {
				return segment.Row{}, false, false, err
			}
			return r, true, false, nil
		}
	}
	return segment.Row{}, false, false, nil
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
		r, ok, restart, err := s.findInSegmentWithRestart(ctx, projectID, m, logID)
		if err != nil {
			return segment.Row{}, false, false, err
		}
		if restart {
			return segment.Row{}, false, true, nil
		}
		if ok {
			return r, true, false, nil
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
