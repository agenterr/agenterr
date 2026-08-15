package enginestore

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store"
)

// segPath resolves a manifest-relative segment path (as stored in
// sqlite.SegmentMeta.Path) to an absolute path under the engine data
// directory. Every manifest path open goes through this helper.
func (s *Store) segPath(rel string) string {
	return filepath.Join(s.dir, rel)
}

// collectRows returns all rows for a project across the memtable and
// every manifest segment overlapping [since, until] (zero times = no
// bound), optionally filtered by service via segment footers.
//
// The manifest query and the memtable snapshot are taken together under
// ps.mu so a concurrent flushProject (which holds ps.mu across
// InsertSegment → wal.Reset → mem.Reset) cannot be observed mid-flight:
// callers either see the pre-flush state (new segment absent, memtable
// still holding its rows) or the post-flush state (new segment present,
// memtable already reset) — never a mix that duplicates or drops rows.
func (s *Store) collectRows(ctx context.Context, projectID int64, since, until time.Time, service string) ([]segment.Row, error) {
	var out []segment.Row
	sinceM, untilM := int64(-1<<62), int64(1<<62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	if !until.IsZero() {
		untilM = until.UTC().UnixMicro()
	}
	ps, err := s.proj(projectID)
	if err != nil {
		return nil, err
	}
	ps.mu.Lock()
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		ps.mu.Unlock()
		return nil, err
	}
	memRows := ps.mem.Snapshot()
	ps.mu.Unlock()

	for _, m := range segs {
		if m.MaxTs < sinceM || m.MinTs > untilM {
			continue
		}
		if service != "" && !contains(m.Services, service) {
			continue
		}
		_, rows, err := segment.Read(s.segPath(m.Path))
		if err != nil {
			return nil, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
		}
		for _, r := range rows {
			if r.TsMicros >= sinceM && r.TsMicros <= untilM {
				out = append(out, r)
			}
		}
	}
	for _, r := range memRows {
		if r.TsMicros >= sinceM && r.TsMicros <= untilM {
			out = append(out, r)
		}
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
		body, ok = s.ex.Reconstruct(projectID, r.TemplateID, r.Vars)
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
	rows, err := s.collectRows(ctx, f.ProjectID, f.Since, f.Until, f.Service)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].TsMicros != rows[j].TsMicros {
			return rows[i].TsMicros > rows[j].TsMicros
		}
		return rows[i].LogID > rows[j].LogID
	})
	out := make([]core.Log, 0, limit)
	for _, r := range rows {
		if f.Service != "" && r.Service != f.Service {
			continue
		}
		if f.Environment != "" && r.Environment != f.Environment {
			continue
		}
		if r.Severity < int(f.MinSeverity) {
			continue
		}
		l, err := s.rowToLog(f.ProjectID, r)
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
// then manifest segments whose id range covers it). The memtable
// snapshot and the manifest query are taken together under ps.mu — see
// collectRows for why that is required to avoid racing flushProject.
func (s *Store) logByID(ctx context.Context, projectID, logID int64) (segment.Row, bool, error) {
	ps, err := s.proj(projectID)
	if err != nil {
		return segment.Row{}, false, err
	}
	ps.mu.Lock()
	memRows := ps.mem.Snapshot()
	segs, err := s.DB.Segments(ctx, projectID)
	ps.mu.Unlock()
	if err != nil {
		return segment.Row{}, false, err
	}
	for _, r := range memRows {
		if r.LogID == logID {
			return r, true, nil
		}
	}
	for _, m := range segs {
		if logID < m.MinLogID || logID > m.MaxLogID {
			continue
		}
		_, rows, err := segment.Read(s.segPath(m.Path))
		if err != nil {
			return segment.Row{}, false, err
		}
		for _, r := range rows {
			if r.LogID == logID {
				return r, true, nil
			}
		}
	}
	return segment.Row{}, false, nil
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
	if len(before) > n {
		before = before[:n]
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
	segs, err := s.DB.Segments(ctx, 0)
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
	logs, events, perDay, err := s.DB.RollupStats(ctx, f.ProjectID, f.Since)
	if err != nil {
		return store.Stats{}, err
	}
	sinceM := int64(-1 << 62)
	if !f.Since.IsZero() {
		sinceM = f.Since.UTC().UnixMicro()
	}
	ps, err := s.proj(f.ProjectID)
	if err != nil {
		return store.Stats{}, err
	}
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
	open, err := s.DB.OpenIssueCount(ctx, f.ProjectID)
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
	counts, err := s.DB.RollupServiceCounts(ctx, projectID, since)
	if err != nil {
		return nil, err
	}
	sinceM := int64(-1 << 62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	ps, err := s.proj(projectID)
	if err != nil {
		return nil, err
	}
	for _, r := range ps.mem.Snapshot() {
		if r.TsMicros >= sinceM {
			counts[r.Service]++
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

// Issue returns the issue plus its retained events, newest first, with
// each event's log resolved from the engine. A ref whose log has been
// pruned yields the event with only its LogID populated.
func (s *Store) Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error) {
	iss, refs, err := s.DB.IssueRefs(ctx, id)
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
