package enginestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store"
	sqlitestore "github.com/agenterr/agenterr/internal/store/sqlite"
)

// WriteBatch converts entries to engine rows (assigning monotonic log
// ids and extracting templates), makes them durable in the per-project
// WAL, serves them from the memtable, and upserts issues in SQLite.
// Rows are durable (WAL fsync'd) before this returns — the pipeline's
// ack. Issue upserts happen after log durability; a failure there
// surfaces as the batch error (the pipeline logs and drops).
func (s *Store) WriteBatch(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error) {
	byProject := map[int64][]segment.Row{}
	for i := range entries {
		e := &entries[i]
		id := s.nextLogID.Add(1)
		e.Log.ID = id
		row, err := s.toRow(ctx, e)
		if err != nil {
			return nil, err
		}
		byProject[e.Log.ProjectID] = append(byProject[e.Log.ProjectID], row)
	}

	var flushPids []int64
	for pid, rows := range byProject {
		ps, err := s.proj(pid)
		if err != nil {
			return nil, err
		}
		ps.mu.Lock()
		if err := ps.wal.Append(rows); err == nil {
			err = ps.wal.Sync()
		} else {
			ps.mu.Unlock()
			return nil, fmt.Errorf("enginestore: wal append: %w", err)
		}
		ps.mem.Append(rows)
		need := ps.mem.Len() >= s.opts.FlushRows
		ps.mu.Unlock()
		if need {
			flushPids = append(flushPids, pid)
		}
	}

	outcomes, err := s.DB.UpsertIssues(ctx, entries)
	if err != nil {
		return nil, err
	}
	for _, pid := range flushPids {
		if err := s.flushProject(pid); err != nil {
			slog.Error("enginestore: threshold flush failed", "project", pid, "error", err)
		}
	}
	return outcomes, nil
}

// toRow converts one entry to its engine row, extracting the template
// (raw fallback on ok=false) and canonicalizing attrs exactly as the
// legacy sqlite store did (json.Marshal; nil map → "null").
func (s *Store) toRow(ctx context.Context, e *store.Entry) (segment.Row, error) {
	attrs, err := json.Marshal(e.Log.Attrs)
	if err != nil {
		return segment.Row{}, fmt.Errorf("enginestore: marshal attrs: %w", err)
	}
	row := segment.Row{
		LogID: e.Log.ID, TsMicros: e.Log.Time.UTC().UnixMicro(),
		Severity: int(e.Log.Severity),
		Service:  e.Log.Service, Environment: e.Log.Environment,
		Release: e.Log.Release, TraceID: e.Log.TraceID,
		Attrs: string(attrs), IsEvent: e.IsEvent,
	}
	tid, vars, ok, err := s.ex.Extract(ctx, e.Log.ProjectID, e.Log.Body)
	if err != nil {
		return segment.Row{}, err
	}
	if ok {
		row.TemplateID, row.Vars = tid, vars
	} else {
		row.TemplateID, row.Raw = 0, e.Log.Body
	}
	return row, nil
}

// flushProject writes the project's memtable to a new immutable segment
// following the spec's sequencing: segment.Write → manifest insert →
// rollups → WAL.Reset → Memtable.Reset. An empty memtable is a no-op.
func (s *Store) flushProject(projectID int64) error {
	ps, err := s.proj(projectID)
	if err != nil {
		return err
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	rows := ps.mem.Snapshot()
	if len(rows) == 0 {
		return nil
	}
	ps.seq++
	rel := filepath.Join("segments", fmt.Sprintf("%d", projectID), fmt.Sprintf("%06d-%d.seg", ps.seq, rows[0].LogID))
	abs := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("enginestore: mkdir segment dir: %w", err)
	}
	foot, err := segment.Write(abs, rows)
	if err != nil {
		return fmt.Errorf("enginestore: write segment: %w", err)
	}
	meta := sqlitestore.SegmentMeta{
		ProjectID: projectID, Path: rel,
		MinTs: foot.MinTs, MaxTs: foot.MaxTs,
		MinLogID: foot.MinLogID, MaxLogID: foot.MaxLogID,
		Count: int64(foot.Count), Events: foot.Events, Services: foot.Services,
	}
	if _, err := s.DB.InsertSegment(context.Background(), meta); err != nil {
		// The file exists but the manifest doesn't know it: remove the
		// orphan so a retry doesn't collide, keep memtable+WAL intact.
		_ = os.Remove(abs)
		return err
	}
	if err := s.DB.AddRollups(context.Background(), rollupsFrom(projectID, rows)); err != nil {
		slog.Error("enginestore: rollup update failed (counts will undercount)", "error", err)
	}
	if err := ps.wal.Reset(); err != nil {
		return fmt.Errorf("enginestore: wal reset: %w", err)
	}
	ps.mem.Reset()
	return nil
}

// rollupsFrom aggregates rows into hourly rollup increments for
// projectID. segment.Row carries no ProjectID (spec decision), so the
// project is threaded in by the caller rather than read off each row.
func rollupsFrom(projectID int64, rows []segment.Row) map[sqlitestore.RollupKey]sqlitestore.RollupAdd {
	out := map[sqlitestore.RollupKey]sqlitestore.RollupAdd{}
	for _, r := range rows {
		hour := time.UnixMicro(r.TsMicros).UTC().Format("2006-01-02T15")
		k := sqlitestore.RollupKey{ProjectID: projectID, Service: r.Service, Severity: r.Severity, Hour: hour}
		a := out[k]
		a.Logs++
		if r.IsEvent {
			a.Events++
		}
		out[k] = a
	}
	return out
}

// SearchLogs (minimal, Task-2 scope): project + time filters only —
// Task 3 replaces this with the full implementation.
func (s *Store) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	rows, err := s.collectRows(ctx, f.ProjectID, f.Since, f.Until, "")
	if err != nil {
		return nil, err
	}
	out := make([]core.Log, 0, len(rows))
	for _, r := range rows {
		l, err := s.rowToLog(f.ProjectID, r)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, nil
}

// collectRows returns all rows for a project across the memtable and
// every manifest segment overlapping [since, until] (zero times = no
// bound), optionally filtered by service via segment footers.
func (s *Store) collectRows(ctx context.Context, projectID int64, since, until time.Time, service string) ([]segment.Row, error) {
	var out []segment.Row
	sinceM, untilM := int64(-1<<62), int64(1<<62)
	if !since.IsZero() {
		sinceM = since.UTC().UnixMicro()
	}
	if !until.IsZero() {
		untilM = until.UTC().UnixMicro()
	}
	segs, err := s.DB.Segments(ctx, projectID)
	if err != nil {
		return nil, err
	}
	for _, m := range segs {
		if m.MaxTs < sinceM || m.MinTs > untilM {
			continue
		}
		if service != "" && !contains(m.Services, service) {
			continue
		}
		_, rows, err := segment.Read(filepath.Join(s.dir, m.Path))
		if err != nil {
			return nil, fmt.Errorf("enginestore: read segment %s: %w", m.Path, err)
		}
		for _, r := range rows {
			if r.TsMicros >= sinceM && r.TsMicros <= untilM {
				out = append(out, r)
			}
		}
	}
	ps, err := s.proj(projectID)
	if err != nil {
		return nil, err
	}
	for _, r := range ps.mem.Snapshot() {
		if r.TsMicros >= sinceM && r.TsMicros <= untilM {
			out = append(out, r)
		}
	}
	return out, nil
}

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
