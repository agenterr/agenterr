package enginestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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
//
// Multi-project batches are processed sequentially, one project at a
// time. If a WAL append/sync fails partway through, WriteBatch returns
// immediately: rows for projects already processed are durable in their
// WAL and already visible via the memtable, rows for the failing and any
// remaining projects are not written at all, and issue upserts are
// skipped for the ENTIRE batch (including entries from already-durable
// projects). This is a partial-failure state, not an atomic one. It is
// safe today because the only caller, the ingest pipeline, logs and
// drops a failed batch outright and never retries it — retrying the same
// entries would re-append the already-durable rows under new log ids
// (harmless duplication) but skip issue accounting a second time is not
// guaranteed either way. Any future caller MUST NOT retry a failed batch
// with the same entries; a redesign (e.g. per-project error slices, or
// buffering all WAL writes before any Sync) is required before this
// method can safely support partial-batch retry.
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
		if err := ps.wal.Append(rows); err != nil {
			ps.mu.Unlock()
			return nil, fmt.Errorf("enginestore: wal append: %w", err)
		}
		if err := ps.wal.Sync(); err != nil {
			ps.mu.Unlock()
			return nil, fmt.Errorf("enginestore: wal sync: %w", err)
		}
		ps.mem.Append(rows)
		need := ps.mem.Len() >= s.opts.FlushRows
		ps.mu.Unlock()
		if need {
			flushPids = append(flushPids, pid)
		}
	}

	outcomes, err := s.UpsertIssues(ctx, entries)
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
	var rawRows int64
	for _, r := range rows {
		if r.TemplateID == 0 {
			rawRows++
		}
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return fmt.Errorf("enginestore: stat segment: %w", err)
	}
	meta := sqlitestore.SegmentMeta{
		ProjectID: projectID, Path: rel,
		MinTs: foot.MinTs, MaxTs: foot.MaxTs,
		MinLogID: foot.MinLogID, MaxLogID: foot.MaxLogID,
		Count: int64(foot.Count), Events: foot.Events, Services: foot.Services,
		RawRows: rawRows, SizeBytes: fi.Size(),
	}
	if _, err := s.InsertSegment(context.Background(), meta); err != nil {
		// The file exists but the manifest doesn't know it: remove the
		// orphan so a retry doesn't collide, keep memtable+WAL intact.
		_ = os.Remove(abs)
		return fmt.Errorf("enginestore: insert segment manifest: %w", err)
	}
	if err := s.AddRollups(context.Background(), rollupsFrom(projectID, rows)); err != nil {
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
