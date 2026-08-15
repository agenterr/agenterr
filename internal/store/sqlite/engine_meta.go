package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/template"
)

// This file holds the metadata surface the template storage engine
// (internal/store/enginestore) needs from SQLite: template persistence,
// the segment manifest, hourly rollups, and issue upserts decoupled from
// the legacy logs table (event samples live in issue_events, which has no
// FK to logs — log bodies are the engine's).

// InsertTemplate persists one template text for a project and returns its
// id. Text is stored as a BLOB because it embeds NUL wildcard bytes.
func (db *DB) InsertTemplate(ctx context.Context, projectID int64, text string) (int64, error) {
	res, err := db.sql.ExecContext(ctx,
		`INSERT INTO templates (project_id, text, created_at) VALUES (?, ?, ?)`,
		projectID, []byte(text), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("sqlite: insert template: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlite: template id: %w", err)
	}
	return id, nil
}

// LoadTemplates returns a project's templates ordered by ascending id.
func (db *DB) LoadTemplates(ctx context.Context, projectID int64) ([]template.Row, error) {
	rows, err := db.sql.QueryContext(ctx,
		`SELECT id, text FROM templates WHERE project_id = ? ORDER BY id`, projectID)
	if err != nil {
		return nil, fmt.Errorf("sqlite: load templates: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []template.Row
	for rows.Next() {
		var r template.Row
		var text []byte
		if err := rows.Scan(&r.ID, &text); err != nil {
			return nil, fmt.Errorf("sqlite: scan template: %w", err)
		}
		r.Text = string(text)
		out = append(out, r)
	}
	return out, rows.Err()
}

// SegmentMeta is one row of the segment manifest — the durable record of
// an on-disk segment file and its pruning metadata.
type SegmentMeta struct {
	ID        int64
	ProjectID int64
	Path      string // relative to the engine data dir
	MinTs     int64  // epoch micros
	MaxTs     int64
	MinLogID  int64
	MaxLogID  int64
	Count     int64
	Events    int64
	Services  []string
}

// InsertSegment records a freshly written segment and returns its
// manifest id.
func (db *DB) InsertSegment(ctx context.Context, m SegmentMeta) (int64, error) {
	svc, err := json.Marshal(m.Services)
	if err != nil {
		return 0, fmt.Errorf("sqlite: marshal services: %w", err)
	}
	res, err := db.sql.ExecContext(ctx, `
INSERT INTO segment_manifest (project_id, path, min_ts, max_ts, min_log_id, max_log_id, count, events, services, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ProjectID, m.Path, m.MinTs, m.MaxTs, m.MinLogID, m.MaxLogID, m.Count, m.Events,
		string(svc), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("sqlite: insert segment: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("sqlite: segment id: %w", err)
	}
	return id, nil
}

// Segments lists the manifest for one project (0 = all projects),
// ordered by ascending MinTs.
func (db *DB) Segments(ctx context.Context, projectID int64) ([]SegmentMeta, error) {
	q := `SELECT id, project_id, path, min_ts, max_ts, min_log_id, max_log_id, count, events, services
FROM segment_manifest`
	var args []any
	if projectID != 0 {
		q += ` WHERE project_id = ?`
		args = append(args, projectID)
	}
	q += ` ORDER BY min_ts ASC`
	rows, err := db.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: segments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SegmentMeta
	for rows.Next() {
		var m SegmentMeta
		var svc string
		if err := rows.Scan(&m.ID, &m.ProjectID, &m.Path, &m.MinTs, &m.MaxTs,
			&m.MinLogID, &m.MaxLogID, &m.Count, &m.Events, &svc); err != nil {
			return nil, fmt.Errorf("sqlite: scan segment: %w", err)
		}
		if err := json.Unmarshal([]byte(svc), &m.Services); err != nil {
			return nil, fmt.Errorf("sqlite: segment services: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteSegment removes one manifest row (after its file is deleted or
// replaced by Prune).
func (db *DB) DeleteSegment(ctx context.Context, id int64) error {
	if _, err := db.sql.ExecContext(ctx, `DELETE FROM segment_manifest WHERE id = ?`, id); err != nil {
		return fmt.Errorf("sqlite: delete segment: %w", err)
	}
	return nil
}

// RollupKey identifies one hourly rollup bucket. Hour is UTC,
// formatted "2006-01-02T15".
type RollupKey struct {
	ProjectID int64
	Service   string
	Severity  int
	Hour      string
}

// RollupAdd is the increment AddRollups applies to a bucket.
type RollupAdd struct {
	Logs   int64
	Events int64
}

// AddRollups accumulates hourly log/event counts, upserting buckets.
// Applied in one transaction at segment-flush time.
func (db *DB) AddRollups(ctx context.Context, counts map[RollupKey]RollupAdd) error {
	if len(counts) == 0 {
		return nil
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: rollups begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	for k, v := range counts {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO log_rollups (project_id, service, severity, hour, logs, events)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id, service, severity, hour) DO UPDATE SET
  logs = logs + excluded.logs, events = events + excluded.events`,
			k.ProjectID, k.Service, k.Severity, k.Hour, v.Logs, v.Events); err != nil {
			return fmt.Errorf("sqlite: rollup upsert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: rollups commit: %w", err)
	}
	return nil
}

// RollupStats sums flushed log/event counts for a project since the
// given time, plus per-day buckets keyed "YYYY-MM-DD".
func (db *DB) RollupStats(ctx context.Context, projectID int64, since time.Time) (int64, int64, map[string]store.DayCount, error) {
	hour := since.UTC().Truncate(time.Hour).Format("2006-01-02T15")
	rows, err := db.sql.QueryContext(ctx, `
SELECT substr(hour, 1, 10) AS day, SUM(logs), SUM(events)
FROM log_rollups WHERE project_id = ? AND hour >= ?
GROUP BY day`, projectID, hour)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("sqlite: rollup stats: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var logs, events int64
	perDay := map[string]store.DayCount{}
	for rows.Next() {
		var d store.DayCount
		if err := rows.Scan(&d.Day, &d.Logs, &d.Events); err != nil {
			return 0, 0, nil, fmt.Errorf("sqlite: scan rollup: %w", err)
		}
		perDay[d.Day] = d
		logs += d.Logs
		events += d.Events
	}
	return logs, events, perDay, rows.Err()
}

// RollupServiceCounts sums flushed per-service log counts since the
// given time.
func (db *DB) RollupServiceCounts(ctx context.Context, projectID int64, since time.Time) (map[string]int64, error) {
	hour := since.UTC().Truncate(time.Hour).Format("2006-01-02T15")
	rows, err := db.sql.QueryContext(ctx, `
SELECT service, SUM(logs) FROM log_rollups
WHERE project_id = ? AND hour >= ? GROUP BY service`, projectID, hour)
	if err != nil {
		return nil, fmt.Errorf("sqlite: rollup services: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int64{}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, fmt.Errorf("sqlite: scan rollup service: %w", err)
		}
		out[s] = n
	}
	return out, rows.Err()
}

// UpsertIssues is the issue half of the legacy WriteBatch, decoupled from
// the logs table: same upsert/reopen/count/outcome semantics (see
// store.Writer), but event samples are recorded in issue_events keyed by
// the engine-assigned Log.ID, trimmed to the 50 newest per issue.
func (db *DB) UpsertIssues(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error) {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: upsert issues begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var outcomes []store.IssueOutcome
	for _, e := range entries {
		if !e.IsEvent {
			continue
		}
		o, err := upsertIssueEvent(ctx, tx, e)
		if err != nil {
			return nil, err
		}
		outcomes = append(outcomes, o)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: upsert issues commit: %w", err)
	}
	return outcomes, nil
}

func upsertIssueEvent(ctx context.Context, tx *sql.Tx, e store.Entry) (store.IssueOutcome, error) {
	ts := e.Log.Time.UTC().Format(time.RFC3339Nano)

	var prevStatus string
	err := tx.QueryRowContext(ctx, selectIssueStatusByFingerprint, e.Log.ProjectID, e.Fingerprint).Scan(&prevStatus)
	existed := true
	switch {
	case errors.Is(err, sql.ErrNoRows):
		existed = false
	case err != nil:
		return store.IssueOutcome{}, fmt.Errorf("sqlite: select issue status: %w", err)
	}

	if _, err := tx.ExecContext(ctx, upsertIssue,
		e.Log.ProjectID, e.Fingerprint, e.Title, int(e.Log.Severity), ts, ts); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: upsert issue: %w", err)
	}
	var issueID int64
	if err := tx.QueryRowContext(ctx, selectIssueIDByFingerprint, e.Log.ProjectID, e.Fingerprint).Scan(&issueID); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: select issue id: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO issue_events (issue_id, log_id, project_id, environment, ts) VALUES (?, ?, ?, ?, ?)`,
		issueID, e.Log.ID, e.Log.ProjectID, e.Log.Environment, ts); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: insert issue event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM issue_events WHERE issue_id = ? AND id NOT IN (
  SELECT id FROM issue_events WHERE issue_id = ? ORDER BY ts DESC, id DESC LIMIT 50)`,
		issueID, issueID); err != nil {
		return store.IssueOutcome{}, fmt.Errorf("sqlite: trim issue events: %w", err)
	}

	return store.IssueOutcome{
		IssueID:  issueID,
		New:      !existed,
		Reopened: existed && prevStatus == string(core.StatusResolved),
	}, nil
}

// EventRef is one retained event sample: the issue/log linkage without
// the log body (the engine resolves bodies by LogID).
type EventRef struct {
	LogID   int64
	IssueID int64
	Ts      time.Time
}

// IssueRefs returns an issue plus its retained event refs, newest first,
// or store.ErrNotFound.
func (db *DB) IssueRefs(ctx context.Context, id int64) (core.Issue, []EventRef, error) {
	row := db.sql.QueryRowContext(ctx, selectIssueByID, id)
	iss, err := scanIssue(row)
	if errors.Is(err, sql.ErrNoRows) {
		return core.Issue{}, nil, store.ErrNotFound
	}
	if err != nil {
		return core.Issue{}, nil, err
	}
	rows, err := db.sql.QueryContext(ctx,
		`SELECT log_id, issue_id, ts FROM issue_events WHERE issue_id = ? ORDER BY ts DESC, id DESC`, id)
	if err != nil {
		return core.Issue{}, nil, fmt.Errorf("sqlite: issue refs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var refs []EventRef
	for rows.Next() {
		var r EventRef
		var ts string
		if err := rows.Scan(&r.LogID, &r.IssueID, &ts); err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: scan issue ref: %w", err)
		}
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: issue ref ts: %w", err)
		}
		r.Ts = t
		refs = append(refs, r)
	}
	return iss, refs, rows.Err()
}

// OpenIssueCount counts a project's open issues.
func (db *DB) OpenIssueCount(ctx context.Context, projectID int64) (int64, error) {
	var n int64
	err := db.sql.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM issues WHERE project_id = ? AND status = 'open'`, projectID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("sqlite: open issue count: %w", err)
	}
	return n, nil
}

// DeleteIssueEventsBefore removes event refs older than before for a
// project — the retention companion to the engine's segment pruning.
func (db *DB) DeleteIssueEventsBefore(ctx context.Context, projectID int64, before time.Time) error {
	if _, err := db.sql.ExecContext(ctx,
		`DELETE FROM issue_events WHERE project_id = ? AND ts < ?`,
		projectID, before.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("sqlite: delete issue events: %w", err)
	}
	return nil
}
