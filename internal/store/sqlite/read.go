package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

const logColumns = `id, project_id, ts, severity, body, service, environment, release, trace_id, attrs`

const issueColumns = `id, project_id, fingerprint, title, severity, status, first_seen, last_seen, count`

const selectIssueByID = `SELECT ` + issueColumns + ` FROM issues WHERE id = ?`

var selectEventsByIssue = `
SELECT e.log_id, e.issue_id, e.ts, ` + logColumnsWithPrefix("l") + `
FROM events e JOIN logs l ON l.id = e.log_id
WHERE e.issue_id = ?
ORDER BY e.ts DESC, e.id DESC`

func logColumnsWithPrefix(alias string) string {
	cols := strings.Split(logColumns, ", ")
	prefixed := make([]string, len(cols))
	for i, c := range cols {
		prefixed[i] = alias + "." + c
	}
	return strings.Join(prefixed, ", ")
}

// Issues returns issues matching f, sorted by last_seen descending.
func (db *DB) Issues(ctx context.Context, f store.IssueFilter) ([]core.Issue, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}

	var b strings.Builder
	b.WriteString("SELECT " + issueColumns + " FROM issues WHERE 1=1")
	var args []any

	if f.ProjectID != 0 {
		b.WriteString(" AND project_id = ?")
		args = append(args, f.ProjectID)
	}
	if f.Environment != "" {
		b.WriteString(` AND id IN (SELECT issue_id FROM logs WHERE issue_id IS NOT NULL AND environment = ?)`)
		args = append(args, f.Environment)
	}
	if f.Status != "" {
		b.WriteString(" AND status = ?")
		args = append(args, string(f.Status))
	}
	if !f.Since.IsZero() {
		b.WriteString(" AND last_seen >= ?")
		args = append(args, f.Since.UTC().Format(time.RFC3339Nano))
	}
	if !f.Until.IsZero() {
		b.WriteString(" AND last_seen <= ?")
		args = append(args, f.Until.UTC().Format(time.RFC3339Nano))
	}
	b.WriteString(" ORDER BY last_seen DESC LIMIT ?")
	args = append(args, limit)

	rows, err := db.sql.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: issues query: %w", err)
	}
	defer rows.Close()

	var out []core.Issue
	for rows.Next() {
		iss, err := scanIssue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, iss)
	}
	return out, rows.Err()
}

// Issue returns the issue with the given ID plus its retained event samples
// (newest first), or store.ErrNotFound if no such issue exists.
func (db *DB) Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error) {
	row := db.sql.QueryRowContext(ctx, selectIssueByID, id)
	iss, err := scanIssue(row)
	if err == sql.ErrNoRows {
		return core.Issue{}, nil, store.ErrNotFound
	}
	if err != nil {
		return core.Issue{}, nil, fmt.Errorf("sqlite: issue: %w", err)
	}

	rows, err := db.sql.QueryContext(ctx, selectEventsByIssue, id)
	if err != nil {
		return core.Issue{}, nil, fmt.Errorf("sqlite: issue events: %w", err)
	}
	defer rows.Close()

	var events []core.Event
	for rows.Next() {
		var ev core.Event
		var evTsStr, logTsStr, attrsJSON string
		var log core.Log
		var severity int
		if err := rows.Scan(&ev.LogID, &ev.IssueID, &evTsStr,
			&log.ID, &log.ProjectID, &logTsStr, &severity, &log.Body,
			&log.Service, &log.Environment, &log.Release, &log.TraceID, &attrsJSON); err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: scan event: %w", err)
		}
		evTs, err := time.Parse(time.RFC3339Nano, evTsStr)
		if err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: parse event ts: %w", err)
		}
		logTs, err := time.Parse(time.RFC3339Nano, logTsStr)
		if err != nil {
			return core.Issue{}, nil, fmt.Errorf("sqlite: parse log ts: %w", err)
		}
		ev.Time = evTs
		log.Time = logTs
		log.Severity = core.Severity(severity)
		if err := unmarshalAttrs(attrsJSON, &log.Attrs); err != nil {
			return core.Issue{}, nil, err
		}
		ev.Log = log
		events = append(events, ev)
	}
	return iss, events, rows.Err()
}

// rowScanner abstracts *sql.Row and *sql.Rows for shared scan logic.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanIssue(r rowScanner) (core.Issue, error) {
	var iss core.Issue
	var firstSeen, lastSeen string
	var status string
	var severity int
	if err := r.Scan(&iss.ID, &iss.ProjectID, &iss.Fingerprint, &iss.Title, &severity, &status, &firstSeen, &lastSeen, &iss.Count); err != nil {
		return core.Issue{}, err
	}
	iss.Severity = core.Severity(severity)
	iss.Status = core.IssueStatus(status)
	fs, err := time.Parse(time.RFC3339Nano, firstSeen)
	if err != nil {
		return core.Issue{}, fmt.Errorf("sqlite: parse first_seen: %w", err)
	}
	ls, err := time.Parse(time.RFC3339Nano, lastSeen)
	if err != nil {
		return core.Issue{}, fmt.Errorf("sqlite: parse last_seen: %w", err)
	}
	iss.FirstSeen = fs
	iss.LastSeen = ls
	return iss, nil
}

func scanLog(r rowScanner) (core.Log, error) {
	var l core.Log
	var tsStr, attrsJSON string
	var severity int
	if err := r.Scan(&l.ID, &l.ProjectID, &tsStr, &severity, &l.Body, &l.Service, &l.Environment, &l.Release, &l.TraceID, &attrsJSON); err != nil {
		return core.Log{}, err
	}
	l.Severity = core.Severity(severity)
	ts, err := time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return core.Log{}, fmt.Errorf("sqlite: parse ts: %w", err)
	}
	l.Time = ts
	if err := unmarshalAttrs(attrsJSON, &l.Attrs); err != nil {
		return core.Log{}, err
	}
	return l, nil
}

func unmarshalAttrs(raw string, out *map[string]string) error {
	if raw == "" || raw == "null" {
		*out = nil
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return fmt.Errorf("sqlite: unmarshal attrs: %w", err)
	}
	if len(m) == 0 {
		m = nil
	}
	*out = m
	return nil
}

// SearchLogs returns logs matching f, most recent first, capped at f.Limit
// (0 defaults to 50).
func (db *DB) SearchLogs(ctx context.Context, f store.LogFilter) ([]core.Log, error) {
	limit := f.Limit
	if limit == 0 {
		limit = 50
	}

	var b strings.Builder
	var args []any

	if f.Query != "" {
		b.WriteString("SELECT " + logColumnsWithPrefix("logs") + ` FROM logs
JOIN logs_fts ON logs_fts.rowid = logs.id
WHERE logs_fts MATCH ?`)
		args = append(args, quoteFTS(f.Query))
	} else {
		b.WriteString("SELECT " + logColumns + " FROM logs WHERE 1=1")
	}

	if f.ProjectID != 0 {
		b.WriteString(" AND project_id = ?")
		args = append(args, f.ProjectID)
	}
	if f.MinSeverity != 0 {
		b.WriteString(" AND severity >= ?")
		args = append(args, int(f.MinSeverity))
	}
	if f.Service != "" {
		b.WriteString(" AND service = ?")
		args = append(args, f.Service)
	}
	if f.Environment != "" {
		b.WriteString(" AND environment = ?")
		args = append(args, f.Environment)
	}
	if !f.Since.IsZero() {
		b.WriteString(" AND ts >= ?")
		args = append(args, f.Since.UTC().Format(time.RFC3339Nano))
	}
	if !f.Until.IsZero() {
		b.WriteString(" AND ts <= ?")
		args = append(args, f.Until.UTC().Format(time.RFC3339Nano))
	}
	b.WriteString(" ORDER BY ts DESC LIMIT ?")
	args = append(args, limit)

	rows, err := db.sql.QueryContext(ctx, b.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: search logs: %w", err)
	}
	defer rows.Close()

	var out []core.Log
	for rows.Next() {
		l, err := scanLog(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// quoteFTS wraps an FTS5 query string in double quotes so user input is
// treated as a literal phrase rather than parsed as FTS5 query syntax
// (which would error on tokens like "-", "*", or unbalanced quotes).
func quoteFTS(q string) string {
	return `"` + strings.ReplaceAll(q, `"`, `""`) + `"`
}

const selectLogMeta = `SELECT project_id, service, ts FROM logs WHERE id = ?`

const selectContextBefore = `
SELECT ` + logColumns + ` FROM logs
WHERE project_id = ? AND service = ? AND ts <= ?
ORDER BY ts DESC LIMIT ?`

const selectContextAfter = `
SELECT ` + logColumns + ` FROM logs
WHERE project_id = ? AND service = ? AND ts > ?
ORDER BY ts ASC LIMIT ?`

// LogContext returns up to n logs before and n logs after the log with the
// given ID (inclusive of the target itself), restricted to the same
// project and service, ordered by time ascending.
func (db *DB) LogContext(ctx context.Context, logID int64, n int) ([]core.Log, error) {
	var projectID int64
	var service, ts string
	if err := db.sql.QueryRowContext(ctx, selectLogMeta, logID).Scan(&projectID, &service, &ts); err != nil {
		if err == sql.ErrNoRows {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("sqlite: log meta: %w", err)
	}

	beforeRows, err := db.sql.QueryContext(ctx, selectContextBefore, projectID, service, ts, n+1)
	if err != nil {
		return nil, fmt.Errorf("sqlite: context before: %w", err)
	}
	var before []core.Log
	for beforeRows.Next() {
		l, err := scanLog(beforeRows)
		if err != nil {
			beforeRows.Close()
			return nil, err
		}
		before = append(before, l)
	}
	if err := beforeRows.Err(); err != nil {
		beforeRows.Close()
		return nil, err
	}
	beforeRows.Close()

	afterRows, err := db.sql.QueryContext(ctx, selectContextAfter, projectID, service, ts, n)
	if err != nil {
		return nil, fmt.Errorf("sqlite: context after: %w", err)
	}
	var after []core.Log
	for afterRows.Next() {
		l, err := scanLog(afterRows)
		if err != nil {
			afterRows.Close()
			return nil, err
		}
		after = append(after, l)
	}
	if err := afterRows.Err(); err != nil {
		afterRows.Close()
		return nil, err
	}
	afterRows.Close()

	// before is DESC (target first, then older); reverse to ascending.
	for i, j := 0, len(before)-1; i < j; i, j = i+1, j-1 {
		before[i], before[j] = before[j], before[i]
	}

	out := make([]core.Log, 0, len(before)+len(after))
	out = append(out, before...)
	out = append(out, after...)
	return out, nil
}

const statsLogs = `SELECT COUNT(*) FROM logs WHERE project_id = ? AND ts >= ?`
const statsEvents = `SELECT COUNT(*) FROM events e JOIN logs l ON l.id = e.log_id WHERE l.project_id = ? AND e.ts >= ?`
const statsOpenIssues = `SELECT COUNT(*) FROM issues WHERE project_id = ? AND status = 'open'`
const statsPerDay = `
SELECT substr(ts, 1, 10) AS day,
  SUM(1) AS logs,
  SUM(CASE WHEN issue_id IS NOT NULL THEN 1 ELSE 0 END) AS events
FROM logs
WHERE project_id = ? AND ts >= ?
GROUP BY day
ORDER BY day ASC`

// Stats computes aggregate log/event/issue counts and a per-day breakdown
// for the given project since f.Since.
func (db *DB) Stats(ctx context.Context, f store.StatsFilter) (store.Stats, error) {
	since := f.Since.UTC().Format(time.RFC3339Nano)

	var stats store.Stats
	if err := db.sql.QueryRowContext(ctx, statsLogs, f.ProjectID, since).Scan(&stats.Logs); err != nil {
		return store.Stats{}, fmt.Errorf("sqlite: stats logs: %w", err)
	}
	if err := db.sql.QueryRowContext(ctx, statsEvents, f.ProjectID, since).Scan(&stats.Events); err != nil {
		return store.Stats{}, fmt.Errorf("sqlite: stats events: %w", err)
	}
	if err := db.sql.QueryRowContext(ctx, statsOpenIssues, f.ProjectID).Scan(&stats.OpenIssues); err != nil {
		return store.Stats{}, fmt.Errorf("sqlite: stats open issues: %w", err)
	}

	rows, err := db.sql.QueryContext(ctx, statsPerDay, f.ProjectID, since)
	if err != nil {
		return store.Stats{}, fmt.Errorf("sqlite: stats per day: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d store.DayCount
		if err := rows.Scan(&d.Day, &d.Logs, &d.Events); err != nil {
			return store.Stats{}, fmt.Errorf("sqlite: scan day count: %w", err)
		}
		stats.PerDay = append(stats.PerDay, d)
	}
	if err := rows.Err(); err != nil {
		return store.Stats{}, err
	}
	return stats, nil
}
