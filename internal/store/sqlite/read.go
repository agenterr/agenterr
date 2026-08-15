package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

const issueColumns = `id, project_id, fingerprint, title, severity, status, first_seen, last_seen, count`

const selectIssueByID = `SELECT ` + issueColumns + ` FROM issues WHERE id = ?`

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
		b.WriteString(` AND id IN (SELECT issue_id FROM issue_events WHERE environment = ?)`)
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
	defer func() { _ = rows.Close() }()

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
