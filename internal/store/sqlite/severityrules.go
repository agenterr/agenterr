package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

const severityRuleColumns = `id, project_id, service, pattern, severity, enabled, lifted_count, created_at`

const selectSeverityRulesAll = `SELECT ` + severityRuleColumns + ` FROM severity_rules ORDER BY id`

const selectSeverityRulesByProject = `SELECT ` + severityRuleColumns + ` FROM severity_rules WHERE project_id = ? ORDER BY id`

const insertSeverityRule = `
INSERT INTO severity_rules (project_id, service, pattern, severity, enabled, created_at)
VALUES (?, ?, ?, ?, ?, ?)`

const updateSeverityRule = `
UPDATE severity_rules SET project_id = ?, service = ?, pattern = ?, severity = ?, enabled = ?
WHERE id = ?`

const selectSeverityRuleByID = `SELECT ` + severityRuleColumns + ` FROM severity_rules WHERE id = ?`

const deleteSeverityRuleStmt = `DELETE FROM severity_rules WHERE id = ?`

const addSeverityLiftStmt = `UPDATE severity_rules SET lifted_count = lifted_count + ? WHERE id = ?`

// SeverityRules returns rules for projectID (0 = all projects), ordered by
// ascending ID.
func (db *DB) SeverityRules(ctx context.Context, projectID int64) ([]store.SeverityRuleRow, error) {
	var rows *sql.Rows
	var err error
	if projectID == 0 {
		rows, err = db.sql.QueryContext(ctx, selectSeverityRulesAll)
	} else {
		rows, err = db.sql.QueryContext(ctx, selectSeverityRulesByProject, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: severity rules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []store.SeverityRuleRow
	for rows.Next() {
		r, err := scanSeverityRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertSeverityRule inserts r (ID 0) or updates the existing row (ID set),
// returning the stored row. Updating a missing ID returns store.ErrNotFound.
// Invalid patterns or severity values are rejected before touching the database.
func (db *DB) UpsertSeverityRule(ctx context.Context, r core.SeverityRule) (store.SeverityRuleRow, error) {
	// Validate pattern is not empty
	if r.Pattern == "" {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: severity rule pattern cannot be empty")
	}

	// Validate pattern compiles as a Go regexp
	if _, err := regexp.Compile(r.Pattern); err != nil {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: invalid severity rule pattern: %w", err)
	}

	// Validate severity is > SeverityInfo
	if r.Severity <= core.SeverityInfo {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: severity rule must lift above info")
	}

	severity := strings.ToLower(r.Severity.String())
	enabled := 0
	if r.Enabled {
		enabled = 1
	}

	if r.ID == 0 {
		createdAt := time.Now().UTC().Format(time.RFC3339Nano)
		res, err := db.sql.ExecContext(ctx, insertSeverityRule,
			r.ProjectID, r.Service, r.Pattern, severity, enabled, createdAt)
		if err != nil {
			return store.SeverityRuleRow{}, fmt.Errorf("sqlite: insert severity rule: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return store.SeverityRuleRow{}, fmt.Errorf("sqlite: severity rule last insert id: %w", err)
		}
		return db.severityRuleByID(ctx, id)
	}

	res, err := db.sql.ExecContext(ctx, updateSeverityRule,
		r.ProjectID, r.Service, r.Pattern, severity, enabled, r.ID)
	if err != nil {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: update severity rule: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: update severity rule rows affected: %w", err)
	}
	if n == 0 {
		return store.SeverityRuleRow{}, store.ErrNotFound
	}
	return db.severityRuleByID(ctx, r.ID)
}

func (db *DB) severityRuleByID(ctx context.Context, id int64) (store.SeverityRuleRow, error) {
	row := db.sql.QueryRowContext(ctx, selectSeverityRuleByID, id)
	r, err := scanSeverityRule(row)
	if err == sql.ErrNoRows {
		return store.SeverityRuleRow{}, store.ErrNotFound
	}
	if err != nil {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: severity rule by id: %w", err)
	}
	return r, nil
}

func scanSeverityRule(r rowScanner) (store.SeverityRuleRow, error) {
	var row store.SeverityRuleRow
	var severity, createdAt string
	var enabled int
	if err := r.Scan(&row.ID, &row.ProjectID, &row.Service, &row.Pattern, &severity, &enabled, &row.LiftedCount, &createdAt); err != nil {
		return store.SeverityRuleRow{}, err
	}
	row.Severity = core.ParseSeverity(severity)
	row.Enabled = enabled != 0
	ts, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return store.SeverityRuleRow{}, fmt.Errorf("sqlite: parse severity rule created_at: %w", err)
	}
	row.CreatedAt = ts
	return row, nil
}

// DeleteSeverityRule deletes the rule with the given ID, or returns
// store.ErrNotFound if no such rule exists.
func (db *DB) DeleteSeverityRule(ctx context.Context, id int64) error {
	res, err := db.sql.ExecContext(ctx, deleteSeverityRuleStmt, id)
	if err != nil {
		return fmt.Errorf("sqlite: delete severity rule: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete severity rule rows affected: %w", err)
	}
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// AddSeverityLifts atomically adds the given per-rule lift counts in a single
// transaction. Unknown rule IDs (rule deleted since counting began) are
// skipped silently rather than erroring — lifting must be fail-open, and a
// missing counter target is not a reason to lose the rest of the batch.
func (db *DB) AddSeverityLifts(ctx context.Context, counts map[int64]int64) error {
	if len(counts) == 0 {
		return nil
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: add severity lifts begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for id, n := range counts {
		if _, err := tx.ExecContext(ctx, addSeverityLiftStmt, n, id); err != nil {
			return fmt.Errorf("sqlite: add severity lifts: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: add severity lifts commit: %w", err)
	}
	return nil
}
