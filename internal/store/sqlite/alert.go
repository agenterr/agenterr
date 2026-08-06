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

const alertRuleColumns = `id, project_id, name, kind, service, environment, min_severity, n, window_minutes, cooldown_seconds, url, headers, enabled, last_fired, last_error, created_at`

const selectAlertRulesAll = `SELECT ` + alertRuleColumns + ` FROM alert_rules ORDER BY id`

const selectAlertRulesByProject = `SELECT ` + alertRuleColumns + ` FROM alert_rules WHERE project_id = ? ORDER BY id`

const insertAlertRule = `
INSERT INTO alert_rules (project_id, name, kind, service, environment, min_severity, n, window_minutes, cooldown_seconds, url, headers, enabled, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const updateAlertRule = `
UPDATE alert_rules SET project_id = ?, name = ?, kind = ?, service = ?, environment = ?, min_severity = ?, n = ?, window_minutes = ?, cooldown_seconds = ?, url = ?, headers = ?, enabled = ?
WHERE id = ?`

const selectAlertRuleByID = `SELECT ` + alertRuleColumns + ` FROM alert_rules WHERE id = ?`

const deleteAlertRuleStmt = `DELETE FROM alert_rules WHERE id = ?`

const recordAlertResultStmt = `UPDATE alert_rules SET last_fired = ?, last_error = ? WHERE id = ?`

// validAlertKinds mirrors the alert_rules.kind CHECK constraint: a typed
// validation error here is friendlier than letting the constraint fail.
var validAlertKinds = map[core.AlertRuleKind]bool{
	core.AlertNewIssue:   true,
	core.AlertRegression: true,
	core.AlertThreshold:  true,
}

// AlertRules returns rules for projectID (0 = all projects), ordered by
// ascending ID.
func (db *DB) AlertRules(ctx context.Context, projectID int64) ([]store.AlertRuleRow, error) {
	var rows *sql.Rows
	var err error
	if projectID == 0 {
		rows, err = db.sql.QueryContext(ctx, selectAlertRulesAll)
	} else {
		rows, err = db.sql.QueryContext(ctx, selectAlertRulesByProject, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: alert rules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []store.AlertRuleRow
	for rows.Next() {
		r, err := scanAlertRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertAlertRule inserts r (ID 0) or updates the existing row (ID set),
// returning the stored row. Updating a missing ID returns store.ErrNotFound.
// Unknown kinds are rejected before touching the database — the CHECK
// constraint would also reject them, but this gives a clearer error. Update
// does not touch last_fired/last_error — those are owned by
// RecordAlertResult.
func (db *DB) UpsertAlertRule(ctx context.Context, r core.AlertRule) (store.AlertRuleRow, error) {
	if !validAlertKinds[r.Kind] {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: invalid alert rule kind %q", r.Kind)
	}

	minSeverity := strings.ToLower(r.MinSeverity.String())
	enabled := 0
	if r.Enabled {
		enabled = 1
	}
	headers, err := marshalHeaders(r.Headers)
	if err != nil {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: marshal alert rule headers: %w", err)
	}

	if r.ID == 0 {
		createdAt := time.Now().UTC().Format(time.RFC3339Nano)
		res, err := db.sql.ExecContext(ctx, insertAlertRule,
			r.ProjectID, r.Name, string(r.Kind), r.Service, r.Environment, minSeverity,
			r.N, r.WindowMinutes, r.CooldownSeconds, r.URL, headers, enabled, createdAt)
		if err != nil {
			return store.AlertRuleRow{}, fmt.Errorf("sqlite: insert alert rule: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return store.AlertRuleRow{}, fmt.Errorf("sqlite: alert rule last insert id: %w", err)
		}
		return db.alertRuleByID(ctx, id)
	}

	res, err := db.sql.ExecContext(ctx, updateAlertRule,
		r.ProjectID, r.Name, string(r.Kind), r.Service, r.Environment, minSeverity,
		r.N, r.WindowMinutes, r.CooldownSeconds, r.URL, headers, enabled, r.ID)
	if err != nil {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: update alert rule: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: update alert rule rows affected: %w", err)
	}
	if n == 0 {
		return store.AlertRuleRow{}, store.ErrNotFound
	}
	return db.alertRuleByID(ctx, r.ID)
}

func (db *DB) alertRuleByID(ctx context.Context, id int64) (store.AlertRuleRow, error) {
	row := db.sql.QueryRowContext(ctx, selectAlertRuleByID, id)
	r, err := scanAlertRule(row)
	if err == sql.ErrNoRows {
		return store.AlertRuleRow{}, store.ErrNotFound
	}
	if err != nil {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: alert rule by id: %w", err)
	}
	return r, nil
}

// marshalHeaders stores headers as a JSON object. A nil map is stored as
// "{}" — the canonical empty form — so reads never have to distinguish
// "never set" from "set empty".
func marshalHeaders(h map[string]string) (string, error) {
	if len(h) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func scanAlertRule(r rowScanner) (store.AlertRuleRow, error) {
	var row store.AlertRuleRow
	var kind, minSeverity, headers, createdAt, lastError string
	var enabled int
	var lastFired sql.NullString
	if err := r.Scan(&row.ID, &row.ProjectID, &row.Name, &kind, &row.Service, &row.Environment, &minSeverity,
		&row.N, &row.WindowMinutes, &row.CooldownSeconds, &row.URL, &headers, &enabled, &lastFired, &lastError, &createdAt); err != nil {
		return store.AlertRuleRow{}, err
	}
	row.Kind = core.AlertRuleKind(kind)
	row.MinSeverity = core.ParseSeverity(minSeverity)
	row.Enabled = enabled != 0
	row.LastError = lastError

	m := map[string]string{}
	if err := json.Unmarshal([]byte(headers), &m); err != nil {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: unmarshal alert rule headers: %w", err)
	}
	if len(m) > 0 {
		row.Headers = m
	}

	ts, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return store.AlertRuleRow{}, fmt.Errorf("sqlite: parse alert rule created_at: %w", err)
	}
	row.CreatedAt = ts

	if lastFired.Valid && lastFired.String != "" {
		ts, err := time.Parse(time.RFC3339Nano, lastFired.String)
		if err != nil {
			return store.AlertRuleRow{}, fmt.Errorf("sqlite: parse alert rule last_fired: %w", err)
		}
		row.LastFired = ts
	}

	return row, nil
}

// DeleteAlertRule deletes the rule with the given ID, or returns
// store.ErrNotFound if no such rule exists.
func (db *DB) DeleteAlertRule(ctx context.Context, id int64) error {
	res, err := db.sql.ExecContext(ctx, deleteAlertRuleStmt, id)
	if err != nil {
		return fmt.Errorf("sqlite: delete alert rule: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete alert rule rows affected: %w", err)
	}
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// RecordAlertResult stores the outcome of a delivery attempt. A missing ID
// (rule deleted mid-flight) is a silent no-op — there is nothing left to
// record the outcome against.
func (db *DB) RecordAlertResult(ctx context.Context, id int64, firedAt time.Time, lastError string) error {
	if _, err := db.sql.ExecContext(ctx, recordAlertResultStmt, firedAt.UTC().Format(time.RFC3339Nano), lastError, id); err != nil {
		return fmt.Errorf("sqlite: record alert result: %w", err)
	}
	return nil
}
