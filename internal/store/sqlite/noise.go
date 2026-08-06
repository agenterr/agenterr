package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

const noiseRuleColumns = `id, project_id, kind, service, severity, pattern, n, enabled, dropped_count, created_at`

const selectNoiseRulesAll = `SELECT ` + noiseRuleColumns + ` FROM noise_rules ORDER BY id`

const selectNoiseRulesByProject = `SELECT ` + noiseRuleColumns + ` FROM noise_rules WHERE project_id = ? ORDER BY id`

const insertNoiseRule = `
INSERT INTO noise_rules (project_id, kind, service, severity, pattern, n, enabled, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

const updateNoiseRule = `
UPDATE noise_rules SET project_id = ?, kind = ?, service = ?, severity = ?, pattern = ?, n = ?, enabled = ?
WHERE id = ?`

const selectNoiseRuleByID = `SELECT ` + noiseRuleColumns + ` FROM noise_rules WHERE id = ?`

const deleteNoiseRuleStmt = `DELETE FROM noise_rules WHERE id = ?`

const addNoiseDropStmt = `UPDATE noise_rules SET dropped_count = dropped_count + ? WHERE id = ?`

const updateProjectParseBodies = `UPDATE projects SET parse_bodies = ? WHERE id = ?`

// validNoiseKinds mirrors the noise_rules.kind CHECK constraint: a typed
// validation error here is friendlier than letting the constraint fail.
var validNoiseKinds = map[core.NoiseRuleKind]bool{
	core.NoiseSeverityFloor: true,
	core.NoiseDropMatch:     true,
	core.NoiseSample:        true,
}

// NoiseRules returns rules for projectID (0 = all projects), ordered by
// ascending ID.
func (db *DB) NoiseRules(ctx context.Context, projectID int64) ([]store.NoiseRuleRow, error) {
	var rows *sql.Rows
	var err error
	if projectID == 0 {
		rows, err = db.sql.QueryContext(ctx, selectNoiseRulesAll)
	} else {
		rows, err = db.sql.QueryContext(ctx, selectNoiseRulesByProject, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf("sqlite: noise rules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []store.NoiseRuleRow
	for rows.Next() {
		r, err := scanNoiseRule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertNoiseRule inserts r (ID 0) or updates the existing row (ID set),
// returning the stored row. Updating a missing ID returns store.ErrNotFound.
// Unknown kinds are rejected before touching the database — the CHECK
// constraint would also reject them, but this gives a clearer error.
func (db *DB) UpsertNoiseRule(ctx context.Context, r core.NoiseRule) (store.NoiseRuleRow, error) {
	if !validNoiseKinds[r.Kind] {
		return store.NoiseRuleRow{}, fmt.Errorf("sqlite: invalid noise rule kind %q", r.Kind)
	}

	severity := strings.ToLower(r.Severity.String())
	enabled := 0
	if r.Enabled {
		enabled = 1
	}

	if r.ID == 0 {
		createdAt := time.Now().UTC().Format(time.RFC3339Nano)
		res, err := db.sql.ExecContext(ctx, insertNoiseRule,
			r.ProjectID, string(r.Kind), r.Service, severity, r.Pattern, r.N, enabled, createdAt)
		if err != nil {
			return store.NoiseRuleRow{}, fmt.Errorf("sqlite: insert noise rule: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return store.NoiseRuleRow{}, fmt.Errorf("sqlite: noise rule last insert id: %w", err)
		}
		return db.noiseRuleByID(ctx, id)
	}

	res, err := db.sql.ExecContext(ctx, updateNoiseRule,
		r.ProjectID, string(r.Kind), r.Service, severity, r.Pattern, r.N, enabled, r.ID)
	if err != nil {
		return store.NoiseRuleRow{}, fmt.Errorf("sqlite: update noise rule: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return store.NoiseRuleRow{}, fmt.Errorf("sqlite: update noise rule rows affected: %w", err)
	}
	if n == 0 {
		return store.NoiseRuleRow{}, store.ErrNotFound
	}
	return db.noiseRuleByID(ctx, r.ID)
}

func (db *DB) noiseRuleByID(ctx context.Context, id int64) (store.NoiseRuleRow, error) {
	row := db.sql.QueryRowContext(ctx, selectNoiseRuleByID, id)
	r, err := scanNoiseRule(row)
	if err == sql.ErrNoRows {
		return store.NoiseRuleRow{}, store.ErrNotFound
	}
	if err != nil {
		return store.NoiseRuleRow{}, fmt.Errorf("sqlite: noise rule by id: %w", err)
	}
	return r, nil
}

func scanNoiseRule(r rowScanner) (store.NoiseRuleRow, error) {
	var row store.NoiseRuleRow
	var kind, severity, createdAt string
	var enabled int
	if err := r.Scan(&row.ID, &row.ProjectID, &kind, &row.Service, &severity, &row.Pattern, &row.N, &enabled, &row.DroppedCount, &createdAt); err != nil {
		return store.NoiseRuleRow{}, err
	}
	row.Kind = core.NoiseRuleKind(kind)
	row.Severity = core.ParseSeverity(severity)
	row.Enabled = enabled != 0
	ts, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return store.NoiseRuleRow{}, fmt.Errorf("sqlite: parse noise rule created_at: %w", err)
	}
	row.CreatedAt = ts
	return row, nil
}

// DeleteNoiseRule deletes the rule with the given ID, or returns
// store.ErrNotFound if no such rule exists.
func (db *DB) DeleteNoiseRule(ctx context.Context, id int64) error {
	res, err := db.sql.ExecContext(ctx, deleteNoiseRuleStmt, id)
	if err != nil {
		return fmt.Errorf("sqlite: delete noise rule: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: delete noise rule rows affected: %w", err)
	}
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// AddNoiseDrops atomically adds the given per-rule drop counts in a single
// transaction. Unknown rule IDs (rule deleted since counting began) are
// skipped silently rather than erroring — dropping must be fail-open, and a
// missing counter target is not a reason to lose the rest of the batch.
func (db *DB) AddNoiseDrops(ctx context.Context, counts map[int64]int64) error {
	if len(counts) == 0 {
		return nil
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: add noise drops begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for id, n := range counts {
		if _, err := tx.ExecContext(ctx, addNoiseDropStmt, n, id); err != nil {
			return fmt.Errorf("sqlite: add noise drops: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: add noise drops commit: %w", err)
	}
	return nil
}

// SetProjectParseBodies flips the per-project parse-bodies toggle.
func (db *DB) SetProjectParseBodies(ctx context.Context, projectID int64, on bool) error {
	val := 0
	if on {
		val = 1
	}
	if _, err := db.sql.ExecContext(ctx, updateProjectParseBodies, val, projectID); err != nil {
		return fmt.Errorf("sqlite: set project parse_bodies: %w", err)
	}
	return nil
}
