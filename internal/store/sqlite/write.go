package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

const insertLog = `
INSERT INTO logs (project_id, ts, severity, body, service, environment, release, trace_id, attrs)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

const upsertIssue = `
INSERT INTO issues (project_id, fingerprint, title, severity, status, first_seen, last_seen, count)
VALUES (?, ?, ?, ?, 'open', ?, ?, 1)
ON CONFLICT(project_id, fingerprint) DO UPDATE SET
  count = count + 1,
  last_seen = excluded.last_seen,
  status = CASE WHEN status = 'resolved' THEN 'open' ELSE status END`

const selectIssueIDByFingerprint = `
SELECT id FROM issues WHERE project_id = ? AND fingerprint = ?`

const selectIssueStatusByFingerprint = `
SELECT status FROM issues WHERE project_id = ? AND fingerprint = ?`

const setLogIssueID = `UPDATE logs SET issue_id = ? WHERE id = ?`

const insertEvent = `INSERT INTO events (issue_id, log_id, ts) VALUES (?, ?, ?)`

const trimEvents = `
DELETE FROM events WHERE issue_id = ? AND id NOT IN (
  SELECT id FROM events WHERE issue_id = ? ORDER BY ts DESC, id DESC LIMIT 50
)`

const insertKey = `
INSERT INTO keys (project_id, kind, hash, prefix, created_at) VALUES (?, ?, ?, ?, ?)`

// WriteBatch persists entries and atomically upserts any associated issues.
// See store.Writer for the full semantics contract.
func (db *DB) WriteBatch(ctx context.Context, entries []store.Entry) ([]store.IssueOutcome, error) {
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sqlite: begin tx: %w", err)
	}
	// Rollback error is unactionable: after a successful Commit it is
	// always sql.ErrTxDone, and on any earlier failure we already return
	// the real error.
	defer func() { _ = tx.Rollback() }()

	var outcomes []store.IssueOutcome

	for _, e := range entries {
		outcome, err := writeEntry(ctx, tx, e)
		if err != nil {
			return nil, err
		}
		if outcome != nil {
			outcomes = append(outcomes, *outcome)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sqlite: commit: %w", err)
	}
	return outcomes, nil
}

// writeEntry inserts one log row within tx and, if e.IsEvent, upserts its
// issue and returns the resulting IssueOutcome. Non-event entries return
// (nil, nil) — WriteBatch only appends to outcomes when the result is
// non-nil.
func writeEntry(ctx context.Context, tx *sql.Tx, e store.Entry) (*store.IssueOutcome, error) {
	attrsJSON, err := json.Marshal(e.Log.Attrs)
	if err != nil {
		return nil, fmt.Errorf("sqlite: marshal attrs: %w", err)
	}
	ts := e.Log.Time.UTC().Format(time.RFC3339Nano)

	res, err := tx.ExecContext(ctx, insertLog,
		e.Log.ProjectID, ts, int(e.Log.Severity), e.Log.Body,
		e.Log.Service, e.Log.Environment, e.Log.Release, e.Log.TraceID, string(attrsJSON))
	if err != nil {
		return nil, fmt.Errorf("sqlite: insert log: %w", err)
	}
	logID, err := res.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("sqlite: log last insert id: %w", err)
	}

	if !e.IsEvent {
		return nil, nil
	}

	// Read the issue's status before the upsert, inside this same
	// transaction: that's the only way to tell "first sight" (no row)
	// apart from "was resolved" (row, status='resolved') apart from
	// "already open/ignored" (row, other status) — the upsert itself
	// only tells us insert-vs-update, not what the prior status was.
	var prevStatus string
	err = tx.QueryRowContext(ctx, selectIssueStatusByFingerprint, e.Log.ProjectID, e.Fingerprint).Scan(&prevStatus)
	existed := true
	switch {
	case errors.Is(err, sql.ErrNoRows):
		existed = false
	case err != nil:
		return nil, fmt.Errorf("sqlite: select issue status: %w", err)
	}

	if _, err := tx.ExecContext(ctx, upsertIssue,
		e.Log.ProjectID, e.Fingerprint, e.Title, int(e.Log.Severity), ts, ts); err != nil {
		return nil, fmt.Errorf("sqlite: upsert issue: %w", err)
	}

	var issueID int64
	if err := tx.QueryRowContext(ctx, selectIssueIDByFingerprint, e.Log.ProjectID, e.Fingerprint).Scan(&issueID); err != nil {
		return nil, fmt.Errorf("sqlite: select issue id: %w", err)
	}

	if _, err := tx.ExecContext(ctx, setLogIssueID, issueID, logID); err != nil {
		return nil, fmt.Errorf("sqlite: set log issue id: %w", err)
	}

	if _, err := tx.ExecContext(ctx, insertEvent, issueID, logID, ts); err != nil {
		return nil, fmt.Errorf("sqlite: insert event: %w", err)
	}

	if _, err := tx.ExecContext(ctx, trimEvents, issueID, issueID); err != nil {
		return nil, fmt.Errorf("sqlite: trim events: %w", err)
	}

	return &store.IssueOutcome{
		IssueID:  issueID,
		New:      !existed,
		Reopened: existed && prevStatus == string(core.StatusResolved),
	}, nil
}

// MintKey generates a new high-entropy API key of the form
// agt_<kind>_<32 base64url chars>, stores its bcrypt hash plus a 12-char
// lookup prefix, and returns the plaintext exactly once (it is never
// recoverable afterward).
//
// kind "admin" is instance-level, not project-bound: its project_id is
// stored as NULL regardless of the projectID argument (by convention 0),
// matching the nullable keys.project_id column added in
// 0003_admin_keys.sql.
//
// bcrypt.MinCost is used deliberately: these are not user-chosen,
// low-entropy passwords but 24 bytes (192 bits) of crypto/rand output —
// brute-forcing the hash is infeasible regardless of cost factor, so a
// higher cost only adds latency without adding security here.
func (db *DB) MintKey(ctx context.Context, projectID int64, kind string) (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("sqlite: generate key: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	plaintext := fmt.Sprintf("agt_%s_%s", kind, token)

	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.MinCost)
	if err != nil {
		return "", fmt.Errorf("sqlite: hash key: %w", err)
	}

	prefix := plaintext
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}

	var projectIDArg any = projectID
	if kind == "admin" {
		projectIDArg = nil
	}

	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.sql.ExecContext(ctx, insertKey, projectIDArg, kind, hash, prefix, createdAt); err != nil {
		return "", fmt.Errorf("sqlite: insert key: %w", err)
	}
	return plaintext, nil
}

const selectKeysByPrefix = `SELECT project_id, kind, hash FROM keys WHERE prefix = ?`

// LookupKey resolves a plaintext API key to its project and kind by
// narrowing candidates on the stored prefix, then bcrypt-comparing against
// each candidate's hash. A key with a NULL project_id (instance-level
// "admin" keys) resolves to projectID 0.
func (db *DB) LookupKey(ctx context.Context, plaintext string) (int64, string, error) {
	prefix := plaintext
	if len(prefix) > 12 {
		prefix = prefix[:12]
	}

	rows, err := db.sql.QueryContext(ctx, selectKeysByPrefix, prefix)
	if err != nil {
		return 0, "", fmt.Errorf("sqlite: query keys: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type candidate struct {
		projectID sql.NullInt64
		kind      string
		hash      []byte
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.projectID, &c.kind, &c.hash); err != nil {
			return 0, "", fmt.Errorf("sqlite: scan key: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return 0, "", err
	}

	for _, c := range candidates {
		if bcrypt.CompareHashAndPassword(c.hash, []byte(plaintext)) == nil {
			return c.projectID.Int64, c.kind, nil
		}
	}
	return 0, "", store.ErrNotFound
}
