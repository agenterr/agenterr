package sqlite

import (
	"context"
	"fmt"
	"time"
)

const deleteEventsForOldLogs = `
DELETE FROM events WHERE log_id IN (SELECT id FROM logs WHERE project_id = ? AND ts < ?)`

const deleteOldLogs = `DELETE FROM logs WHERE project_id = ? AND ts < ?`

// Prune deletes all logs (and their FTS index rows, via the logs_ad
// trigger) for projectID older than before. events.log_id has a foreign
// key to logs(id) with no cascade, and foreign_keys is on, so the events
// referencing those logs must be deleted first in the same transaction or
// the log delete fails with "FOREIGN KEY constraint failed" — the primary
// case being any old log that produced an event. The issue row itself is
// untouched; only its old event samples are removed. It returns the number
// of logs removed.
func (db *DB) Prune(ctx context.Context, projectID int64, before time.Time) (int64, error) {
	cutoff := before.UTC().Format(time.RFC3339Nano)

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, deleteEventsForOldLogs, projectID, cutoff); err != nil {
		return 0, fmt.Errorf("sqlite: prune delete events: %w", err)
	}

	res, err := tx.ExecContext(ctx, deleteOldLogs, projectID, cutoff)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune delete logs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune rows affected: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: prune commit: %w", err)
	}
	return n, nil
}
