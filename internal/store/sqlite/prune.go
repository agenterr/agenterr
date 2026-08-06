package sqlite

import (
	"context"
	"fmt"
	"time"
)

const deleteOldLogs = `DELETE FROM logs WHERE project_id = ? AND ts < ?`

const deleteOrphanEvents = `DELETE FROM events WHERE log_id NOT IN (SELECT id FROM logs)`

// Prune deletes all logs (and their FTS index rows, via the logs_ad
// trigger) for projectID older than before, then removes any events left
// orphaned by that deletion. It returns the number of logs removed.
func (db *DB) Prune(ctx context.Context, projectID int64, before time.Time) (int64, error) {
	cutoff := before.UTC().Format(time.RFC3339Nano)

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune begin tx: %w", err)
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, deleteOldLogs, projectID, cutoff)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune delete logs: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune rows affected: %w", err)
	}

	if _, err := tx.ExecContext(ctx, deleteOrphanEvents); err != nil {
		return 0, fmt.Errorf("sqlite: prune orphan events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: prune commit: %w", err)
	}
	return n, nil
}
