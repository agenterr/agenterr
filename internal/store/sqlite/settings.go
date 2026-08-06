package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/agenterr/agenterr/internal/store"
)

const selectSetting = `SELECT value FROM settings WHERE key = ?`

const upsertSetting = `
INSERT INTO settings (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`

const selectAdminKeyExists = `SELECT 1 FROM keys WHERE kind = 'admin' LIMIT 1`

// Setting returns the value stored under key, or store.ErrNotFound if no
// such setting exists. This — along with SetSetting and HasAdminKey below
// — is a small internal helper for process bootstrap, deliberately kept
// as a plain *DB method rather than added to store.Admin: it is not
// domain data, and putting it on the interface would ripple through
// every store implementation and every hand-written test fake that
// implements store.Admin (internal/api, internal/web, internal/mcp).
func (db *DB) Setting(ctx context.Context, key string) (string, error) {
	var value string
	err := db.sql.QueryRowContext(ctx, selectSetting, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", store.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("sqlite: get setting %q: %w", key, err)
	}
	return value, nil
}

// SetSetting upserts key=value.
func (db *DB) SetSetting(ctx context.Context, key, value string) error {
	if _, err := db.sql.ExecContext(ctx, upsertSetting, key, value); err != nil {
		return fmt.Errorf("sqlite: set setting %q: %w", key, err)
	}
	return nil
}

// HasAdminKey reports whether an instance-level "admin" key has ever been
// minted. First-run bootstrap uses this (rather than any marker outside
// the database) to decide whether to mint and print a new admin key, so
// a file-only restore of the database — which carries the key row with
// it — correctly boots silently instead of minting a redundant key.
func (db *DB) HasAdminKey(ctx context.Context) (bool, error) {
	var exists int
	err := db.sql.QueryRowContext(ctx, selectAdminKeyExists).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("sqlite: has admin key: %w", err)
	}
	return true, nil
}
