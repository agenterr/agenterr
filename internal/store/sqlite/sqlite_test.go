package sqlite

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
)

// TestMigrateAppliesAndVacuumsOnFreshDB guards the I1 fix: a brand new
// database applies every migration including 0008_drop_logs.sql in the
// same Open() call, so it must VACUUM exactly once and log the
// "database vacuumed" line documented on migrate — the signal
// enforceMaxDBBytes's operators rely on to know the guardrail's freelist
// concern was actually addressed for this database.
func TestMigrateAppliesAndVacuumsOnFreshDB(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	db, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if !strings.Contains(buf.String(), "database vacuumed") {
		t.Errorf("expected a vacuum log line on first-ever open, got log output: %s", buf.String())
	}

	applied, err := db.loadAppliedMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if !applied[migration0008] {
		t.Errorf("migration0008 (%s) not recorded as applied", migration0008)
	}
}

// TestMigrateReopenSkipsVacuum guards the "only when 0008 was just
// applied THIS process run" half of the fix: reopening an already-
// migrated database must not VACUUM again — VACUUM rewrites the entire
// file and is deliberately not something to pay for on every boot.
func TestMigrateReopenSkipsVacuum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db2.Close() }()

	if strings.Contains(buf.String(), "database vacuumed") {
		t.Errorf("reopen of an already-migrated database vacuumed again, want no-op: %s", buf.String())
	}
}
