package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/agenterr/agenterr/internal/store"
	"github.com/agenterr/agenterr/internal/store/sqlite"
)

func openTestDB(t *testing.T) *sqlite.DB {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSetting_NotFound(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	_, err := db.Setting(ctx, "nope")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Setting(missing) err = %v, want store.ErrNotFound", err)
	}
}

func TestSetSetting_RoundTripAndUpsert(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := db.SetSetting(ctx, "k", "v1"); err != nil {
		t.Fatalf("SetSetting: %v", err)
	}
	got, err := db.Setting(ctx, "k")
	if err != nil {
		t.Fatalf("Setting: %v", err)
	}
	if got != "v1" {
		t.Fatalf("Setting = %q, want v1", got)
	}

	// Upsert: writing the same key again overwrites, it does not error
	// or create a second row.
	if err := db.SetSetting(ctx, "k", "v2"); err != nil {
		t.Fatalf("SetSetting (overwrite): %v", err)
	}
	got, err = db.Setting(ctx, "k")
	if err != nil {
		t.Fatalf("Setting after overwrite: %v", err)
	}
	if got != "v2" {
		t.Fatalf("Setting after overwrite = %q, want v2", got)
	}
}

func TestHasAdminKey(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	has, err := db.HasAdminKey(ctx)
	if err != nil {
		t.Fatalf("HasAdminKey (before mint): %v", err)
	}
	if has {
		t.Fatal("HasAdminKey = true before any admin key was minted")
	}

	if _, err := db.MintKey(ctx, 0, "admin"); err != nil {
		t.Fatalf("MintKey(admin): %v", err)
	}

	has, err = db.HasAdminKey(ctx)
	if err != nil {
		t.Fatalf("HasAdminKey (after mint): %v", err)
	}
	if !has {
		t.Fatal("HasAdminKey = false after minting an admin key")
	}

	// A non-admin key must not satisfy HasAdminKey.
	db2 := openTestDB(t)
	p, err := db2.CreateProject(ctx, "proj", 14)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	if _, err := db2.MintKey(ctx, p.ID, "ingest"); err != nil {
		t.Fatalf("MintKey(ingest): %v", err)
	}
	has, err = db2.HasAdminKey(ctx)
	if err != nil {
		t.Fatalf("HasAdminKey (ingest-only db): %v", err)
	}
	if has {
		t.Fatal("HasAdminKey = true with only a non-admin key minted")
	}
}
