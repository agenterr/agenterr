// Package sqlite implements store.Store on top of a local SQLite database
// via the cgo-free modernc.org/sqlite driver.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver used by Open

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const createMigrationsTable = `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY)`

const insertMigration = `INSERT INTO schema_migrations (version) VALUES (?)`

const selectAppliedMigrations = `SELECT version FROM schema_migrations`

const insertProject = `
INSERT INTO projects (name, slug, retention_days, created_at) VALUES (?, ?, ?, ?)`

const selectProjects = `SELECT id, name, slug, retention_days, created_at, parse_bodies FROM projects ORDER BY id`

const updateIssueStatus = `UPDATE issues SET status = ? WHERE id = ?`

// DB is a store.Store backed by SQLite.
type DB struct {
	sql *sql.DB
}

// Open opens (creating if necessary) the SQLite database at path, applies
// any pending migrations, and returns a ready-to-use *DB.
//
// The default connection pool (no SetMaxOpenConns cap) is used deliberately:
// WAL mode lets readers proceed without blocking on a writer, and
// busy_timeout(5000) has any writer-vs-writer contention wait instead of
// failing outright. Capping MaxOpenConns(1) would serialize reads behind
// writes and defeat the point of WAL. Multiple components write
// concurrently on separate pool connections in practice — pipeline batch
// flush (WriteBatch), noise drop-counter flush (AddNoiseDrops), alert
// state (RecordAlertResult), and retention (Prune) — so this relies on
// WAL + busy_timeout for correctness, not a self-managed mutex.
//
// _txlock=immediate makes every BeginTx acquire the writer lock at BEGIN
// (SQLite "BEGIN IMMEDIATE") instead of the driver's default deferred
// begin. This matters because a deferred transaction only consults the
// busy handler (what installs busy_timeout) while still outside a
// transaction; once it has stepped into the read half of a deferred tx,
// a writer-lock conflict on its first write statement fails immediately
// with SQLITE_BUSY without ever waiting. Beginning IMMEDIATE moves the
// lock acquisition to BEGIN, where the busy handler still applies, so
// concurrent writers wait (up to busy_timeout) instead of erroring.
func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_txlock=immediate", path)
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}

	db := &DB{sql: sqlDB}
	if err := db.migrate(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("sqlite: migrate: %w", err)
	}
	return db, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	return db.sql.Close()
}

// CreateProject creates a new project. The slug is derived from name
// (lowercased, spaces to hyphens) with the new row's ID appended to
// guarantee uniqueness without a retry loop.
func (db *DB) CreateProject(ctx context.Context, name string, retentionDays int) (core.Project, error) {
	createdAt := time.Now().UTC()
	base := slugify(name)

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return core.Project{}, fmt.Errorf("sqlite: create project begin tx: %w", err)
	}
	// Rollback error is unactionable: after a successful Commit it is
	// always sql.ErrTxDone, and on any earlier failure we already return
	// the real error.
	defer func() { _ = tx.Rollback() }()

	// The slug column has a UNIQUE constraint, so seed the insert with a
	// UUID-free placeholder derived from name and rewrite it below once the
	// row's ID is known — that guarantees uniqueness without a retry loop.
	res, err := tx.ExecContext(ctx, insertProject, name, fmt.Sprintf("%s-%d", base, createdAt.UnixNano()), retentionDays, createdAt.Format(time.RFC3339Nano))
	if err != nil {
		return core.Project{}, fmt.Errorf("sqlite: insert project: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return core.Project{}, fmt.Errorf("sqlite: project last insert id: %w", err)
	}
	slug := fmt.Sprintf("%s-%d", base, id)
	if _, err := tx.ExecContext(ctx, `UPDATE projects SET slug = ? WHERE id = ?`, slug, id); err != nil {
		return core.Project{}, fmt.Errorf("sqlite: set project slug: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return core.Project{}, fmt.Errorf("sqlite: create project commit: %w", err)
	}

	return core.Project{
		ID:            id,
		Name:          name,
		Slug:          slug,
		RetentionDays: retentionDays,
		CreatedAt:     createdAt,
		ParseBodies:   true, // matches the parse_bodies column default
	}, nil
}

func slugify(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = strings.ReplaceAll(s, " ", "-")
	if s == "" {
		s = "project"
	}
	return s
}

// Projects returns all projects, ordered by ID.
func (db *DB) Projects(ctx context.Context) ([]core.Project, error) {
	rows, err := db.sql.QueryContext(ctx, selectProjects)
	if err != nil {
		return nil, fmt.Errorf("sqlite: projects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []core.Project
	for rows.Next() {
		var p core.Project
		var createdAt string
		var parseBodies int
		if err := rows.Scan(&p.ID, &p.Name, &p.Slug, &p.RetentionDays, &createdAt, &parseBodies); err != nil {
			return nil, fmt.Errorf("sqlite: scan project: %w", err)
		}
		ts, err := time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("sqlite: parse project created_at: %w", err)
		}
		p.CreatedAt = ts
		p.ParseBodies = parseBodies != 0
		out = append(out, p)
	}
	return out, rows.Err()
}

// SetIssueStatus updates the status of the issue with the given ID, or
// returns store.ErrNotFound if no such issue exists.
func (db *DB) SetIssueStatus(ctx context.Context, id int64, s core.IssueStatus) error {
	res, err := db.sql.ExecContext(ctx, updateIssueStatus, string(s), id)
	if err != nil {
		return fmt.Errorf("sqlite: set issue status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("sqlite: set issue status rows affected: %w", err)
	}
	if n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// migrate applies any migration files under migrations/*.sql that have not
// yet been recorded in schema_migrations, in filename order, each inside
// its own transaction.
func (db *DB) migrate() error {
	if _, err := db.sql.Exec(createMigrationsTable); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := db.sql.Query(selectAppliedMigrations)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan applied migration: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		if applied[name] {
			continue
		}
		content, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		tx, err := db.sql.Begin()
		if err != nil {
			return fmt.Errorf("begin tx for migration %s: %w", name, err)
		}
		if _, err := tx.Exec(string(content)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(insertMigration, name); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}
