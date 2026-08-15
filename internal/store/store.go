// Package store defines how Agenterr persists and queries its domain —
// implementations live in subpackages and must pass storetest.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/agenterr/agenterr/internal/core"
)

// Entry is one ingested log, pipeline-annotated with issue-grouping
// results.
type Entry struct {
	Log         core.Log
	IsEvent     bool
	Fingerprint string // set only when IsEvent
	Title       string // set only when IsEvent
}

// IssueFilter narrows an Issues query.
type IssueFilter struct {
	ProjectID    int64 // 0 = all
	Environment  string
	Status       core.IssueStatus // "" = any
	Since, Until time.Time
	Limit        int // Limit 0 = 50
}

// LogFilter narrows a SearchLogs query.
type LogFilter struct {
	ProjectID    int64
	Query        string // substring match on body; "" = all
	MinSeverity  core.Severity
	Service      string
	Environment  string
	Since, Until time.Time
	Limit        int
}

// StatsFilter narrows a Stats query.
type StatsFilter struct {
	ProjectID int64
	Since     time.Time
}

// Stats summarizes log/event/issue volume for a project.
type Stats struct {
	Logs       int64
	Events     int64
	OpenIssues int64
	PerDay     []DayCount
}

// DayCount is one day's log/event volume, used to build Stats.PerDay.
type DayCount struct {
	Day    string // "YYYY-MM-DD" (UTC)
	Logs   int64
	Events int64
}

// IssueOutcome describes what WriteBatch did to the issue behind one event
// entry, computed inside the same transaction as the upsert. Outcomes are
// returned one per entry with IsEvent true, in entry order (non-event
// entries contribute nothing to the slice).
type IssueOutcome struct {
	IssueID  int64
	New      bool // fingerprint first seen for this project
	Reopened bool // issue was 'resolved' immediately before this event's upsert
}

// Writer persists ingested log/event data.
type Writer interface {
	// WriteBatch persists logs and, atomically in the same transaction,
	// upserts issues by (project, fingerprint): insert as open on first
	// sight, else increment count / update last_seen; a resolved issue
	// seeing a new event reopens (regression). Event sample rows are
	// capped at 50 newest per issue. The returned []IssueOutcome has one
	// element per entry with IsEvent, in entry order; two entries in the
	// same batch sharing a new fingerprint report New only for the first.
	WriteBatch(ctx context.Context, entries []Entry) ([]IssueOutcome, error)
	Prune(ctx context.Context, projectID int64, before time.Time) (int64, error)
}

// Reader queries issues, logs, and stats.
type Reader interface {
	Issues(ctx context.Context, f IssueFilter) ([]core.Issue, error)
	Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error)
	SearchLogs(ctx context.Context, f LogFilter) ([]core.Log, error)
	LogContext(ctx context.Context, logID int64, n int) ([]core.Log, error)
	Stats(ctx context.Context, f StatsFilter) (Stats, error)
	// ServiceCounts returns the top 20 services by log count for
	// projectID since the given time, ordered descending by count.
	ServiceCounts(ctx context.Context, projectID int64, since time.Time) ([]ServiceCount, error)
}

// ServiceCount is one service's log volume, used to build the noise
// report's top_services list.
type ServiceCount struct {
	Service string
	Logs    int64
}

// Admin manages projects, issue status, and API keys.
type Admin interface {
	CreateProject(ctx context.Context, name string, retentionDays int) (core.Project, error)
	Projects(ctx context.Context) ([]core.Project, error)
	SetIssueStatus(ctx context.Context, id int64, s core.IssueStatus) error
	// Keys: kind is "ingest" or "api". Mint returns the plaintext once.
	MintKey(ctx context.Context, projectID int64, kind string) (plaintext string, err error)
	LookupKey(ctx context.Context, plaintext string) (projectID int64, kind string, err error)
}

// NoiseRules manages per-project ingest filtering rules and their
// drop accounting, and the per-project parse-bodies toggle.
type NoiseRules interface {
	// NoiseRules returns rules for a project (projectID 0 = all
	// projects), ordered by ascending ID. DroppedCount reflects
	// persisted drops only.
	NoiseRules(ctx context.Context, projectID int64) ([]NoiseRuleRow, error)
	// UpsertNoiseRule inserts (ID 0) or updates (ID set) and returns
	// the stored row. Updating a missing ID returns ErrNotFound.
	// Unknown kinds are rejected with an error.
	UpsertNoiseRule(ctx context.Context, r core.NoiseRule) (NoiseRuleRow, error)
	DeleteNoiseRule(ctx context.Context, id int64) error // missing → ErrNotFound
	// AddNoiseDrops atomically adds the given per-rule drop counts.
	// Unknown rule IDs are skipped (rule deleted since counting began).
	AddNoiseDrops(ctx context.Context, counts map[int64]int64) error
	// SetProjectParseBodies flips the per-project parse toggle.
	SetProjectParseBodies(ctx context.Context, projectID int64, on bool) error
}

// NoiseRuleRow is a stored rule plus persistence-side fields.
type NoiseRuleRow struct {
	core.NoiseRule
	DroppedCount int64
	CreatedAt    time.Time
}

// AlertRules manages per-project alert rules and their delivery outcomes.
type AlertRules interface {
	// AlertRules returns rules for a project (projectID 0 = all
	// projects), ordered by ascending ID.
	AlertRules(ctx context.Context, projectID int64) ([]AlertRuleRow, error)
	// UpsertAlertRule inserts (ID 0) or updates (ID set) and returns the
	// stored row. Updating a missing ID returns ErrNotFound. Unknown
	// kinds are rejected with an error.
	UpsertAlertRule(ctx context.Context, r core.AlertRule) (AlertRuleRow, error)
	DeleteAlertRule(ctx context.Context, id int64) error // missing → ErrNotFound
	// RecordAlertResult stores the outcome of a delivery attempt: firedAt
	// is the attempt time, lastError "" on success. A missing ID (rule
	// deleted mid-flight) is a silent no-op.
	RecordAlertResult(ctx context.Context, id int64, firedAt time.Time, lastError string) error
}

// AlertRuleRow is a stored alert rule plus persistence-side fields.
type AlertRuleRow struct {
	core.AlertRule
	LastFired time.Time // zero = never
	LastError string
	CreatedAt time.Time
}

// Store is the full persistence surface a backend must implement.
type Store interface {
	Reader
	Writer
	Admin
	NoiseRules
	AlertRules
	Close() error
}

// ErrNotFound is returned by Reader/Admin lookups that find no matching row.
var ErrNotFound = errors.New("not found")
