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
	Query        string // FTS match on body; "" = all
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

// Writer persists ingested log/event data.
type Writer interface {
	// WriteBatch persists logs and, atomically in the same transaction,
	// upserts issues by (project, fingerprint): insert as open on first
	// sight, else increment count / update last_seen; a resolved issue
	// seeing a new event reopens (regression). Event sample rows are
	// capped at 50 newest per issue.
	WriteBatch(ctx context.Context, entries []Entry) error
	Prune(ctx context.Context, projectID int64, before time.Time) (int64, error)
}

// Reader queries issues, logs, and stats.
type Reader interface {
	Issues(ctx context.Context, f IssueFilter) ([]core.Issue, error)
	Issue(ctx context.Context, id int64) (core.Issue, []core.Event, error)
	SearchLogs(ctx context.Context, f LogFilter) ([]core.Log, error)
	LogContext(ctx context.Context, logID int64, n int) ([]core.Log, error)
	Stats(ctx context.Context, f StatsFilter) (Stats, error)
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

// Store is the full persistence surface a backend must implement.
type Store interface {
	Reader
	Writer
	Admin
	Close() error
}

// ErrNotFound is returned by Reader/Admin lookups that find no matching row.
var ErrNotFound = errors.New("not found")
