package core

import (
	"strconv"
	"strings"
	"time"
)

// Severity represents the severity level of a log entry.
type Severity int8

const (
	SeverityTrace Severity = iota
	SeverityDebug
	SeverityInfo
	SeverityWarn
	SeverityError
	SeverityFatal
)

// ParseSeverity parses a severity level from a string.
// Supports common names (trace, debug, info, warn, warning, error, err, fatal, panic, critical)
// and OTLP numeric severity numbers (1-24).
// Unknown values default to SeverityInfo.
func ParseSeverity(s string) Severity {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return SeverityTrace
	case "debug":
		return SeverityDebug
	case "info":
		return SeverityInfo
	case "warn", "warning":
		return SeverityWarn
	case "error", "err":
		return SeverityError
	case "fatal", "panic", "critical":
		return SeverityFatal
	}
	if n, err := strconv.Atoi(s); err == nil { // OTLP SeverityNumber 1..24
		switch {
		case n >= 21:
			return SeverityFatal
		case n >= 17:
			return SeverityError
		case n >= 13:
			return SeverityWarn
		case n >= 9:
			return SeverityInfo
		case n >= 5:
			return SeverityDebug
		case n >= 1:
			return SeverityTrace
		}
	}
	return SeverityInfo
}

// String returns the canonical uppercase name of the severity level.
func (s Severity) String() string {
	switch s {
	case SeverityTrace:
		return "TRACE"
	case SeverityDebug:
		return "DEBUG"
	case SeverityInfo:
		return "INFO"
	case SeverityWarn:
		return "WARN"
	case SeverityError:
		return "ERROR"
	case SeverityFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// IssueStatus represents the status of an issue.
type IssueStatus string

const (
	StatusOpen     IssueStatus = "open"
	StatusResolved IssueStatus = "resolved"
	StatusIgnored  IssueStatus = "ignored"
)

// Project represents a project in Agenterr.
type Project struct {
	ID            int64
	Name          string
	Slug          string
	RetentionDays int
	CreatedAt     time.Time
}

// Log represents a log entry.
type Log struct {
	ID          int64
	ProjectID   int64
	Time        time.Time
	Severity    Severity
	Body        string
	Service     string
	Environment string
	Release     string
	TraceID     string
	Attrs       map[string]string // flattened dotted keys, e.g. "exception.type"
}

// Issue represents an issue (grouped logs).
type Issue struct {
	ID          int64
	ProjectID   int64
	Fingerprint string
	Title       string
	Severity    Severity
	Status      IssueStatus
	FirstSeen   time.Time
	LastSeen    time.Time
	Count       int64
}

// Event represents a log event associated with an issue.
type Event struct {
	LogID   int64
	IssueID int64
	Time    time.Time
	Log     Log
}
