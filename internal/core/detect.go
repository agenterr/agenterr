package core

import (
	"strings"
	"unicode/utf8"
)

// IsEvent returns true if the log is an error event.
// A log is considered an event if:
// - Its severity is SeverityError or SeverityFatal, OR
// - Any of the exception attributes (exception.type, exception.message, exception.stacktrace) is present (non-empty)
// Note: exception attributes with empty string values are treated as absent.
func IsEvent(l Log) bool {
	if l.Severity >= SeverityError {
		return true
	}

	// Check if any exception attributes are present and non-empty.
	// Nil map lookups are safe in Go, so no guard needed.
	for _, key := range []string{"exception.type", "exception.message", "exception.stacktrace"} {
		if val, ok := l.Attrs[key]; ok && val != "" {
			return true
		}
	}

	return false
}

// Title returns the issue title for a log.
// It extracts the first line of the Body, trims it, and prefixes it with
// "<exception.type>: " if the exception.type attribute is present and non-empty.
// If the first line is empty, only exception.type is returned (no trailing ": ").
// The result is capped at 200 runes.
// Note: exception.type with empty string value is treated as absent.
func Title(l Log) string {
	// Extract first line
	firstLine := l.Body
	if idx := strings.Index(l.Body, "\n"); idx >= 0 {
		firstLine = l.Body[:idx]
	}
	firstLine = strings.TrimSpace(firstLine)

	// Prefix with exception.type if present and non-empty.
	// Nil map lookups are safe in Go, so no guard needed.
	if excType, ok := l.Attrs["exception.type"]; ok && excType != "" {
		if firstLine == "" {
			firstLine = excType
		} else {
			firstLine = excType + ": " + firstLine
		}
	}

	// Cap at 200 runes
	if utf8.RuneCountInString(firstLine) > 200 {
		runes := []rune(firstLine)
		firstLine = string(runes[:200])
	}

	return firstLine
}

// DetectPanicSeverity raises a log whose body starts with a Go runtime
// crash prefix ("panic:" or "fatal error:") to FATAL, so plain-text
// panic dumps shipped without a severity become grouped, alertable
// issues. Mirrors ParseSeverity's mapping of "panic"/"fatal" names.
// Like structured-body lifting, it only overrides the parser-default
// INFO — an explicitly set severity always wins.
func DetectPanicSeverity(l Log) Log {
	if l.Severity != SeverityInfo {
		return l
	}
	body := strings.TrimLeft(l.Body, " \t")
	if strings.HasPrefix(body, "panic:") || strings.HasPrefix(body, "fatal error:") {
		l.Severity = SeverityFatal
	}
	return l
}
