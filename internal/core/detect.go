package core

import (
	"strings"
	"unicode/utf8"
)

// IsEvent returns true if the log is an error event.
// A log is considered an event if:
// - Its severity is SeverityError or SeverityFatal, OR
// - Any of the exception attributes (exception.type, exception.message, exception.stacktrace) is present
func IsEvent(l Log) bool {
	if l.Severity >= SeverityError {
		return true
	}

	// Check if any exception attributes are present and non-empty
	if l.Attrs != nil {
		for _, key := range []string{"exception.type", "exception.message", "exception.stacktrace"} {
			if val, ok := l.Attrs[key]; ok && val != "" {
				return true
			}
		}
	}

	return false
}

// Title returns the issue title for a log.
// It extracts the first line of the Body, trims it, and prefixes it with
// "<exception.type>: " if the exception.type attribute is present.
// The result is capped at 200 runes.
func Title(l Log) string {
	// Extract first line
	firstLine := l.Body
	if idx := strings.Index(l.Body, "\n"); idx >= 0 {
		firstLine = l.Body[:idx]
	}
	firstLine = strings.TrimSpace(firstLine)

	// Prefix with exception.type if present
	if l.Attrs != nil {
		if excType, ok := l.Attrs["exception.type"]; ok && excType != "" {
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
