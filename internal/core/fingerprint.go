package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// Order matters: each pattern is applied in sequence to avoid partial matches
// clobbering a more specific pattern later (e.g. a UUID contains hex digits and
// decimal digits, so it must be normalized before the hex and integer patterns run).
var (
	uuidRe = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	ipv4Re = regexp.MustCompile(`\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b`)
	hexRe  = regexp.MustCompile(`\b[0-9a-fA-F]{8,}\b`)
	strRe  = regexp.MustCompile("\"[^\"]*\"|`[^`]*`")
	intRe  = regexp.MustCompile(`\b[0-9]+\b`)
)

// NormalizeMessage replaces volatile substrings (UUIDs, IPv4 addresses, long
// hex runs, quoted strings, and integers) with stable placeholder tokens so
// that structurally identical log messages collapse to the same template.
//
// Replacement order matters: UUIDs contain both hex digits and decimal
// digits, so they must be matched before the hex and integer patterns; IPv4
// addresses are dotted decimal runs, so they must be matched before the bare
// integer pattern, which would otherwise consume the individual octets.
func NormalizeMessage(s string) string {
	s = uuidRe.ReplaceAllString(s, "<uuid>")
	s = ipv4Re.ReplaceAllString(s, "<ip>")
	// hexRe's character class also matches pure-decimal runs (digits are a
	// subset of hex digits), so only collapse a candidate run to <hex> when
	// it contains at least one actual hex letter (a-f/A-F). Pure-decimal
	// runs are left in place for intRe to normalize to <n> below.
	s = hexRe.ReplaceAllStringFunc(s, func(match string) string {
		if strings.ContainsAny(match, "abcdefABCDEF") {
			return "<hex>"
		}
		return match
	})
	s = strRe.ReplaceAllString(s, "<str>")
	s = intRe.ReplaceAllString(s, "<n>")
	return s
}

// Fingerprint computes a stable 16-hex-character identifier used to group
// logs into issues. Precedence, highest first:
//  1. An explicit "agenterr.fingerprint" attribute, used verbatim.
//  2. exception.type + normalized exception.message, when present.
//  3. Severity class + normalized first line of Body.
//
// The ProjectID is always mixed into the hashed material so that identical
// templates in different projects never collide.
func Fingerprint(l Log) string {
	var material string

	if fp, ok := l.Attrs["agenterr.fingerprint"]; ok && fp != "" {
		material = fp
	} else if excType, ok := l.Attrs["exception.type"]; ok && excType != "" {
		material = excType + "|" + NormalizeMessage(l.Attrs["exception.message"])
	} else {
		firstLine := l.Body
		if idx := strings.Index(l.Body, "\n"); idx >= 0 {
			firstLine = l.Body[:idx]
		}
		material = l.Severity.String() + "|" + NormalizeMessage(firstLine)
	}

	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s", l.ProjectID, material)))
	return hex.EncodeToString(sum[:8])
}

// DefaultGrouper satisfies the pipeline's Grouper interface by delegating to
// the package-level Fingerprint function.
type DefaultGrouper struct{}

// Fingerprint delegates to the package-level Fingerprint function.
func (DefaultGrouper) Fingerprint(l Log) string {
	return Fingerprint(l)
}
