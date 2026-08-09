package core

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Caps keep a hostile or pathological structured line from ballooning a
// record: past these limits extra keys are dropped (lexicographically
// deterministic) and long values truncated.
const (
	maxLiftedAttrs    = 50
	maxLiftedValueLen = 2000
	timeSanityWindow  = 48 * time.Hour
)

var (
	messageKeys = []string{"msg", "message"}
	levelKeys   = []string{"level", "severity", "lvl"}
	timeKeys    = []string{"time", "ts", "timestamp"}
)

// ParseStructuredBody lifts fields out of a body that is itself a
// structured line (a single JSON object, or logfmt — Task 2). It is
// deliberately conservative: unless the body decodes cleanly and carries a
// recognizable message or level key, the input is returned unchanged. A
// body-derived level is applied only when the record's severity is the
// parser default (SeverityInfo), so an explicitly set severity always wins.
func ParseStructuredBody(l Log) Log {
	trimmed := strings.TrimSpace(l.Body)
	if strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}") {
		if m, ok := decodeJSONObject(trimmed); ok && hasLiftKeys(m) {
			return liftFields(l, m)
		}
	}
	// A braced body is JSON or garbage — never logfmt; skip the logfmt branch.
	if !strings.HasPrefix(trimmed, "{") {
		if m, ok := parseLogfmtLine(trimmed); ok && hasLiftKeys(m) {
			return liftFields(l, m)
		}
	}
	return l
}

// decodeJSONObject decodes body as exactly one JSON object; trailing
// content (a second object, stray text) rejects the whole body.
func decodeJSONObject(body string) (map[string]any, bool) {
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil || dec.More() {
		return nil, false
	}
	return m, true
}

// parseLogfmtLine scans a single line of strictly key=value tokens
// (values optionally double-quoted with backslash escapes). Any token
// that is not a pair — prose, a bare word, a second line — rejects the
// whole body: logfmt detection has no partial credit, which is what keeps
// ordinary sentences containing "=" out. Whitespace (space or tab) separates
// tokens; newlines/carriage returns are hard rejections.
func parseLogfmtLine(body string) (map[string]any, bool) {
	if strings.ContainsAny(body, "\n\r") {
		return nil, false
	}
	m := make(map[string]any)
	i := 0
	for i < len(body) {
		for i < len(body) && (body[i] == ' ' || body[i] == '\t') {
			i++
		}
		if i >= len(body) {
			break
		}
		key, val, next, ok := parseLogfmtPair(body, i)
		if !ok {
			return nil, false
		}
		i = next
		if i < len(body) && body[i] != ' ' && body[i] != '\t' {
			return nil, false
		}
		m[key] = val
	}
	if len(m) < 2 {
		return nil, false
	}
	return m, true
}

// parseLogfmtPair reads a single key=value token starting at body[i] (past
// any leading whitespace) and returns the key, the decoded value, and the
// index just past the token. ok is false for anything that isn't a clean
// key=value pair (no '=', an empty/quoted-looking key, or an unterminated
// quoted value) — the caller treats that as a hard rejection of the body.
func parseLogfmtPair(body string, i int) (key, val string, next int, ok bool) {
	eq := strings.IndexByte(body[i:], '=')
	if eq <= 0 {
		return "", "", 0, false
	}
	key = body[i : i+eq]
	if strings.ContainsAny(key, " \t\"") {
		return "", "", 0, false
	}
	i += eq + 1
	if i < len(body) && body[i] == '"' {
		s, n, ok := scanQuoted(body, i)
		if !ok {
			return "", "", 0, false
		}
		return key, s, n, true
	}
	end := strings.IndexAny(body[i:], " \t")
	if end < 0 {
		end = len(body) - i
	}
	return key, body[i : i+end], i + end, true
}

// scanQuoted reads a double-quoted value starting at body[start] == '"',
// honoring backslash escapes; returns the unquoted value and the index
// just past the closing quote.
func scanQuoted(body string, start int) (string, int, bool) {
	var b strings.Builder
	i := start + 1
	for i < len(body) {
		switch body[i] {
		case '\\':
			if i+1 >= len(body) {
				return "", 0, false
			}
			b.WriteByte(body[i+1])
			i += 2
		case '"':
			return b.String(), i + 1, true
		default:
			b.WriteByte(body[i])
			i++
		}
	}
	return "", 0, false
}

// hasLiftKeys reports whether m carries at least one message or level key —
// the signal that this is a log line, not arbitrary JSON data.
func hasLiftKeys(m map[string]any) bool {
	for _, k := range append(append([]string{}, messageKeys...), levelKeys...) {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

// liftFields applies the lifting semantics to l, consuming recognized keys
// from m and folding the remainder into Attrs.
func liftFields(l Log, m map[string]any) Log {
	if s, ok := takeFirstNonEmptyString(m, messageKeys); ok {
		l.Body = s
	}
	if s, ok := takeFirstScalar(m, levelKeys); ok && l.Severity == SeverityInfo {
		l.Severity = ParseSeverity(s)
	}
	if t, ok := takeFirstTime(m, timeKeys); ok {
		if d := t.Sub(l.Time); d < timeSanityWindow && d > -timeSanityWindow {
			l.Time = t
		}
	}
	if s, ok := takeFirstString(m, []string{"service"}); ok && l.Service == "" {
		l.Service = s
	}
	if s, ok := takeFirstString(m, []string{"trace_id"}); ok && l.TraceID == "" {
		l.TraceID = s
	}

	if len(m) == 0 {
		return l
	}
	return liftAttrs(l, m)
}

// liftAttrs folds remaining keys from m into l.Attrs, respecting caps and
// cloning l.Attrs if non-nil to avoid mutating the caller's map.
func liftAttrs(l Log, m map[string]any) Log {
	if l.Attrs == nil {
		l.Attrs = make(map[string]string, len(m))
	} else {
		// Clone to avoid mutating caller's Attrs map.
		cloned := make(map[string]string, len(l.Attrs)+len(m))
		for k, v := range l.Attrs {
			cloned[k] = v
		}
		l.Attrs = cloned
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lifted := 0
	for _, k := range keys {
		if lifted >= maxLiftedAttrs {
			break
		}
		if nested, ok := m[k].(map[string]any); ok {
			nkeys := make([]string, 0, len(nested))
			for nk := range nested {
				nkeys = append(nkeys, nk)
			}
			sort.Strings(nkeys)
			for _, nk := range nkeys {
				if lifted >= maxLiftedAttrs {
					break
				}
				lifted += setAttr(l.Attrs, k+"."+nk, scalarString(nested[nk]))
			}
			continue
		}
		lifted += setAttr(l.Attrs, k, scalarString(m[k]))
	}
	return l
}

// setAttr stores v under k unless the key already exists (ingest-provided
// attributes always win over body-derived ones) and returns 1 if stored.
func setAttr(attrs map[string]string, k, v string) int {
	if _, exists := attrs[k]; exists {
		return 0
	}
	if len(v) > maxLiftedValueLen {
		v = v[:maxLiftedValueLen]
	}
	attrs[k] = v
	return 1
}

// scalarString renders a decoded JSON value for attribute storage; nested
// structures keep their compact JSON encoding so nothing is lost.
func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return "null"
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// takeFirstString returns (and removes) the first of keys present in m
// whose value is a string.
func takeFirstString(m map[string]any, keys []string) (string, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			delete(m, k)
			if s, ok := v.(string); ok {
				return s, true
			}
			return "", false
		}
	}
	return "", false
}

// takeFirstNonEmptyString returns (and removes) the first of keys present
// in m whose value is a non-empty string; empty strings fall through to the
// next key. This ensures {"msg":"","message":"hi"} lifts "hi".
func takeFirstNonEmptyString(m map[string]any, keys []string) (string, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			delete(m, k)
			if s, ok := v.(string); ok && s != "" {
				return s, true
			}
		}
	}
	return "", false
}

// takeFirstScalar is takeFirstString but also accepts numbers, rendered as
// their literal text (ParseSeverity understands OTLP numeric levels).
func takeFirstScalar(m map[string]any, keys []string) (string, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			delete(m, k)
			switch x := v.(type) {
			case string:
				return x, true
			case json.Number:
				return x.String(), true
			}
			return "", false
		}
	}
	return "", false
}

// takeFirstTime accepts RFC3339(Nano) strings and epoch numbers
// (>= 1e12 means milliseconds, else seconds). All-digit strings are
// parsed as epoch times using the same threshold.
func takeFirstTime(m map[string]any, keys []string) (time.Time, bool) {
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			continue
		}
		delete(m, k)
		switch x := v.(type) {
		case string:
			if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
				return t, true
			}
			if t, err := time.Parse(time.RFC3339, x); err == nil {
				return t, true
			}
			// If all-digit string, parse as epoch time.
			if len(x) > 0 && isAllDigits(x) {
				if f, err := strconv.ParseFloat(x, 64); err == nil {
					if f >= 1e12 {
						return time.UnixMilli(int64(f)).UTC(), true
					}
					return time.Unix(int64(f), 0).UTC(), true
				}
			}
		case json.Number:
			if f, err := x.Float64(); err == nil {
				if f >= 1e12 {
					return time.UnixMilli(int64(f)).UTC(), true
				}
				return time.Unix(int64(f), 0).UTC(), true
			}
		}
		return time.Time{}, false
	}
	return time.Time{}, false
}

// isAllDigits reports whether s contains only ASCII digit bytes '0'..'9'.
func isAllDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
