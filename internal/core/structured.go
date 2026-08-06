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
// (>= 1e12 means milliseconds, else seconds).
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
