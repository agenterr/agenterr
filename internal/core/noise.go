package core

import "strings"

// NoiseRuleKind names a noise-rule matching strategy.
type NoiseRuleKind string

// Noise-rule kinds.
const (
	NoiseSeverityFloor NoiseRuleKind = "severity_floor"
	NoiseDropMatch     NoiseRuleKind = "drop_match"
	NoiseSample        NoiseRuleKind = "sample"
)

// NoiseRule is one per-project ingest filtering rule. Matching is pure;
// the stateful parts of filtering (sampling counters, drop accounting)
// live with the engine that owns the rules at runtime.
type NoiseRule struct {
	ID        int64
	ProjectID int64
	Kind      NoiseRuleKind
	Service   string   // "" = any service
	Severity  Severity // floor for severity_floor; band ceiling for sample
	Pattern   string   // drop_match substring
	N         int      // sample: keep 1 in N
	Enabled   bool
}

// Matches reports whether l falls inside this rule's scope. For sample
// rules that means "in the sampled band", not "must drop" — the caller's
// counter decides which banded records survive. Unknown kinds and
// degenerate parameters (empty pattern, n<=1) never match, so a
// misconfigured rule fails open instead of black-holing ingest.
func (r NoiseRule) Matches(l Log) bool {
	if !r.Enabled {
		return false
	}
	if r.Service != "" && r.Service != l.Service {
		return false
	}
	switch r.Kind {
	case NoiseSeverityFloor:
		return l.Severity < r.Severity
	case NoiseDropMatch:
		return r.Pattern != "" && strings.Contains(l.Body, r.Pattern)
	case NoiseSample:
		return r.N > 1 && l.Severity <= r.Severity
	}
	return false
}
