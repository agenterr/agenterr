package core

// AlertRuleKind names an alert-rule matching strategy.
type AlertRuleKind string

// Alert-rule kinds.
const (
	AlertNewIssue   AlertRuleKind = "new_issue"
	AlertRegression AlertRuleKind = "regression"
	AlertThreshold  AlertRuleKind = "threshold"
)

// AlertRule represents an alert rule that fires when matching events occur.
// Kind-specific conditions (newness, reopen, window counts) are the engine's job.
type AlertRule struct {
	ID              int64
	ProjectID       int64
	Name            string
	Kind            AlertRuleKind
	Service         string // "" = any service
	Environment     string // "" = any environment
	MinSeverity     Severity
	N               int // threshold only
	WindowMinutes   int // threshold only
	CooldownSeconds int
	URL             string
	Headers         map[string]string
	Enabled         bool
}

// MatchesEvent reports whether a log event falls inside this rule's scope
// (service/environment/min-severity). Kind-specific conditions (newness, reopen,
// window counts) are the engine's job.
func (r AlertRule) MatchesEvent(l Log) bool {
	if !r.Enabled {
		return false
	}
	if r.Service != "" && r.Service != l.Service {
		return false
	}
	if r.Environment != "" && r.Environment != l.Environment {
		return false
	}
	// MinSeverity is a floor: events below it don't match; at or above match.
	// Zero value (SeverityTrace) means match all (nothing is below trace).
	if l.Severity < r.MinSeverity {
		return false
	}
	return true
}
