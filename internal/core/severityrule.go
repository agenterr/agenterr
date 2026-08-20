package core

// SeverityRule lifts the severity of logs whose body matches Pattern
// (a Go regexp) and whose severity is still at-or-below the ingest
// default (SeverityInfo) after the earlier lift-chain slots (spec §1
// item 4). Rules only lift — downgrading is the severity_floor noise
// rule's domain — and the first matching rule by ascending ID wins.
type SeverityRule struct {
	ID        int64
	ProjectID int64
	Service   string   // "" = any service
	Pattern   string   // Go regexp matched against the log body
	Severity  Severity // lifted-to severity; must be > SeverityInfo
	Enabled   bool
}
