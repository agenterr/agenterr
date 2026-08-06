package core

import "testing"

func TestAlertRuleMatchesEvent(t *testing.T) {
	log := func(service, environment string, sev Severity) Log {
		return Log{Service: service, Environment: environment, Severity: sev}
	}
	tests := []struct {
		name string
		rule AlertRule
		log  Log
		want bool
	}{
		// Disabled rules never match
		{"disabled never matches", AlertRule{Enabled: false, Service: "", Environment: "", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityInfo), false},

		// Service matching: exact match required unless empty
		{"service exact match", AlertRule{Enabled: true, Service: "api", Environment: "", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityInfo), true},
		{"service mismatch", AlertRule{Enabled: true, Service: "api", Environment: "", MinSeverity: SeverityTrace},
			log("web", "prod", SeverityInfo), false},
		{"empty service means any", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityInfo), true},

		// Environment matching: exact match required unless empty
		{"environment exact match", AlertRule{Enabled: true, Service: "", Environment: "prod", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityInfo), true},
		{"environment mismatch", AlertRule{Enabled: true, Service: "", Environment: "prod", MinSeverity: SeverityTrace},
			log("api", "staging", SeverityInfo), false},
		{"empty environment means any", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityInfo), true},

		// MinSeverity: floor matching
		{"min-severity below matches false", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityWarn},
			log("api", "prod", SeverityInfo), false},
		{"min-severity at floor matches true", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityWarn},
			log("api", "prod", SeverityWarn), true},
		{"min-severity above floor matches true", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityWarn},
			log("api", "prod", SeverityError), true},
		{"zero severity matches all", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityDebug), true},
		{"zero severity matches all even trace", AlertRule{Enabled: true, Service: "", Environment: "", MinSeverity: SeverityTrace},
			log("api", "prod", SeverityTrace), true},

		// Combined scopes
		{"all scopes match", AlertRule{Enabled: true, Service: "api", Environment: "prod", MinSeverity: SeverityWarn},
			log("api", "prod", SeverityError), true},
		{"service matches but environment fails", AlertRule{Enabled: true, Service: "api", Environment: "prod", MinSeverity: SeverityTrace},
			log("api", "staging", SeverityInfo), false},
		{"service matches but severity fails", AlertRule{Enabled: true, Service: "api", Environment: "prod", MinSeverity: SeverityWarn},
			log("api", "prod", SeverityInfo), false},
		{"all scopes and fields", AlertRule{Enabled: true, Service: "web", Environment: "staging", MinSeverity: SeverityDebug},
			log("web", "staging", SeverityDebug), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rule.MatchesEvent(tt.log); got != tt.want {
				t.Errorf("MatchesEvent = %v, want %v", got, tt.want)
			}
		})
	}
}
