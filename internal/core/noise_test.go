package core

import "testing"

func TestNoiseRuleMatches(t *testing.T) {
	log := func(service string, sev Severity, body string) Log {
		return Log{Service: service, Severity: sev, Body: body}
	}
	tests := []struct {
		name string
		rule NoiseRule
		log  Log
		want bool
	}{
		{"floor drops below", NoiseRule{Kind: NoiseSeverityFloor, Service: "traefik", Severity: SeverityWarn, Enabled: true},
			log("traefik", SeverityInfo, "poll"), true},
		{"floor keeps at floor", NoiseRule{Kind: NoiseSeverityFloor, Service: "traefik", Severity: SeverityWarn, Enabled: true},
			log("traefik", SeverityWarn, "warn"), false},
		{"floor ignores other service", NoiseRule{Kind: NoiseSeverityFloor, Service: "traefik", Severity: SeverityWarn, Enabled: true},
			log("api", SeverityDebug, "x"), false},
		{"empty service means any", NoiseRule{Kind: NoiseSeverityFloor, Service: "", Severity: SeverityWarn, Enabled: true},
			log("anything", SeverityDebug, "x"), true},
		{"disabled never matches", NoiseRule{Kind: NoiseSeverityFloor, Service: "", Severity: SeverityWarn, Enabled: false},
			log("anything", SeverityDebug, "x"), false},
		{"drop_match substring", NoiseRule{Kind: NoiseDropMatch, Service: "api", Pattern: "health check ok", Enabled: true},
			log("api", SeverityInfo, "GET /healthz health check ok"), true},
		{"drop_match no substring", NoiseRule{Kind: NoiseDropMatch, Service: "api", Pattern: "health check ok", Enabled: true},
			log("api", SeverityInfo, "GET /users 500"), false},
		{"drop_match empty pattern never matches", NoiseRule{Kind: NoiseDropMatch, Service: "api", Pattern: "", Enabled: true},
			log("api", SeverityInfo, "anything"), false},
		{"sample matches band", NoiseRule{Kind: NoiseSample, Service: "api", Severity: SeverityInfo, N: 10, Enabled: true},
			log("api", SeverityDebug, "x"), true},
		{"sample above band no match", NoiseRule{Kind: NoiseSample, Service: "api", Severity: SeverityInfo, N: 10, Enabled: true},
			log("api", SeverityError, "boom"), false},
		{"sample n<=1 never matches", NoiseRule{Kind: NoiseSample, Service: "api", Severity: SeverityInfo, N: 1, Enabled: true},
			log("api", SeverityDebug, "x"), false},
		{"unknown kind never matches", NoiseRule{Kind: "bogus", Enabled: true},
			log("api", SeverityDebug, "x"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rule.Matches(tt.log); got != tt.want {
				t.Errorf("Matches = %v, want %v", got, tt.want)
			}
		})
	}
}
