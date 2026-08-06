package core

import "testing"

func TestIsEvent(t *testing.T) {
	cases := []struct {
		name string
		log  Log
		want bool
	}{
		{"error severity", Log{Severity: SeverityError}, true},
		{"fatal severity", Log{Severity: SeverityFatal}, true},
		{"warn alone", Log{Severity: SeverityWarn}, false},
		{"info alone", Log{Severity: SeverityInfo}, false},
		{"warn with exception attrs", Log{Severity: SeverityWarn,
			Attrs: map[string]string{"exception.type": "*net.OpError"}}, true},
		{"info with exception.message only", Log{Severity: SeverityInfo,
			Attrs: map[string]string{"exception.message": "boom"}}, true},
		{"nil attrs no panic", Log{Severity: SeverityDebug}, false},
	}
	for _, c := range cases {
		if got := IsEvent(c.log); got != c.want {
			t.Errorf("%s: IsEvent = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestTitle(t *testing.T) {
	l := Log{Body: "connection refused\nstack: ...",
		Attrs: map[string]string{"exception.type": "*fiber.Error"}}
	if got := Title(l); got != "*fiber.Error: connection refused" {
		t.Errorf("Title = %q", got)
	}
	if got := Title(Log{Body: "plain failure"}); got != "plain failure" {
		t.Errorf("Title = %q", got)
	}
}
