package core

import (
	"strings"
	"testing"
	"unicode/utf8"
)

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
		{"empty exception.type not event", Log{Severity: SeverityWarn,
			Attrs: map[string]string{"exception.type": ""}}, false},
		{"empty exception.message not event", Log{Severity: SeverityInfo,
			Attrs: map[string]string{"exception.message": ""}}, false},
		{"exception.stacktrace present", Log{Severity: SeverityInfo,
			Attrs: map[string]string{"exception.stacktrace": "at main()"}}, true},
		{"empty exception.stacktrace not event", Log{Severity: SeverityWarn,
			Attrs: map[string]string{"exception.stacktrace": ""}}, false},
	}
	for _, c := range cases {
		if got := IsEvent(c.log); got != c.want {
			t.Errorf("%s: IsEvent = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestTitle(t *testing.T) {
	cases := []struct {
		name string
		log  Log
		want string
	}{
		{
			"with exception type and multiline body",
			Log{Body: "connection refused\nstack: ...",
				Attrs: map[string]string{"exception.type": "*fiber.Error"}},
			"*fiber.Error: connection refused",
		},
		{
			"plain body no exception",
			Log{Body: "plain failure"},
			"plain failure",
		},
		{
			"empty body with exception type",
			Log{Body: "", Attrs: map[string]string{"exception.type": "CustomError"}},
			"CustomError",
		},
		{
			"empty exception.type ignored",
			Log{Body: "error happened",
				Attrs: map[string]string{"exception.type": ""}},
			"error happened",
		},
		{
			"200 rune cap with ASCII",
			Log{Body: strings.Repeat("a", 250)},
			strings.Repeat("a", 200),
		},
		{
			"200 rune cap with multi-byte runes",
			Log{Body: strings.Repeat("é", 250)},
			strings.Repeat("é", 200),
		},
		{
			"exception type prefix with 200 rune cap",
			Log{Body: strings.Repeat("日", 200),
				Attrs: map[string]string{"exception.type": "DBError"}},
			"DBError: " + strings.Repeat("日", 191),
		},
	}

	for _, c := range cases {
		got := Title(c.log)
		if got != c.want {
			t.Errorf("%s:\n  got  %q (len=%d runes)\n  want %q (len=%d runes)",
				c.name, got, utf8.RuneCountInString(got), c.want, utf8.RuneCountInString(c.want))
		}
		// Verify no rune is split (result is valid UTF-8)
		if utf8.ValidString(got) == false {
			t.Errorf("%s: result contains split runes", c.name)
		}
		// Verify cap is enforced
		if utf8.RuneCountInString(got) > 200 {
			t.Errorf("%s: result exceeds 200 runes (got %d)", c.name, utf8.RuneCountInString(got))
		}
	}
}
