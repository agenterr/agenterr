package core

import "testing"

func TestParseSeverity(t *testing.T) {
	cases := map[string]Severity{
		"error": SeverityError, "ERROR": SeverityError, "err": SeverityError,
		"warn": SeverityWarn, "warning": SeverityWarn,
		"fatal": SeverityFatal, "panic": SeverityFatal, "critical": SeverityFatal,
		"info": SeverityInfo, "debug": SeverityDebug, "trace": SeverityTrace,
		"21":       SeverityFatal, // OTLP numeric severity 21-24 = FATAL
		"17":       SeverityError, // OTLP numeric severity 17-20 = ERROR
		"13":       SeverityWarn,  // OTLP numeric severity 13-16 = WARN
		"9":        SeverityInfo,  // OTLP numeric severity 9-12 = INFO
		"5":        SeverityDebug, // OTLP numeric severity 5-8 = DEBUG
		"1":        SeverityTrace, // OTLP numeric severity 1-4 = TRACE
		"0":        SeverityInfo,  // OTLP UNSPECIFIED falls through every band
		"nonsense": SeverityInfo, "": SeverityInfo,
	}
	for in, want := range cases {
		if got := ParseSeverity(in); got != want {
			t.Errorf("ParseSeverity(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestParseSeverityStrict(t *testing.T) {
	cases := []struct {
		in      string
		want    Severity
		wantOK  bool
		comment string
	}{
		{"", SeverityTrace, true, "empty string is not-supplied, accepted as the zero value"},
		{"warn", SeverityWarn, true, "known name"},
		{"WARN", SeverityWarn, true, "case-insensitive"},
		{" warn ", SeverityWarn, true, "trims whitespace"},
		{"wrn", 0, false, "typo of a known name is rejected, not defaulted to info"},
		{"21", 0, false, "OTLP numeric severities are not accepted by the strict variant"},
		{"nonsense", 0, false, "unknown name rejected"},
	}
	for _, c := range cases {
		got, ok := ParseSeverityStrict(c.in)
		if ok != c.wantOK || (ok && got != c.want) {
			t.Errorf("ParseSeverityStrict(%q) = (%v, %v), want (%v, %v) — %s", c.in, got, ok, c.want, c.wantOK, c.comment)
		}
	}
}

func TestSeverityString(t *testing.T) {
	cases := map[Severity]string{
		SeverityTrace: "TRACE", SeverityDebug: "DEBUG", SeverityInfo: "INFO",
		SeverityWarn: "WARN", SeverityError: "ERROR", SeverityFatal: "FATAL",
		Severity(99): "UNKNOWN", // out-of-range value hits the default case
	}
	for sev, want := range cases {
		if got := sev.String(); got != want {
			t.Errorf("Severity(%d).String() = %q, want %q", sev, got, want)
		}
	}
}
