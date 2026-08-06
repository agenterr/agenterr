package core

import "testing"

func TestParseSeverity(t *testing.T) {
	cases := map[string]Severity{
		"error": SeverityError, "ERROR": SeverityError, "err": SeverityError,
		"warn": SeverityWarn, "warning": SeverityWarn,
		"fatal": SeverityFatal, "panic": SeverityFatal,
		"info": SeverityInfo, "debug": SeverityDebug, "trace": SeverityTrace,
		"17": SeverityError, // OTLP numeric severity 17-20 = ERROR
		"9":  SeverityInfo,
		"nonsense": SeverityInfo, "": SeverityInfo,
	}
	for in, want := range cases {
		if got := ParseSeverity(in); got != want {
			t.Errorf("ParseSeverity(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestSeverityString(t *testing.T) {
	if SeverityError.String() != "ERROR" || SeverityWarn.String() != "WARN" {
		t.Fatal("String() must return canonical uppercase names")
	}
}
