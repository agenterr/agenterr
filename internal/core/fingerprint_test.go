package core

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

func TestNormalizeMessage(t *testing.T) {
	cases := map[string]string{
		"user 4812 not found":                             "user <n> not found",
		"user 9944 not found":                             "user <n> not found",
		`open "/tmp/f83a.txt": no such file`:              `open <str>: no such file`,
		"conn to 10.0.3.7:5432 refused":                   "conn to <ip>:<n> refused",
		"req 550e8400-e29b-41d4-a716-446655440000 failed": "req <uuid> failed",
		"token deadbeefcafe1234 expired":                  "token <hex> expired",
	}
	for in, want := range cases {
		if got := NormalizeMessage(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFingerprintGroupsVariants(t *testing.T) {
	a := Fingerprint(Log{ProjectID: 1, Severity: SeverityError, Body: "user 4812 not found"})
	b := Fingerprint(Log{ProjectID: 1, Severity: SeverityError, Body: "user 9944 not found"})
	c := Fingerprint(Log{ProjectID: 2, Severity: SeverityError, Body: "user 4812 not found"})
	if a != b {
		t.Error("same template must share a fingerprint")
	}
	if a == c {
		t.Error("different projects must not share fingerprints")
	}
}

func TestFingerprintPrecedence(t *testing.T) {
	override := Log{ProjectID: 1, Attrs: map[string]string{"agenterr.fingerprint": "custom-x"}}
	if Fingerprint(override) != Fingerprint(override) || Fingerprint(override) == Fingerprint(Log{ProjectID: 1}) {
		t.Error("explicit agenterr.fingerprint must dominate")
	}
	exc := Log{ProjectID: 1, Severity: SeverityError, Body: "totally different text each time 12345",
		Attrs: map[string]string{"exception.type": "*pq.Error", "exception.message": "conn refused"}}
	exc2 := exc
	exc2.Body = "other text 999"
	if Fingerprint(exc) != Fingerprint(exc2) {
		t.Error("with exception attrs, fingerprint keys on them, not the body")
	}
}

func TestFingerprintDefaultGrouper(t *testing.T) {
	l := Log{ProjectID: 1, Severity: SeverityError, Body: "user 4812 not found"}
	var g DefaultGrouper
	if g.Fingerprint(l) != Fingerprint(l) {
		t.Error("DefaultGrouper.Fingerprint must delegate to package Fingerprint")
	}
}

func TestFingerprintIsHex16(t *testing.T) {
	fp := Fingerprint(Log{ProjectID: 1, Severity: SeverityError, Body: "boom"})
	if len(fp) != 16 {
		t.Fatalf("fingerprint length = %d, want 16", len(fp))
	}
	for _, r := range fp {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("fingerprint %q contains non-hex char %q", fp, r)
		}
	}
}

// TestFingerprintCorpus reads testdata/corpus.txt (format: "expected-group-id\traw log line")
// and asserts lines sharing a group id fingerprint identically, while lines from different
// group ids never collide.
func TestFingerprintCorpus(t *testing.T) {
	f, err := os.Open("testdata/corpus.txt")
	if err != nil {
		t.Fatalf("open corpus: %v", err)
	}
	defer f.Close()

	groupToFP := map[string]string{}
	fpToGroup := map[string]string{}

	scanner := bufio.NewScanner(f)
	lineNo := 0
	count := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			t.Fatalf("corpus.txt:%d: expected TAB-separated group-id and log line, got %q", lineNo, line)
		}
		group, raw := parts[0], parts[1]

		l := Log{ProjectID: 1, Severity: SeverityError, Body: raw}
		fp := Fingerprint(l)
		count++

		if wantFP, ok := groupToFP[group]; ok {
			if fp != wantFP {
				t.Errorf("corpus.txt:%d: group %q line %q fingerprinted to %s, want %s (same as other members of group)", lineNo, group, raw, fp, wantFP)
			}
		} else {
			groupToFP[group] = fp
		}

		if otherGroup, ok := fpToGroup[fp]; ok && otherGroup != group {
			t.Errorf("corpus.txt:%d: group %q line %q collides with group %q (fingerprint %s)", lineNo, group, raw, otherGroup, fp)
		} else {
			fpToGroup[fp] = group
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan corpus: %v", err)
	}
	if count < 15 {
		t.Fatalf("corpus.txt: expected at least 15 log lines, got %d", count)
	}
	if len(groupToFP) < 5 {
		t.Fatalf("corpus.txt: expected at least 5 distinct groups, got %d", len(groupToFP))
	}
}
