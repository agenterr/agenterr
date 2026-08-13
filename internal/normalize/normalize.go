// Package normalize cleans raw log bodies before anything downstream —
// body parsing, severity detection, fingerprinting, storage, search —
// ever sees them. Stripping happens exactly once, at the top of the
// pipeline, so every consumer works with the same clean bytes.
package normalize

import "strings"

// StripANSI removes ANSI escape sequences from s and reports whether any
// SGR sequence set the red or bright-red foreground (parameter 31 or 91).
// The red hint exists for the optional severity heuristic (spec §1);
// stripping itself is unconditional. CSI sequences (ESC '[' params
// final-byte in 0x40..0x7E) are removed whole; a bare ESC followed by a
// single non-'[' byte is removed as a pair; a trailing lone or
// unterminated escape is dropped to end of string. The escape bytes are
// deliberately not preserved anywhere — this is the system's one
// intentional data loss.
func StripANSI(s string) (string, bool) {
	if !strings.ContainsRune(s, 0x1b) {
		return s, false
	}
	var b strings.Builder
	b.Grow(len(s))
	red := false
	for i := 0; i < len(s); {
		if s[i] != 0x1b {
			b.WriteByte(s[i])
			i++
			continue
		}
		if i+1 >= len(s) { // lone ESC at end
			break
		}
		if s[i+1] != '[' { // bare ESC pair (e.g. ESC c)
			i += 2
			continue
		}
		j := i + 2
		for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
			j++
		}
		if j >= len(s) { // unterminated CSI: drop the rest
			break
		}
		if s[j] == 'm' && hasRedParam(s[i+2:j]) {
			red = true
		}
		i = j + 1
	}
	return b.String(), red
}

func hasRedParam(params string) bool {
	for _, p := range strings.Split(params, ";") {
		if p == "31" || p == "91" {
			return true
		}
	}
	return false
}
