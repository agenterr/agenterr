// Package process implements pure, synchronous line processing for the
// agenterr-ship log tailer: ANSI escape stripping and multiline joining.
// No goroutines, no timers — the join-window timer is the caller's job
// (it calls Flush when the window elapses). Stdlib only.
package process

import (
	"regexp"
	"time"
)

// Line is one raw source line with its source-assigned timestamp.
type Line struct {
	Text string
	Time time.Time
}

// Record is one cleaned, possibly-joined log record.
type Record struct {
	Text string
	Time time.Time
}

// csiRe matches ANSI CSI sequences: ESC '[' parameter bytes (0x30-0x3F)
// intermediate bytes (0x20-0x2F) then a single final byte (0x40-0x7E).
// This covers SGR (color/style, e.g. \x1b[31;1m) and cursor-movement
// codes (e.g. \x1b[2K, \x1b[1G) alike. A lone ESC not followed by '['
// does not match and is left intact.
var csiRe = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

// StripANSI removes ANSI CSI/SGR escape sequences from s, leaving plain
// text (including multibyte UTF-8) and any lone ESC byte untouched.
func StripANSI(s string) string {
	return csiRe.ReplaceAllString(s, "")
}

// Continuation-rule patterns, verbatim from the ship semantics doc:
//   - starts with space or tab (checked without regex, see isContinuation)
//   - ^(at |Caused by:|... N more)   (Java-style traces)
//   - ^goroutine N [                 (Go panic dumps, only right after a
//     line that itself matched panicFatalRe)
var (
	contRe       = regexp.MustCompile(`^(at |Caused by:|\.\.\. \d+ more)`)
	goroutineRe  = regexp.MustCompile(`^goroutine \d+ \[`)
	panicFatalRe = regexp.MustCompile(`^(panic:|fatal error:)`)
)

// isContinuation reports whether text continues the pending record, given
// whether the immediately preceding fed line matched panicFatalRe.
func isContinuation(text string, prevMatchedPanicFatal bool) bool {
	if len(text) > 0 && (text[0] == ' ' || text[0] == '\t') {
		return true
	}
	if contRe.MatchString(text) {
		return true
	}
	if prevMatchedPanicFatal && goroutineRe.MatchString(text) {
		return true
	}
	return false
}

// pending holds the in-progress joined record plus its running byte size
// (len of Text, tracked separately so we don't re-scan on every Feed).
type pending struct {
	text string
	time time.Time
	size int
}

func (p *pending) toRecord() Record {
	return Record{Text: p.text, Time: p.time}
}

// Joiner accumulates lines into records. Feed returns any records
// completed by this line; Flush returns the pending record (if any) —
// call it on the join-window timer and at source EOF. Not goroutine-safe;
// one Joiner per source.
type Joiner struct {
	maxBytes int
	pending  *pending
	// lastMatchedPanicFatal reflects whether the most recently fed raw
	// line (not record) matched panicFatalRe — this is what gates the
	// "goroutine N [" continuation rule.
	lastMatchedPanicFatal bool
	capHits               int
}

// NewJoiner returns a Joiner that flushes a join in progress once its
// joined text would exceed maxBytes.
func NewJoiner(maxBytes int) *Joiner {
	return &Joiner{maxBytes: maxBytes}
}

// CapHits returns how many times a join was force-flushed for hitting
// maxBytes (rather than by a natural non-continuation line).
func (j *Joiner) CapHits() int {
	return j.capHits
}

// Feed processes one line, returning any records it completed. A line
// either continues the pending record, starts a new one (flushing the old
// pending record as a completed Record), or — if joining it would push the
// pending record past maxBytes — force-flushes the pending record and
// starts a new one with this line.
func (j *Joiner) Feed(l Line) []Record {
	var completed []Record

	matchedPanicFatal := panicFatalRe.MatchString(l.Text)

	switch {
	case j.pending == nil:
		j.pending = &pending{text: l.Text, time: l.Time, size: len(l.Text)}

	case isContinuation(l.Text, j.lastMatchedPanicFatal):
		newSize := j.pending.size + 1 + len(l.Text) // +1 for the joining \n
		if newSize > j.maxBytes {
			completed = append(completed, j.pending.toRecord())
			j.capHits++
			j.pending = &pending{text: l.Text, time: l.Time, size: len(l.Text)}
		} else {
			j.pending.text += "\n" + l.Text
			j.pending.size = newSize
		}

	default:
		completed = append(completed, j.pending.toRecord())
		j.pending = &pending{text: l.Text, time: l.Time, size: len(l.Text)}
	}

	j.lastMatchedPanicFatal = matchedPanicFatal
	return completed
}

// Flush returns the pending record, if any, and resets the Joiner's state
// (including the panic/fatal continuation gate — a fresh record after
// Flush starts with no continuation history).
func (j *Joiner) Flush() []Record {
	if j.pending == nil {
		return nil
	}
	r := []Record{j.pending.toRecord()}
	j.pending = nil
	j.lastMatchedPanicFatal = false
	return r
}
