package alerts

import "time"

// window tracks a threshold rule's sliding event count as per-minute
// buckets, pruned lazily on access rather than on a timer. This keeps
// IssueEvent's hot path free of background goroutines per rule.
type window struct {
	buckets map[int64]int // minute epoch (unix seconds / 60) -> count
}

func newWindow() *window {
	return &window{buckets: map[int64]int{}}
}

// add records one event at time t, prunes buckets that have aged out of a
// windowMinutes-wide window ending at t's minute, and returns the
// resulting in-window count.
func (w *window) add(t time.Time, windowMinutes int) int {
	minute := t.Unix() / 60
	w.prune(minute, windowMinutes)
	w.buckets[minute]++
	total := 0
	for _, n := range w.buckets {
		total += n
	}
	return total
}

// prune drops buckets older than windowMinutes relative to nowMinute. A
// non-positive windowMinutes (misconfigured rule) is treated as a
// zero-width window: every prior bucket is dropped, keeping only the
// current minute.
func (w *window) prune(nowMinute int64, windowMinutes int) {
	if windowMinutes < 1 {
		windowMinutes = 1
	}
	cutoff := nowMinute - int64(windowMinutes) + 1
	for k := range w.buckets {
		if k < cutoff {
			delete(w.buckets, k)
		}
	}
}
