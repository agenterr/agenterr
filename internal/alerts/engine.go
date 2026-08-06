// Package alerts owns alert-rule evaluation (new-issue, regression,
// threshold windows), cooldown bookkeeping, and asynchronous webhook
// delivery. Evaluation runs on the pipeline's single writer goroutine and
// must never block on I/O; delivery happens on a separate worker fed by a
// bounded channel.
package alerts

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// queueCap is the delivery queue's buffer size. IssueEvent never blocks on
// it: a full queue drops the fire (logged, counted) rather than stall the
// writer goroutine.
const queueCap = 256

// defaultTimeout is used for the HTTP client when New is given a nil one.
const defaultTimeout = 5 * time.Second

// Engine caches every project's alert rules in memory and evaluates them
// against pipeline events. Reads are lock-cheap (RWMutex); mutations write
// through the store and then refresh the cache, mirroring rules.Engine.
type Engine struct {
	ar     store.AlertRules
	client *http.Client

	mu             sync.RWMutex
	loaded         bool
	rulesByProject map[int64][]store.AlertRuleRow
	rulesByID      map[int64]store.AlertRuleRow
	windows        map[int64]*window   // rule ID -> threshold ring, in-memory only
	lastFired      map[int64]time.Time // rule ID -> cooldown anchor

	queue   chan fireJob
	dropped counter

	// backoff is the delay before retry attempts 2 and 3 (index 0, 1).
	// Unexported so production keeps the documented 1s/4s while tests
	// inject sub-millisecond values instead of sleeping for real.
	backoff []time.Duration
}

// New constructs an Engine. client nil selects a default 5s-timeout
// *http.Client for webhook delivery.
func New(ar store.AlertRules, client *http.Client) *Engine {
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}
	return &Engine{
		ar:             ar,
		client:         client,
		rulesByProject: map[int64][]store.AlertRuleRow{},
		rulesByID:      map[int64]store.AlertRuleRow{},
		windows:        map[int64]*window{},
		lastFired:      map[int64]time.Time{},
		queue:          make(chan fireJob, queueCap),
		backoff:        []time.Duration{time.Second, 4 * time.Second},
	}
}

// Load performs a full cache refresh from the store: every alert rule
// (enabled or not — MatchesEvent filters disabled rules at evaluation
// time, but TestFire needs to reach disabled rules too), keyed both by
// project and by ID. Cooldown state for rule IDs no longer present is
// pruned; rules seen for the first time have their in-memory cooldown
// seeded from the stored LastFired so a process restart doesn't forget an
// active cooldown. Rules already tracked in memory keep their in-memory
// value (which may be ahead of the store if delivery hasn't recorded yet).
func (e *Engine) Load(ctx context.Context) error {
	// No defensive sort: store.AlertRules is documented to return rows
	// ordered by ascending ID, and IssueEvent's deterministic fan-out
	// order relies on that being preserved per-project below.
	rows, err := e.ar.AlertRules(ctx, 0)
	if err != nil {
		return err
	}

	byProject := make(map[int64][]store.AlertRuleRow, len(rows))
	byID := make(map[int64]store.AlertRuleRow, len(rows))
	for _, r := range rows {
		byProject[r.ProjectID] = append(byProject[r.ProjectID], r)
		byID[r.ID] = r
	}

	e.mu.Lock()
	e.rulesByProject = byProject
	e.rulesByID = byID
	e.loaded = true
	for id, row := range byID {
		if _, ok := e.lastFired[id]; !ok && !row.LastFired.IsZero() {
			e.lastFired[id] = row.LastFired
		}
	}
	for id := range e.windows {
		if _, ok := byID[id]; !ok {
			delete(e.windows, id)
		}
	}
	for id := range e.lastFired {
		if _, ok := byID[id]; !ok {
			delete(e.lastFired, id)
		}
	}
	e.mu.Unlock()
	return nil
}

// IssueEvent implements pipeline.Notifier. It evaluates every enabled rule
// scoped to the entry's project: kind-specific matching (newness, reopen,
// threshold window), a cooldown check, and — only for rules that fire — a
// non-blocking enqueue onto the delivery queue. No I/O happens here; this
// runs on the pipeline's single writer goroutine.
func (e *Engine) IssueEvent(en store.Entry, o store.IssueOutcome) {
	e.mu.RLock()
	loaded := e.loaded
	rules := e.rulesByProject[en.Log.ProjectID]
	e.mu.RUnlock()
	if !loaded || len(rules) == 0 {
		return
	}

	now := en.Log.Time
	if now.IsZero() {
		now = time.Now()
	}

	// rules is ordered by ascending ID (see Load); iterating it directly
	// gives deterministic fan-out order without a defensive copy — the
	// slice itself is never mutated in place, only wholesale-replaced.
	for _, r := range rules {
		if !r.MatchesEvent(en.Log) {
			continue
		}

		var eventCount, windowMinutes int
		var kindMatch bool
		switch r.Kind {
		case core.AlertNewIssue:
			kindMatch = o.New
		case core.AlertRegression:
			kindMatch = o.Reopened
		case core.AlertThreshold:
			eventCount = e.recordThresholdEvent(r.ID, now, r.WindowMinutes)
			windowMinutes = r.WindowMinutes
			kindMatch = r.N > 0 && eventCount >= r.N
		default:
			continue // unknown kind — never matches
		}
		if !kindMatch {
			continue
		}

		if !e.tryFire(r.ID, r.CooldownSeconds, now) {
			continue // suppressed by cooldown; not queued
		}

		e.enqueue(fireJob{
			rule:          r,
			issueID:       o.IssueID,
			title:         en.Title,
			severity:      en.Log.Severity,
			eventTime:     now,
			eventCount:    eventCount,
			windowMinutes: windowMinutes,
		})
	}
}

// tryFire reports whether rule id may fire now given its cooldown, and if
// so records now as the new cooldown anchor. Suppression counts from
// enqueue time, not delivery completion, per the alert semantics doc.
func (e *Engine) tryFire(id int64, cooldownSeconds int, now time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if last, ok := e.lastFired[id]; ok && cooldownSeconds > 0 {
		if now.Sub(last) < time.Duration(cooldownSeconds)*time.Second {
			return false
		}
	}
	e.lastFired[id] = now
	return true
}

// recordThresholdEvent records one matching event for rule id's sliding
// window and returns the current in-window count.
func (e *Engine) recordThresholdEvent(id int64, t time.Time, windowMinutes int) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	w, ok := e.windows[id]
	if !ok {
		w = newWindow()
		e.windows[id] = w
	}
	return w.add(t, windowMinutes)
}

// enqueue hands a fire off to the delivery worker without ever blocking
// the caller: a full queue drops the notification, incrementing the
// dropped counter and logging a warning — alerting is best-effort, ingest
// is not.
func (e *Engine) enqueue(job fireJob) {
	select {
	case e.queue <- job:
	default:
		e.dropped.add(1)
		slog.Warn("alerts: delivery queue full, dropping fire", "rule_id", job.rule.ID, "project_id", job.rule.ProjectID)
	}
}

// Dropped reports how many fires were dropped because the delivery queue
// was full.
func (e *Engine) Dropped() int64 {
	return e.dropped.load()
}

// Upsert writes through the store, then reloads the cache so IssueEvent
// and TestFire see the change immediately.
func (e *Engine) Upsert(ctx context.Context, r core.AlertRule) (store.AlertRuleRow, error) {
	row, err := e.ar.UpsertAlertRule(ctx, r)
	if err != nil {
		return store.AlertRuleRow{}, err
	}
	if err := e.Load(ctx); err != nil {
		return row, err
	}
	return row, nil
}

// Delete writes through the store, then reloads the cache.
func (e *Engine) Delete(ctx context.Context, id int64) error {
	if err := e.ar.DeleteAlertRule(ctx, id); err != nil {
		return err
	}
	return e.Load(ctx)
}

// ruleByID looks up a cached rule regardless of project or enabled state,
// used by TestFire.
func (e *Engine) ruleByID(id int64) (store.AlertRuleRow, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	r, ok := e.rulesByID[id]
	return r, ok
}
