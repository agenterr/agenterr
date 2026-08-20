// Package rules owns the runtime state of noise filtering: cached rules,
// sampling counters, drop accounting.
package rules

import (
	"context"
	"log/slog"
	"regexp"
	"sync"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// Engine caches every project's noise rules, severity-lift rules, and
// parse-bodies toggle in memory. Reads are lock-cheap (RWMutex);
// mutations write through the store and then refresh the cache, so the
// pipeline never queries SQLite per record.
type Engine struct {
	nr  store.NoiseRules
	sr  store.SeverityRules
	adm store.Admin

	mu             sync.RWMutex
	loaded         bool
	rulesByProject map[int64][]store.NoiseRuleRow
	sevByProject   map[int64][]compiledSevRule
	parseBodies    map[int64]bool
	counters       map[int64]uint64 // sample rule ID -> banded records seen so far
	pendingDrops   map[int64]int64  // rule ID -> drops since last flush
	pendingLifts   map[int64]int64  // severity rule ID -> lifts since last flush
}

// compiledSevRule pairs a stored severity rule with its pre-compiled
// pattern, so Lift never compiles a regexp on the hot path.
type compiledSevRule struct {
	row store.SeverityRuleRow
	re  *regexp.Regexp
}

// New constructs an Engine. It starts unloaded (fail-open: Decide and
// Lift keep/leave everything alone) until Load succeeds. adm supplies the
// per-project parse-bodies toggle via Admin.Projects, since that lives on
// core.Project rather than the noise-rule store.
func New(nr store.NoiseRules, sr store.SeverityRules, adm store.Admin) *Engine {
	return &Engine{
		nr:           nr,
		sr:           sr,
		adm:          adm,
		counters:     map[int64]uint64{},
		pendingDrops: map[int64]int64{},
		pendingLifts: map[int64]int64{},
	}
}

// Load performs a full cache refresh: all noise rules (across all
// projects) and every project's parse-bodies toggle. Call at startup and
// after mutations. On error the previous cache (or unloaded state) is
// left untouched, so a failed reload never widens what gets dropped.
func (e *Engine) Load(ctx context.Context) error {
	// No defensive sort here: store.NoiseRules and store.SeverityRules are
	// documented (store.go) to return rows ordered by ascending ID, and
	// Decide's/Lift's first-match-wins evaluation relies on that order
	// being preserved per-project below.
	rows, err := e.nr.NoiseRules(ctx, 0)
	if err != nil {
		return err
	}
	sevRows, err := e.sr.SeverityRules(ctx, 0)
	if err != nil {
		return err
	}
	projects, err := e.adm.Projects(ctx)
	if err != nil {
		return err
	}

	byProject := make(map[int64][]store.NoiseRuleRow, len(rows))
	for _, r := range rows {
		byProject[r.ProjectID] = append(byProject[r.ProjectID], r)
	}

	// A pattern that fails to compile (a corrupt row predating upsert
	// validation) is skipped with a warning rather than failing Load —
	// one bad row must not disable severity lifting for the rest of the
	// project, or the project's whole ingest fleet.
	sevByProject := make(map[int64][]compiledSevRule, len(sevRows))
	for _, r := range sevRows {
		re, err := regexp.Compile(r.Pattern)
		if err != nil {
			slog.Warn("severity rule pattern failed to compile, skipping",
				"rule_id", r.ID, "project_id", r.ProjectID, "pattern", r.Pattern, "error", err)
			continue
		}
		sevByProject[r.ProjectID] = append(sevByProject[r.ProjectID], compiledSevRule{row: r, re: re})
	}

	parseBodies := make(map[int64]bool, len(projects))
	for _, p := range projects {
		parseBodies[p.ID] = p.ParseBodies
	}

	e.mu.Lock()
	e.rulesByProject = byProject
	e.sevByProject = sevByProject
	e.parseBodies = parseBodies
	e.loaded = true
	e.mu.Unlock()
	return nil
}

// Decide returns (true, ruleID) when l should be dropped. Sample rules
// keep the 1st and every nth banded record per rule. Also increments the
// in-memory drop counter for the winning rule.
//
// Rules for l.ProjectID are read once under RLock (the cached slice is
// never mutated in place — Load and the mutation helpers always swap in a
// new map/slice — so it's safe to keep using it after unlocking). Pure
// matching never needs more than that RLock; only a sample rule's counter
// or a winning rule's drop count take the write lock, and each such
// section is a single atomic increment-and-decide, so a concurrent
// FlushDrops can never observe a torn counter.
func (e *Engine) Decide(l core.Log) (drop bool, ruleID int64) {
	e.mu.RLock()
	loaded := e.loaded
	var rules []store.NoiseRuleRow
	if loaded {
		rules = e.rulesByProject[l.ProjectID]
	}
	e.mu.RUnlock()

	if !loaded || len(rules) == 0 {
		return false, 0 // fail-open: unloaded engine or no rules for this project
	}

	for _, r := range rules {
		if !r.Matches(l) {
			continue
		}
		if r.Kind == core.NoiseSample {
			if e.sampleSurvives(r.ID, r.N) {
				continue // in band but this one survives; keep checking later rules
			}
			e.recordDrop(r.ID)
			return true, r.ID
		}
		e.recordDrop(r.ID)
		return true, r.ID
	}
	return false, 0
}

// sampleSurvives atomically advances rule id's band counter and reports
// whether the just-counted record is a survivor (index 0, n, 2n, ...).
func (e *Engine) sampleSurvives(id int64, n int) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.counters[id]
	e.counters[id] = c + 1
	return c%uint64(n) == 0
}

// recordDrop atomically bumps id's pending drop count.
func (e *Engine) recordDrop(id int64) {
	e.mu.Lock()
	e.pendingDrops[id]++
	e.mu.Unlock()
}

// Lift returns l with its severity raised, plus the winning rule's ID (0
// = no lift), when a severity rule matches. Lift only ever raises: it
// fires solely when l.Severity is still at-or-below core.SeverityInfo
// (the ingest default), never on a log that already carries a meaningful
// severity from its own record. The first matching enabled rule by
// ascending ID wins, mirroring Decide's first-match-wins evaluation.
//
// Same locking discipline as Decide: rules for l.ProjectID are read once
// under RLock (the cached slice is never mutated in place), and only a
// winning rule's lift count takes the write lock.
func (e *Engine) Lift(l core.Log) (core.Log, int64) {
	e.mu.RLock()
	loaded := e.loaded
	var rules []compiledSevRule
	if loaded {
		rules = e.sevByProject[l.ProjectID]
	}
	e.mu.RUnlock()

	if !loaded || len(rules) == 0 {
		return l, 0 // fail-open: unloaded engine or no rules for this project
	}
	if l.Severity > core.SeverityInfo {
		return l, 0 // already meaningful; lifting is only for the ingest default and below
	}

	for _, r := range rules {
		if !r.row.Enabled {
			continue
		}
		if r.row.Service != "" && r.row.Service != l.Service {
			continue
		}
		if !r.re.MatchString(l.Body) {
			continue
		}
		l.Severity = r.row.Severity
		e.recordLift(r.row.ID)
		return l, r.row.ID
	}
	return l, 0
}

// recordLift atomically bumps id's pending lift count.
func (e *Engine) recordLift(id int64) {
	e.mu.Lock()
	e.pendingLifts[id]++
	e.mu.Unlock()
}

// ParseBodies reports the per-project toggle (default true for unknown
// projects, and for an unloaded engine — fail-open means parsing stays on
// rather than silently disabling until Load succeeds).
func (e *Engine) ParseBodies(projectID int64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.loaded {
		return true
	}
	on, ok := e.parseBodies[projectID]
	if !ok {
		return true
	}
	return on
}

// FlushDrops persists and resets in-memory drop counters. Safe to call
// concurrently with Decide: the swap-then-unlock happens under the same
// mutex Decide's recordDrop uses, so every increment is either reflected
// in this flush's snapshot or the next one, never lost or double-counted.
// Always calls through to the store, even with an empty map, so callers
// relying on a fixed flush cadence see a consistent no-op rather than a
// silently skipped call.
func (e *Engine) FlushDrops(ctx context.Context) error {
	e.mu.Lock()
	counts := e.pendingDrops
	e.pendingDrops = map[int64]int64{}
	e.mu.Unlock()

	if err := e.nr.AddNoiseDrops(ctx, counts); err != nil {
		// Best-effort: fold the un-persisted counts back in rather than
		// lose them, per "no drop is invisible".
		e.mu.Lock()
		for id, n := range counts {
			e.pendingDrops[id] += n
		}
		e.mu.Unlock()
		return err
	}
	return nil
}

// FlushLifts persists and resets in-memory lift counters. Safe to call
// concurrently with Lift: the swap-then-unlock happens under the same
// mutex Lift's recordLift uses, so every increment is either reflected in
// this flush's snapshot or the next one, never lost or double-counted.
// Always calls through to the store, even with an empty map, so callers
// relying on a fixed flush cadence see a consistent no-op rather than a
// silently skipped call.
func (e *Engine) FlushLifts(ctx context.Context) error {
	e.mu.Lock()
	counts := e.pendingLifts
	e.pendingLifts = map[int64]int64{}
	e.mu.Unlock()

	if err := e.sr.AddSeverityLifts(ctx, counts); err != nil {
		// Best-effort: fold the un-persisted counts back in rather than
		// lose them, per "no lift is invisible".
		e.mu.Lock()
		for id, n := range counts {
			e.pendingLifts[id] += n
		}
		e.mu.Unlock()
		return err
	}
	return nil
}

// UpsertSeverity writes through the store, then reloads the cache so Lift
// sees the change immediately.
func (e *Engine) UpsertSeverity(ctx context.Context, r core.SeverityRule) (store.SeverityRuleRow, error) {
	row, err := e.sr.UpsertSeverityRule(ctx, r)
	if err != nil {
		return store.SeverityRuleRow{}, err
	}
	if err := e.Load(ctx); err != nil {
		return row, err
	}
	return row, nil
}

// DeleteSeverity writes through the store, then reloads the cache.
func (e *Engine) DeleteSeverity(ctx context.Context, id int64) error {
	if err := e.sr.DeleteSeverityRule(ctx, id); err != nil {
		return err
	}
	return e.Load(ctx)
}

// Upsert writes through the store, then reloads the cache so Decide sees
// the change immediately.
func (e *Engine) Upsert(ctx context.Context, r core.NoiseRule) (store.NoiseRuleRow, error) {
	row, err := e.nr.UpsertNoiseRule(ctx, r)
	if err != nil {
		return store.NoiseRuleRow{}, err
	}
	if err := e.Load(ctx); err != nil {
		return row, err
	}
	return row, nil
}

// Delete writes through the store, then reloads the cache.
func (e *Engine) Delete(ctx context.Context, id int64) error {
	if err := e.nr.DeleteNoiseRule(ctx, id); err != nil {
		return err
	}
	return e.Load(ctx)
}

// SetParseBodies writes through the store, then reloads the cache.
func (e *Engine) SetParseBodies(ctx context.Context, projectID int64, on bool) error {
	if err := e.nr.SetProjectParseBodies(ctx, projectID, on); err != nil {
		return err
	}
	return e.Load(ctx)
}
