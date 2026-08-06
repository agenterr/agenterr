package alerts

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// fakeStore is an in-memory store.AlertRules, sharing one mutex across
// every method so concurrent engine tests exercise realistic contention —
// mirrors internal/rules's fakeStore.
type fakeStore struct {
	mu         sync.Mutex
	rules      map[int64]store.AlertRuleRow
	nextRuleID int64

	recordCalls []recordCall
}

type recordCall struct {
	id      int64
	firedAt time.Time
	lastErr string
}

func newFakeStore() *fakeStore {
	return &fakeStore{rules: map[int64]store.AlertRuleRow{}}
}

// seedRule inserts a rule directly (bypassing validation) and returns the
// stored row, for tests that want to control the ID/LastFired precisely.
func (f *fakeStore) seedRule(r core.AlertRule) store.AlertRuleRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextRuleID++
	r.ID = f.nextRuleID
	row := store.AlertRuleRow{AlertRule: r}
	f.rules[r.ID] = row
	return row
}

// seedRuleWithLastFired is like seedRule but also sets LastFired, for
// restart-survivability cooldown tests.
func (f *fakeStore) seedRuleWithLastFired(r core.AlertRule, lastFired time.Time) store.AlertRuleRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextRuleID++
	r.ID = f.nextRuleID
	row := store.AlertRuleRow{AlertRule: r, LastFired: lastFired}
	f.rules[r.ID] = row
	return row
}

func (f *fakeStore) AlertRules(ctx context.Context, projectID int64) ([]store.AlertRuleRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.rules))
	for id := range f.rules {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []store.AlertRuleRow
	for _, id := range ids {
		r := f.rules[id]
		if projectID == 0 || r.ProjectID == projectID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeStore) UpsertAlertRule(ctx context.Context, r core.AlertRule) (store.AlertRuleRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Kind != core.AlertNewIssue && r.Kind != core.AlertRegression && r.Kind != core.AlertThreshold {
		return store.AlertRuleRow{}, fmt.Errorf("unknown kind %q", r.Kind)
	}
	if r.ID == 0 {
		f.nextRuleID++
		r.ID = f.nextRuleID
	} else if _, ok := f.rules[r.ID]; !ok {
		return store.AlertRuleRow{}, store.ErrNotFound
	}
	row := store.AlertRuleRow{AlertRule: r}
	if existing, ok := f.rules[r.ID]; ok {
		row.LastFired = existing.LastFired
		row.LastError = existing.LastError
		row.CreatedAt = existing.CreatedAt
	}
	f.rules[r.ID] = row
	return row, nil
}

func (f *fakeStore) DeleteAlertRule(ctx context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rules[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.rules, id)
	return nil
}

func (f *fakeStore) RecordAlertResult(ctx context.Context, id int64, firedAt time.Time, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordCalls = append(f.recordCalls, recordCall{id: id, firedAt: firedAt, lastErr: lastError})
	row, ok := f.rules[id]
	if !ok {
		return nil // silent no-op on missing ID, per the store contract
	}
	row.LastFired = firedAt
	row.LastError = lastError
	f.rules[id] = row
	return nil
}

func (f *fakeStore) recordCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.recordCalls)
}

func (f *fakeStore) lastRecordCall() (recordCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.recordCalls) == 0 {
		return recordCall{}, false
	}
	return f.recordCalls[len(f.recordCalls)-1], true
}
