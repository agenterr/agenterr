package rules

import (
	"context"
	"sort"
	"sync"

	"github.com/agenterr/agenterr/internal/core"
	"github.com/agenterr/agenterr/internal/store"
)

// fakeStore is an in-memory implementation of store.NoiseRules and
// store.Admin, sharing state the way the real SQLite backend does
// (parse-bodies lives on core.Project, rules live in their own table).
// Every method takes the same mutex so concurrent engine tests exercise
// realistic contention.
type fakeStore struct {
	mu         sync.Mutex
	rules      map[int64]store.NoiseRuleRow
	nextRuleID int64
	projects   map[int64]core.Project
	nextProjID int64
	dropCalls  []map[int64]int64
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		rules:    map[int64]store.NoiseRuleRow{},
		projects: map[int64]core.Project{},
	}
}

func (f *fakeStore) addProject(id int64, parseBodies bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.projects[id] = core.Project{ID: id, ParseBodies: parseBodies}
}

func (f *fakeStore) seedRule(r core.NoiseRule) store.NoiseRuleRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextRuleID++
	r.ID = f.nextRuleID
	row := store.NoiseRuleRow{NoiseRule: r}
	f.rules[r.ID] = row
	return row
}

func (f *fakeStore) NoiseRules(_ context.Context, projectID int64) ([]store.NoiseRuleRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.rules))
	for id := range f.rules {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []store.NoiseRuleRow
	for _, id := range ids {
		r := f.rules[id]
		if projectID == 0 || r.ProjectID == projectID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeStore) UpsertNoiseRule(_ context.Context, r core.NoiseRule) (store.NoiseRuleRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.ID == 0 {
		f.nextRuleID++
		r.ID = f.nextRuleID
	} else if _, ok := f.rules[r.ID]; !ok {
		return store.NoiseRuleRow{}, store.ErrNotFound
	}
	row := store.NoiseRuleRow{NoiseRule: r}
	if existing, ok := f.rules[r.ID]; ok {
		row.DroppedCount = existing.DroppedCount
		row.CreatedAt = existing.CreatedAt
	}
	f.rules[r.ID] = row
	return row, nil
}

func (f *fakeStore) DeleteNoiseRule(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.rules[id]; !ok {
		return store.ErrNotFound
	}
	delete(f.rules, id)
	return nil
}

func (f *fakeStore) AddNoiseDrops(_ context.Context, counts map[int64]int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[int64]int64, len(counts))
	for id, n := range counts {
		cp[id] = n
		if row, ok := f.rules[id]; ok {
			row.DroppedCount += n
			f.rules[id] = row
		}
	}
	f.dropCalls = append(f.dropCalls, cp)
	return nil
}

func (f *fakeStore) SetProjectParseBodies(_ context.Context, projectID int64, on bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.projects[projectID]
	if !ok {
		p = core.Project{ID: projectID}
	}
	p.ParseBodies = on
	f.projects[projectID] = p
	return nil
}

// Admin methods beyond Projects are unused by the engine; stubs keep
// fakeStore satisfying store.Admin.
func (f *fakeStore) CreateProject(_ context.Context, name string, retentionDays int) (core.Project, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextProjID++
	p := core.Project{ID: f.nextProjID, Name: name, RetentionDays: retentionDays, ParseBodies: true}
	f.projects[p.ID] = p
	return p, nil
}

func (f *fakeStore) Projects(_ context.Context) ([]core.Project, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]core.Project, 0, len(f.projects))
	for _, p := range f.projects {
		out = append(out, p)
	}
	return out, nil
}

func (f *fakeStore) SetIssueStatus(_ context.Context, _ int64, _ core.IssueStatus) error {
	return nil
}

func (f *fakeStore) MintKey(_ context.Context, _ int64, _ string) (string, error) {
	return "", nil
}

func (f *fakeStore) LookupKey(_ context.Context, _ string) (int64, string, error) {
	return 0, "", nil
}
