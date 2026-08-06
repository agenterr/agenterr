package rules

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/agenterr/agenterr/internal/core"
)

func mustLoad(t *testing.T, e *Engine) {
	t.Helper()
	if err := e.Load(context.Background()); err != nil {
		t.Fatalf("Load: %v", err)
	}
}

// 1. Sample rules keep the 1st and every nth banded record; the rest drop.
// Records are distinguishable (r0..r8) so this pins WHICH records survive
// (indices 0, 3, 6), not just how many — an off-by-one that shifted the
// band (e.g. surviving 1,4,7 instead of 0,3,6) would still pass a
// count-only assertion but fails this one.
func TestDecide_SampleKeepsEveryNth(t *testing.T) {
	fs := newFakeStore()
	fs.seedRule(core.NoiseRule{
		ProjectID: 1, Kind: core.NoiseSample, Service: "api",
		Severity: core.SeverityInfo, N: 3, Enabled: true,
	})
	e := New(fs, fs)
	mustLoad(t, e)

	var kept []string
	var dropped []string
	for i := 0; i < 9; i++ {
		body := fmt.Sprintf("r%d", i)
		l := core.Log{ProjectID: 1, Service: "api", Severity: core.SeverityInfo, Body: body}
		if drop, _ := e.Decide(l); drop {
			dropped = append(dropped, body)
		} else {
			kept = append(kept, body)
		}
	}

	wantKept := []string{"r0", "r3", "r6"}
	wantDropped := []string{"r1", "r2", "r4", "r5", "r7", "r8"}
	if !slices.Equal(kept, wantKept) {
		t.Errorf("kept = %v, want %v", kept, wantKept)
	}
	if !slices.Equal(dropped, wantDropped) {
		t.Errorf("dropped = %v, want %v", dropped, wantDropped)
	}
}

// 2. First-drop-wins in ascending rule-ID order; a record matching no
// rule is kept.
func TestDecide_FirstDropWinsAscendingID(t *testing.T) {
	fs := newFakeStore()
	lowID := fs.seedRule(core.NoiseRule{
		ProjectID: 1, Kind: core.NoiseDropMatch, Service: "api",
		Pattern: "boom", Enabled: true,
	})
	highID := fs.seedRule(core.NoiseRule{
		ProjectID: 1, Kind: core.NoiseSeverityFloor, Service: "api",
		Severity: core.SeverityFatal, Enabled: true, // would also match (anything below fatal)
	})
	if lowID.ID >= highID.ID {
		t.Fatalf("test setup: expected lowID < highID, got %d, %d", lowID.ID, highID.ID)
	}
	e := New(fs, fs)
	mustLoad(t, e)

	// Matches both rules: the lower-ID drop_match rule must win.
	drop, ruleID := e.Decide(core.Log{ProjectID: 1, Service: "api", Severity: core.SeverityWarn, Body: "boom happened"})
	if !drop || ruleID != lowID.ID {
		t.Errorf("Decide = (%v, %d), want (true, %d)", drop, ruleID, lowID.ID)
	}

	// Matches neither: kept.
	drop, ruleID = e.Decide(core.Log{ProjectID: 1, Service: "other", Severity: core.SeverityFatal, Body: "clean"})
	if drop {
		t.Errorf("Decide = (%v, %d), want (false, 0) for non-matching record", drop, ruleID)
	}
}

// 3. Records from projects with no rules are kept.
func TestDecide_NoRulesForProjectKept(t *testing.T) {
	fs := newFakeStore()
	fs.seedRule(core.NoiseRule{ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "x", Enabled: true})
	e := New(fs, fs)
	mustLoad(t, e)

	drop, ruleID := e.Decide(core.Log{ProjectID: 999, Body: "x is here"})
	if drop {
		t.Errorf("Decide = (%v, %d), want (false, 0) for project with no rules", drop, ruleID)
	}
}

// 4. Unloaded engine (Load never called or failed) keeps everything.
func TestDecide_UnloadedEngineFailsOpen(t *testing.T) {
	fs := newFakeStore()
	fs.seedRule(core.NoiseRule{ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true})
	e := New(fs, fs) // Load never called

	drop, ruleID := e.Decide(core.Log{ProjectID: 1, Body: "boom"})
	if drop {
		t.Errorf("Decide = (%v, %d), want (false, 0) for unloaded engine", drop, ruleID)
	}
}

// 5. Drop counters accumulate, FlushDrops persists exact counts and
// resets; a second flush with no new drops is a no-op (no phantom counts).
func TestFlushDrops_PersistsAndResets(t *testing.T) {
	fs := newFakeStore()
	r := fs.seedRule(core.NoiseRule{ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true})
	e := New(fs, fs)
	mustLoad(t, e)

	for i := 0; i < 5; i++ {
		if drop, _ := e.Decide(core.Log{ProjectID: 1, Body: "boom"}); !drop {
			t.Fatalf("expected drop on iteration %d", i)
		}
	}

	if err := e.FlushDrops(context.Background()); err != nil {
		t.Fatalf("FlushDrops: %v", err)
	}
	fs.mu.Lock()
	if len(fs.dropCalls) != 1 || fs.dropCalls[0][r.ID] != 5 {
		fs.mu.Unlock()
		t.Fatalf("first flush = %+v, want [{%d: 5}]", fs.dropCalls, r.ID)
	}
	fs.mu.Unlock()

	if err := e.FlushDrops(context.Background()); err != nil {
		t.Fatalf("FlushDrops (2nd): %v", err)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.dropCalls) != 2 {
		t.Fatalf("expected 2 flush calls, got %d", len(fs.dropCalls))
	}
	if len(fs.dropCalls[1]) != 0 {
		t.Errorf("second flush counts = %+v, want empty (no phantom counts)", fs.dropCalls[1])
	}
}

// 6. ParseBodies is false only for projects stored false; unknown → true.
func TestParseBodies_DefaultTrueUnlessStoredFalse(t *testing.T) {
	fs := newFakeStore()
	fs.addProject(1, false)
	fs.addProject(2, true)
	e := New(fs, fs)
	mustLoad(t, e)

	if e.ParseBodies(1) {
		t.Error("ParseBodies(1) = true, want false (stored false)")
	}
	if !e.ParseBodies(2) {
		t.Error("ParseBodies(2) = false, want true (stored true)")
	}
	if !e.ParseBodies(999) {
		t.Error("ParseBodies(999) = false, want true (unknown project defaults true)")
	}
}

// 7. Mutations write through and are visible to Decide immediately.
func TestMutations_VisibleImmediately(t *testing.T) {
	fs := newFakeStore()
	fs.addProject(1, true)
	e := New(fs, fs)
	mustLoad(t, e)

	// Nothing dropped yet.
	if drop, _ := e.Decide(core.Log{ProjectID: 1, Body: "boom"}); drop {
		t.Fatal("expected keep before any rule exists")
	}

	row, err := e.Upsert(context.Background(), core.NoiseRule{
		ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true,
	})
	if err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if drop, ruleID := e.Decide(core.Log{ProjectID: 1, Body: "boom"}); !drop || ruleID != row.ID {
		t.Fatalf("Decide after Upsert = (%v, %d), want (true, %d)", drop, ruleID, row.ID)
	}

	if err := e.Delete(context.Background(), row.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if drop, _ := e.Decide(core.Log{ProjectID: 1, Body: "boom"}); drop {
		t.Fatal("expected keep after Delete")
	}

	if !e.ParseBodies(1) {
		t.Fatal("expected ParseBodies(1) = true before SetParseBodies")
	}
	if err := e.SetParseBodies(context.Background(), 1, false); err != nil {
		t.Fatalf("SetParseBodies: %v", err)
	}
	if e.ParseBodies(1) {
		t.Fatal("expected ParseBodies(1) = false after SetParseBodies")
	}
}

// 8. Concurrency: parallel Decide + FlushDrops + Upsert under -race, no
// races, no lost counts (total decided drops == sum of flushed counts).
func TestConcurrency_NoLostDrops(t *testing.T) {
	fs := newFakeStore()
	dropRule := fs.seedRule(core.NoiseRule{
		ProjectID: 1, Kind: core.NoiseDropMatch, Pattern: "boom", Enabled: true,
	})
	e := New(fs, fs)
	mustLoad(t, e)

	const (
		decideWorkers = 8
		iterations    = 400
		flushWorkers  = 3
		upsertWorkers = 2
	)

	var totalDropped int64
	var workWG sync.WaitGroup // decide + upsert workers: the "real" work
	var flushWG sync.WaitGroup

	workWG.Add(decideWorkers)
	for i := 0; i < decideWorkers; i++ {
		go func() {
			defer workWG.Done()
			for j := 0; j < iterations; j++ {
				if drop, id := e.Decide(core.Log{ProjectID: 1, Body: "boom"}); drop {
					if id != dropRule.ID {
						t.Errorf("unexpected winning rule ID %d", id)
					}
					atomic.AddInt64(&totalDropped, 1)
				}
			}
		}()
	}

	workWG.Add(upsertWorkers)
	for i := 0; i < upsertWorkers; i++ {
		go func() {
			defer workWG.Done()
			for j := 0; j < iterations/10; j++ {
				// Unrelated rule/project: exercises concurrent cache
				// reload without perturbing the drop-counting rule.
				_, _ = e.Upsert(context.Background(), core.NoiseRule{
					ProjectID: 2, Kind: core.NoiseSeverityFloor, Service: "svc",
					Severity: core.SeverityWarn, Enabled: true,
				})
			}
		}()
	}

	// Flush workers race against Decide/Upsert for the duration of the
	// real work, then stop once it's done; a final flush after they exit
	// drains anything left regardless of timing.
	stop := make(chan struct{})
	flushWG.Add(flushWorkers)
	for i := 0; i < flushWorkers; i++ {
		go func() {
			defer flushWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = e.FlushDrops(context.Background())
				}
			}
		}()
	}

	workWG.Wait()
	close(stop)
	flushWG.Wait()

	if err := e.FlushDrops(context.Background()); err != nil {
		t.Fatalf("final FlushDrops: %v", err)
	}

	fs.mu.Lock()
	var totalFlushed int64
	for _, call := range fs.dropCalls {
		for _, n := range call {
			totalFlushed += n
		}
	}
	fs.mu.Unlock()

	got := atomic.LoadInt64(&totalDropped)
	if got != totalFlushed {
		t.Errorf("totalDropped = %d, totalFlushed = %d, want equal (no lost counts)", got, totalFlushed)
	}
	if got != decideWorkers*iterations {
		t.Errorf("totalDropped = %d, want %d (every record matched)", got, decideWorkers*iterations)
	}
}
