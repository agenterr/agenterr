package ship

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/ship/docker"
	"github.com/agenterr/agenterr/internal/ship/process"
)

// --- Finding 1 (ship code review): dead docker tailers never resurrected ---
//
// countingDocker is a dockerSurface stub, distinct from ship_test.go's
// stubDocker, that additionally counts Logs calls per container ID — the
// observable proxy these tests use for "a tailer was (re)started for this
// container".

type countingDocker struct {
	mu         sync.Mutex
	containers []docker.Container
	logs       map[string]chan process.Line
	logsCount  map[string]int
	events     chan docker.Event
}

func newCountingDocker() *countingDocker {
	return &countingDocker{
		logs:      map[string]chan process.Line{},
		logsCount: map[string]int{},
		events:    make(chan docker.Event, 8),
	}
}

func (d *countingDocker) Ping(context.Context) error { return nil }

func (d *countingDocker) Containers(context.Context) ([]docker.Container, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]docker.Container, len(d.containers))
	copy(out, d.containers)
	return out, nil
}

func (d *countingDocker) Events(context.Context) (<-chan docker.Event, error) {
	return d.events, nil
}

func (d *countingDocker) Logs(_ context.Context, id string, _ time.Time) (<-chan process.Line, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.logsCount[id]++
	ch, ok := d.logs[id]
	if !ok {
		return nil, fmt.Errorf("countingDocker: no log stream registered for %s", id)
	}
	return ch, nil
}

func (d *countingDocker) addContainer(ct docker.Container, lines chan process.Line) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.containers = append(d.containers, ct)
	d.logs[ct.ID] = lines
}

func (d *countingDocker) logsCallCount(id string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.logsCount[id]
}

// reconnectDocker is a dockerSurface stub whose Events() always returns an
// already-closed channel, so runDockerSource's reconnect loop cycles
// through disconnect/backoff/reconnect on its own — used to drive the
// events-reconnect relist (Finding 1c) without a test needing to fake a
// second explicit "connection".
type reconnectDocker struct {
	mu         sync.Mutex
	containers []docker.Container
	logs       map[string]chan process.Line
	logsCount  map[string]int
}

func newReconnectDocker() *reconnectDocker {
	return &reconnectDocker{logs: map[string]chan process.Line{}, logsCount: map[string]int{}}
}

func (d *reconnectDocker) Ping(context.Context) error { return nil }

func (d *reconnectDocker) Containers(context.Context) ([]docker.Container, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]docker.Container, len(d.containers))
	copy(out, d.containers)
	return out, nil
}

// Events returns a fresh, already-closed channel on every call: from
// runDockerSource's perspective this is a "successful" connect (err == nil)
// immediately followed by the stream ending, so the reconnect loop keeps
// cycling (with growing backoff) for as long as the test lets it run.
func (d *reconnectDocker) Events(context.Context) (<-chan docker.Event, error) {
	ch := make(chan docker.Event)
	close(ch)
	return ch, nil
}

func (d *reconnectDocker) Logs(_ context.Context, id string, _ time.Time) (<-chan process.Line, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.logsCount[id]++
	ch, ok := d.logs[id]
	if !ok {
		return nil, fmt.Errorf("reconnectDocker: no log stream registered for %s", id)
	}
	return ch, nil
}

func (d *reconnectDocker) addContainer(ct docker.Container, lines chan process.Line) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.containers = append(d.containers, ct)
	d.logs[ct.ID] = lines
}

func (d *reconnectDocker) logsCallCount(id string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.logsCount[id]
}

// drainEvCh consumes and discards every sourceEvent sent to ch until ctx is
// done, so a tailer's eof/line sends never block these docker_source-level
// tests, which don't care about the joiner side of the pipeline.
func drainEvCh(ctx context.Context, ch <-chan sourceEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
		}
	}
}

// TestDockerSourceRelistResurrectsTailerAfterStreamEOFWithoutDie is
// scenario (i) from the review: a container's Logs stream EOFs (docker
// daemon restart / live-restore) with NO preceding "die" event. Before the
// fix, tailContainer returning left a stale entry in the internal cancels
// map forever, permanently blocking any future attempt (relist or start
// event) to re-tail that container. After the fix, the tailer's exit
// cleans up its own cancels entry, and the next events-reconnect relist
// (Finding 1c) starts a brand new tailer for the still-running container —
// observed here as a second Logs() call for the same ID.
func TestDockerSourceRelistResurrectsTailerAfterStreamEOFWithoutDie(t *testing.T) {
	sp := openTestSpool(t)
	dc := newReconnectDocker()

	// The lines channel is pre-closed: the very first Logs() call already
	// sees an immediate EOF with no die event ever having been observed for
	// this container, exactly the abnormal path Finding 1b warns about.
	closedLines := make(chan process.Line)
	close(closedLines)
	dc.addContainer(docker.Container{ID: "c1", Name: "web", Labels: nil}, closedLines)

	cfg := Config{Docker: true}
	evCh := make(chan sourceEvent, 64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drainEvCh(ctx, evCh)

	done := make(chan struct{})
	go func() {
		runDockerSource(ctx, cfg, dc, sp, evCh)
		close(done)
	}()

	// reconnectDocker's Events() closes immediately every call, so the
	// reconnect loop cycles: connect -> relist -> stream-ended -> backoff
	// -> connect -> relist -> ... Each relist calls startTailer for c1;
	// once the first tailer's cleanup has run (a couple of mutex ops after
	// its already-closed Logs channel is read — microseconds, comfortably
	// inside the >=1s a single backoff cycle takes), a later relist's
	// startTailer call is no longer blocked by a stale cancels entry and
	// issues a brand new Logs() call.
	waitFor(t, 5*time.Second, func() bool {
		return dc.logsCallCount("c1") >= 2
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runDockerSource did not return after ctx cancel")
	}
}

// TestDockerSourceStartEventRestartsTailerAfterOldOneDied is scenario (ii):
// once a container's old tailer has died (stream EOF, no die event), a
// "start" event for that same container ID must be able to start a new
// tailer rather than being silently swallowed by the exists-check finding
// a stale cancels entry.
func TestDockerSourceStartEventRestartsTailerAfterOldOneDied(t *testing.T) {
	sp := openTestSpool(t)
	dc := newCountingDocker()

	lines := make(chan process.Line, 4)
	dc.addContainer(docker.Container{ID: "c1", Name: "web", Labels: nil}, lines)

	cfg := Config{Docker: true}
	evCh := make(chan sourceEvent, 64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drainEvCh(ctx, evCh)

	done := make(chan struct{})
	go func() {
		runDockerSource(ctx, cfg, dc, sp, evCh)
		close(done)
	}()

	waitFor(t, 5*time.Second, func() bool { return dc.logsCallCount("c1") == 1 })

	// Kill the tailer's stream without a die event.
	close(lines)

	// Send a "start" event for the same ID, retrying it until it lands:
	// the very first send can race the old tailer's cleanup (still holding
	// the cancels entry, in which case startTailer's exists-check would
	// correctly no-op that particular delivery), but cleanup is a couple of
	// uncontended mutex operations — a handful of retries at 5ms intervals
	// comfortably outlasts it.
	waitFor(t, 5*time.Second, func() bool {
		if dc.logsCallCount("c1") >= 2 {
			return true
		}
		select {
		case dc.events <- docker.Event{Action: "start", ID: "c1"}:
		default:
		}
		return false
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runDockerSource did not return after ctx cancel")
	}
}
