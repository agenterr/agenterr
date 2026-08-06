package ship

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/agenterr/agenterr/internal/ship/buffer"
	"github.com/agenterr/agenterr/internal/ship/docker"
	"github.com/agenterr/agenterr/internal/ship/process"
)

// dockerSurface is the narrow slice of docker.Client this package consumes.
// Declaring it lets ship_test.go drive the orchestrator with a stub instead
// of a real Docker socket — the docker package's own tests already prove
// the real Client against a fake daemon (see docker_test.go's unix-socket
// harness); this package's tests focus on the wiring, not re-proving Logs/
// Events framing.
type dockerSurface interface {
	Ping(ctx context.Context) error
	Containers(ctx context.Context) ([]docker.Container, error)
	Events(ctx context.Context) (<-chan docker.Event, error)
	Logs(ctx context.Context, containerID string, since time.Time) (<-chan process.Line, error)
}

// runDockerSource discovers currently-running selected containers, starts a
// log tailer for each, and follows /events to add/remove tailers as
// containers start/die. It reconnects the event stream with backoff on
// disconnect and returns once ctx is done and every tailer it started has
// exited.
func runDockerSource(ctx context.Context, cfg Config, dc dockerSurface, spool *buffer.Spool, evCh chan<- sourceEvent) {
	var mu sync.Mutex
	// cancels holds a live tailer's cancel func, keyed by container ID.
	// Values are pointers (not bare context.CancelFuncs) so a tailer's exit
	// cleanup can tell "the entry I own" apart from "a newer entry that
	// replaced mine" by identity (func values aren't comparable in Go) —
	// see startTailer's cleanup and stopTailer's race-guard comment.
	cancels := make(map[string]*tailerHandle)
	// diedByEvent marks container IDs whose tailer was stopped because of an
	// observed "die" event (as opposed to its Logs stream just ending on its
	// own — daemon restart, live-restore — which is the abnormal case
	// tailContainer's cleanup below warns about).
	diedByEvent := make(map[string]bool)
	var tailersWG sync.WaitGroup

	startTailer := func(ct docker.Container) {
		svc := docker.ServiceName(ct)
		if !docker.Selected(svc, ct.Labels, cfg.Only, cfg.Exclude) {
			return
		}
		mu.Lock()
		if _, exists := cancels[ct.ID]; exists {
			mu.Unlock()
			return
		}
		// Clear any stale died-flag from a previous tailer instance of this
		// same ID (container ID reuse is not something Docker normally
		// does, but nothing here depends on that never happening).
		delete(diedByEvent, ct.ID)
		cctx, cancel := context.WithCancel(ctx)
		handle := &tailerHandle{cancel: cancel}
		cancels[ct.ID] = handle
		mu.Unlock()

		tailersWG.Add(1)
		go func() {
			defer tailersWG.Done()
			naturalEOF := tailContainer(cctx, dc, spool, ct.ID, svc, evCh)

			mu.Lock()
			died := diedByEvent[ct.ID]
			delete(diedByEvent, ct.ID)
			// Only remove OUR entry: if stopTailer already removed it (die
			// event) that's a no-op; if a start event raced in and this ID
			// already has a NEWER handle, removing it here would wrongly
			// unblock startTailer's exists-check for a tailer that's still
			// alive under that newer handle.
			if cancels[ct.ID] == handle {
				delete(cancels, ct.ID)
			}
			mu.Unlock()

			if naturalEOF && !died {
				// The Logs stream ended on its own, with no preceding die
				// event observed for this container — e.g. a docker daemon
				// restart with live-restore. This tailer is now gone and
				// its cancels entry has just been freed, so a later "start"
				// event for this ID would no longer be exists-blocked; but
				// nothing re-lists containers on its own here, hence the
				// WARN rather than a quieter log level: this container
				// needs a relist to actually get resumed (see the
				// events-reconnect relist below, and Finding 3's comment on
				// consumeEvents for the sibling gap).
				log.Printf("ship: WARN log stream for container %s (%s) ended without a preceding die event (daemon restart or live-restore?); it stays untailed until the next containers relist", ct.ID, svc)
			}
		}()
	}
	stopTailer := func(id string) {
		mu.Lock()
		handle, exists := cancels[id]
		delete(cancels, id)
		diedByEvent[id] = true
		mu.Unlock()
		if exists {
			handle.cancel()
		}
	}

	// listAndStartTailers lists currently-running containers and starts a
	// tailer for each selected one not already tailed (startTailer's
	// exists-check makes this idempotent). Used both at startup and after
	// every events-stream (re)connect — see the loop below and Finding 1's
	// comment there for why the reconnect call matters.
	listAndStartTailers := func() {
		cts, err := dc.Containers(ctx)
		if err != nil {
			log.Printf("ship: WARN listing containers: %v", err)
			return
		}
		for _, ct := range cts {
			startTailer(ct)
		}
	}

	listAndStartTailers()

	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for ctx.Err() == nil {
		events, err := dc.Events(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("ship: WARN docker events connection failed, retrying in %s: %v", backoff, err)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = time.Second
		// Re-list running containers on every successful (re)connect, not
		// just at startup. This is the fix for two related gaps: (1) a
		// "start" event missed while disconnected (events don't replay —
		// see consumeEvents) never otherwise gets picked up; (2) a tailer
		// whose Logs stream died on its own (daemon restart/live-restore,
		// no "die" event — see tailContainer's cleanup above) leaves its
		// cancels entry removed but nothing else ever notices the
		// container is still running until this relist happens.
		// startTailer's exists-check makes this a no-op for containers
		// already tailed.
		listAndStartTailers()
		streamEnded := consumeEvents(ctx, events, startTailer, stopTailer, dc)
		if ctx.Err() != nil {
			break
		}
		if streamEnded {
			log.Printf("ship: WARN docker events stream ended, reconnecting in %s", backoff)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff, maxBackoff)
		}
	}

	tailersWG.Wait()
}

// tailerHandle wraps a tailer's cancel func so the map that tracks it can be
// compared by pointer identity (context.CancelFunc values, like all Go func
// values, aren't comparable with ==) — see startTailer's exit cleanup.
type tailerHandle struct {
	cancel context.CancelFunc
}

// consumeEvents reads ev from events until ctx is done or the channel
// closes (streamEnded=true — the caller should reconnect). It is written as
// a select on both events and ctx.Done() rather than a plain `range events`
// so that a channel which, unlike the real docker.Client contract, doesn't
// close on ctx cancellation (e.g. a test stub) still lets this loop exit
// promptly on shutdown instead of blocking forever.
func consumeEvents(ctx context.Context, events <-chan docker.Event, startTailer func(docker.Container), stopTailer func(string), dc dockerSurface) (streamEnded bool) {
	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-events:
			if !ok {
				return true
			}
			switch ev.Action {
			case "start":
				cts, err := dc.Containers(ctx)
				if err != nil {
					// Swallowing this silently would hide a real listing
					// failure; it's still not fatal, though, because
					// runDockerSource's events-reconnect relist (see
					// listAndStartTailers) retries every running container,
					// including this one, on the next reconnect — so a
					// WARN rather than an error return is enough here.
					log.Printf("ship: WARN listing containers for start event (container %s): %v", ev.ID, err)
					continue
				}
				for _, ct := range cts {
					if ct.ID == ev.ID {
						startTailer(ct)
						break
					}
				}
			case "die":
				stopTailer(ev.ID)
			}
		}
	}
}

// tailContainer streams one container's logs into evCh from its persisted
// "since" checkpoint (minus 1s overlap, per the restart-semantics doc),
// updating the checkpoint as lines arrive. It returns once the Logs channel
// closes (container died and drained) or ctx is cancelled, after trying to
// send an eof event so the joiner loop flushes and forgets this container's
// joiner (a best-effort send: on shutdown the joiner loop's own evCh-close
// handling flushes every remaining joiner anyway, so a dropped eof here
// isn't a correctness gap — see runJoinerLoop).
//
// naturalEOF reports whether this call returned because the Logs channel
// itself closed (true) as opposed to ctx being cancelled first (false,
// whether that cancellation came from a "die" event via stopTailer or from
// orchestrator shutdown). The caller (startTailer's goroutine) uses this,
// together with whether a "die" event was actually observed for this
// container, to decide whether the stream ending was expected or worth a
// WARN — see Finding 1's dead-tailer-resurrection fix in runDockerSource.
//
// The read loop selects on ctx.Done() explicitly rather than a plain
// `range lines`: the dockerSurface contract (matching the real
// docker.Client.Logs) says the channel closes on ctx cancellation too, but
// this loop doesn't take that on faith — an implementation slow to close
// after cancellation (or, in a test, a stub that doesn't replicate the
// close-on-cancel behavior) must still not block orchestrator shutdown.
func tailContainer(ctx context.Context, dc dockerSurface, spool *buffer.Spool, containerID, service string, evCh chan<- sourceEvent) (naturalEOF bool) {
	since := time.Time{}
	if t, ok := spool.Since(containerID); ok {
		since = t.Add(-time.Second)
	}

	lines, err := dc.Logs(ctx, containerID, since)
	if err != nil {
		log.Printf("ship: WARN starting log stream for container %s (%s): %v", containerID, service, err)
		return false
	}
readLoop:
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				naturalEOF = true
				break readLoop
			}
			select {
			case evCh <- sourceEvent{key: containerID, service: service, isDocker: true, line: line}:
			case <-ctx.Done():
				break readLoop
			}
		case <-ctx.Done():
			break readLoop
		}
	}
	select {
	case evCh <- sourceEvent{key: containerID, eof: true}:
	case <-ctx.Done():
	}
	return naturalEOF
}
