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
	cancels := make(map[string]context.CancelFunc)
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
		cctx, cancel := context.WithCancel(ctx)
		cancels[ct.ID] = cancel
		mu.Unlock()

		tailersWG.Add(1)
		go func() {
			defer tailersWG.Done()
			tailContainer(cctx, dc, spool, ct.ID, svc, evCh)
		}()
	}
	stopTailer := func(id string) {
		mu.Lock()
		cancel, exists := cancels[id]
		delete(cancels, id)
		mu.Unlock()
		if exists {
			cancel()
		}
	}

	if cts, err := dc.Containers(ctx); err != nil {
		log.Printf("ship: WARN listing containers at startup: %v", err)
	} else {
		for _, ct := range cts {
			startTailer(ct)
		}
	}

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
// The read loop selects on ctx.Done() explicitly rather than a plain
// `range lines`: the dockerSurface contract (matching the real
// docker.Client.Logs) says the channel closes on ctx cancellation too, but
// this loop doesn't take that on faith — an implementation slow to close
// after cancellation (or, in a test, a stub that doesn't replicate the
// close-on-cancel behavior) must still not block orchestrator shutdown.
func tailContainer(ctx context.Context, dc dockerSurface, spool *buffer.Spool, containerID, service string, evCh chan<- sourceEvent) {
	since := time.Time{}
	if t, ok := spool.Since(containerID); ok {
		since = t.Add(-time.Second)
	}

	lines, err := dc.Logs(ctx, containerID, since)
	if err != nil {
		log.Printf("ship: WARN starting log stream for container %s (%s): %v", containerID, service, err)
		return
	}
readLoop:
	for {
		select {
		case line, ok := <-lines:
			if !ok {
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
}
