package ship

import (
	"context"
	"log"
	"time"

	"github.com/agenterr/agenterr/internal/ship/buffer"
	"github.com/agenterr/agenterr/internal/ship/file"
	"github.com/agenterr/agenterr/internal/ship/process"
	"github.com/agenterr/agenterr/internal/ship/shared"
)

// maxJoinBytes is the per-record join cap passed to every process.Joiner
// (64KB, per the ship semantics doc).
const maxJoinBytes = 64 * 1024

// sourceEvent is what a producer (docker tailer or file tailer) sends to
// the central joiner loop. eof signals the source has drained (container
// died, file tailer's ctx was cancelled) so its joiner can be flushed and
// forgotten; isDocker gates the SetSince call, which only makes sense for
// container sources (key is then the container ID).
type sourceEvent struct {
	key      string
	service  string
	isDocker bool
	line     process.Line
	eof      bool
}

// runJoinerLoop is the single owner of every process.Joiner in the
// pipeline: one per source key (container ID or "file:"+glob), fed by evCh
// and flushed either by the shared join-window ticker or by a source's eof
// event. Owning all joiners from one goroutine sidesteps process.Joiner's
// "not goroutine-safe" contract entirely rather than adding per-joiner
// locking. It returns once evCh is closed, after flushing every joiner with
// pending content.
func runJoinerLoop(spool *buffer.Spool, evCh <-chan sourceEvent, joinWindow time.Duration) {
	joiners := make(map[string]*process.Joiner)
	services := make(map[string]string)

	ticker := time.NewTicker(joinWindow)
	defer ticker.Stop()

	flushOne := func(key string) {
		j, ok := joiners[key]
		if !ok {
			return
		}
		for _, r := range j.Flush() {
			appendRecord(spool, services[key], r)
		}
	}

	for {
		select {
		case ev, ok := <-evCh:
			if !ok {
				for key := range joiners {
					flushOne(key)
				}
				return
			}
			if ev.eof {
				flushOne(ev.key)
				delete(joiners, ev.key)
				delete(services, ev.key)
				continue
			}
			j, ok := joiners[ev.key]
			if !ok {
				j = process.NewJoiner(maxJoinBytes)
				joiners[ev.key] = j
				services[ev.key] = ev.service
			}
			clean := process.Line{Text: process.StripANSI(ev.line.Text), Time: ev.line.Time}
			for _, r := range j.Feed(clean) {
				appendRecord(spool, ev.service, r)
			}
			if ev.isDocker {
				if err := spool.SetSince(ev.key, ev.line.Time); err != nil {
					log.Printf("ship: WARN persisting since for container %s: %v", ev.key, err)
				}
			}

		case <-ticker.C:
			for key := range joiners {
				flushOne(key)
			}
		}
	}
}

// runFileSource wraps file.Tail, forwarding its shared.Sourced lines into
// evCh keyed by the glob (file.Tail gives us no finer-grained per-matched-
// file identity — see internal/ship/file's Tail signature) and sending an
// eof event once Tail returns (ctx cancelled).
func runFileSource(ctx context.Context, glob, service string, evCh chan<- sourceEvent) {
	out := make(chan shared.Sourced, 256)
	fwdDone := make(chan struct{})
	go func() {
		defer close(fwdDone)
		for s := range out {
			select {
			case evCh <- sourceEvent{key: "file:" + glob, service: s.Service, line: s.Line}:
			case <-ctx.Done():
				return
			}
		}
	}()

	if err := file.Tail(ctx, glob, service, out); err != nil {
		log.Printf("ship: WARN file tailer for %q: %v", glob, err)
	}
	close(out)
	<-fwdDone

	select {
	case evCh <- sourceEvent{key: "file:" + glob, eof: true}:
	case <-ctx.Done():
	}
}
