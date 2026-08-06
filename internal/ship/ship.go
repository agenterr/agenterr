// Package ship implements agenterr-ship: the sidecar that tails Docker
// container logs and/or local files, joins multiline records, buffers them
// to disk, and ships them to an Agenterr ingest endpoint. This file is the
// orchestrator's entry point: Run/run wire the docker/file sources (see
// docker_source.go), the joiner loop (see joiner.go), the buffer.Spool, and
// the sender.Sender together and own shutdown ordering.
package ship

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agenterr/agenterr/internal/ship/buffer"
	"github.com/agenterr/agenterr/internal/ship/docker"
	"github.com/agenterr/agenterr/internal/ship/process"
	"github.com/agenterr/agenterr/internal/ship/sender"
)

// wireRecord is the on-the-wire shape: marshaled exactly once, at
// spool-append time, and never re-marshaled downstream (the sender sends
// spooled bytes verbatim).
type wireRecord struct {
	Timestamp string `json:"timestamp"`
	Service   string `json:"service"`
	Message   string `json:"message"`
}

// Run wires and runs the full ship pipeline until ctx is cancelled. It
// returns a startup error (docker unreachable with --docker set, or the
// server rejecting the key) without ever having started shipping; once
// running, transient errors are logged/retried per the ship semantics doc
// rather than returned.
//
// Shutdown order on ctx cancel: producers (docker/file tailers) stop first
// and drain their EOF into the joiner loop, which flushes every pending
// record to the spool and exits; only then is the spool closed. The sender
// keeps draining the spool throughout and is given a chance for its
// currently in-flight attempt to finish (see sender.Config.RequestTimeout)
// before Run returns — some of what the shutdown-time flush just wrote may
// not get shipped in this run, but it's durably on disk and resumes on next
// start (at-least-once, not exactly-once, per the ship semantics doc).
func Run(ctx context.Context, cfg Config) error {
	spool, err := buffer.Open(cfg.DataDir, cfg.MaxBufferBytes)
	if err != nil {
		return fmt.Errorf("ship: open spool at %s: %w", cfg.DataDir, err)
	}

	snd := sender.New(sender.Config{URL: cfg.URL, Key: cfg.Key})
	if err := snd.Preflight(ctx); err != nil {
		spool.Close()
		return err
	}

	var dc dockerSurface
	if cfg.Docker {
		dc = docker.NewClient(cfg.DockerSock)
		if err := dc.Ping(ctx); err != nil {
			spool.Close()
			return fmt.Errorf("ship: docker unavailable at %s: %w", cfg.DockerSock, err)
		}
	}

	return run(ctx, cfg, spool, snd, dc)
}

// senderRunner is the narrow surface of *sender.Sender this package drives,
// so tests can inject a stub sender alongside the stub docker surface.
type senderRunner interface {
	Run(ctx context.Context, spool *buffer.Spool)
	Stats() (shipped, oversizedDropped int64, lastErr string)
}

// run is Run's testable core: it assumes preflight/docker-ping already
// happened (or were skipped) and just wires the pipeline.
func run(ctx context.Context, cfg Config, spool *buffer.Spool, snd senderRunner, dc dockerSurface) error {
	defer spool.Close()

	evCh := make(chan sourceEvent, 1024)
	joinWindow := time.Duration(cfg.JoinWindowMS) * time.Millisecond

	// appendDropped counts records lost on the append path (marshal or
	// spool.Append failure) — see appendRecord. Shared by the joiner loop
	// (the only writer) and runSelfLog (the reader), so it must survive
	// both goroutines outliving this function's stack frame.
	appendDropped := new(atomic.Int64)

	joinerDone := make(chan struct{})
	go func() {
		runJoinerLoop(spool, evCh, joinWindow, appendDropped)
		close(joinerDone)
	}()

	var producersWG sync.WaitGroup
	if cfg.Docker && dc != nil {
		producersWG.Add(1)
		go func() {
			defer producersWG.Done()
			runDockerSource(ctx, cfg, dc, spool, evCh)
		}()
	}
	for _, spec := range cfg.Files {
		glob, svc, err := parseFileSpec(spec)
		if err != nil {
			log.Printf("ship: WARN skipping invalid --file %q: %v", spec, err)
			continue
		}
		producersWG.Add(1)
		go func(glob, svc string) {
			defer producersWG.Done()
			runFileSource(ctx, glob, svc, evCh)
		}(glob, svc)
	}

	senderDone := make(chan struct{})
	go func() {
		snd.Run(ctx, spool)
		close(senderDone)
	}()

	selfLogDone := make(chan struct{})
	go func() {
		runSelfLog(ctx, spool, snd, appendDropped)
		close(selfLogDone)
	}()

	<-ctx.Done()

	// Producers first: their EOF events tell the joiner loop it has seen
	// everything there is to flush.
	producersWG.Wait()
	close(evCh)
	<-joinerDone

	<-senderDone
	<-selfLogDone
	return nil
}

// parseFileSpec splits a --file 'GLOB=SERVICE' entry.
func parseFileSpec(spec string) (glob, service string, err error) {
	idx := strings.IndexByte(spec, '=')
	if idx <= 0 || idx == len(spec)-1 {
		return "", "", fmt.Errorf("expected 'GLOB=SERVICE'")
	}
	return spec[:idx], spec[idx+1:], nil
}

// appendRecord marshals rec as the wire record shape once and appends it to
// spool. Both failure paths — a marshal error (in practice unreachable: a
// wireRecord is only strings, which encoding/json always encodes
// successfully even when not valid UTF-8; this branch exists so a future
// field addition can't silently regress that) and a spool.Append error
// (e.g. a disk-full or already-closed spool) — are drops per the ship
// semantics doc's "never lose silently" rule: logged AND counted via
// appendDropped, so they show up in the self-log line rather than only in
// scrollback.
func appendRecord(spool *buffer.Spool, service string, rec process.Record, appendDropped *atomic.Int64) {
	wr := wireRecord{
		Timestamp: rec.Time.Format(time.RFC3339Nano),
		Service:   service,
		Message:   rec.Text,
	}
	b, err := json.Marshal(wr)
	if err != nil {
		appendDropped.Add(1)
		log.Printf("ship: WARN dropping unmarshalable record for service %s: %v", service, err)
		return
	}
	if err := spool.Append(b); err != nil {
		appendDropped.Add(1)
		log.Printf("ship: WARN spool append failed for service %s: %v", service, err)
	}
}

// selfLogLine formats the periodic self-log line's counters. Split out from
// runSelfLog so the exact format (and the fact that every drop counter is
// represented) is unit-testable without wiring up a whole orchestrator run.
func selfLogLine(shipped, bufferDropped, oversizedDropped, appendDropped int64, lastErr string) string {
	return fmt.Sprintf("ship: INFO shipped=%d buffer_dropped=%d oversized_dropped=%d append_dropped=%d last_error=%q",
		shipped, bufferDropped, oversizedDropped, appendDropped, lastErr)
}

// runSelfLog emits one INFO line per minute (or once at start, so a short
// test/run isn't silent) summarizing sender and append counters — never
// losing a drop silently, per the global constraints.
func runSelfLog(ctx context.Context, spool *buffer.Spool, snd senderRunner, appendDropped *atomic.Int64) {
	logOnce := func() {
		shipped, oversized, lastErr := snd.Stats()
		log.Print(selfLogLine(shipped, spool.Dropped(), oversized, appendDropped.Load(), lastErr))
	}
	logOnce()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			logOnce()
		}
	}
}

// sleepCtx waits d or returns early if ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// nextBackoff doubles d, capped at max.
func nextBackoff(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}
