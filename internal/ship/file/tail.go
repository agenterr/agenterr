// Package file implements a glob-based file tailer for agenterr-ship:
// it watches a glob pattern for matching files, tails each to EOF and then
// polls for growth, and reopens a file from the start when it detects
// rotation (truncate-in-place or rename+recreate). Stdlib + internal/ship
// only.
package file

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/agenterr/agenterr/internal/ship"
	"github.com/agenterr/agenterr/internal/ship/process"
)

// scanInterval and pollInterval are the glob-rescan and growth-poll periods.
// They're package-level vars (not exported, not struct fields on Tail's
// signature — the brief pins that signature) so tests can shrink them for
// fast, generous-bound -race runs without changing the public API.
var (
	scanInterval = 2 * time.Second
	pollInterval = 500 * time.Millisecond
)

// readChunk is the buffer size used for each file.Read call while draining
// a file to EOF.
const readChunk = 64 * 1024

// Tail watches glob for matching files and tails each one, sending every
// complete line as a ship.Sourced (tagged with service) on out. It blocks
// until ctx is cancelled, at which point it returns nil once every per-file
// goroutine it started has exited. glob is rescanned every scanInterval so
// files created after Tail starts are picked up; only regular files are
// tailed (directories matched by the glob are skipped).
func Tail(ctx context.Context, glob, service string, out chan<- ship.Sourced) error {
	started := make(map[string]bool)
	var wg sync.WaitGroup
	defer wg.Wait() // no leaked per-file goroutines: wait for them to observe ctx.Done and exit

	scan := func() {
		matches, err := filepath.Glob(glob)
		if err != nil {
			return // malformed pattern; nothing to do until the caller fixes it
		}
		for _, path := range matches {
			if started[path] {
				continue
			}
			if fi, statErr := os.Stat(path); statErr == nil && fi.IsDir() {
				continue // only files, not dirs
			}
			started[path] = true
			wg.Add(1)
			go func(path string) {
				defer wg.Done()
				tailFile(ctx, path, service, out)
			}(path)
		}
	}

	scan()

	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			scan()
		}
	}
}

// tailFile tails a single file until ctx is cancelled. It reads to EOF,
// then polls pollInterval for growth; each poll also checks for rotation
// (size-shrink, i.e. copytruncate, or an inode/identity change, i.e.
// rename+recreate) and reopens from the start when detected. An unreadable
// file is WARN-logged once (not once per retry) and polled until it
// becomes readable or ctx is cancelled.
func tailFile(ctx context.Context, path, service string, out chan<- ship.Sourced) {
	var (
		f          *os.File
		openedInfo os.FileInfo // identity snapshot from the currently-open handle, for os.SameFile
		readBytes  int64       // bytes consumed from the current handle, for shrink detection
		pending    []byte      // partial (no trailing \n yet) line bytes held across polls
		warned     bool
	)
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	openFresh := func() bool {
		nf, err := os.Open(path)
		if err != nil {
			if !warned {
				log.Printf("file: WARN cannot open %s: %v", path, err)
				warned = true
			}
			return false
		}
		fi, err := nf.Stat()
		if err != nil {
			nf.Close()
			if !warned {
				log.Printf("file: WARN cannot stat %s: %v", path, err)
				warned = true
			}
			return false
		}
		if f != nil {
			f.Close()
		}
		f, openedInfo, readBytes, pending = nf, fi, 0, nil
		return true
	}

	// drain reads the currently-open file to EOF, emitting complete lines
	// and holding back any trailing partial line in pending.
	drain := func() {
		buf := make([]byte, readChunk)
		for {
			n, err := f.Read(buf)
			if n > 0 {
				readBytes += int64(n)
				pending = append(pending, buf[:n]...)
				pending = emitLines(ctx, pending, service, out)
			}
			if err != nil {
				return // EOF (normal — wait for the next poll) or a read error
			}
		}
	}

	// checkRotation reopens from the start if the file at path is no longer
	// the one we have open (rename+recreate, different identity) or has
	// shrunk below what we've already read (truncate-in-place).
	checkRotation := func() {
		if f == nil {
			return
		}
		fi, err := os.Stat(path)
		if err != nil {
			return // path missing right now; keep the current handle, retry next poll
		}
		if !os.SameFile(openedInfo, fi) || fi.Size() < readBytes {
			openFresh()
		}
	}

	if f == nil {
		openFresh()
	}

	for {
		if f != nil {
			drain()
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pollInterval):
			checkRotation()
			if f == nil {
				openFresh()
			}
		}
	}
}

// emitLines splits buf on '\n', sending each complete line as a
// ship.Sourced with a read-time timestamp, and returns the trailing
// incomplete line (if any) unsent — a partial line at EOF waits for its
// newline rather than being emitted early.
func emitLines(ctx context.Context, buf []byte, service string, out chan<- ship.Sourced) []byte {
	for {
		idx := bytes.IndexByte(buf, '\n')
		if idx < 0 {
			return buf
		}
		text := string(buf[:idx])
		buf = buf[idx+1:]
		select {
		case out <- ship.Sourced{Service: service, Line: process.Line{Text: text, Time: time.Now()}}:
		case <-ctx.Done():
			return buf
		}
	}
}
