// Package enginestore assembles the template storage engine into a full
// store.Store: SQLite (embedded) keeps issues, triage, rules, keys,
// settings, templates, the segment manifest, and rollups; log bodies live
// in per-project WAL + memtable + immutable columnar segments
// (spec §2–§4 and the decisions log in
// docs/superpowers/specs/2026-08-12-template-storage-engine-design.md).
package enginestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agenterr/agenterr/internal/engine"
	"github.com/agenterr/agenterr/internal/segment"
	"github.com/agenterr/agenterr/internal/store/sqlite"
	"github.com/agenterr/agenterr/internal/template"
)

// Options tunes the flush policy. Zero values select the defaults.
type Options struct {
	FlushRows  int           // segment flush threshold; default 64_000
	FlushEvery time.Duration // background flush interval; default 5m
}

// Store is the engine-backed store.Store. The embedded *sqlite.DB serves
// every metadata method unchanged; log-path methods are overridden here.
type Store struct {
	*sqlite.DB
	dir  string // <dir(dbPath)>/engine
	ex   *template.Extractor
	opts Options

	mu       sync.Mutex
	projects map[int64]*projState

	nextLogID atomic.Int64

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

type projState struct {
	mu  sync.Mutex
	wal *engine.WAL
	mem *engine.Memtable
	seq int64
}

// Open opens the SQLite metadata store at dbPath, prepares the engine
// data directory beside it, replays per-project WALs (deduping rows the
// manifest already covers), and starts the background flush ticker.
//
// The engine data directory (filepath.Dir(dbPath)/engine) is owned by a
// single Store within a single process. Opening a second live Store over
// the same dbPath concurrently is unsupported: both instances would mint
// overlapping LogIDs and segment file names, corrupting the manifest.
// (Reopening after a prior Store's Close is fine — that is the recovery
// path this function implements.)
func Open(dbPath string, opts Options) (*Store, error) {
	if opts.FlushRows <= 0 {
		opts.FlushRows = 64_000
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 5 * time.Minute
	}
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(filepath.Dir(dbPath), "engine")
	for _, d := range []string{filepath.Join(dir, "wal"), filepath.Join(dir, "segments")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("enginestore: mkdir %s: %w", d, err)
		}
	}
	s := &Store{DB: db, dir: dir, opts: opts, projects: map[int64]*projState{}, stop: make(chan struct{})}
	s.ex = template.NewExtractor(db, 0)

	if err := s.recover(context.Background()); err != nil {
		return nil, err
	}

	s.wg.Add(1)
	go s.flushLoop()
	return s, nil
}

// recover seeds nextLogID from the manifest and replays every WAL file
// (listed from the directory, never the manifest, so an orphaned WAL is
// never skipped), deduping rows whose LogID the manifest already covers.
func (s *Store) recover(ctx context.Context) error {
	segs, err := s.DB.Segments(ctx, 0)
	if err != nil {
		return err
	}
	maxByProject := map[int64]int64{}
	for _, m := range segs {
		if m.MaxLogID > maxByProject[m.ProjectID] {
			maxByProject[m.ProjectID] = m.MaxLogID
		}
		if m.MaxLogID > s.nextLogID.Load() {
			s.nextLogID.Store(m.MaxLogID)
		}
	}
	walFiles, err := filepath.Glob(filepath.Join(s.dir, "wal", "*.wal"))
	if err != nil {
		return fmt.Errorf("enginestore: list wals: %w", err)
	}
	for _, wf := range walFiles {
		base := strings.TrimSuffix(filepath.Base(wf), ".wal")
		pid, err := strconv.ParseInt(base, 10, 64)
		if err != nil {
			return fmt.Errorf("enginestore: unexpected wal file %s", wf)
		}
		rows, err := engine.ReplayWAL(wf)
		if err != nil {
			return fmt.Errorf("enginestore: replay %s: %w", wf, err)
		}
		var keep []segment.Row
		for _, r := range rows {
			if r.LogID > maxByProject[pid] {
				keep = append(keep, r)
			}
			if r.LogID > s.nextLogID.Load() {
				s.nextLogID.Store(r.LogID)
			}
		}
		ps, err := s.proj(pid)
		if err != nil {
			return err
		}
		ps.mem.Append(keep)
	}
	return nil
}

// proj returns (creating on first use) the per-project engine state.
func (s *Store) proj(projectID int64) (*projState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ps, ok := s.projects[projectID]; ok {
		return ps, nil
	}
	w, err := engine.OpenWAL(filepath.Join(s.dir, "wal", fmt.Sprintf("%d.wal", projectID)))
	if err != nil {
		return nil, fmt.Errorf("enginestore: open wal: %w", err)
	}
	ps := &projState{wal: w, mem: engine.NewMemtable()}
	s.projects[projectID] = ps
	return ps, nil
}

func (s *Store) flushLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.FlushEvery)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			_ = s.FlushAll() // errors are logged inside flushProject
		}
	}
}

// FlushAll flushes every project's memtable to a segment.
func (s *Store) FlushAll() error {
	s.mu.Lock()
	pids := make([]int64, 0, len(s.projects))
	for pid := range s.projects {
		pids = append(pids, pid)
	}
	s.mu.Unlock()
	var firstErr error
	for _, pid := range pids {
		if err := s.flushProject(pid); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close stops the flush loop, flushes everything, closes WALs, and
// closes the metadata DB. It is safe to call more than once — every call
// after the first is a no-op that returns the first call's result — since
// callers such as a health check probing "is the store still usable" may
// reasonably close it independently of the owning lifecycle hook.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.wg.Wait()
		err := s.FlushAll()
		s.mu.Lock()
		pss := make([]*projState, 0, len(s.projects))
		for _, ps := range s.projects {
			pss = append(pss, ps)
		}
		s.mu.Unlock()
		// Each WAL's own mutex is acquired around its Close so an in-flight
		// WriteBatch holding ps.mu (Append/Sync) can never race a concurrent
		// Close on the same *engine.WAL.
		for _, ps := range pss {
			ps.mu.Lock()
			if cerr := ps.wal.Close(); cerr != nil && err == nil {
				err = cerr
			}
			ps.mu.Unlock()
		}
		if cerr := s.DB.Close(); cerr != nil && err == nil {
			err = cerr
		}
		s.closeErr = err
	})
	return s.closeErr
}
