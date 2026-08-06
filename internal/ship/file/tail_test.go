package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agenterr/agenterr/internal/ship/shared"
)

// TestMain shrinks the scan/poll intervals for the whole package so tests
// run fast under -race without flaking: the bounds below (recvTimeout,
// noEventWait) stay generous relative to these, not to the production
// defaults.
func TestMain(m *testing.M) {
	scanInterval = 20 * time.Millisecond
	pollInterval = 20 * time.Millisecond
	os.Exit(m.Run())
}

const (
	recvTimeout = 3 * time.Second
	noEventWait = 150 * time.Millisecond // > pollInterval, used to assert "nothing arrived yet"
)

func startTail(t *testing.T, glob, service string) (chan shared.Sourced, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan shared.Sourced, 64)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := Tail(ctx, glob, service, out); err != nil {
			t.Errorf("Tail returned error: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(recvTimeout):
			t.Fatal("Tail did not return after ctx cancel — goroutine leak")
		}
	})
	return out, cancel
}

func recv(t *testing.T, out <-chan shared.Sourced) shared.Sourced {
	t.Helper()
	select {
	case s := <-out:
		return s
	case <-time.After(recvTimeout):
		t.Fatal("timed out waiting for a line")
		return shared.Sourced{}
	}
}

func expectNone(t *testing.T, out <-chan shared.Sourced) {
	t.Helper()
	select {
	case s := <-out:
		t.Fatalf("expected no line yet, got %+v", s)
	case <-time.After(noEventWait):
	}
}

func TestAppendDetection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("first\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	s := recv(t, out)
	if s.Line.Text != "first" || s.Service != "svc" {
		t.Fatalf("got %+v", s)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("second\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s = recv(t, out)
	if s.Line.Text != "second" {
		t.Fatalf("got %+v", s)
	}
}

func TestRotationViaTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("before\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	s := recv(t, out)
	if s.Line.Text != "before" {
		t.Fatalf("got %+v", s)
	}

	// copytruncate: truncate the same inode to 0, then write new (shorter)
	// content so a naive offset-based reader would otherwise miss it or
	// misread it.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("after\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s = recv(t, out)
	if s.Line.Text != "after" {
		t.Fatalf("got %+v, want rotation to be detected and reopened from start", s)
	}
}

func TestRotationViaRenameRecreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("old-content\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	s := recv(t, out)
	if s.Line.Text != "old-content" {
		t.Fatalf("got %+v", s)
	}

	if err := os.Rename(path, filepath.Join(dir, "app.log.1")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new-content\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s = recv(t, out)
	if s.Line.Text != "new-content" {
		t.Fatalf("got %+v, want the recreated file (new inode) to be picked up", s)
	}
}

// TestRotationViaRenameRecreateDrainsPreRotationBurst covers a line written
// to the old path and NOT drained before the rename+recreate fires: the
// tailer must still read it off the old fd (which stays fully valid and
// readable after the rename) before swapping to the new file, rather than
// losing it when the old fd is closed.
func TestRotationViaRenameRecreateDrainsPreRotationBurst(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("seed\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	s := recv(t, out)
	if s.Line.Text != "seed" {
		t.Fatalf("got %+v", s)
	}

	// Write a line and immediately rename+recreate, before the tailer gets
	// a chance to poll/drain it off the old fd.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("pre-rotation-burst\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if err := os.Rename(path, filepath.Join(dir, "app.log.1")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new-file-first-line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Both lines must arrive, in order: the un-drained burst on the old fd,
	// then the new file's content.
	s = recv(t, out)
	if s.Line.Text != "pre-rotation-burst" {
		t.Fatalf("got %+v, want the pre-rotation burst to be drained before the swap", s)
	}
	s = recv(t, out)
	if s.Line.Text != "new-file-first-line" {
		t.Fatalf("got %+v", s)
	}
}

func TestNewGlobMatchPickedUp(t *testing.T) {
	dir := t.TempDir()
	// No matching files exist yet when Tail starts.
	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	expectNone(t, out)

	path := filepath.Join(dir, "new.log")
	if err := os.WriteFile(path, []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s := recv(t, out)
	if s.Line.Text != "hello" {
		t.Fatalf("got %+v", s)
	}
}

func TestTimestampsAreApproxNow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("line\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	before := time.Now()
	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")
	s := recv(t, out)
	after := time.Now()

	if s.Line.Time.Before(before) || s.Line.Time.After(after) {
		t.Fatalf("Line.Time = %v, want between %v and %v", s.Line.Time, before, after)
	}
}

func TestPartialLineHeldUntilNewline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	if err := os.WriteFile(path, []byte("no newline yet"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	// Give the tailer multiple poll cycles to prove it's not emitting the
	// half-written line.
	expectNone(t, out)
	expectNone(t, out)

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(" now complete\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s := recv(t, out)
	if s.Line.Text != "no newline yet now complete" {
		t.Fatalf("got %+v", s)
	}
}

func TestDirectoriesAreSkipped(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub.log"), 0o755); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(dir, "real.log")
	if err := os.WriteFile(filePath, []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := startTail(t, filepath.Join(dir, "*.log"), "svc")

	s := recv(t, out)
	if s.Line.Text != "hi" {
		t.Fatalf("got %+v", s)
	}
	expectNone(t, out) // nothing more — the directory match must not have produced anything
}
