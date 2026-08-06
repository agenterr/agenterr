package buffer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// checkpointFile is the on-disk shape of checkpoint.json: the ack position
// (segment sequence + byte offset of the last acked record) plus the
// per-container "since" timestamps used to resume docker log tailing.
type checkpointFile struct {
	Seq    int64             `json:"seq"`
	Offset int64             `json:"offset"`
	Since  map[string]string `json:"since"`
}

// checkpointPath returns the fixed checkpoint file location for dir.
func checkpointPath(dir string) string {
	return filepath.Join(dir, "checkpoint.json")
}

// loadCheckpoint reads checkpoint.json from dir. A missing file is not an
// error: it means a fresh spool, and the zero value (nothing acked, no
// since timestamps) is returned.
func loadCheckpoint(dir string) (checkpointFile, error) {
	b, err := os.ReadFile(checkpointPath(dir))
	if os.IsNotExist(err) {
		return checkpointFile{Since: map[string]string{}}, nil
	}
	if err != nil {
		return checkpointFile{}, err
	}
	var cp checkpointFile
	if err := json.Unmarshal(b, &cp); err != nil {
		return checkpointFile{}, err
	}
	if cp.Since == nil {
		cp.Since = map[string]string{}
	}
	return cp, nil
}

// saveCheckpoint persists cp atomically: write to a temp file in the same
// directory, fsync it, then rename over checkpoint.json. The rename is
// atomic on POSIX filesystems, so a crash mid-write never leaves a torn
// checkpoint — readers either see the old file or the fully-written new
// one, never a partial one.
func saveCheckpoint(dir string, cp checkpointFile) error {
	b, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := checkpointPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, checkpointPath(dir))
}

// sinceToTime parses a stored RFC3339Nano timestamp; ok is false if key is
// absent or unparsable.
func sinceToTime(since map[string]string, container string) (time.Time, bool) {
	s, found := since[container]
	if !found {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
