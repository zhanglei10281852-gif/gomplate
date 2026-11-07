package gomplate

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/hairyhenderson/gomplate/v5/internal/datafs"
)

// loadReplaySnapshot reads and validates a datasource replay snapshot from a
// local path. The snapshot is independent of template inputs and render
// outputs, and is always read from the local filesystem.
func loadReplaySnapshot(path string) (*datafs.Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open datasource replay snapshot %q: %w", path, err)
	}
	defer f.Close()

	s, err := datafs.ReadSnapshot(f)
	if err != nil {
		return nil, fmt.Errorf("datasource replay snapshot %q: %w", path, err)
	}
	return s, nil
}

// publishSnapshot atomically writes the recorded snapshot to path: it is
// first written to a temporary file in the same directory, then renamed over
// the target. If writing fails, the temporary file is removed and any
// pre-existing snapshot at path is left untouched.
func publishSnapshot(path string, r *datafs.SnapshotRecorder) (err error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	tmp, err := os.CreateTemp(dir, ".#"+base+".*")
	if err != nil {
		return fmt.Errorf("create temp datasource snapshot file in %q: %w", dir, err)
	}
	tmpName := tmp.Name()

	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err = r.WriteTo(tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write datasource snapshot %q: %w", path, err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync datasource snapshot %q: %w", path, err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close datasource snapshot %q: %w", path, err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("publish datasource snapshot %q: %w", path, err)
	}
	cleanup = false

	return nil
}
