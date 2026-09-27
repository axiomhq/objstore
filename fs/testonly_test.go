package fs

import (
	"context"
	"path/filepath"
	"syscall"
)

// Test-only helpers: nothing outside this package's tests calls them.

// lockRoot takes the exclusive root flock used by older file-store writers.
// New writers share this lock and serialize on a key stripe instead.
func (f *Backend) lockRoot(ctx context.Context) (unlock func(), err error) {
	return flock(ctx, filepath.Join(f.root, ".lock"), syscall.LOCK_EX)
}
