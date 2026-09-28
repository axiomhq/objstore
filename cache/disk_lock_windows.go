//go:build windows

package cache

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// sweepable: an exclusive LockFileEx proves a directory's Disk is gone, so
// NewDisk may remove the directories whose lock it can take.
const sweepable = true

// deleteOpenFiles is false: Go opens files without FILE_SHARE_DELETE, so
// the lock file cannot be removed while it is open (see removeHome).
const deleteOpenFiles = false

// lockFile opens path, creating it, and takes an exclusive, non-blocking
// lock on its first byte. ok is false when another handle (this process's
// or another's) holds the lock. The lock lasts until f is closed.
func lockFile(path string) (f *os.File, ok bool, err error) {
	f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, false, err
	}
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
	if err != nil {
		f.Close()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return f, true, nil
}
