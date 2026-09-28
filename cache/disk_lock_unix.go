//go:build unix && !aix

package cache

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// sweepable: an exclusive flock proves a directory's Disk is gone, so
// NewDisk may remove the directories whose lock it can take.
const sweepable = true

// deleteOpenFiles: an open file can be unlinked, so a Disk removes its
// directory, lock file included, while still holding the lock.
const deleteOpenFiles = true

// lockFile opens path, creating it, and takes an exclusive, non-blocking
// flock on it. ok is false when another open file (this process's or
// another's) holds the lock. The lock lasts until f is closed.
func lockFile(path string) (f *os.File, ok bool, err error) {
	f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, false, err
	}
	for {
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return f, true, nil
}
