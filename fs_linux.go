//go:build linux

package objstore

import (
	"os"

	"golang.org/x/sys/unix"
)

// writeBack writes f's bytes [off, off+n) to the device and waits for
// them, without a cache flush or a journal commit: it only moves write-back
// earlier, so the later fsync still decides durability. Its error must be
// fatal to the write: the wait consumes the file's write-back error
// (errseq), and the fsync on the same file would then report success.
func writeBack(f *os.File, off, n int64) error {
	return unix.SyncFileRange(int(f.Fd()), off, n,
		unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
}
