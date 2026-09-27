//go:build unix && !linux

package fs

import "os"

// writeBack is a no-op off Linux: the fsync that follows writes everything.
func writeBack(*os.File, int64, int64) error { return nil }
