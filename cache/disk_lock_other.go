//go:build !unix || aix

package cache

import "os"

// sweepable is false: without flock a live Disk's directory cannot be told
// from a stale one, so NewDisk removes nothing.
const sweepable = false

// lockFile creates path and reports it held; nothing else can observe it.
func lockFile(path string) (*os.File, bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, false, err
	}
	return f, true, nil
}
