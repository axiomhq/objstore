package rangeread

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/axiomhq/objstore/cache"
)

// ErrCorrupt marks a read whose bytes cannot be what was asked for: an
// invalid extent, or a short range.
var ErrCorrupt = errors.New("rangeread: corrupt")

// Config bounds physical range coalescing. Zero fields use defaults.
// Extra bytes are limited per execution wave; in-flight bytes are limited
// across the process. These limits never drop a requested extent.
type Config struct {
	MaxGapBytes      int64
	MaxExtraBytes    int64
	MaxRangeBytes    int64
	MaxInFlightBytes int64
	Concurrency      int
}

func (c Config) Normalized() (Config, error) {
	if c.MaxGapBytes < 0 || c.MaxExtraBytes < 0 || c.MaxRangeBytes < 0 || c.MaxInFlightBytes < 0 || c.Concurrency < 0 {
		return c, fmt.Errorf("rangeread: range read limits must be nonnegative")
	}
	if c.MaxGapBytes == 0 {
		c.MaxGapBytes = 32 << 10
	}
	if c.MaxExtraBytes == 0 {
		c.MaxExtraBytes = 256 << 10
	}
	if c.MaxRangeBytes == 0 {
		c.MaxRangeBytes = 8 << 20
	}
	if c.MaxInFlightBytes == 0 {
		c.MaxInFlightBytes = 64 << 20
	}
	if c.Concurrency == 0 {
		c.Concurrency = 8
	}
	if c.MaxRangeBytes > c.MaxInFlightBytes || c.Concurrency > cache.GateWidth {
		return c, fmt.Errorf("rangeread: range size must fit in-flight bytes and concurrency must not exceed %d", cache.GateWidth)
	}
	return c, nil
}

// Counters count physical work of the coalesced range executor,
// separately from the cache's logical/decompressed byte accounting.
type Counters struct {
	Gets, Bytes, ExtraBytes, Waves atomic.Int64
}
