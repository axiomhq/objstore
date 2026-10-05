package rangeread

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// ErrCorrupt marks a read whose bytes cannot be what was asked for: the
// store returned a range shorter or longer than requested. A conforming
// backend never does (a range past EOF is objstore.ErrRange, see
// storetest.Conformance), so ErrCorrupt fires only on a non-conforming
// store or a truncated transfer.
var ErrCorrupt = errors.New("rangeread: corrupt")

// ErrInvalidExtent marks caller input that cannot name a byte range: an
// empty object name, one containing '#', a negative offset, a non-positive
// length, an end past MaxInt64, a negative DecodedBytes, an empty Load.Key,
// or one Load.Key given two different loads (Extent, DecodedBytes or
// Transient) in one FetchRanges.
var ErrInvalidExtent = errors.New("rangeread: invalid extent")

// Config bounds physical range coalescing. Zero fields use the defaults
// noted; negative fields are an error (Normalized). Extra bytes are limited
// per execution wave; in-flight bytes are limited across the process.
// These limits never drop a requested extent.
type Config struct {
	// MaxGapBytes is the largest unrequested gap, in bytes, that two
	// extents of one object may be merged across. Default 32 KiB.
	MaxGapBytes int64
	// MaxExtraBytes is the total of such gap bytes one plan may read.
	// Default 256 KiB.
	MaxExtraBytes int64
	// MaxRangeBytes is the largest single ranged GET, in bytes; longer
	// extents are split. Default 8 MiB. At most MaxInFlightBytes.
	MaxRangeBytes int64
	// MaxInFlightBytes bounds the bytes of ranged GETs in flight across
	// the Reader, and the bytes one FetchRanges call may retain for its
	// scope. Default 64 MiB.
	MaxInFlightBytes int64
	// Concurrency is the number of ranged GETs one FetchRanges call runs
	// at once. Default 8, at most the cache's gate width (New checks).
	Concurrency int
}

// Normalized returns c with zero fields set to their defaults, or an error
// if a field is negative or the limits are inconsistent.
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
	if c.MaxRangeBytes > c.MaxInFlightBytes {
		return c, fmt.Errorf("rangeread: range size must fit in-flight bytes")
	}
	return c, nil
}

// Counters count physical work of the coalesced range executor,
// separately from the cache's logical/decompressed byte accounting.
type Counters struct {
	Gets       atomic.Int64 // ranged GETs sent to the store
	Bytes      atomic.Int64 // bytes those GETs returned
	ExtraBytes atomic.Int64 // of Bytes, gap bytes no extent asked for
	Waves      atomic.Int64 // FetchRanges calls that sent at least one GET
}
