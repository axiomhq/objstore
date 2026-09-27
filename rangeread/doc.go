// Package rangeread reads immutable objects by byte range through a
// cache.Cache: Plan unions and coalesces the extents of one read stage
// under gap, extra-byte and range-size limits, and Reader.FetchRanges runs
// the plan on bounded workers, publishing each requested child in the
// returned context (cache.Scoped) and caching it by its logical key.
package rangeread
