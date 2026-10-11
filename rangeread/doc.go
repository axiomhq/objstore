// Package rangeread reads byte ranges of immutable objects in few GETs:
// Plan unions and coalesces extents under gap, extra-byte and range-size
// limits, and Read runs the plan on bounded workers and returns each
// extent's bytes. It caches nothing; caching is the caller's business.
package rangeread
