// Package cache is a process's read path to immutable objects in a Store:
// a striped in-memory byte cache (ByteCache) with decoded values riding on
// their bytes, an optional disposable disk tier with 4 KiB block checksums
// (Disk), one gate bounding concurrent store GETs, and one singleflight
// across concurrent misses of a key (Cache). Request contexts carry
// hit/miss accounting (WithRequestStats) and per-stage results
// (WithResults) through every tier.
package cache
