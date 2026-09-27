// Package cache is a process's read path to immutable objects in a Store:
// a striped in-memory byte cache (ByteCache) with decoded values riding on
// their bytes, an optional disposable disk tier with 4 KiB block checksums
// (Disk), one gate bounding concurrent store GETs, and one singleflight
// across concurrent misses of a key (Cache). Every lookup counts once as a
// memory hit, a disk hit or a load (ClassCounts), for the process and for
// the request context that made it (WithRequestStats); request contexts
// also carry per-stage results (WithResults) through every tier.
package cache
