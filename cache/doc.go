// Package cache is a process's read path to immutable objects in a Store:
// a striped in-memory byte cache (ByteCache) with decoded values riding on
// their bytes, an optional disposable disk tier with 4 KiB block checksums
// (Disk), one gate bounding concurrent store GETs, and one singleflight
// across concurrent misses of a key (Cache). Every lookup counts once as a
// memory hit, a disk hit or a load (ClassCounts), for the process and for
// the request context that made it (WithRequestStats); request contexts
// also carry per-stage results (WithResults) through every tier, and a
// background job's context may carry a store request Budget (WithBudget).
//
// Keys under ns/<name>/ belong to namespace <name>. InvalidateNamespace
// retires one namespace in every tier without touching the others; the
// disk tier pins reservations per namespace (Disk.Pin) and evicts
// namespaces nobody read for a while (Disk.ExpireInactive). Resident holds
// decoded log pages outside the memory budget, without recency eviction,
// until the log is truncated past them.
package cache
